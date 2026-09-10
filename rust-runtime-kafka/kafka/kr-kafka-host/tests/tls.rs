use std::{
    io::{Cursor, Read, Write},
    sync::Arc,
    time::Duration,
};

use kr_kafka_host::{
    SecurityError,
    sasl::{SaslLimits, plain_response},
    tls::{TlsClient, TlsLimits, TlsProgress},
};
use kr_runtime::SimRuntime;
use kr_runtime_io::network::{
    ByteStreamSubmit, ColdStream, LinkConfig, MemoryNetwork, MemoryNetworkConfig, NetworkConfig,
    NodeId, ReadRequest, SimNetwork, WriteRequest,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig, ServerConnection,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
    time_provider::TimeProvider,
};

#[derive(Debug)]
struct FixedTime(u64);
impl TimeProvider for FixedTime {
    fn current_time(&self) -> Option<UnixTime> {
        Some(UnixTime::since_unix_epoch(Duration::from_secs(self.0)))
    }
}

fn configs(time: u64) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    configs_with_providers(time, provider.clone(), provider)
}

fn configs_with_providers(
    time: u64,
    client_provider: Arc<rustls::crypto::CryptoProvider>,
    server_provider: Arc<rustls::crypto::CryptoProvider>,
) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(
            include_bytes!("fixtures/ca.der").to_vec(),
        ))
        .unwrap();
    let mut client = ClientConfig::builder_with_details(client_provider, Arc::new(FixedTime(time)))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.resumption = rustls::client::Resumption::disabled();
    let mut server = ServerConfig::builder_with_details(server_provider, Arc::new(FixedTime(time)))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(
                include_bytes!("fixtures/localhost.der").to_vec(),
            )],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("fixtures/localhost-key.der").to_vec(),
            )),
        )
        .unwrap();
    server.send_tls13_tickets = 0;
    (Arc::new(client), Arc::new(server))
}

fn peers(time: u64, name: &'static str) -> (TlsClient, ServerConnection) {
    let (client, server) = configs(time);
    (
        TlsClient::new(
            client,
            ServerName::try_from(name).unwrap(),
            TlsLimits::default(),
        )
        .unwrap(),
        ServerConnection::new(server).unwrap(),
    )
}

fn server_receive(server: &mut ServerConnection, bytes: Vec<u8>) -> Result<(), SecurityError> {
    let len = bytes.len() as u64;
    let mut cursor = Cursor::new(bytes);
    while cursor.position() < len {
        server
            .read_tls(&mut cursor)
            .map_err(|_| SecurityError::TlsFailed)?;
        server
            .process_new_packets()
            .map_err(|_| SecurityError::TlsFailed)?;
    }
    Ok(())
}

async fn through_stream<S: ByteStreamSubmit>(
    from: &ColdStream<S>,
    to: &ColdStream<S>,
    bytes: &[u8],
) -> Vec<u8> {
    let mut output = Vec::new();
    while output.len() < bytes.len() {
        let result = from
            .write(WriteRequest {
                buffer: bytes[output.len()..].to_vec(),
            })
            .await
            .unwrap();
        let mut remaining = result.bytes_written;
        while remaining != 0 {
            let received = to
                .read(ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: remaining,
                })
                .await
                .unwrap();
            remaining -= received.buffer.len();
            output.extend_from_slice(&received.buffer);
        }
    }
    output
}

async fn handshake<S: ByteStreamSubmit>(
    client: &mut TlsClient,
    server: &mut ServerConnection,
    left: &ColdStream<S>,
    right: &ColdStream<S>,
    chunk: usize,
) -> Result<Vec<u8>, SecurityError> {
    let mut transcript = Vec::new();
    for _ in 0..100_000 {
        let progress = client.drive()?;
        if progress == TlsProgress::Ready && !server.is_handshaking() && !server.wants_write() {
            return Ok(transcript);
        }
        if progress == TlsProgress::Transmit {
            let count = chunk.min(client.outbound_ciphertext().len());
            let data = through_stream(left, right, &client.outbound_ciphertext()[..count]).await;
            transcript.extend_from_slice(&data);
            client.consume_outbound(count)?;
            server_receive(server, data)?;
        }
        if server.wants_write() {
            let mut output = Vec::new();
            server.write_tls(&mut output).unwrap();
            for data in output.chunks(chunk) {
                let data = through_stream(right, left, data).await;
                transcript.extend_from_slice(&data);
                assert_eq!(client.receive_ciphertext(&data)?, data.len());
                // Input fragmentation exercises rustls's retained deframer.
                if client.drive()? == TlsProgress::Plaintext {
                    panic!("unexpected handshake application data");
                }
            }
        }
    }
    panic!("TLS handshake did not converge")
}

async fn traffic<S: ByteStreamSubmit>(left: ColdStream<S>, right: ColdStream<S>, chunk: usize) {
    let (mut client, mut server) = peers(1_800_000_000, "localhost");
    let capacity = client.retained_capacity();
    assert!(client.peer_verified().is_none());
    handshake(&mut client, &mut server, &left, &right, chunk)
        .await
        .unwrap();
    let token = client.peer_verified().expect("certificate authenticated");
    assert_eq!(
        &*plain_response(&token, "user", "password", SaslLimits::default()).unwrap(),
        b"\0user\0password"
    );
    assert!(plain_response(&token, "user\0", "password", SaslLimits::default()).is_err());
    let plaintext: Vec<u8> = (0..16384).map(|n| (n % 251) as u8).collect();
    client.encrypt(&plaintext).unwrap();
    let first_ciphertext = client.outbound_ciphertext().to_vec();
    assert!(
        !first_ciphertext
            .windows(plaintext.len())
            .any(|window| window == plaintext)
    );
    let output_before = client.outbound_ciphertext().len();
    assert_eq!(
        client.consume_outbound(output_before + 1),
        Err(SecurityError::InvalidState)
    );
    assert_eq!(client.outbound_ciphertext().len(), output_before);
    while !client.outbound_ciphertext().is_empty() {
        let count = chunk.min(client.outbound_ciphertext().len());
        let data = through_stream(&left, &right, &client.outbound_ciphertext()[..count]).await;
        client.consume_outbound(count).unwrap();
        server_receive(&mut server, data).unwrap();
    }
    let mut decoded = vec![0; plaintext.len()];
    server.reader().read_exact(&mut decoded).unwrap();
    assert_eq!(decoded, plaintext);
    client.encrypt(&plaintext).unwrap();
    assert_ne!(
        client.outbound_ciphertext(),
        first_ciphertext,
        "record sequence changes encryption"
    );
    let count = client.outbound_ciphertext().len();
    let data = through_stream(&left, &right, client.outbound_ciphertext()).await;
    client.consume_outbound(count).unwrap();
    server_receive(&mut server, data).unwrap();
    server.reader().read_exact(&mut decoded).unwrap();
    server.writer().write_all(b"Kafka acknowledgement").unwrap();
    let mut output = Vec::new();
    server.write_tls(&mut output).unwrap();
    for bytes in output.chunks(chunk) {
        let data = through_stream(&right, &left, bytes).await;
        assert_eq!(client.receive_ciphertext(&data).unwrap(), data.len());
        client.drive().unwrap();
    }
    assert_eq!(client.plaintext(), b"Kafka acknowledgement");
    assert!(
        client
            .consume_plaintext(client.plaintext().len() + 1)
            .is_err()
    );
    client.consume_plaintext(6).unwrap();
    assert_eq!(client.plaintext(), b"acknowledgement");
    client.consume_plaintext(15).unwrap();
    client.drive().unwrap();
    client.close_notify().unwrap();
    let data = through_stream(&left, &right, client.outbound_ciphertext()).await;
    client.consume_outbound(data.len()).unwrap();
    server_receive(&mut server, data).unwrap();
    assert!(server.process_new_packets().unwrap().peer_has_closed());
    server.send_close_notify();
    let mut output = Vec::new();
    server.write_tls(&mut output).unwrap();
    let data = through_stream(&right, &left, &output).await;
    client.receive_ciphertext(&data).unwrap();
    assert_eq!(client.drive().unwrap(), TlsProgress::PeerClosed);
    client.transport_eof().unwrap();
    assert_eq!(client.retained_capacity(), capacity);
}

#[test]
fn actual_tls_records_cross_memory_and_simulated_cold_streams_with_partial_progress() {
    for chunk in [1, 7, 4096] {
        let mut runtime = SimRuntime::default();
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            max_chunk_bytes: chunk,
            ..MemoryNetworkConfig::default()
        })
        .unwrap();
        let (left, right) = network.connected_pair().unwrap();
        runtime
            .block_on(traffic(
                ColdStream::new(left),
                ColdStream::new(right),
                chunk,
            ))
            .unwrap();
        assert_eq!(network.status().inflight_operations, 0);
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(
            runtime.handle(),
            NetworkConfig {
                default_link: LinkConfig {
                    max_chunk_bytes: chunk,
                    ..LinkConfig::default()
                },
                ..NetworkConfig::default()
            },
        )
        .unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        runtime
            .block_on(traffic(
                ColdStream::new(left),
                ColdStream::new(right),
                chunk,
            ))
            .unwrap();
    }
}

#[test]
fn certificate_name_and_validity_fail_closed_before_plain_credentials() {
    for (time, name) in [(1_800_000_000, "wrong.example"), (1, "localhost")] {
        let mut runtime = SimRuntime::default();
        let network = MemoryNetwork::new(MemoryNetworkConfig::default()).unwrap();
        let (left, right) = network.connected_pair().unwrap();
        let (mut client, mut server) = peers(time, name);
        assert_eq!(
            runtime
                .block_on(handshake(
                    &mut client,
                    &mut server,
                    &ColdStream::new(left),
                    &ColdStream::new(right),
                    31
                ))
                .unwrap()
                .unwrap_err(),
            SecurityError::TlsFailed
        );
        assert!(client.peer_verified().is_none());
        assert_eq!(client.drive(), Err(SecurityError::TlsFailed));
    }
}

#[test]
fn modified_records_and_unannounced_eof_fence_tls() {
    let mut runtime = SimRuntime::default();
    let network = MemoryNetwork::new(MemoryNetworkConfig::default()).unwrap();
    let (left, right) = network.connected_pair().unwrap();
    let (mut client, mut server) = peers(1_800_000_000, "localhost");
    runtime
        .block_on(handshake(
            &mut client,
            &mut server,
            &ColdStream::new(left),
            &ColdStream::new(right),
            127,
        ))
        .unwrap()
        .unwrap();
    server.writer().write_all(b"authenticated data").unwrap();
    let mut output = Vec::new();
    server.write_tls(&mut output).unwrap();
    *output.last_mut().unwrap() ^= 1;
    client.receive_ciphertext(&output).unwrap();
    assert_eq!(client.drive(), Err(SecurityError::TlsFailed));
    assert!(client.plaintext().is_empty());
    assert!(client.peer_verified().is_none());
    let (mut client, _) = peers(1_800_000_000, "localhost");
    assert_eq!(client.transport_eof(), Err(SecurityError::TruncatedTls));
}

// Recorded RFC 7748 section 6.1 ECDH material supplies the injected ephemeral
// entropy in this replay fixture. The separate interoperability tests above
// execute ring's real ephemeral key generation and agreement. Here the public
// key is checked exactly and its published shared secret is replayed; rustls
// still performs real transcript hashing, certificate verification, Ed25519
// signatures, HKDF, AEAD record encryption, and record authentication.
mod deterministic {
    use super::*;
    use rustls::{
        NamedGroup,
        crypto::{ActiveKeyExchange, CryptoProvider, SecureRandom, SharedSecret, SupportedKxGroup},
    };

    const ALICE: [u8; 32] = hex("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
    const BOB: [u8; 32] = hex("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");
    const SECRET: [u8; 32] =
        hex("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742");
    const fn hex(s: &str) -> [u8; 32] {
        const fn digit(b: u8) -> u8 {
            if b <= b'9' { b - b'0' } else { b - b'a' + 10 }
        }
        let s = s.as_bytes();
        let mut result = [0; 32];
        let mut i = 0;
        while i < 32 {
            result[i] = digit(s[i * 2]) * 16 + digit(s[i * 2 + 1]);
            i += 1;
        }
        result
    }

    #[derive(Debug)]
    struct Random(u8);
    impl SecureRandom for Random {
        fn fill(&self, bytes: &mut [u8]) -> Result<(), rustls::crypto::GetRandomFailed> {
            bytes.fill(self.0);
            Ok(())
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct RecordedExchange {
        local: &'static [u8; 32],
        peer: &'static [u8; 32],
    }
    impl SupportedKxGroup for RecordedExchange {
        fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, rustls::Error> {
            Ok(Box::new(*self))
        }
        fn name(&self) -> NamedGroup {
            NamedGroup::X25519
        }
    }
    impl ActiveKeyExchange for RecordedExchange {
        fn complete(self: Box<Self>, peer: &[u8]) -> Result<SharedSecret, rustls::Error> {
            if peer != self.peer {
                return Err(rustls::Error::General("recorded ECDH peer mismatch".into()));
            }
            Ok(SharedSecret::from(SECRET.as_slice()))
        }
        fn pub_key(&self) -> &[u8] {
            self.local
        }
        fn group(&self) -> NamedGroup {
            NamedGroup::X25519
        }
    }
    static CLIENT_RANDOM: Random = Random(0x35);
    static SERVER_RANDOM: Random = Random(0x97);
    static CLIENT_KX: RecordedExchange = RecordedExchange {
        local: &ALICE,
        peer: &BOB,
    };
    static SERVER_KX: RecordedExchange = RecordedExchange {
        local: &BOB,
        peer: &ALICE,
    };

    fn provider(client: bool) -> Arc<CryptoProvider> {
        let mut provider = rustls::crypto::ring::default_provider();
        provider.secure_random = if client {
            &CLIENT_RANDOM
        } else {
            &SERVER_RANDOM
        };
        provider.kx_groups = vec![if client { &CLIENT_KX } else { &SERVER_KX }];
        Arc::new(provider)
    }

    fn replay() -> Vec<u8> {
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(runtime.handle(), NetworkConfig::default()).unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        let (client, server) =
            configs_with_providers(1_800_000_000, provider(true), provider(false));
        let mut client = TlsClient::new(
            client,
            ServerName::try_from("localhost").unwrap(),
            TlsLimits::default(),
        )
        .unwrap();
        let mut server = ServerConnection::new(server).unwrap();
        let mut transcript = runtime
            .block_on(handshake(
                &mut client,
                &mut server,
                &ColdStream::new(left),
                &ColdStream::new(right),
                7,
            ))
            .unwrap()
            .unwrap();
        client
            .encrypt(b"replayable encrypted Kafka request")
            .unwrap();
        transcript.extend_from_slice(client.outbound_ciphertext());
        transcript
    }

    #[test]
    fn fixed_time_and_ephemeral_entropy_replay_identical_actual_tls_bytes() {
        let first = replay();
        assert_eq!(first, replay());
        assert!(
            first.len() > 500,
            "handshake and encrypted request were exercised"
        );
    }
}
