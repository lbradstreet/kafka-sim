use super::*;
use kr_runtime::SimRuntime;
use kr_runtime_io::network::{
    AfterFaultCertainty, LinkConfig, LinkKey, LinkState, MemoryNetwork, MemoryNetworkConfig,
    MemoryStream, NetworkConfig, NetworkOperationKind, NodeId, ScriptedFault, SimNetwork,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig, ServerConnection,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
    time_provider::TimeProvider,
};
use std::{
    io::{Cursor, Read, Write},
    time::Duration,
};

#[derive(Debug)]
struct FixedTime;
impl TimeProvider for FixedTime {
    fn current_time(&self) -> Option<UnixTime> {
        Some(UnixTime::since_unix_epoch(Duration::from_secs(
            1_800_000_000,
        )))
    }
}
fn peers(name: &'static str) -> (TlsClient, ServerConnection) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(
            include_bytes!("../tests/fixtures/ca.der").to_vec(),
        ))
        .unwrap();
    let mut client = ClientConfig::builder_with_details(provider.clone(), Arc::new(FixedTime))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.resumption = rustls::client::Resumption::disabled();
    let mut server = ServerConfig::builder_with_details(provider, Arc::new(FixedTime))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(
                include_bytes!("../tests/fixtures/localhost.der").to_vec(),
            )],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("../tests/fixtures/localhost-key.der").to_vec(),
            )),
        )
        .unwrap();
    server.send_tls13_tickets = 0;
    (
        TlsClient::new(
            Arc::new(client),
            ServerName::try_from(name).unwrap(),
            Default::default(),
        )
        .unwrap(),
        ServerConnection::new(Arc::new(server)).unwrap(),
    )
}
async fn join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let mut ra = None;
    let mut rb = None;
    std::future::poll_fn(move |cx| {
        if ra.is_none()
            && let Poll::Ready(result) = a.as_mut().poll(cx)
        {
            ra = Some(result);
        }
        if rb.is_none()
            && let Poll::Ready(result) = b.as_mut().poll(cx)
        {
            rb = Some(result);
        }
        if ra.is_some() && rb.is_some() {
            Poll::Ready((ra.take().unwrap(), rb.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}
async fn send<S: ByteStreamSubmit>(stream: &S, mut bytes: Vec<u8>) -> bool {
    while !bytes.is_empty() {
        let Ok(result) = stream.submit_write(WriteRequest { buffer: bytes }).await else {
            return false;
        };
        assert!(result.bytes_written > 0 && result.bytes_written <= result.buffer.len());
        bytes = result.buffer;
        let len = bytes.len();
        bytes.copy_within(result.bytes_written..len, 0);
        bytes.truncate(len - result.bytes_written);
    }
    true
}
async fn server<S: ByteStreamSubmit>(stream: S, mut tls: ServerConnection) -> Vec<u8> {
    let mut plaintext = Vec::new();
    let mut closed = false;
    loop {
        if tls.wants_write() {
            let mut bytes = Vec::new();
            tls.write_tls(&mut bytes).unwrap();
            if !send(&stream, bytes).await {
                return plaintext;
            }
        }
        if closed {
            stream.submit_shutdown_write().await.unwrap();
            return plaintext;
        }
        let Ok(received) = stream
            .submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 16 * 1024,
            })
            .await
        else {
            return plaintext;
        };
        if received.end_of_stream {
            return plaintext;
        }
        let mut cursor = Cursor::new(received.buffer);
        tls.read_tls(&mut cursor).unwrap();
        let Ok(state) = tls.process_new_packets() else {
            return plaintext;
        };
        let mut scratch = [0; 4096];
        loop {
            match tls.reader().read(&mut scratch) {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    plaintext.extend_from_slice(&scratch[..count]);
                    tls.writer().write_all(&scratch[..count]).unwrap();
                }
            }
        }
        if state.peer_has_closed() {
            tls.send_close_notify();
            closed = true;
        }
    }
}
async fn traffic<S: TlsTransport>(left: S, right: S) {
    let (client, peer) = peers("localhost");
    let tls = TlsStream::new(left, client, TlsStreamLimits::default()).unwrap();
    let telemetry = Arc::new(kr_kafka_client::telemetry::TransportTelemetry::default());
    tls.attach_telemetry(telemetry.clone()).unwrap();
    let fixed = tls.status().fixed_buffer_bytes;
    let client = async {
        assert!(tls.peer_verified().is_none());
        let premature = tls
            .submit_write(WriteRequest {
                buffer: b"password".to_vec(),
            })
            .await
            .unwrap_err();
        assert_eq!(premature.certainty(), CompletionCertainty::NotApplied);
        tls.handshake().await.unwrap();
        let guard = tls.peer_verified().unwrap();
        assert_eq!(
            &*crate::sasl::plain_response(&guard.proof(), "user", "password", Default::default())
                .unwrap(),
            b"\0user\0password"
        );
        drop(guard);
        let empty = tls
            .submit_read(ReadRequest {
                buffer: b"prefix".to_vec(),
                max_bytes: 0,
            })
            .await
            .unwrap();
        assert_eq!(empty.buffer, b"prefix");
        assert!(!empty.end_of_stream);
        let data: Vec<u8> = (0..20000).map(|i| (i % 251) as u8).collect();
        let original = SharedBytes::from(data.clone());
        let mut sent = 0;
        let mut echoed = Vec::new();
        while sent < data.len() {
            let middle = (sent + 13).min(data.len());
            let mut segments = vec![WriteSegment {
                bytes: original.clone(),
                range: sent as u32..middle as u32,
            }];
            if middle < data.len() {
                segments.push(WriteSegment {
                    bytes: original.clone(),
                    range: middle as u32..data.len() as u32,
                });
            }
            let result = tls
                .submit_write_vectored(VectoredWriteRequest { segments })
                .await
                .unwrap();
            assert!(result.bytes_written > 0 && result.bytes_written <= 16 * 1024);
            assert!(result.segments[0].bytes.shares_allocation(&original));
            sent += result.bytes_written;
            while echoed.len() < sent {
                let result = tls
                    .submit_read(ReadRequest {
                        buffer: b"prefix".to_vec(),
                        max_bytes: 731,
                    })
                    .await
                    .unwrap();
                assert_eq!(&result.buffer[..6], b"prefix");
                assert!(!result.end_of_stream && result.bytes_read > 0);
                echoed.extend_from_slice(&result.buffer[6..]);
            }
        }
        assert_eq!(echoed, data);
        tls.submit_shutdown_write().await.unwrap();
        tls.submit_shutdown_write().await.unwrap();
        assert!(
            tls.submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 1
            })
            .await
            .unwrap()
            .end_of_stream
        );
        let rejected = tls
            .submit_write(WriteRequest { buffer: vec![1] })
            .await
            .unwrap_err();
        assert_eq!(rejected.certainty(), CompletionCertainty::NotApplied);
        tls.retire().await.unwrap();
        tls.retire().await.unwrap();
        let status = tls.status();
        assert_eq!(status.fixed_buffer_bytes, fixed);
        assert_eq!(
            (
                status.read_operations,
                status.write_operations,
                status.control_operations
            ),
            (0, 0, 0)
        );
        assert!(!status.terminal_state_pinned);
        assert!(status.ciphertext_copied > data.len() as u64);
        assert!(status.plaintext_copied >= 2 * data.len() as u64);
        let measured = telemetry.snapshot();
        assert!(measured.tls_instrumented);
        assert_eq!(measured.tls_ciphertext_copy_bytes, status.ciphertext_copied);
        assert_eq!(measured.tls_plaintext_copy_bytes, status.plaintext_copied);
        assert!(measured.tls_ciphertext_bytes_confirmed > data.len() as u64);
        assert!(!measured.overflowed);
        data
    };
    let (expected, observed) = join(client, server(right, peer)).await;
    assert_eq!(expected, observed);
}

#[test]
fn shared_memory_and_sim_contract_partial_vectored_tls_and_authenticated_eof() {
    for chunk in [7, 1024] {
        let mut runtime = SimRuntime::default();
        let memory = MemoryNetwork::new(MemoryNetworkConfig {
            max_chunk_bytes: chunk,
            ..Default::default()
        })
        .unwrap();
        let (left, right) = memory.connected_pair().unwrap();
        runtime.block_on(traffic(left, right)).unwrap();
        assert_eq!(memory.status().inflight_operations, 0);
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(
            runtime.handle(),
            NetworkConfig {
                default_link: LinkConfig {
                    max_chunk_bytes: chunk,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        runtime.block_on(traffic(left, right)).unwrap();
        assert_eq!(network.status().inflight_operations, 0);
    }
}

#[test]
fn portable_stream_and_owned_responses_are_send_for_memory_provider() {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}
    send_sync::<TlsStream<MemoryStream>>();
    send::<TlsReadResponse<MemoryStream>>();
    send::<TlsWriteResponse<MemoryStream>>();
    send::<TlsVectoredWriteResponse<MemoryStream>>();
    send::<TlsControlResponse<MemoryStream>>();
    send::<TlsHandshake<MemoryStream>>();
}

#[test]
fn rejected_certificate_never_admits_credentials_and_can_be_retired() {
    let mut runtime = SimRuntime::default();
    let network = MemoryNetwork::new(Default::default()).unwrap();
    let (left, right) = network.connected_pair().unwrap();
    let (client, peer) = peers("wrong.example");
    let tls = TlsStream::new(left, client, Default::default()).unwrap();
    let client = async {
        assert_eq!(tls.handshake().await, Err(SecurityError::TlsFailed));
        assert!(tls.peer_verified().is_none());
        assert_eq!(
            tls.submit_write(WriteRequest {
                buffer: b"password".to_vec()
            })
            .await
            .unwrap_err()
            .certainty(),
            CompletionCertainty::NotApplied
        );
        tls.retire().await.unwrap();
    };
    let (_, received) = runtime.block_on(join(client, server(right, peer))).unwrap();
    assert!(received.is_empty());
    assert_eq!(network.status().inflight_operations, 0);
}

fn assert_retired<S: TlsTransport>(tls: &TlsStream<S>) {
    let status = tls.status();
    assert!(!status.verified);
    assert_eq!(status.read_operations, 0);
    assert_eq!(status.write_operations, 0);
    assert_eq!(status.control_operations, 0);
    assert_eq!(status.retained_read_bytes, 0);
    assert_eq!(status.retained_write_bytes, 0);
    assert!(!status.underlying_read);
    assert!(!status.underlying_write);
    assert!(!status.underlying_control);
}

#[test]
fn handshake_read_and_write_faults_preserve_network_cause_and_retire() {
    for operation in [NetworkOperationKind::Read, NetworkOperationKind::Write] {
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
        let (left, _right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        let (client, _peer) = peers("localhost");
        let tls = TlsStream::new(left, client, Default::default()).unwrap();
        network
            .push_fault(ScriptedFault::fail_before(operation, 41))
            .unwrap();
        let expected = SecurityError::Network(NetworkError::Injected { tag: 41 });
        assert_eq!(
            runtime.block_on(tls.handshake()).unwrap(),
            Err(expected.clone())
        );
        assert_eq!(tls.status().failure, Some(expected));
        assert!(tls.peer_verified().is_none());
        runtime.block_on(tls.retire()).unwrap().unwrap();
        assert_retired(&tls);
        assert_eq!(network.status().inflight_operations, 0);
    }
}

#[test]
fn memory_transport_closed_before_handshake_is_not_authentication_failure() {
    let mut runtime = SimRuntime::default();
    let network = MemoryNetwork::new(Default::default()).unwrap();
    let (left, right) = network.connected_pair().unwrap();
    runtime.block_on(left.submit_close()).unwrap().unwrap();
    let (client, _peer) = peers("localhost");
    let tls = TlsStream::new(left, client, Default::default()).unwrap();
    assert_eq!(
        runtime.block_on(tls.handshake()).unwrap(),
        Err(SecurityError::Network(NetworkError::ConnectionClosed))
    );
    assert!(tls.peer_verified().is_none());
    runtime.block_on(tls.retire()).unwrap().unwrap();
    runtime.block_on(right.submit_close()).unwrap().unwrap();
    assert_retired(&tls);
    let status = network.status();
    assert_eq!(status.inflight_operations, 0);
    assert_eq!(status.pending_reads, 0);
    assert_eq!(status.pending_writes, 0);
    assert_eq!(status.buffered_bytes, 0);
}

#[test]
fn shutdown_fault_preserves_network_cause_and_retires_all_lower_operations() {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    let (client, mut peer) = peers("localhost");
    let tls = TlsStream::new(left, client, Default::default()).unwrap();
    runtime
        .block_on(established(&tls, &right, &mut peer))
        .unwrap();
    network
        .push_fault(ScriptedFault::fail_before(
            NetworkOperationKind::ShutdownWrite,
            42,
        ))
        .unwrap();
    let failure = runtime
        .block_on(tls.submit_shutdown_write())
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(failure.error().error(), &NetworkError::Injected { tag: 42 });
    assert_eq!(
        tls.status().failure,
        Some(SecurityError::Network(NetworkError::Injected { tag: 42 }))
    );
    assert!(tls.peer_verified().is_none());
    runtime.block_on(tls.retire()).unwrap().unwrap();
    assert_retired(&tls);
    assert_eq!(network.status().inflight_operations, 0);
}

async fn established<S: TlsTransport>(
    tls: &TlsStream<S>,
    stream: &S,
    server: &mut ServerConnection,
) {
    let mut handshake = Box::pin(tls.handshake());
    let mut rx: Option<Pin<Box<S::ReadResponse>>> = None;
    let mut tx: Option<Pin<Box<S::WriteResponse>>> = None;
    let mut pending_tx = Vec::new();
    std::future::poll_fn(|cx| {
        let ready = handshake.as_mut().poll(cx);
        if let Poll::Ready(result) = ready {
            result.unwrap();
            return Poll::Ready(());
        }
        for _ in 0..64 {
            let mut progress = false;
            if let Some(read) = &mut rx
                && let Poll::Ready(result) = read.as_mut().poll(cx)
            {
                rx = None;
                let result = result.unwrap();
                assert!(!result.end_of_stream);
                server.read_tls(&mut Cursor::new(result.buffer)).unwrap();
                server.process_new_packets().unwrap();
                progress = true;
            }
            if let Some(write) = &mut tx
                && let Poll::Ready(result) = write.as_mut().poll(cx)
            {
                tx = None;
                let result = result.unwrap();
                pending_tx = result.buffer;
                let len = pending_tx.len();
                pending_tx.copy_within(result.bytes_written..len, 0);
                pending_tx.truncate(len - result.bytes_written);
                progress = true;
            }
            if tx.is_none() {
                if pending_tx.is_empty() && server.wants_write() {
                    server.write_tls(&mut pending_tx).unwrap();
                }
                if !pending_tx.is_empty() {
                    tx = Some(Box::pin(stream.submit_write(WriteRequest {
                        buffer: mem::take(&mut pending_tx),
                    })));
                    progress = true;
                }
            }
            if rx.is_none() && server.is_handshaking() {
                rx = Some(Box::pin(stream.submit_read(ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: 65536,
                })));
                progress = true;
            }
            if !progress {
                break;
            }
        }
        Poll::Pending
    })
    .await;
    // Client verification can precede consumption of its final handshake write.
    // Finish that server-side read without leaving an abandoned receive behind.
    while server.is_handshaking() {
        let read = rx.take().unwrap_or_else(|| {
            Box::pin(stream.submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 65536,
            }))
        });
        let result = read.await.unwrap();
        server.read_tls(&mut Cursor::new(result.buffer)).unwrap();
        server.process_new_packets().unwrap();
    }
    if let Some(write) = tx {
        write.await.unwrap();
    }
}

#[test]
fn bounded_admission_and_abandoned_write_are_driven_by_the_read_waiter() {
    let mut runtime = SimRuntime::default();
    let memory = MemoryNetwork::new(Default::default()).unwrap();
    let (left, right) = memory.connected_pair().unwrap();
    let (client, peer) = peers("localhost");
    let tls = TlsStream::new(
        left,
        client,
        TlsStreamLimits {
            write_operations: 1,
            write_bytes: 65536,
            ..Default::default()
        },
    )
    .unwrap();
    let client = async {
        tls.handshake().await.unwrap();
        let abandoned = tls.submit_write(WriteRequest {
            buffer: b"abandoned but admitted".to_vec(),
        });
        assert_eq!(tls.status().write_operations, 1);
        let error = tls
            .submit_write(WriteRequest { buffer: vec![1] })
            .await
            .unwrap_err();
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        drop(abandoned);
        assert_eq!(tls.status().write_operations, 1);
        let received = tls
            .submit_read(ReadRequest {
                buffer: Vec::new(),
                max_bytes: 1024,
            })
            .await
            .unwrap();
        assert_eq!(received.buffer, b"abandoned but admitted");
        assert_eq!(tls.status().write_operations, 0);
        tls.retire().await.unwrap();
    };
    let (_, received) = runtime.block_on(join(client, server(right, peer))).unwrap();
    assert_eq!(received, b"abandoned but admitted");
    assert_eq!(memory.status().inflight_operations, 0);
}

struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

// Retain a raw provider handle independently of TLS so a dropped TLS owner
// cannot make the test pass merely by closing the lower stream immediately.
struct SharedTestStream<S>(std::rc::Rc<S>);
impl<S: ByteStreamSubmit> ByteStreamSubmit for SharedTestStream<S> {
    type ReadResponse = S::ReadResponse;
    type WriteResponse = S::WriteResponse;
    type ControlResponse = S::ControlResponse;
    fn submit_read(&self, r: ReadRequest) -> Self::ReadResponse {
        self.0.submit_read(r)
    }
    fn submit_write(&self, r: WriteRequest) -> Self::WriteResponse {
        self.0.submit_write(r)
    }
    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        self.0.submit_shutdown_write()
    }
    fn submit_close(&self) -> Self::ControlResponse {
        self.0.submit_close()
    }
}
impl<S: ByteStreamVectoredSubmit> ByteStreamVectoredSubmit for SharedTestStream<S> {
    type WriteVectoredResponse = S::WriteVectoredResponse;
    fn max_segments(&self) -> usize {
        self.0.max_segments()
    }
    fn submit_write_vectored(&self, r: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        self.0.submit_write_vectored(r)
    }
}

#[test]
fn dropping_waiter_and_stream_releases_plaintext_at_provider_terminal_without_tls_polling() {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    let held_left = std::rc::Rc::new(left);
    let left = SharedTestStream(held_left.clone());
    let right = SharedTestStream(std::rc::Rc::new(right));
    let (client, mut peer) = peers("localhost");
    let tls = TlsStream::new(left, client, Default::default()).unwrap();
    runtime
        .block_on(established(&tls, &right, &mut peer))
        .unwrap();
    let link = LinkKey {
        from: NodeId(1),
        to: NodeId(2),
    };
    network
        .set_link(
            link,
            LinkConfig {
                state: LinkState::Clogged,
                ..Default::default()
            },
        )
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let bytes = SharedBytes::from(vec![7; 32768])
        .attach_guard(Arc::new(Guard(drops.clone())))
        .unwrap();
    let mut write = Box::pin(tls.submit_write_vectored(VectoredWriteRequest {
        segments: vec![WriteSegment {
            bytes,
            range: 0..32768,
        }],
    }));
    assert!(
        write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(tls.status().underlying_write);
    let core = Arc::downgrade(&tls.core);
    let accounting = lock(&tls.core).accounting.clone();
    drop(write);
    drop(tls);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(accounting.writes.load(Ordering::Relaxed), 1);
    assert!(core.upgrade().is_none(), "TLS must not retain a self-cycle");
    network.set_link(link, LinkConfig::default()).unwrap();
    runtime.run_until_stalled().unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(accounting.writes.load(Ordering::Relaxed), 0);
    assert!(core.upgrade().is_none());
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn transport_failure_certainty_fences_tls_and_never_reuses_record_sequence() {
    for applied in [false, true] {
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        let (client, mut peer) = peers("localhost");
        let tls = TlsStream::new(left, client, Default::default()).unwrap();
        runtime
            .block_on(established(&tls, &right, &mut peer))
            .unwrap();
        network
            .push_fault(if applied {
                ScriptedFault::fail_after(
                    NetworkOperationKind::Write,
                    11,
                    AfterFaultCertainty::MayHaveApplied,
                )
            } else {
                ScriptedFault::fail_before(NetworkOperationKind::Write, 11)
            })
            .unwrap();
        let result = runtime
            .block_on(tls.submit_write(WriteRequest {
                buffer: b"must not resume TLS".to_vec(),
            }))
            .unwrap()
            .unwrap_err();
        assert_eq!(
            result.certainty(),
            if applied {
                CompletionCertainty::MayHaveApplied
            } else {
                CompletionCertainty::NotApplied
            }
        );
        assert_eq!(result.error().bytes_transferred(), 0);
        assert_eq!(
            result.into_parts().1.into_buffer().unwrap(),
            b"must not resume TLS"
        );
        assert!(tls.status().failure.is_some());
        assert!(tls.peer_verified().is_none());
        let error = runtime
            .block_on(tls.submit_write(WriteRequest { buffer: vec![1] }))
            .unwrap()
            .unwrap_err();
        assert_eq!(error.certainty(), CompletionCertainty::NotApplied);
        runtime.block_on(tls.retire()).unwrap().unwrap();
        assert_eq!(network.status().inflight_operations, 0);
    }
}

#[test]
fn abandoned_read_releases_input_and_connection_guards_at_actual_provider_terminal() {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    let held_left = std::rc::Rc::new(left);
    let left = SharedTestStream(held_left.clone());
    let right = SharedTestStream(std::rc::Rc::new(right));
    let (client, mut peer) = peers("localhost");
    let tls = TlsStream::new(left, client, Default::default()).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    tls.attach_lifetime_guard(Arc::new(Guard(drops.clone())))
        .unwrap();
    runtime
        .block_on(established(&tls, &right, &mut peer))
        .unwrap();
    let link = LinkKey {
        from: NodeId(2),
        to: NodeId(1),
    };
    network
        .set_link(
            link,
            LinkConfig {
                state: LinkState::Clogged,
                ..Default::default()
            },
        )
        .unwrap();
    let mut buffer = Vec::with_capacity(65536);
    buffer.extend_from_slice(b"owned prefix");
    let mut read = Box::pin(tls.submit_read(ReadRequest {
        buffer,
        max_bytes: 32,
    }));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let core = Arc::downgrade(&tls.core);
    let accounting = lock(&tls.core).accounting.clone();
    assert!(tls.status().underlying_read);
    assert_eq!(tls.status().retained_read_bytes, 65536);
    drop(read);
    drop(tls);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(accounting.read_bytes.load(Ordering::Relaxed), 65536);
    assert!(
        core.upgrade().is_none(),
        "TLS state is not needed for actual terminal release"
    );
    network.set_link(link, LinkConfig::default()).unwrap();
    assert!(runtime.block_on(send(&right, vec![1; 32])).unwrap());
    runtime.run_until_stalled().unwrap();
    assert_eq!(accounting.read_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert!(core.upgrade().is_none());
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn runtime_shutdown_releases_abandoned_tls_reads_without_a_remaining_observer() {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    let held_left = std::rc::Rc::new(left);
    let left = SharedTestStream(held_left.clone());
    let right = SharedTestStream(std::rc::Rc::new(right));
    let (client, mut peer) = peers("localhost");
    let tls = TlsStream::new(left, client, Default::default()).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    tls.attach_lifetime_guard(Arc::new(Guard(drops.clone())))
        .unwrap();
    runtime
        .block_on(established(&tls, &right, &mut peer))
        .unwrap();
    network
        .set_link(
            LinkKey {
                from: NodeId(2),
                to: NodeId(1),
            },
            LinkConfig {
                latency: kr_runtime::SimDuration::from_nanos(60_000_000_000),
                ..Default::default()
            },
        )
        .unwrap();
    let mut read = Box::pin(tls.submit_read(ReadRequest {
        buffer: Vec::with_capacity(4096),
        max_bytes: 16,
    }));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let accounting = lock(&tls.core).accounting.clone();
    let core = Arc::downgrade(&tls.core);
    drop(read);
    drop(tls);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(accounting.read_bytes.load(Ordering::Relaxed), 4096);
    assert!(core.upgrade().is_none());
    runtime.shutdown().unwrap();
    // Dropping the raw provider closes the pending receive after the runtime's
    // delayed-completion task is canceled. No TLS state is available to poll.
    drop(held_left);
    drop(right);
    drop(network);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(accounting.read_bytes.load(Ordering::Relaxed), 0);
}

#[test]
fn memory_provider_releases_abandoned_tls_input_without_a_tls_observer() {
    for reading in [true, false] {
        let mut runtime = SimRuntime::default();
        let network = MemoryNetwork::new(MemoryNetworkConfig {
            directional_buffer_bytes: 64,
            ..Default::default()
        })
        .unwrap();
        let (left, right) = network.connected_pair().unwrap();
        let held_left = std::rc::Rc::new(left);
        let left = SharedTestStream(held_left.clone());
        let right = SharedTestStream(std::rc::Rc::new(right));
        let (client, mut peer) = peers("localhost");
        let tls = TlsStream::new(left, client, Default::default()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        tls.attach_lifetime_guard(Arc::new(Guard(drops.clone())))
            .unwrap();
        runtime
            .block_on(established(&tls, &right, &mut peer))
            .unwrap();
        let accounting = lock(&tls.core).accounting.clone();
        let core = Arc::downgrade(&tls.core);
        if reading {
            let mut read = Box::pin(tls.submit_read(ReadRequest {
                buffer: Vec::with_capacity(4096),
                max_bytes: 32,
            }));
            assert!(
                read.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(read);
            drop(tls);
            assert!(core.upgrade().is_none());
            assert_eq!(accounting.read_bytes.load(Ordering::Relaxed), 4096);
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            runtime
                .block_on(right.submit_write(WriteRequest {
                    buffer: vec![7; 32],
                }))
                .unwrap()
                .unwrap();
            assert_eq!(accounting.read_bytes.load(Ordering::Relaxed), 0);
        } else {
            runtime
                .block_on(held_left.submit_write(WriteRequest {
                    buffer: vec![7; 64],
                }))
                .unwrap()
                .unwrap();
            let mut write = Box::pin(tls.submit_write(WriteRequest {
                buffer: vec![8; 4096],
            }));
            assert!(
                write
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(write);
            drop(tls);
            assert!(core.upgrade().is_none());
            assert_eq!(accounting.write_bytes.load(Ordering::Relaxed), 4096);
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            runtime
                .block_on(right.submit_read(ReadRequest {
                    buffer: Vec::new(),
                    max_bytes: 64,
                }))
                .unwrap()
                .unwrap();
            assert_eq!(accounting.write_bytes.load(Ordering::Relaxed), 0);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(network.status().inflight_operations, 0);
    }
}

#[test]
fn abandoned_shutdown_guard_releases_when_delayed_provider_control_completes() {
    let mut runtime = SimRuntime::default();
    let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
    let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
    let held_left = std::rc::Rc::new(left);
    let left = SharedTestStream(held_left.clone());
    let right = SharedTestStream(std::rc::Rc::new(right));
    let (client, mut peer) = peers("localhost");
    let tls = TlsStream::new(left, client, Default::default()).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    tls.attach_lifetime_guard(Arc::new(Guard(drops.clone())))
        .unwrap();
    runtime
        .block_on(established(&tls, &right, &mut peer))
        .unwrap();
    network
        .set_link(
            LinkKey {
                from: NodeId(1),
                to: NodeId(2),
            },
            LinkConfig {
                latency: kr_runtime::SimDuration::from_nanos(1_000_000_000),
                ..Default::default()
            },
        )
        .unwrap();
    let mut shutdown = Box::pin(tls.submit_shutdown_write());
    for _ in 0..8 {
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        if tls.status().underlying_control {
            break;
        }
        runtime.run_until_stalled().unwrap();
    }
    assert!(tls.status().underlying_control);
    let accounting = lock(&tls.core).accounting.clone();
    let core = Arc::downgrade(&tls.core);
    drop(shutdown);
    drop(tls);
    assert!(core.upgrade().is_none());
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(accounting.controls.load(Ordering::Relaxed), 1);
    runtime.run_until_stalled().unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(accounting.controls.load(Ordering::Relaxed), 0);
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn memory_completed_shutdown_releases_without_another_tls_poll() {
    let mut runtime = SimRuntime::default();
    let network = MemoryNetwork::new(Default::default()).unwrap();
    let (left, right) = network.connected_pair().unwrap();
    let held_left = std::rc::Rc::new(left);
    let left = SharedTestStream(held_left.clone());
    let right = SharedTestStream(std::rc::Rc::new(right));
    let (client, mut peer) = peers("localhost");
    let tls = TlsStream::new(
        left,
        client,
        TlsStreamLimits {
            transitions_per_poll: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    tls.attach_lifetime_guard(Arc::new(Guard(drops.clone())))
        .unwrap();
    runtime
        .block_on(established(&tls, &right, &mut peer))
        .unwrap();
    let mut shutdown = Box::pin(tls.submit_shutdown_write());
    for _ in 0..32 {
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        if tls.status().underlying_control {
            break;
        }
    }
    assert!(tls.status().underlying_control);
    let accounting = lock(&tls.core).accounting.clone();
    let core = Arc::downgrade(&tls.core);
    assert_eq!(accounting.controls.load(Ordering::Relaxed), 1);
    drop(shutdown);
    drop(tls);
    assert!(core.upgrade().is_none());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(accounting.controls.load(Ordering::Relaxed), 0);
    assert_eq!(network.status().inflight_operations, 0);
}

#[test]
fn read_failure_returns_original_prefix_and_correct_consumption_certainty() {
    for applied in [false, true] {
        let mut runtime = SimRuntime::default();
        let network = SimNetwork::new(runtime.handle(), Default::default()).unwrap();
        let (left, right) = network.connected_pair(NodeId(1), NodeId(2)).unwrap();
        let (client, mut peer) = peers("localhost");
        let tls = TlsStream::new(left, client, Default::default()).unwrap();
        runtime
            .block_on(established(&tls, &right, &mut peer))
            .unwrap();
        assert!(
            !tls.status().underlying_read,
            "handshake left a receive: {:?}",
            tls.status()
        );
        if applied {
            assert!(!peer.is_handshaking(), "server handshake incomplete");
            peer.writer().write_all(b"may have been consumed").unwrap();
            let mut bytes = Vec::new();
            peer.write_tls(&mut bytes).unwrap();
            assert!(!bytes.is_empty(), "server buffered no TLS record");
            assert!(runtime.block_on(send(&right, bytes)).unwrap());
        }
        network
            .push_fault(if applied {
                ScriptedFault::fail_after(
                    NetworkOperationKind::Read,
                    12,
                    AfterFaultCertainty::MayHaveApplied,
                )
            } else {
                ScriptedFault::fail_before(NetworkOperationKind::Read, 12)
            })
            .unwrap();
        let failure = runtime
            .block_on(tls.submit_read(ReadRequest {
                buffer: b"prefix".to_vec(),
                max_bytes: 1024,
            }))
            .unwrap_or_else(|error| {
                panic!(
                    "applied={applied} status={:?} network={:?} error={error:?}",
                    tls.status(),
                    network.status()
                )
            })
            .unwrap_err();
        assert_eq!(
            failure.certainty(),
            if applied {
                CompletionCertainty::MayHaveApplied
            } else {
                CompletionCertainty::NotApplied
            }
        );
        assert_eq!(failure.into_parts().1.into_buffer().unwrap(), b"prefix");
        runtime.block_on(tls.retire()).unwrap().unwrap();
        assert_eq!(network.status().inflight_operations, 0);
    }
}
