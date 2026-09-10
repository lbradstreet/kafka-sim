use super::*;
use kr_kafka_client::{
    config::{BrokerEndpoint, Secret, TlsConfig},
    transport::DriverConfig,
};
use kr_kafka_protocol::{
    self as wire, Request, Response as KafkaResponse,
    wire::{DecodeLimits, EncodeLimits},
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};

fn response(response: KafkaResponse<'_>, version: i16, correlation: i32) -> Vec<u8> {
    response
        .plan_frame(version, correlation, EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap()
}
fn broker(mut stream: impl Read + Write, produce: i16, sasl: bool) {
    let count = if sasl { 3 } else { 2 };
    for expected in 0..count {
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let size = i32::from_be_bytes(prefix);
        assert!((0..=65536).contains(&size));
        let mut bytes = prefix.to_vec();
        bytes.resize(size as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).unwrap();
        let request = wire::frame::decode_request(&bytes, DecodeLimits::default()).unwrap();
        let correlation = i32::from_be_bytes(bytes[8..12].try_into().unwrap());
        assert!(correlation < 0);
        let frame = match request.body {
            Request::ApiVersionsRequest(_) => {
                use wire::api_versions_response::{self as api, v3::*};
                let keys = [
                    ApiVersion {
                        api_key: 18,
                        min_version: 0,
                        max_version: 3,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: 0,
                        min_version: 0,
                        max_version: produce,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: 3,
                        min_version: 0,
                        max_version: 12,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: 22,
                        min_version: 0,
                        max_version: 4,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: 17,
                        min_version: 0,
                        max_version: 1,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: 36,
                        min_version: 0,
                        max_version: 2,
                        ..Default::default()
                    },
                ];
                response(
                    KafkaResponse::ApiVersionsResponse(api::View::V3(ApiVersionsResponse {
                        api_keys: (&keys[..]).into(),
                        ..Default::default()
                    })),
                    3,
                    correlation,
                )
            }
            Request::SaslHandshakeRequest(_) => {
                assert!(sasl && expected == 1);
                use wire::sasl_handshake_response::{self as api, v1::*};
                let mechanisms = ["PLAIN"];
                response(
                    KafkaResponse::SaslHandshakeResponse(api::View::V1(SaslHandshakeResponse {
                        mechanisms: (&mechanisms[..]).into(),
                        ..Default::default()
                    })),
                    1,
                    correlation,
                )
            }
            Request::SaslAuthenticateRequest(wire::sasl_authenticate_request::View::V2(auth)) => {
                assert!(sasl && expected == 2);
                assert_eq!(auth.auth_bytes, b"\0user\0password");
                use wire::sasl_authenticate_response::{self as api, v2::*};
                response(
                    KafkaResponse::SaslAuthenticateResponse(api::View::V2(
                        SaslAuthenticateResponse::default(),
                    )),
                    2,
                    correlation,
                )
            }
            _ => panic!("unexpected request"),
        };
        for fragment in frame.chunks(3) {
            stream.write_all(fragment).unwrap();
        }
        stream.flush().unwrap();
        if produce < 13 {
            break;
        }
    }
    let mut byte = [0];
    let _ = stream.read(&mut byte);
}
fn config() -> ConnectionConfig {
    ConnectionConfig {
        client_id: "host-test".into(),
        max_connections: 2,
        max_operation_bytes: 1024 * 1024,
        rx_bytes_per_connection: 1024 * 1024,
        staging_bytes_per_connection: 64 * 1024,
        control_jobs: 2,
        control_bytes: 512 * 1024,
        connect_timeout: kr_runtime::RuntimeDuration::from_nanos(5_000_000_000),
        tls_plaintext_bytes: 32 * 1024,
        tls_ciphertext_bytes: 64 * 1024,
        security: SecurityConfig::Plaintext,
        transport: TransportPolicy::Readiness,
    }
}
#[derive(Clone, Default)]
struct Budget(Arc<std::sync::atomic::AtomicUsize>);
struct BudgetGuard {
    held: Arc<std::sync::atomic::AtomicUsize>,
    bytes: usize,
}
impl Drop for BudgetGuard {
    fn drop(&mut self) {
        self.held
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Budget {
    fn held(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl SetupBudget for Budget {
    fn reserve(&self, bytes: usize) -> Result<Arc<dyn Send + Sync>, ConnectError> {
        self.0
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |old| old.checked_add(bytes).filter(|n| *n <= 256 * 1024),
            )
            .map_err(|_| ConnectError::ResourceExhausted)?;
        Ok(Arc::new(BudgetGuard {
            held: self.0.clone(),
            bytes,
        }))
    }
}
fn connector(handle: HostHandle, config: ConnectionConfig, budget: Budget) -> HostConnector {
    let codec = ControlCodec::new(
        config.client_id.clone(),
        kr_kafka_client::control::ControlLimits {
            owned_bytes: 64 * 1024,
            auth_bytes: 16 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    HostConnector::new(handle, config, codec, Arc::new(budget)).unwrap()
}
fn target(handle: &HostHandle, address: SocketAddr) -> ConnectTarget {
    ConnectTarget {
        endpoint: BrokerEndpoint {
            host: address.ip().to_string(),
            port: address.port(),
        },
        broker_id: Some(0),
        lane: 0,
        deadline: handle
            .now()
            .checked_add(kr_runtime::RuntimeDuration::from_nanos(5_000_000_000))
            .unwrap(),
        driver: DriverConfig::default(),
        lifetime_guard: Some(Arc::new(())),
    }
}
#[test]
fn native_setup_is_cold_and_retains_driver_for_the_next_correlated_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        broker(stream, 13, false);
    });
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let config = config();
    let credits = Budget::default();
    let mut connector = connector(handle.clone(), config, credits.clone());
    let diagnostics = crate::diagnostics::HostDiagnostics::default();
    connector
        .attach_diagnostics(&diagnostics, runtime.control().observer())
        .unwrap();
    let rejected = crate::diagnostics::HostDiagnostics::default();
    assert!(matches!(
        connector.attach_diagnostics(&rejected, runtime.control().observer()),
        Err(ConnectError::InvalidConfiguration)
    ));
    assert!(diagnostics.snapshot().provider.is_some());
    assert!(rejected.snapshot().provider.is_none());
    assert!(Arc::ptr_eq(
        connector.settings.completions.get().unwrap(),
        &diagnostics.completions
    ));
    let target = target(&handle, address);
    let future = connector.connect(target.clone());
    assert_eq!(connector.control_usage(), (0, 0));
    assert_eq!(credits.held(), 0);
    let mut connected = runtime.block_on(future).unwrap().unwrap();
    assert!(connected.capabilities.supports(0, 13));
    runtime
        .block_on(async {
            let target = SetupTarget {
                target,
                working: Arc::new(()),
            };
            let request = connector
                .settings
                .codec
                .api_versions_request(-100, Probe::V3)
                .unwrap();
            assert!(matches!(
                exchange(
                    &handle,
                    &target,
                    &mut connected.driver,
                    -100,
                    request,
                    |frame| connector
                        .settings
                        .codec
                        .parse_api_versions(frame, -100, Probe::V3)
                )
                .await
                .unwrap(),
                Negotiation::Ready(_)
            ));
            connected.driver.retire(RetireReason::Requested);
            drain(&handle, &mut connected.driver).await;
        })
        .unwrap();
    let advertised = connected.capabilities.clone();
    drop(connected);
    assert_eq!(credits.held(), kr_kafka_client::connector::DATA_SETUP_BYTES);
    assert!(advertised.supports(0, 13));
    drop(advertised);
    assert_eq!(credits.held(), 0);
    server.join().unwrap();
    runtime.finish().unwrap();
}
#[test]
fn generic_setup_reports_older_capabilities_and_caller_retires_original_driver() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        broker(stream, 9, false);
    });
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let config = config();
    let credits = Budget::default();
    let mut connector = connector(handle.clone(), config, credits.clone());
    let mut connected = runtime
        .block_on(connector.connect(target(&handle, address)))
        .unwrap()
        .unwrap();
    assert!(connected.capabilities.supports(0, 9));
    assert!(connected.capabilities.require(0, 13).is_err());
    runtime
        .block_on(async {
            connected.driver.retire(RetireReason::Requested);
            drain(&handle, &mut connected.driver).await;
        })
        .unwrap();
    drop(connected);
    assert_eq!(credits.held(), 0);
    server.join().unwrap();
    runtime.finish().unwrap();
}
#[test]
fn plain_sasl_is_sent_only_after_real_certificate_verified_tls() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(
                include_bytes!("../../tests/fixtures/localhost.der").to_vec(),
            )],
            rustls::pki_types::PrivatePkcs8KeyDer::from(
                include_bytes!("../../tests/fixtures/localhost-key.der").to_vec(),
            )
            .into(),
        )
        .unwrap();
        let connection = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();
        broker(rustls::StreamOwned::new(connection, stream), 13, true);
    });
    let config = ConnectionConfig {
        security: SecurityConfig::SaslTls {
            tls: TlsConfig {
                roots_der: vec![include_bytes!("../../tests/fixtures/ca.der").to_vec()],
                server_name: Some("localhost".into()),
                use_system_roots: false,
            },
            mechanism: SaslMechanism::Plain,
            username: "user".into(),
            password: Secret::new("password".into()),
        },
        ..config()
    };
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let credits = Budget::default();
    let mut connector = connector(handle.clone(), config, credits.clone());
    let mut connected = runtime
        .block_on(connector.connect(target(&handle, address)))
        .unwrap()
        .unwrap();
    runtime
        .block_on(async {
            connected.driver.retire(RetireReason::Requested);
            drain(&handle, &mut connected.driver).await;
        })
        .unwrap();
    drop(connected);
    assert_eq!(credits.held(), 0);
    server.join().unwrap();
    runtime.finish().unwrap();
}

#[derive(Clone, Copy, Debug)]
enum SetupFailure {
    Eof,
    InvalidLength,
    TruncatedPayload,
    WrongCorrelation,
}
fn read_api_versions(stream: &mut impl Read) -> i32 {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).unwrap();
    let length = i32::from_be_bytes(prefix);
    assert!((0..=65536).contains(&length));
    let mut request = prefix.to_vec();
    request.resize(length as usize + 4, 0);
    stream.read_exact(&mut request[4..]).unwrap();
    assert!(matches!(
        wire::frame::decode_request(&request, DecodeLimits::default())
            .unwrap()
            .body,
        Request::ApiVersionsRequest(_)
    ));
    i32::from_be_bytes(request[8..12].try_into().unwrap())
}

#[test]
fn setup_eof_and_malformed_frames_preserve_categories_and_retire_on_both_backends() {
    for transport in [TransportPolicy::Readiness, TransportPolicy::Uring] {
        for failure in [
            SetupFailure::Eof,
            SetupFailure::InvalidLength,
            SetupFailure::TruncatedPayload,
            SetupFailure::WrongCorrelation,
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let correlation = read_api_versions(&mut stream);
                let frame = match failure {
                    SetupFailure::Eof => return,
                    SetupFailure::InvalidLength => (-1i32).to_be_bytes().to_vec(),
                    SetupFailure::TruncatedPayload => {
                        [4i32.to_be_bytes(), correlation.to_be_bytes()].concat()
                    }
                    SetupFailure::WrongCorrelation => {
                        [4i32.to_be_bytes(), (correlation - 1).to_be_bytes()].concat()
                    }
                };
                stream.write_all(&frame).unwrap();
                stream.flush().unwrap();
                let mut byte = [0];
                let _ = stream.read(&mut byte);
            });
            let mut runtime = HostRuntime::default();
            let handle = runtime.handle();
            let credits = Budget::default();
            let mut connector = connector(
                handle.clone(),
                ConnectionConfig {
                    transport,
                    ..config()
                },
                credits.clone(),
            );
            let result = runtime
                .block_on(connector.connect(target(&handle, address)))
                .unwrap();
            let error = match result {
                Ok(_) => panic!("{transport:?}/{failure:?} unexpectedly connected"),
                Err(error) => error,
            };
            match failure {
                SetupFailure::Eof => assert!(
                    matches!(error, ConnectError::Network(NetworkError::ConnectionClosed)),
                    "{transport:?}: {error:?}"
                ),
                SetupFailure::WrongCorrelation => assert!(
                    matches!(
                        error,
                        ConnectError::Protocol(ControlError::Wire(
                            wire::wire::Error::CorrelationMismatch {
                                expected: -1,
                                actual: -2
                            }
                        ))
                    ),
                    "{transport:?}: {error:?}"
                ),
                _ => assert!(
                    matches!(error, ConnectError::Protocol(ControlError::Wire(_))),
                    "{transport:?}/{failure:?}: {error:?}"
                ),
            }
            assert_eq!(credits.held(), 0, "setup working allocation retired");
            assert_eq!(connector.control_usage(), (0, 0));
            server.join().unwrap();
            runtime.finish().unwrap();
        }
    }
}

#[test]
fn rejected_native_tls_certificate_is_authentication_without_repoll_or_guard_leak() {
    for transport in [TransportPolicy::Readiness, TransportPolicy::Uring] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(
                    include_bytes!("../../tests/fixtures/localhost.der").to_vec(),
                )],
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    include_bytes!("../../tests/fixtures/localhost-key.der").to_vec(),
                )
                .into(),
            )
            .unwrap();
            let connection = rustls::ServerConnection::new(Arc::new(config)).unwrap();
            let mut tls = rustls::StreamOwned::new(connection, stream);
            let mut byte = [0];
            let result = tls.read(&mut byte);
            assert!(
                !matches!(result, Ok(n) if n > 0),
                "no Kafka plaintext after certificate rejection"
            );
        });
        let config = ConnectionConfig {
            transport,
            security: SecurityConfig::Tls {
                tls: TlsConfig {
                    roots_der: vec![include_bytes!("../../tests/fixtures/ca.der").to_vec()],
                    server_name: Some("wrong-host.example".into()),
                    use_system_roots: false,
                },
            },
            ..config()
        };
        let mut runtime = HostRuntime::default();
        let handle = runtime.handle();
        let credits = Budget::default();
        let mut connector = connector(handle.clone(), config, credits.clone());
        let result = runtime
            .block_on(connector.connect(target(&handle, address)))
            .unwrap();
        assert!(
            matches!(result, Err(ConnectError::Authentication)),
            "{transport:?}"
        );
        assert_eq!(
            credits.held(),
            0,
            "failed TLS setup must retire its actual owner"
        );
        assert_eq!(connector.control_usage(), (0, 0));
        server.join().unwrap();
        runtime.finish().unwrap();
    }
}

#[test]
fn native_setup_classification_does_not_relabel_transport_as_credentials() {
    assert!(matches!(
        retire_error(RetireReason::Deadline { correlation: 1 }),
        ConnectError::Timeout
    ));
    assert!(matches!(
        retire_error(RetireReason::Transport(TransportError::AllocationFailed)),
        ConnectError::ResourceExhausted
    ));
    assert!(matches!(
        retire_error(RetireReason::Transport(TransportError::InvalidStage)),
        ConnectError::InvalidConfiguration
    ));
    assert!(matches!(
        retire_error(RetireReason::Requested),
        ConnectError::Network(NetworkError::ConnectionClosed)
    ));
    assert!(matches!(
        security_error(SecurityError::TruncatedTls),
        ConnectError::Network(NetworkError::ConnectionClosed)
    ));
    assert!(matches!(
        security_error(SecurityError::Network(NetworkError::ConnectionRefused)),
        ConnectError::Network(NetworkError::ConnectionRefused)
    ));
    assert!(matches!(
        security_error(SecurityError::AuthenticationRejected),
        ConnectError::Authentication
    ));
    assert!(matches!(
        security_error(SecurityError::InvalidServerSignature),
        ConnectError::Authentication
    ));
}

#[test]
fn resolver_normalization_is_bounded_and_reports_network_failures_without_host_text() {
    let address = SocketAddr::from(([127, 0, 0, 1], 9092));
    let visits = std::cell::Cell::new(0);
    let duplicates = std::iter::repeat(address).inspect(|_| visits.set(visits.get() + 1));
    let output = normalize_addresses(Vec::with_capacity(16), Ok(duplicates)).unwrap();
    assert_eq!(visits.get(), 16);
    assert_eq!(output, [address]);
    assert_eq!(output.capacity(), 16);
    let empty = normalize_addresses(Vec::with_capacity(16), Ok(std::iter::empty()));
    assert!(
        matches!(empty, Err(SecurityError::Network(NetworkError::Backend {
        operation: NetworkOperationKind::Connect, raw_os_error: None, message,
    })) if message == "DNS returned no socket addresses")
    );
    let error = normalize_addresses(
        Vec::with_capacity(16),
        Err::<std::iter::Empty<SocketAddr>, _>(std::io::Error::from_raw_os_error(2)),
    );
    assert!(
        matches!(error, Err(SecurityError::Network(NetworkError::Backend {
        operation: NetworkOperationKind::Connect, raw_os_error: Some(2), message,
    })) if message == "DNS address resolution failed")
    );
    let error = normalize_addresses(
        Vec::with_capacity(16),
        Err::<std::iter::Empty<SocketAddr>, _>(std::io::Error::other(
            "private-host-and-unbounded-resolver-text",
        )),
    );
    assert!(
        matches!(error, Err(SecurityError::Network(NetworkError::Backend { message, .. })) if message == "DNS address resolution failed")
    );
}

#[test]
fn native_tls_timeout_drains_pending_handshake_and_retires_guards_on_both_backends() {
    for transport in [TransportPolicy::Readiness, TransportPolicy::Uring] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut header = [0; 5];
            stream.read_exact(&mut header).unwrap();
            assert_eq!(header[0], 22, "first TLS record is a handshake");
            let length = u16::from_be_bytes([header[3], header[4]]) as usize;
            assert!(length > 0);
            let mut hello = vec![0; length];
            stream.read_exact(&mut hello).unwrap();
            // Deliberately supply no ServerHello. The connector's supplied
            // deadline must win, then real socket close must unblock this read.
            let mut bytes = [0; 4096];
            for _ in 0..16 {
                if stream.read(&mut bytes).unwrap() == 0 {
                    return;
                }
            }
            panic!("pending handshake socket did not retire within its bounded traffic");
        });
        let config = ConnectionConfig {
            transport,
            security: SecurityConfig::Tls {
                tls: TlsConfig {
                    roots_der: vec![include_bytes!("../../tests/fixtures/ca.der").to_vec()],
                    server_name: Some("localhost".into()),
                    use_system_roots: false,
                },
            },
            ..config()
        };
        let mut runtime = HostRuntime::default();
        let handle = runtime.handle();
        let credits = Budget::default();
        let mut connector = connector(handle.clone(), config, credits.clone());
        let mut connect_target = target(&handle, address);
        connect_target.deadline = handle
            .now()
            .checked_add(kr_runtime::RuntimeDuration::from_nanos(500_000_000))
            .unwrap();
        let result = runtime.block_on(connector.connect(connect_target)).unwrap();
        assert!(
            matches!(result, Err(ConnectError::Timeout)),
            "{transport:?}"
        );
        assert_eq!(credits.held(), 0);
        assert_eq!(connector.control_usage(), (0, 0));
        server.join().unwrap();
        runtime.finish().unwrap();
    }
}
