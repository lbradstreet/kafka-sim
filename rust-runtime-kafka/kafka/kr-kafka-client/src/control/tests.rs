use super::*;
use kr_kafka_protocol::wire::EncodeLimits;

const CORRELATION: i32 = 41;
fn codec() -> ControlCodec {
    ControlCodec::new("shared-client".into(), ControlLimits::default()).unwrap()
}
fn frame(response: Response<'_>, version: i16) -> Vec<u8> {
    response
        .plan_frame(version, CORRELATION, EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap()
}
fn advertised(version: i16, error: i16, ranges: &[(i16, i16, i16)]) -> Vec<u8> {
    use wire::api_versions_response as api;
    if version == 3 {
        let keys: Vec<_> = ranges
            .iter()
            .map(|&(api_key, min_version, max_version)| api::v3::ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect();
        frame(
            Response::ApiVersionsResponse(api::View::V3(api::v3::ApiVersionsResponse {
                error_code: error,
                api_keys: keys.as_slice().into(),
                ..Default::default()
            })),
            3,
        )
    } else {
        let keys: Vec<_> = ranges
            .iter()
            .map(|&(api_key, min_version, max_version)| api::v0::ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect();
        frame(
            Response::ApiVersionsResponse(api::View::V0(api::v0::ApiVersionsResponse {
                error_code: error,
                api_keys: keys.as_slice().into(),
                ..Default::default()
            })),
            0,
        )
    }
}
#[test]
fn metadata_only_client_negotiates_without_producer_or_authentication_apis() {
    let codec = codec();
    let Negotiation::Ready(capabilities) = codec
        .parse_api_versions(
            &advertised(3, 0, &[(18, 0, 3), (3, 0, 12)]),
            CORRELATION,
            Probe::V3,
        )
        .unwrap()
    else {
        panic!("valid advertisement");
    };
    assert_eq!(capabilities.probe_version(), 3);
    assert_eq!(
        capabilities
            .ranges()
            .iter()
            .map(|r| r.api_key)
            .collect::<Vec<_>>(),
        [3, 18]
    );
    assert!(capabilities.supports(18, 3));
    assert!(capabilities.supports(3, 12));
    assert!(!capabilities.supports(0, 13));
    assert!(!capabilities.supports(22, 4));
    assert!(!capabilities.supports(17, 1));
    assert!(!capabilities.supports(3, 13));
    assert!(!capabilities.supports(3, -1));
    assert_eq!(
        capabilities.require(0, 13),
        Err(ControlError::MissingCapability {
            api_key: 0,
            required: 13
        })
    );

    let selectors = [MetadataSelector::Name("events")];
    let request = codec.metadata_request(CORRELATION, &selectors).unwrap();
    let decoded = wire::frame::decode_request(&request, DecodeLimits::default()).unwrap();
    let Request::MetadataRequest(wire::metadata_request::View::V12(body)) = decoded.body else {
        panic!("metadata request");
    };
    assert!(!body.allow_auto_topic_creation);
    assert_eq!(
        body.topics.unwrap().iter().next().unwrap().unwrap().name,
        Some("events")
    );
    use wire::metadata_response::{self as api, v12::*};
    let broker = [MetadataResponseBroker {
        node_id: 9,
        host: "broker",
        port: 9092,
        ..Default::default()
    }];
    let nodes = [9];
    let partitions = [MetadataResponsePartition {
        partition_index: 0,
        leader_id: 9,
        leader_epoch: 2,
        replica_nodes: nodes.as_slice().into(),
        isr_nodes: nodes.as_slice().into(),
        ..Default::default()
    }];
    let topics = [MetadataResponseTopic {
        topic_id: [7; 16],
        name: Some("events"),
        partitions: partitions.as_slice().into(),
        ..Default::default()
    }];
    let reply = frame(
        Response::MetadataResponse(api::View::V12(MetadataResponse {
            brokers: broker.as_slice().into(),
            topics: topics.as_slice().into(),
            controller_id: 9,
            ..Default::default()
        })),
        12,
    );
    let metadata = codec
        .parse_metadata(&reply, CORRELATION, &selectors)
        .unwrap();
    assert_eq!(metadata.topics[0].id, TopicId([7; 16]));
    assert_eq!(metadata.topics[0].requested_index, 0);
    assert_eq!(
        metadata.topics[0].partitions[0].metadata,
        PartitionMetadata {
            leader: 9,
            leader_epoch: 2
        }
    );
    assert_eq!(metadata.brokers[0].id, 9);
    assert_eq!(metadata.topics[0].partitions[0].replicas, vec![9]);
    assert_eq!(metadata.topics[0].partitions[0].isr, vec![9]);
    assert!(metadata.topics[0].partitions[0].offline.is_empty());
}
#[test]
fn classic_retry_requires_a_valid_unsupported_response_and_checks_all_ranges() {
    let codec = codec();
    let unsupported = advertised(0, code::UNSUPPORTED_VERSION, &[]);
    assert_eq!(
        codec.parse_api_versions(&unsupported, CORRELATION, Probe::V3),
        Ok(Negotiation::ProbeClassic)
    );
    assert_eq!(
        codec.parse_api_versions(&unsupported, CORRELATION, Probe::V0),
        Err(ControlError::Broker {
            api_key: 18,
            error_code: code::UNSUPPORTED_VERSION
        })
    );
    let classic = advertised(0, 0, &[(3, 0, 12)]);
    assert!(
        codec
            .parse_api_versions(&classic, CORRELATION, Probe::V3)
            .is_err()
    );
    let Negotiation::Ready(capabilities) = codec
        .parse_api_versions(&classic, CORRELATION, Probe::V0)
        .unwrap()
    else {
        panic!("classic success");
    };
    assert_eq!(capabilities.probe_version(), 0);
    assert_eq!(capabilities.ranges().len(), 1);
    for ranges in [
        &[(3, 0, 12), (3, 0, 12)][..],
        &[(3, 12, 0)][..],
        &[(-1, 0, 1)][..],
    ] {
        assert_eq!(
            codec.parse_api_versions(&advertised(3, 0, ranges), CORRELATION, Probe::V3),
            Err(ControlError::Invalid("API ranges"))
        );
        assert!(
            codec
                .parse_api_versions(
                    &advertised(0, code::UNSUPPORTED_VERSION, ranges),
                    CORRELATION,
                    Probe::V3
                )
                .is_err()
        );
    }
    let mut wrong = advertised(3, 0, &[(3, 0, 12)]);
    wrong[4..8].copy_from_slice(&(CORRELATION + 1).to_be_bytes());
    assert!(matches!(
        codec.parse_api_versions(&wrong, CORRELATION, Probe::V3),
        Err(ControlError::Wire(
            wire::wire::Error::CorrelationMismatch { .. }
        ))
    ));
    let short = &advertised(3, 0, &[(3, 0, 12)])[..8];
    assert!(
        codec
            .parse_api_versions(short, CORRELATION, Probe::V3)
            .is_err()
    );
}
#[test]
fn common_advertisements_and_auth_bytes_respect_explicit_limits() {
    let limits = ControlLimits {
        api_keys: 1,
        ..Default::default()
    };
    let limited = ControlCodec::new("x".into(), limits).unwrap();
    assert_eq!(
        limited.parse_api_versions(
            &advertised(3, 0, &[(18, 0, 3), (3, 0, 12)]),
            CORRELATION,
            Probe::V3
        ),
        Err(ControlError::Limit("API keys"))
    );
    let tiny = ControlCodec::new(
        "x".into(),
        ControlLimits {
            owned_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let bytes = advertised(3, 0, &[(3, 0, 12)]);
    let count = allocation_counter::measure(|| {
        assert_eq!(
            tiny.parse_api_versions(&bytes, CORRELATION, Probe::V3),
            Err(ControlError::Limit("owned bytes"))
        );
    });
    assert_eq!(
        count.count_total, 0,
        "limit preflight precedes advertised storage allocation"
    );
    use wire::sasl_authenticate_response::{self as api, v2::*};
    let reply = frame(
        Response::SaslAuthenticateResponse(api::View::V2(SaslAuthenticateResponse {
            auth_bytes: b"proof",
            error_message: Some("private-error"),
            session_lifetime_ms: 40,
            ..Default::default()
        })),
        2,
    );
    let response = codec()
        .parse_sasl_authenticate(&reply, CORRELATION)
        .unwrap();
    assert_eq!(response.auth_bytes, b"proof");
    assert!(!format!("{response:?}").contains("proof"));
    assert!(!format!("{response:?}").contains("private-error"));
    let limited = ControlCodec::new(
        "x".into(),
        ControlLimits {
            auth_bytes: 4,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        limited.parse_sasl_authenticate(&reply, CORRELATION),
        Err(ControlError::Limit("auth bytes"))
    );
    assert_eq!(
        limited.sasl_authenticate_request(CORRELATION, b"proof"),
        Err(ControlError::Limit("auth bytes"))
    );
}

fn ready_capabilities() -> Capabilities {
    let Negotiation::Ready(capabilities) = codec()
        .parse_api_versions(
            &advertised(3, 0, &[(3, 0, 12), (18, 0, 3)]),
            CORRELATION,
            Probe::V3,
        )
        .unwrap()
    else {
        panic!("advertised ranges");
    };
    capabilities
}

#[test]
fn capability_clones_keep_one_guarded_backing_after_connection_and_observer_drop() {
    use crate::{
        connector::Connected,
        transport::{ConnectionDriver, DriverConfig},
    };
    use kr_runtime_io::network::{MemoryNetwork, MemoryNetworkConfig};
    let mut capabilities = ready_capabilities();
    let same_values = ready_capabilities();
    let before = format!("{capabilities:?}");
    let source: Arc<dyn Send + Sync> = Arc::new(());
    assert!(capabilities.attach_lifetime_guard(source.clone()).is_ok());
    assert_eq!(Arc::strong_count(&source), 2);
    assert_eq!(capabilities, same_values);
    assert_eq!(
        format!("{capabilities:?}"),
        before,
        "guard must not appear in protocol diagnostics"
    );
    let data = capabilities.ranges().as_ptr();
    let retained = capabilities.retained_capacity_bytes();
    assert_eq!(
        retained,
        capabilities.storage.ranges.capacity() * size_of::<ApiVersionRange>()
            + size_of::<CapabilityStorage>()
    );
    let allocations = allocation_counter::measure(|| {
        let first = capabilities.clone();
        let second = first.clone();
        assert_eq!(first.ranges().as_ptr(), data);
        assert_eq!(second.ranges().as_ptr(), data);
        assert_eq!(second.retained_capacity_bytes(), retained);
    });
    assert_eq!(
        allocations.count_total, 0,
        "clones never duplicate the charged ranges"
    );
    let network = MemoryNetwork::new(MemoryNetworkConfig::default()).unwrap();
    let (stream, _peer) = network.connected_pair().unwrap();
    let connected = Connected {
        driver: ConnectionDriver::new(stream, DriverConfig::default()).unwrap(),
        capabilities,
    };
    let extracted = connected.capabilities.clone();
    drop(connected);
    assert_eq!(
        Arc::strong_count(&source),
        2,
        "the extracted response owns its setup reservation"
    );
    assert_eq!(extracted.ranges().as_ptr(), data);
    drop(extracted);
    assert_eq!(Arc::strong_count(&source), 1);
}

#[test]
fn capability_guard_attachment_is_one_time_and_requires_unshared_backing() {
    let mut capabilities = ready_capabilities();
    let shared = capabilities.clone();
    let first: Arc<dyn Send + Sync> = Arc::new(());
    let returned = capabilities
        .attach_lifetime_guard(first.clone())
        .unwrap_err();
    assert!(Arc::ptr_eq(&returned, &first));
    drop(returned);
    assert_eq!(Arc::strong_count(&first), 1);
    drop(shared);
    assert!(capabilities.attach_lifetime_guard(first.clone()).is_ok());
    let second: Arc<dyn Send + Sync> = Arc::new(());
    let returned = capabilities
        .attach_lifetime_guard(second.clone())
        .unwrap_err();
    assert!(Arc::ptr_eq(&returned, &second));
    assert_eq!(
        Arc::strong_count(&first),
        2,
        "replacement cannot release the original source"
    );
    drop(returned);
    drop(capabilities);
    assert_eq!(Arc::strong_count(&first), 1);
    assert_eq!(Arc::strong_count(&second), 1);
}

#[test]
fn retained_advertisement_and_later_auth_responses_share_a_checked_owned_allowance() {
    let bytes = advertised(3, 0, &[(3, 0, 12), (18, 0, 3)]);
    let retained = ready_capabilities().retained_capacity_bytes();
    let limited = ControlCodec::new(
        "x".into(),
        ControlLimits {
            owned_bytes: retained - 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        limited.parse_api_versions(&bytes, CORRELATION, Probe::V3),
        Err(ControlError::Limit("owned bytes"))
    );
    let budget = retained + 45;
    let codec = ControlCodec::new(
        "x".into(),
        ControlLimits {
            owned_bytes: budget,
            ..Default::default()
        },
    )
    .unwrap();
    let Negotiation::Ready(capabilities) = codec
        .parse_api_versions(&bytes, CORRELATION, Probe::V3)
        .unwrap()
    else {
        panic!("advertised");
    };
    let remaining = budget
        .checked_sub(capabilities.retained_capacity_bytes())
        .unwrap();
    use wire::{sasl_authenticate_response as auth, sasl_handshake_response as hs};
    let mechanisms = ["PLAIN"];
    let handshake = frame(
        Response::SaslHandshakeResponse(hs::View::V1(hs::v1::SaslHandshakeResponse {
            mechanisms: mechanisms.as_slice().into(),
            ..Default::default()
        })),
        1,
    );
    // One returned String slot, its bytes, and one borrowed lookup slot. The
    // expectation is constructed independently of the codec budget accumulator.
    let exact = size_of::<String>() + "PLAIN".len() + size_of::<&str>();
    assert!(exact <= remaining);
    assert_eq!(
        codec.parse_sasl_handshake_with_owned_limit(
            &handshake,
            CORRELATION,
            SaslMechanism::Plain,
            exact - 1
        ),
        Err(ControlError::Limit("owned bytes"))
    );
    assert_eq!(
        codec
            .parse_sasl_handshake_with_owned_limit(
                &handshake,
                CORRELATION,
                SaslMechanism::Plain,
                exact
            )
            .unwrap()
            .mechanisms,
        ["PLAIN"]
    );
    let reply = frame(
        Response::SaslAuthenticateResponse(auth::View::V2(auth::v2::SaslAuthenticateResponse {
            auth_bytes: b"proof",
            ..Default::default()
        })),
        2,
    );
    assert_eq!(
        codec.parse_sasl_authenticate_with_owned_limit(&reply, CORRELATION, 4),
        Err(ControlError::Limit("owned bytes"))
    );
    assert_eq!(
        codec
            .parse_sasl_authenticate_with_owned_limit(&reply, CORRELATION, 5)
            .unwrap()
            .auth_bytes,
        b"proof"
    );
    assert_eq!(
        codec.parse_sasl_authenticate_with_owned_limit(&reply, CORRELATION, budget + 1),
        Err(ControlError::InvalidConfig)
    );
    assert_eq!(
        codec.parse_sasl_handshake_with_owned_limit(
            &handshake,
            CORRELATION,
            SaslMechanism::Plain,
            budget + 1
        ),
        Err(ControlError::InvalidConfig)
    );
}
