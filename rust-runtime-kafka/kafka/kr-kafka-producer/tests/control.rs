use kr_kafka_broker_model::{BrokerAction, BrokerConfig, BrokerEndpoint, BrokerModel, FaultPlan};
use kr_kafka_producer::{
    config::SaslMechanism,
    control::{ControlCodec, ControlError, ControlLimits, Negotiation, Probe},
    topic::{MetadataSelector, TopicCache},
    types::{ProducerIdentity, TopicId, TopicPartition},
};
use kr_kafka_protocol::{self as wire, Request, Response, errors as code, wire::EncodeLimits};
use kr_runtime::{RuntimeDuration, RuntimeInstant};

const CORRELATION: i32 = 73;

fn codec() -> ControlCodec {
    ControlCodec::new("control-test".into(), false, ControlLimits::default()).unwrap()
}
fn model(version: i16) -> (BrokerModel, TopicId) {
    let mut model = BrokerModel::new(BrokerConfig {
        produce_max_version: version,
        ..Default::default()
    })
    .unwrap();
    for id in 0..2 {
        model
            .add_broker(BrokerEndpoint {
                id,
                host: format!("broker-{id}"),
                port: 9092,
            })
            .unwrap();
    }
    let topic = TopicId(model.create_topic("events", &[0, 1]).unwrap());
    (model, topic)
}
fn exchange(model: &mut BrokerModel, request: &[u8]) -> Vec<u8> {
    match model
        .handle_frame(0, request, FaultPlan::default())
        .unwrap()
    {
        BrokerAction::Reply(bytes) => bytes,
        action => panic!("unexpected action {action:?}"),
    }
}
fn response(response: Response<'_>, version: i16) -> Vec<u8> {
    response
        .plan_frame(version, CORRELATION, EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap()
}
fn capabilities(version: i16, error: i16, ranges: &[(i16, i16, i16)]) -> Vec<u8> {
    use wire::api_versions_response as api;
    if version == 0 {
        let keys: Vec<_> = ranges
            .iter()
            .map(|&(api_key, min_version, max_version)| api::v0::ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect();
        response(
            Response::ApiVersionsResponse(api::View::V0(api::v0::ApiVersionsResponse {
                error_code: error,
                api_keys: (&keys[..]).into(),
                ..Default::default()
            })),
            0,
        )
    } else {
        let keys: Vec<_> = ranges
            .iter()
            .map(|&(api_key, min_version, max_version)| api::v3::ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect();
        response(
            Response::ApiVersionsResponse(api::View::V3(api::v3::ApiVersionsResponse {
                error_code: error,
                api_keys: (&keys[..]).into(),
                ..Default::default()
            })),
            3,
        )
    }
}
const REQUIRED: &[(i16, i16, i16)] = &[(18, 0, 3), (0, 0, 13), (3, 0, 12), (22, 0, 4)];

#[test]
fn negotiation_requires_topic_id_produce_and_validated_classic_probe() {
    let codec = codec();
    for version in [9, 13] {
        let (mut model, _) = model(version);
        let bytes = exchange(
            &mut model,
            &codec.api_versions_request(CORRELATION, Probe::V3).unwrap(),
        );
        let parsed = codec.parse_api_versions(&bytes, CORRELATION, Probe::V3);
        if version == 9 {
            assert_eq!(
                parsed,
                Err(ControlError::MissingCapability {
                    api_key: 0,
                    required: 13,
                })
            );
        } else {
            let Negotiation::Ready(ready) = parsed.unwrap() else {
                panic!("modern broker unexpectedly required probe");
            };
            assert_eq!(
                (ready.produce, ready.metadata, ready.init_producer_id),
                (13, 12, 4)
            );
        }
    }
    let unsupported = capabilities(0, code::UNSUPPORTED_VERSION, &[]);
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
    let classic_success = capabilities(0, 0, REQUIRED);
    assert!(
        codec
            .parse_api_versions(&classic_success, CORRELATION, Probe::V3)
            .is_err()
    );
    assert!(matches!(
        codec.parse_api_versions(&classic_success, CORRELATION, Probe::V0),
        Ok(Negotiation::Ready(_))
    ));
    let invalid_fallback = capabilities(0, code::UNSUPPORTED_VERSION, &[(0, 2, 1)]);
    assert!(
        codec
            .parse_api_versions(&invalid_fallback, CORRELATION, Probe::V3)
            .is_err()
    );
    let duplicate = capabilities(3, 0, &[(0, 0, 13), (0, 0, 13)]);
    assert_eq!(
        codec.parse_api_versions(&duplicate, CORRELATION, Probe::V3),
        Err(ControlError::Invalid("API ranges"))
    );
}

#[test]
fn metadata_binds_name_once_and_refreshes_only_the_original_id() {
    let codec = codec();
    let (mut model, id) = model(13);
    let mut cache = TopicCache::new(
        4,
        16,
        RuntimeDuration::from_nanos(10),
        RuntimeDuration::from_nanos(100),
    )
    .unwrap();
    let handle = cache.open("events", RuntimeInstant::ZERO).unwrap();
    let selector = [cache.selector(handle).unwrap()];
    assert_eq!(selector, [MetadataSelector::Name("events")]);
    let request = codec.metadata_request(CORRELATION, &selector).unwrap();
    let bytes = exchange(&mut model, &request);
    let update = codec
        .parse_metadata(&bytes, CORRELATION, &selector)
        .unwrap();
    assert_eq!(update.topics[0].id, id);
    assert_eq!(update.topics[0].partitions.len(), 2);
    let partitions: Vec<_> = update.topics[0]
        .partitions
        .iter()
        .map(|p| p.metadata)
        .collect();
    cache
        .apply(handle, id, &partitions, RuntimeInstant::ZERO)
        .unwrap();
    assert_eq!(cache.selector(handle).unwrap(), MetadataSelector::Id(id));
    model.delete_topic(id.0).unwrap();
    let replacement = TopicId(model.create_topic("events", &[1]).unwrap());
    assert_ne!(replacement, id);
    let selectors = [cache.selector(handle).unwrap()];
    let request = codec.metadata_request(CORRELATION, &selectors).unwrap();
    let bytes = exchange(&mut model, &request);
    let deleted = codec
        .parse_metadata(&bytes, CORRELATION, &selectors)
        .unwrap();
    assert_eq!(deleted.topics[0].id, id);
    assert_eq!(deleted.topics[0].error_code, code::UNKNOWN_TOPIC_ID);
    let request = codec
        .metadata_request(CORRELATION, &[MetadataSelector::Name("events")])
        .unwrap();
    let recreated = exchange(&mut model, &request);
    assert_eq!(
        codec.parse_metadata(&recreated, CORRELATION, &selectors),
        Err(ControlError::IdentityChanged)
    );
    assert_eq!(cache.get(handle).unwrap().id, Some(id));
}

#[test]
fn metadata_validates_the_entire_frame_before_any_cache_update() {
    let codec = codec();
    let (mut model, id) = model(13);
    let selectors = [MetadataSelector::Id(id)];
    let request = codec.metadata_request(CORRELATION, &selectors).unwrap();
    let bytes = exchange(&mut model, &request);
    let snapshot = codec
        .parse_metadata(&bytes, CORRELATION, &selectors)
        .unwrap();
    for end in 0..bytes.len() {
        assert!(
            codec
                .parse_metadata(&bytes[..end], CORRELATION, &selectors)
                .is_err(),
            "truncation {end}"
        );
    }
    assert!(
        codec
            .parse_metadata(&bytes, CORRELATION + 1, &selectors)
            .is_err()
    );
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(
        codec
            .parse_metadata(&trailing, CORRELATION, &selectors)
            .is_err()
    );
    assert_eq!(
        codec
            .parse_metadata(&bytes, CORRELATION, &selectors)
            .unwrap(),
        snapshot
    );

    use wire::metadata_response::{self as api, v12::*};
    let good = [MetadataResponseBroker {
        node_id: 0,
        host: "good",
        port: 9092,
        ..Default::default()
    }];
    let partitions = [MetadataResponsePartition {
        partition_index: 0,
        leader_id: 0,
        leader_epoch: 0,
        ..Default::default()
    }];
    let topics = [MetadataResponseTopic {
        name: Some("events"),
        topic_id: id.0,
        partitions: (&partitions[..]).into(),
        ..Default::default()
    }];
    let invalid = response(
        Response::MetadataResponse(api::View::V12(MetadataResponse {
            brokers: (&good[..]).into(),
            topics: (&topics[..]).into(),
            cluster_id: Some("too-long"),
            ..Default::default()
        })),
        12,
    );
    let limited = ControlCodec::new(
        "x".into(),
        false,
        ControlLimits {
            string_bytes: 6,
            ..Default::default()
        },
    )
    .unwrap();
    // This failure is after every topic and partition has been checked and owned.
    assert_eq!(
        limited.parse_metadata(&invalid, CORRELATION, &selectors),
        Err(ControlError::Limit("string bytes"))
    );
    let small = ControlCodec::new(
        "x".into(),
        false,
        ControlLimits {
            owned_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        small.parse_metadata(&bytes, CORRELATION, &selectors),
        Err(ControlError::Limit("owned bytes"))
    );
}

fn produce_response(
    id: TopicId,
    parts: &[wire::produce_response::v13::PartitionProduceResponse<'_>],
    nodes: &[wire::produce_response::v13::NodeEndpoint<'_>],
) -> Vec<u8> {
    use wire::produce_response::{self as api, v13::*};
    let topics = [TopicProduceResponse {
        topic_id: id.0,
        partition_responses: parts.into(),
        ..Default::default()
    }];
    response(
        Response::ProduceResponse(api::View::V13(ProduceResponse {
            responses: (&topics[..]).into(),
            throttle_time_ms: 25,
            node_endpoints: nodes.into(),
            ..Default::default()
        })),
        13,
    )
}

#[test]
fn produce_requires_exact_partition_set_and_validates_kip951_hints() {
    use wire::produce_response::v13::*;
    let codec = codec();
    let id = TopicId([9; 16]);
    let expected = [
        TopicPartition {
            topic: id,
            partition: 0,
        },
        TopicPartition {
            topic: id,
            partition: 1,
        },
    ];
    let errors = [BatchIndexAndErrorMessage {
        batch_index: 2,
        batch_index_error_message: Some("record rejected"),
        ..Default::default()
    }];
    let parts = [
        PartitionProduceResponse {
            index: 1,
            error_code: code::NOT_LEADER_OR_FOLLOWER,
            base_offset: -1,
            current_leader: LeaderIdAndEpoch {
                leader_id: 7,
                leader_epoch: 12,
                ..Default::default()
            },
            record_errors: (&errors[..]).into(),
            ..Default::default()
        },
        PartitionProduceResponse {
            index: 0,
            base_offset: 42,
            log_append_time_ms: 100,
            ..Default::default()
        },
    ];
    let nodes = [NodeEndpoint {
        node_id: 7,
        host: "new-leader",
        port: 9093,
        rack: Some("rack-2"),
        ..Default::default()
    }];
    let bytes = produce_response(id, &parts, &nodes);
    let update = codec
        .parse_produce13(&bytes, CORRELATION, &expected)
        .unwrap();
    assert_eq!(update.throttle_ms, 25);
    assert_eq!(update.partitions[0].partition, expected[0]);
    assert_eq!(update.partitions[0].base_offset, Some(42));
    assert_eq!(update.partitions[1].base_offset, None);
    assert_eq!(
        update.partitions[1].current_leader.unwrap().leader_epoch,
        12
    );
    assert_eq!(update.partitions[1].record_errors[0].batch_index, 2);
    assert_eq!(update.brokers[0].host, "new-leader");
    for end in 0..bytes.len() {
        assert!(
            codec
                .parse_produce13(&bytes[..end], CORRELATION, &expected)
                .is_err(),
            "truncation {end}"
        );
    }
    assert!(
        codec
            .parse_produce13(&bytes, CORRELATION + 1, &expected)
            .is_err()
    );
    assert!(
        codec
            .parse_produce13(&bytes, CORRELATION, &expected[..1])
            .is_err()
    );
    let missing = produce_response(id, &parts[..1], &nodes);
    assert_eq!(
        codec.parse_produce13(&missing, CORRELATION, &expected),
        Err(ControlError::UnexpectedPartition)
    );
    let duplicate = produce_response(id, &[parts[0].clone(), parts[0].clone()], &nodes);
    assert_eq!(
        codec.parse_produce13(&duplicate, CORRELATION, &expected),
        Err(ControlError::UnexpectedPartition)
    );
    let bad_nodes = [NodeEndpoint {
        port: 65536,
        ..nodes[0].clone()
    }];
    let late_invalid = produce_response(id, &parts, &bad_nodes);
    assert_eq!(
        codec.parse_produce13(&late_invalid, CORRELATION, &expected),
        Err(ControlError::Invalid("broker endpoint"))
    );
    let too_many = ControlCodec::new(
        "x".into(),
        false,
        ControlLimits {
            produce_partitions: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        too_many.parse_produce13(&bytes, CORRELATION, &expected),
        Err(ControlError::Limit("expected partitions"))
    );
}

#[test]
fn independent_java_batch_produce_and_retry_return_the_original_offset() {
    use wire::produce_request::{self as api, v13::*};
    let codec = codec();
    let (mut model, topic) = model(13);
    // Captured by Apache Kafka's MemoryRecords.withIdempotentRecords, not by
    // this producer's record encoder. The passive model verifies its CRC.
    let hex = include_str!("../../kr-kafka-record/tests/fixtures/java-none-0.hex").trim();
    let batch: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let partitions = [PartitionProduceData {
        index: 0,
        records: Some(wire::plan::Records::Borrowed(&batch)),
        ..Default::default()
    }];
    let topics = [TopicProduceData {
        topic_id: topic.0,
        partition_data: (&partitions[..]).into(),
        ..Default::default()
    }];
    let request = Request::ProduceRequest(api::View::V13(ProduceRequest {
        acks: -1,
        timeout_ms: 1000,
        topic_data: (&topics[..]).into(),
        ..Default::default()
    }))
    .plan_frame(
        13,
        CORRELATION,
        Some("control-test"),
        EncodeLimits::default(),
    )
    .unwrap()
    .to_vec()
    .unwrap();
    let expected = [TopicPartition {
        topic,
        partition: 0,
    }];
    for attempt in 0..2 {
        let bytes = exchange(&mut model, &request);
        let parsed = codec
            .parse_produce13(&bytes, CORRELATION, &expected)
            .unwrap();
        assert_eq!(parsed.partitions[0].error_code, 0, "attempt {attempt}");
        assert_eq!(parsed.partitions[0].base_offset, Some(0));
        assert_eq!(model.log().len(), 1);
    }
    let BrokerAction::Reply(bytes) = model
        .handle_frame(
            0,
            &request,
            FaultPlan {
                duplicate_sequence_error: true,
                ..Default::default()
            },
        )
        .unwrap()
    else {
        panic!("duplicate response missing");
    };
    let parsed = codec
        .parse_produce13(&bytes, CORRELATION, &expected)
        .unwrap();
    assert_eq!(
        parsed.partitions[0].error_code,
        code::DUPLICATE_SEQUENCE_NUMBER
    );
    assert_eq!(parsed.partitions[0].base_offset, None);
    assert_eq!(model.log().len(), 1);
}

#[test]
fn identity_refresh_accepts_broker_allocated_fresh_pid_epoch_zero() {
    let codec = codec();
    let (mut model, _) = model(13);
    let bytes = exchange(
        &mut model,
        &codec.init_producer_id_request(CORRELATION, None).unwrap(),
    );
    let first = codec
        .parse_identity(&bytes, CORRELATION)
        .unwrap()
        .identity
        .unwrap();
    let bytes = exchange(
        &mut model,
        &codec
            .init_producer_id_request(CORRELATION, Some(first))
            .unwrap(),
    );
    let second = codec
        .parse_identity(&bytes, CORRELATION)
        .unwrap()
        .identity
        .unwrap();
    assert_ne!(first.producer_id, second.producer_id);
    assert_eq!((first.epoch, second.epoch), (0, 0));
    use wire::init_producer_id_response::{self as api, v4::*};
    let invalid = response(
        Response::InitProducerIdResponse(api::View::V4(InitProducerIdResponse {
            producer_id: -1,
            producer_epoch: 0,
            ..Default::default()
        })),
        4,
    );
    assert_eq!(
        codec.parse_identity(&invalid, CORRELATION),
        Err(ControlError::Invalid("producer identity"))
    );
    let rejected = response(
        Response::InitProducerIdResponse(api::View::V4(InitProducerIdResponse {
            error_code: code::CLUSTER_AUTHORIZATION_FAILED,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        })),
        4,
    );
    assert_eq!(
        codec
            .parse_identity(&rejected, CORRELATION)
            .unwrap()
            .identity,
        None
    );
    assert!(
        codec
            .init_producer_id_request(
                CORRELATION,
                Some(ProducerIdentity {
                    producer_id: -1,
                    epoch: 0
                })
            )
            .is_err()
    );
}

#[test]
fn sasl_capability_handshake_and_authentication_are_bounded() {
    let codec = ControlCodec::new("sasl".into(), true, ControlLimits::default()).unwrap();
    let missing = capabilities(3, 0, REQUIRED);
    assert_eq!(
        codec.parse_api_versions(&missing, CORRELATION, Probe::V3),
        Err(ControlError::MissingCapability {
            api_key: 17,
            required: 1
        })
    );
    let ranges: Vec<_> = REQUIRED
        .iter()
        .copied()
        .chain([(17, 0, 1), (36, 0, 2)])
        .collect();
    let ready = capabilities(3, 0, &ranges);
    assert!(matches!(
        codec.parse_api_versions(&ready, CORRELATION, Probe::V3),
        Ok(Negotiation::Ready(_))
    ));
    use wire::sasl_handshake_response::{self as handshake, v1::*};
    let mechanisms = ["PLAIN", "SCRAM-SHA-256"];
    let reply = response(
        Response::SaslHandshakeResponse(handshake::View::V1(SaslHandshakeResponse {
            mechanisms: (&mechanisms[..]).into(),
            ..Default::default()
        })),
        1,
    );
    codec
        .parse_sasl_handshake(&reply, CORRELATION, SaslMechanism::Plain)
        .unwrap();
    assert_eq!(
        codec.parse_sasl_handshake(&reply, CORRELATION, SaslMechanism::ScramSha512),
        Err(ControlError::UnsupportedMechanism)
    );
    for mechanism in [
        SaslMechanism::Plain,
        SaslMechanism::ScramSha256,
        SaslMechanism::ScramSha512,
    ] {
        let bytes = codec
            .sasl_handshake_request(CORRELATION, mechanism)
            .unwrap();
        let request = wire::frame::decode_request(&bytes, Default::default()).unwrap();
        assert!(matches!(request.body, Request::SaslHandshakeRequest(_)));
    }
    use wire::sasl_authenticate_response::{self as auth, v2::*};
    let reply = response(
        Response::SaslAuthenticateResponse(auth::View::V2(SaslAuthenticateResponse {
            auth_bytes: b"super-secret",
            error_message: Some("also-secret"),
            session_lifetime_ms: 500,
            ..Default::default()
        })),
        2,
    );
    let parsed = codec.parse_sasl_authenticate(&reply, CORRELATION).unwrap();
    assert_eq!(parsed.auth_bytes, b"super-secret");
    assert_eq!(parsed.session_lifetime_ms, 500);
    let debug = format!("{parsed:?}");
    assert!(!debug.contains("super-secret"));
    assert!(!debug.contains("also-secret"));
    let limited = ControlCodec::new(
        "x".into(),
        true,
        ControlLimits {
            auth_bytes: 5,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        limited.parse_sasl_authenticate(&reply, CORRELATION),
        Err(ControlError::Limit("auth bytes"))
    );
    assert_eq!(
        limited.sasl_authenticate_request(CORRELATION, b"secret"),
        Err(ControlError::Limit("auth bytes"))
    );
}

#[test]
fn seeded_response_corruption_never_exposes_an_unexpected_identity_set() {
    use wire::produce_response::v13::*;
    let codec = codec();
    let topic = TopicId([3; 16]);
    let expected = [TopicPartition {
        topic,
        partition: 0,
    }];
    let original = produce_response(topic, &[PartitionProduceResponse::default()], &[]);
    let mut seed = 0x413a_9dbf_1742_u64;
    for case in 0..2048 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let mut bytes = original.clone();
        let index = seed as usize % bytes.len();
        bytes[index] ^= (seed >> 32) as u8 | 1;
        let result = codec.parse_produce13(&bytes, CORRELATION, &expected);
        assert_eq!(
            result,
            codec.parse_produce13(&bytes, CORRELATION, &expected),
            "case {case}"
        );
        if let Ok(update) = result {
            assert_eq!(update.partitions.len(), 1);
            assert_eq!(update.partitions[0].partition, expected[0]);
            if update.partitions[0].error_code == 0 {
                assert!(update.partitions[0].base_offset.unwrap() >= 0);
            }
        }
    }
}

#[test]
fn apache_restart_frame_with_unknown_leader_and_known_epoch_remains_retryable() {
    // Produced by kafka-clients-4.3.0.jar ProduceResponse.serializeWithHeader:
    // version13, correlation73, UUID[9;16], partition0, NOT_LEADER_OR_FOLLOWER,
    // CurrentLeader(-1,8). KafkaApis.scala getCurrentLeader reads the optional
    // leader ID independently of its known epoch during broker startup.
    let hex = "000000490000004900020909090909090909090909090909090902000000000006ffffffffffffffffffffffffffffffffffffffffffffffff0100010009ffffffff0000000800000000000000";
    let bytes: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let expected = [TopicPartition {
        topic: TopicId([9; 16]),
        partition: 0,
    }];
    let update = codec()
        .parse_produce13(&bytes, CORRELATION, &expected)
        .unwrap();
    assert_eq!(update.partitions.len(), 1);
    let row = &update.partitions[0];
    assert_eq!(row.partition, expected[0]);
    assert_eq!(row.error_code, code::NOT_LEADER_OR_FOLLOWER);
    assert_eq!(
        code::classify(row.error_code),
        code::ErrorClass::RefreshMetadata
    );
    assert_eq!(row.current_leader, None, "never route to an unknown broker");
    assert_eq!(row.base_offset, None);
}

#[test]
fn partial_kip951_hints_are_ignored_but_invalid_negative_components_are_rejected() {
    use wire::produce_response::v13::*;
    let codec = codec();
    let id = TopicId([3; 16]);
    let expected = [TopicPartition {
        topic: id,
        partition: 0,
    }];
    for (leader, epoch) in [
        (-1, -1),
        (-1, 0),
        (-1, 8),
        (-1, i32::MAX),
        (0, -1),
        (7, -1),
        (i32::MAX, -1),
    ] {
        let rows = [PartitionProduceResponse {
            error_code: code::NOT_LEADER_OR_FOLLOWER,
            base_offset: -1,
            current_leader: LeaderIdAndEpoch {
                leader_id: leader,
                leader_epoch: epoch,
                ..Default::default()
            },
            ..Default::default()
        }];
        let frame = produce_response(id, &rows, &[]);
        let update = codec
            .parse_produce13(&frame, CORRELATION, &expected)
            .unwrap();
        assert_eq!(
            update.partitions[0].current_leader, None,
            "({leader},{epoch})"
        );
    }
    for (leader, epoch) in [
        (-2, -1),
        (-1, -2),
        (-2, 8),
        (7, -2),
        (i32::MIN, i32::MAX),
        (i32::MAX, i32::MIN),
    ] {
        let rows = [PartitionProduceResponse {
            error_code: code::NOT_LEADER_OR_FOLLOWER,
            base_offset: -1,
            current_leader: LeaderIdAndEpoch {
                leader_id: leader,
                leader_epoch: epoch,
                ..Default::default()
            },
            ..Default::default()
        }];
        let frame = produce_response(id, &rows, &[]);
        assert_eq!(
            codec.parse_produce13(&frame, CORRELATION, &expected),
            Err(ControlError::Invalid("current leader hint")),
            "({leader},{epoch})"
        );
    }
}
