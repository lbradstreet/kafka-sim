use super::*;
use crate::{
    control::{ControlLimits, Negotiation, Probe},
    types::TopicId,
};
use kr_kafka_protocol::{
    api_versions_response as versions,
    fetch_response::v13 as api,
    frame::decode_request,
    wire::{DecodeLimits, EncodeLimits, Error as WireError},
};

const CORRELATION: i32 = 71;
fn codec() -> ControlCodec {
    ControlCodec::new("readback".into(), ControlLimits::default()).unwrap()
}
fn request() -> FetchRequest {
    FetchRequest {
        partition: TopicPartition {
            topic: TopicId([7; 16]),
            partition: 2,
        },
        offset: 9_007_199_254_740_993,
        current_leader_epoch: 17,
        max_bytes: 128 * 1024,
    }
}
fn advertised(codec: &ControlCodec, max: i16) -> Capabilities {
    let keys = [versions::v3::ApiVersion {
        api_key: 1,
        min_version: 4,
        max_version: max,
        ..Default::default()
    }];
    let response =
        Response::ApiVersionsResponse(versions::View::V3(versions::v3::ApiVersionsResponse {
            api_keys: Sequence::from_slice(&keys),
            ..Default::default()
        }))
        .plan_frame(3, 3, EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap();
    match codec.parse_api_versions(&response, 3, Probe::V3).unwrap() {
        Negotiation::Ready(capabilities) => capabilities,
        _ => panic!("successful advertisement"),
    }
}
fn frame(
    topics: &[api::FetchableTopicResponse<'_>],
    error_code: i16,
    session_id: i32,
    throttle: i32,
) -> Vec<u8> {
    Response::FetchResponse(fetch_response::View::V13(api::FetchResponse {
        responses: Sequence::from_slice(topics),
        error_code,
        session_id,
        throttle_time_ms: throttle,
        ..Default::default()
    }))
    .plan_frame(13, CORRELATION, EncodeLimits::default())
    .unwrap()
    .to_vec()
    .unwrap()
}
fn partition_frame(partition: api::PartitionData<'_>) -> Vec<u8> {
    let partitions = [partition];
    let topics = [api::FetchableTopicResponse {
        topic_id: request().partition.topic.0,
        partitions: Sequence::from_slice(&partitions),
        ..Default::default()
    }];
    frame(&topics, 0, 0, 0)
}
fn partition() -> api::PartitionData<'static> {
    api::PartitionData {
        partition_index: 2,
        high_watermark: i64::MAX,
        log_start_offset: 0,
        ..Default::default()
    }
}
#[test]
fn request_requires_only_fetch13_and_encodes_fixed_stateless_fields() {
    let codec = codec();
    let capabilities = advertised(&codec, 13);
    assert!(!capabilities.supports(0, 13));
    assert!(!capabilities.supports(22, 4));
    let bytes = codec
        .fetch13_request(CORRELATION, &capabilities, request())
        .unwrap();
    let frame = decode_request(&bytes, DecodeLimits::default()).unwrap();
    assert_eq!(
        (frame.api_key, frame.version, frame.correlation_id),
        (1, 13, CORRELATION)
    );
    assert_eq!(frame.client_id, Some("readback"));
    let Request::FetchRequest(fetch_request::View::V13(body)) = frame.body else {
        panic!("Fetch13")
    };
    assert_eq!(
        (
            body.replica_id,
            body.max_wait_ms,
            body.min_bytes,
            body.isolation_level
        ),
        (-1, 0, 0, 0)
    );
    assert_eq!((body.session_id, body.session_epoch), (0, -1));
    assert_eq!(body.max_bytes, request().max_bytes as i32);
    assert_eq!(body.cluster_id, None);
    assert_eq!(body.rack_id, "");
    assert!(body.forgotten_topics_data.is_empty());
    assert_eq!(body.topics.len(), 1);
    let topic = body.topics.iter().next().unwrap().unwrap();
    assert_eq!(topic.topic_id, request().partition.topic.0);
    assert_eq!(topic.partitions.len(), 1);
    let part = topic.partitions.iter().next().unwrap().unwrap();
    assert_eq!(
        (part.partition, part.current_leader_epoch, part.fetch_offset),
        (2, 17, request().offset)
    );
    assert_eq!((part.last_fetched_epoch, part.log_start_offset), (-1, -1));
    assert_eq!(part.partition_max_bytes, request().max_bytes as i32);
    assert!(matches!(
        codec.fetch13_request(CORRELATION, &advertised(&codec, 12), request()),
        Err(ControlError::MissingCapability {
            api_key: 1,
            required: 13
        })
    ));
}
#[test]
fn malformed_expected_request_and_encoding_limits_fail_before_a_frame_is_returned() {
    let codec = codec();
    let capabilities = advertised(&codec, 13);
    for field in 0..6 {
        let mut invalid = request();
        match field {
            0 => invalid.partition.topic = TopicId::ZERO,
            1 => invalid.partition.partition = -1,
            2 => invalid.offset = -1,
            3 => invalid.current_leader_epoch = -2,
            4 => invalid.max_bytes = 0,
            _ => invalid.max_bytes = i32::MAX as u32 + 1,
        }
        assert!(
            codec
                .fetch13_request(CORRELATION, &capabilities, invalid)
                .is_err()
        );
        assert!(codec.parse_fetch13(&[], CORRELATION, invalid).is_err());
    }
    let tiny = ControlCodec::new(
        "r".into(),
        ControlLimits {
            frame_bytes: 16,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        tiny.fetch13_request(CORRELATION, &capabilities, request())
            .is_err()
    );
    let tiny = ControlCodec::new(
        "r".into(),
        ControlLimits {
            owned_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        tiny.fetch13_request(CORRELATION, &capabilities, request())
            .is_err()
    );
}
#[test]
fn opaque_records_borrow_frame_without_allocation_and_keep_null_empty_distinct() {
    let codec = codec();
    for payload in [None, Some(&b""[..]), Some(&b"\xff\x00\x80"[..])] {
        let bytes = partition_frame(api::PartitionData {
            records: payload.map(Records::Borrowed),
            ..partition()
        });
        let allocation = allocation_counter::measure(|| {
            let parsed = codec.parse_fetch13(&bytes, CORRELATION, request()).unwrap();
            assert_eq!(parsed.records, payload);
            if let Some(records) = parsed.records {
                let base = bytes.as_ptr() as usize;
                assert!((base..=base + bytes.len()).contains(&(records.as_ptr() as usize)));
                assert!(records.as_ptr() as usize + records.len() <= base + bytes.len());
            }
        });
        assert_eq!(allocation.count_total, 0);
    }
}
#[test]
fn identity_cardinality_correlation_and_entire_frame_are_checked_atomically() {
    let codec = codec();
    let partitions = [partition(), partition()];
    for count in [0, 2] {
        let topics = [api::FetchableTopicResponse {
            topic_id: request().partition.topic.0,
            partitions: Sequence::from_slice(&partitions[..count]),
            ..Default::default()
        }];
        assert!(matches!(
            codec.parse_fetch13(&frame(&topics, 0, 0, 0), CORRELATION, request()),
            Err(ControlError::UnexpectedPartition)
        ));
    }
    let topics = [api::FetchableTopicResponse {
        topic_id: request().partition.topic.0,
        partitions: Sequence::from_slice(&partitions[..1]),
        ..Default::default()
    }; 1];
    let doubled = [topics[0].clone(), topics[0].clone()];
    for topics in [&[][..], &doubled[..]] {
        assert!(matches!(
            codec.parse_fetch13(&frame(topics, 0, 0, 0), CORRELATION, request()),
            Err(ControlError::UnexpectedTopic)
        ));
    }
    let foreign = [api::FetchableTopicResponse {
        topic_id: [8; 16],
        partitions: Sequence::from_slice(&partitions[..1]),
        ..Default::default()
    }];
    assert!(matches!(
        codec.parse_fetch13(&frame(&foreign, 0, 0, 0), CORRELATION, request()),
        Err(ControlError::IdentityChanged)
    ));
    assert!(matches!(
        codec.parse_fetch13(
            &partition_frame(api::PartitionData {
                partition_index: 3,
                ..partition()
            }),
            CORRELATION,
            request()
        ),
        Err(ControlError::UnexpectedPartition)
    ));
    let bytes = frame(&topics, 0, 0, 0);
    assert!(matches!(
        codec.parse_fetch13(&bytes, CORRELATION + 1, request()),
        Err(ControlError::Wire(WireError::CorrelationMismatch { .. }))
    ));
    for length in 0..bytes.len() {
        assert!(
            codec
                .parse_fetch13(&bytes[..length], CORRELATION, request())
                .is_err()
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(
        codec
            .parse_fetch13(&trailing, CORRELATION, request())
            .is_err()
    );
    let declared = (trailing.len() - 4) as i32;
    trailing[..4].copy_from_slice(&declared.to_be_bytes());
    assert!(
        codec
            .parse_fetch13(&trailing, CORRELATION, request())
            .is_err()
    );
}
#[test]
fn broker_errors_throttle_and_partial_leader_hints_remain_explicit() {
    let codec = codec();
    assert!(matches!(
        codec.parse_fetch13(&frame(&[], 70, 0, 0), CORRELATION, request()),
        Err(ControlError::Broker {
            api_key: 1,
            error_code: 70
        })
    ));
    let bytes = partition_frame(api::PartitionData {
        error_code: 100,
        high_watermark: -1,
        log_start_offset: -1,
        current_leader: api::LeaderIdAndEpoch {
            leader_id: -1,
            leader_epoch: 19,
            ..Default::default()
        },
        ..partition()
    });
    let parsed = codec.parse_fetch13(&bytes, CORRELATION, request()).unwrap();
    assert_eq!(
        (
            parsed.error_code,
            parsed.records,
            parsed.current_leader,
            parsed.current_leader_epoch
        ),
        (100, None, None, Some(19))
    );
    assert_eq!(parsed.high_watermark, -1);
    let partitions = [partition()];
    let topics = [api::FetchableTopicResponse {
        topic_id: request().partition.topic.0,
        partitions: Sequence::from_slice(&partitions),
        ..Default::default()
    }];
    assert_eq!(
        codec
            .parse_fetch13(&frame(&topics, 0, 0, 17), CORRELATION, request())
            .unwrap()
            .throttle_ms,
        17
    );
    for (session, throttle) in [(1, 0), (-1, 0), (0, -1)] {
        assert!(
            codec
                .parse_fetch13(
                    &frame(&topics, 0, session, throttle),
                    CORRELATION,
                    request()
                )
                .is_err()
        );
    }
    for field in 0..10 {
        let mut part = partition();
        match field {
            0 => part.high_watermark = -1,
            1 => part.last_stable_offset = -2,
            2 => part.log_start_offset = -2,
            3 => part.current_leader.leader_id = -2,
            4 => part.current_leader.leader_epoch = -2,
            5 => part.preferred_read_replica = -2,
            6 => part.diverging_epoch.epoch = -2,
            7 => part.diverging_epoch.end_offset = -2,
            8 => part.snapshot_id.epoch = -2,
            _ => part.snapshot_id.end_offset = -2,
        }
        assert!(
            codec
                .parse_fetch13(&partition_frame(part), CORRELATION, request())
                .is_err()
        );
    }
}
#[test]
fn wire_limit_is_hard_even_when_kafka_returns_a_first_batch_above_requested_maximum() {
    let codec = codec();
    let request = FetchRequest {
        max_bytes: 1,
        ..request()
    };
    let payload = [3; 128];
    let bytes = partition_frame(api::PartitionData {
        records: Some(Records::Borrowed(&payload)),
        ..partition()
    });
    assert_eq!(
        codec
            .parse_fetch13(&bytes, CORRELATION, request)
            .unwrap()
            .records,
        Some(payload.as_slice())
    );
    let small = ControlCodec::new(
        "r".into(),
        ControlLimits {
            frame_bytes: bytes.len() - 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(small.parse_fetch13(&bytes, CORRELATION, request).is_err());
    let arrays = ControlCodec::new(
        "r".into(),
        ControlLimits {
            max_array_elements: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(arrays.parse_fetch13(&bytes, CORRELATION, request).is_err());
}
