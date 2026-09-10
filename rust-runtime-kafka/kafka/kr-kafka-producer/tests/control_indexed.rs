//! Real generated frames exercise ordering and late invalid identities at the
//! configured cardinalities; the private index tests count comparisons directly.
use kr_kafka_producer::{
    config::SaslMechanism,
    control::{ControlCodec, ControlError, ControlLimits, Negotiation, Probe},
    topic::MetadataSelector,
    types::{TopicId, TopicPartition},
};
use kr_kafka_protocol::{self as wire, Request, Response};

fn codec() -> ControlCodec {
    ControlCodec::new("indexed".into(), false, ControlLimits::default()).unwrap()
}
fn id(index: usize) -> TopicId {
    TopicId(((index + 1) as u128).to_be_bytes())
}
fn frame(response: Response<'_>, version: i16) -> Vec<u8> {
    response
        .plan_frame(version, 7, Default::default())
        .unwrap()
        .to_vec()
        .unwrap()
}
fn metadata(names: &[String], order: &[usize], replicas: &[i32]) -> Vec<u8> {
    use wire::metadata_response::{self as api, v12::*};
    let brokers: Vec<_> = (0..64)
        .rev()
        .map(|node_id| MetadataResponseBroker {
            node_id,
            host: "broker",
            port: 9092,
            ..Default::default()
        })
        .collect();
    let partitions: Vec<_> = (0..2)
        .rev()
        .map(|partition_index| MetadataResponsePartition {
            partition_index,
            leader_id: 0,
            leader_epoch: 1,
            replica_nodes: replicas.into(),
            isr_nodes: replicas.into(),
            offline_replicas: (&[][..]).into(),
            ..Default::default()
        })
        .collect();
    let topics: Vec<_> = order
        .iter()
        .map(|&index| MetadataResponseTopic {
            topic_id: id(index).0,
            name: Some(&names[index]),
            partitions: (&partitions[..]).into(),
            ..Default::default()
        })
        .collect();
    frame(
        Response::MetadataResponse(api::View::V12(MetadataResponse {
            brokers: (&brokers[..]).into(),
            topics: (&topics[..]).into(),
            controller_id: 32,
            ..Default::default()
        })),
        12,
    )
}

#[test]
fn maximum_mixed_selectors_and_reversed_rows_preserve_caller_order_and_request_bytes() {
    let codec = codec();
    let names: Vec<_> = (0..1024).map(|index| format!("topic-{index}")).collect();
    let selectors: Vec<_> = (0..1024)
        .rev()
        .map(|index| {
            if index % 2 == 0 {
                MetadataSelector::Id(id(index))
            } else {
                MetadataSelector::Name(&names[index])
            }
        })
        .collect();
    let order: Vec<_> = (0..1024).map(|index| (index * 719) % 1024).collect();
    let replicas: Vec<_> = (0..8).rev().collect();
    let bytes = metadata(&names, &order, &replicas);
    let parsed = codec.parse_metadata(&bytes, 7, &selectors).unwrap();
    for (index, topic) in parsed.topics.iter().enumerate() {
        assert_eq!(topic.requested_index, index);
        assert_eq!(topic.id, id(1023 - index));
        assert_eq!(topic.name.as_deref(), Some(names[1023 - index].as_str()));
        assert_eq!(
            topic.partitions.iter().map(|p| p.index).collect::<Vec<_>>(),
            [0, 1]
        );
    }
    assert_eq!(
        parsed.brokers.iter().map(|b| b.id).collect::<Vec<_>>(),
        (0..64).rev().collect::<Vec<_>>()
    );
    let request = codec.metadata_request(7, &selectors).unwrap();
    let Request::MetadataRequest(wire::metadata_request::View::V12(request)) =
        wire::frame::decode_request(&request, Default::default())
            .unwrap()
            .body
    else {
        unreachable!()
    };
    assert!(!request.allow_auto_topic_creation);
    for (requested, selector) in request.topics.unwrap().iter().zip(&selectors) {
        let requested = requested.unwrap();
        match selector {
            MetadataSelector::Id(id) => {
                assert_eq!(requested.topic_id, id.0);
                assert_eq!(requested.name, None);
            }
            MetadataSelector::Name(name) => {
                assert_eq!(requested.topic_id, [0; 16]);
                assert_eq!(requested.name, Some(*name));
            }
        }
    }
    let mut duplicate = selectors.clone();
    duplicate[1023] = duplicate[0];
    assert_eq!(
        codec.metadata_request(7, &duplicate),
        Err(ControlError::Invalid("duplicate metadata selector"))
    );
}

#[test]
fn late_duplicate_metadata_identity_and_replica_are_rejected_without_an_update() {
    let codec = codec();
    let names: Vec<_> = (0..64).map(|index| format!("topic-{index}")).collect();
    let selectors: Vec<_> = (0..64)
        .map(|index| MetadataSelector::Id(id(index)))
        .collect();
    let mut order: Vec<_> = (0..64).collect();
    let mut replicas: Vec<_> = (0..64).rev().collect();
    order[63] = 0;
    assert_eq!(
        codec.parse_metadata(&metadata(&names, &order, &replicas), 7, &selectors),
        Err(ControlError::UnexpectedTopic)
    );
    order[63] = 63;
    replicas[63] = replicas[0];
    assert_eq!(
        codec.parse_metadata(&metadata(&names, &order, &replicas), 7, &selectors),
        Err(ControlError::Invalid("replica node"))
    );
    let ambiguous = [
        MetadataSelector::Name(&names[0]),
        MetadataSelector::Id(id(0)),
    ];
    assert_eq!(
        codec.parse_metadata(&metadata(&names, &[0, 0], &[0]), 7, &ambiguous),
        Err(ControlError::UnexpectedTopic)
    );
}

fn produce(topic_order: &[usize], partition_order: &[i32], error_indices: &[i32]) -> Vec<u8> {
    use wire::produce_response::{self as api, v13::*};
    let errors: Vec<_> = error_indices
        .iter()
        .map(|&batch_index| BatchIndexAndErrorMessage {
            batch_index,
            ..Default::default()
        })
        .collect();
    let partitions: Vec<Vec<_>> = topic_order
        .iter()
        .map(|&topic| {
            partition_order
                .iter()
                .map(|&index| PartitionProduceResponse {
                    index,
                    base_offset: (topic as i64 * 32 + i64::from(index)) * 11,
                    record_errors: if index == 0 {
                        (&errors[..]).into()
                    } else {
                        (&[][..]).into()
                    },
                    ..Default::default()
                })
                .collect()
        })
        .collect();
    let topics: Vec<_> = topic_order
        .iter()
        .zip(&partitions)
        .map(|(&topic, partitions)| TopicProduceResponse {
            topic_id: id(topic).0,
            partition_responses: (&partitions[..]).into(),
            ..Default::default()
        })
        .collect();
    frame(
        Response::ProduceResponse(api::View::V13(ProduceResponse {
            responses: (&topics[..]).into(),
            ..Default::default()
        })),
        13,
    )
}

#[test]
fn maximum_produce_permutation_preserves_expected_order_and_record_error_order() {
    let codec = codec();
    let expected: Vec<_> = (0..1024)
        .map(|index| {
            let key = (index * 719) % 1024;
            TopicPartition {
                topic: id(key / 32),
                partition: (key % 32) as i32,
            }
        })
        .collect();
    let bytes = produce(
        &(0..32).rev().collect::<Vec<_>>(),
        &(0..32).rev().collect::<Vec<_>>(),
        &[8, 3, 5],
    );
    let parsed = codec.parse_produce13(&bytes, 7, &expected).unwrap();
    for (index, partition) in parsed.partitions.iter().enumerate() {
        assert_eq!(partition.partition, expected[index]);
        assert_eq!(
            partition.base_offset,
            Some(((index * 719) % 1024) as i64 * 11)
        );
        if partition.partition.partition == 0 {
            assert_eq!(
                partition
                    .record_errors
                    .iter()
                    .map(|e| e.batch_index)
                    .collect::<Vec<_>>(),
                [8, 3, 5]
            );
        }
    }
    let mut duplicate = expected.clone();
    duplicate[1023] = duplicate[0];
    assert_eq!(
        codec.parse_produce13(&bytes, 7, &duplicate),
        Err(ControlError::Invalid("expected partition set"))
    );
    let expected = [TopicPartition {
        topic: id(0),
        partition: 0,
    }];
    assert_eq!(
        codec.parse_produce13(&produce(&[0, 0], &[0], &[]), 7, &expected),
        Err(ControlError::UnexpectedTopic)
    );
    assert_eq!(
        codec.parse_produce13(&produce(&[0], &[0], &[8, 3, 8]), 7, &expected),
        Err(ControlError::Invalid("record error index"))
    );
}

#[test]
fn reversed_api_ranges_and_sasl_names_keep_duplicate_error_classes() {
    use wire::{api_versions_response as versions, sasl_handshake_response as sasl};
    let codec = codec();
    let mut ranges: Vec<_> = (0..512)
        .rev()
        .map(|api_key| versions::v3::ApiVersion {
            api_key,
            min_version: 0,
            max_version: i16::MAX,
            ..Default::default()
        })
        .collect();
    let api_frame = |ranges: &[versions::v3::ApiVersion<'_>]| {
        frame(
            Response::ApiVersionsResponse(versions::View::V3(versions::v3::ApiVersionsResponse {
                api_keys: ranges.into(),
                ..Default::default()
            })),
            3,
        )
    };
    assert!(matches!(
        codec.parse_api_versions(&api_frame(&ranges), 7, Probe::V3),
        Ok(Negotiation::Ready(_))
    ));
    ranges[511] = ranges[0].clone();
    assert_eq!(
        codec.parse_api_versions(&api_frame(&ranges), 7, Probe::V3),
        Err(ControlError::Invalid("API ranges"))
    );
    let names: Vec<_> = (0..511).map(|i| format!("mechanism-{i}")).collect();
    let mut borrowed: Vec<_> = names
        .iter()
        .rev()
        .map(String::as_str)
        .chain(["PLAIN"])
        .collect();
    let sasl_frame = |names: &[&str]| {
        frame(
            Response::SaslHandshakeResponse(sasl::View::V1(sasl::v1::SaslHandshakeResponse {
                mechanisms: names.into(),
                ..Default::default()
            })),
            1,
        )
    };
    assert_eq!(
        codec
            .parse_sasl_handshake(&sasl_frame(&borrowed), 7, SaslMechanism::Plain)
            .unwrap()
            .mechanisms,
        borrowed
    );
    borrowed[511] = borrowed[0];
    assert_eq!(
        codec.parse_sasl_handshake(&sasl_frame(&borrowed), 7, SaslMechanism::Plain),
        Err(ControlError::Invalid("duplicate SASL mechanism"))
    );
}
