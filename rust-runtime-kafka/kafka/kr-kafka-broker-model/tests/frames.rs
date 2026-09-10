use kr_kafka_broker_model::*;
use kr_kafka_protocol::{
    self as wire, Request, Response, errors as code,
    plan::Records,
    wire::{DecodeLimits, EncodeLimits},
};
use kr_kafka_record::{self as record, Identity};
#[path = "frames/fetch.rs"]
mod fetch;

fn cluster(version: i16) -> (BrokerModel, TopicId) {
    let mut b = BrokerModel::new(BrokerConfig {
        produce_max_version: version,
        ..Default::default()
    })
    .unwrap();
    for id in 0..2 {
        b.add_broker(BrokerEndpoint {
            id,
            host: format!("broker-{id}"),
            port: 9092,
        })
        .unwrap();
    }
    let topic = b.create_topic("events", &[0, 1]).unwrap();
    (b, topic)
}
fn frame(request: Request<'_>, version: i16) -> Vec<u8> {
    request
        .plan_frame(version, 73, Some("model-test"), EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap()
}
fn reply(action: BrokerAction) -> Vec<u8> {
    match action {
        BrokerAction::Reply(bytes) => bytes,
        _ => panic!("expected reply, got {action:?}"),
    }
}
fn init(b: &mut BrokerModel, expected: Option<Identity>) -> (i16, i64, i16) {
    use wire::init_producer_id_request::{self as api, v4::*};
    let request = frame(
        Request::InitProducerIdRequest(api::View::V4(InitProducerIdRequest {
            transactional_id: None,
            producer_id: expected.map_or(-1, |i| i.producer_id),
            producer_epoch: expected.map_or(-1, |i| i.producer_epoch),
            ..Default::default()
        })),
        4,
    );
    let bytes = reply(b.handle_frame(0, &request, FaultPlan::default()).unwrap());
    match wire::frame::decode_response(&bytes, 22, 4, 73, DecodeLimits::default())
        .unwrap()
        .body
    {
        Response::InitProducerIdResponse(wire::init_producer_id_response::View::V4(r)) => {
            (r.error_code, r.producer_id, r.producer_epoch)
        }
        _ => unreachable!(),
    }
}
fn batch(identity: Identity, tokens: &[u64], zstd: bool) -> Vec<u8> {
    let config = record::BatchConfig {
        raw_limit: 4096,
        output_limit: 4096,
        chunk_bytes: 1024,
        progressive_threshold: 0,
    };
    let mut codec =
        record::CodecPool::new(usize::from(zstd), record::ZstdConfig::default()).unwrap();
    let output = record::OutputPool::new(config.envelope_bytes() as usize).unwrap();
    let mut b = record::RecordBatchBuilder::new(
        config,
        if zstd {
            record::Compression::Zstd { level: 1 }
        } else {
            record::Compression::None
        },
        output,
    )
    .unwrap();
    for (i, token) in tokens.iter().enumerate() {
        b.push(record::OwnedRecord::copy_from(record::Record {
            timestamp: 100 + i as i64,
            key: Some(b"key"),
            value: Some(&token.to_be_bytes()),
            headers: &[record::Header {
                key: "source",
                value: Some(b"broker-model-test"),
            }],
        }))
        .unwrap();
    }
    b.request_seal().unwrap();
    while !b
        .progress(&mut codec, record::EncodeBudget::default())
        .unwrap()
        .sealed
    {}
    let b = b.take_sealed().unwrap().finalize(identity).unwrap();
    b.chunks()
        .iter()
        .flat_map(|b| b.as_slice())
        .copied()
        .collect()
}
fn produce(version: i16, topic: TopicId, partition: i32, records: &[u8]) -> Vec<u8> {
    if version == 13 {
        use wire::produce_request::{self as api, v13::*};
        let parts = [PartitionProduceData {
            index: partition,
            records: Some(Records::Borrowed(records)),
            ..Default::default()
        }];
        let topics = [TopicProduceData {
            topic_id: topic,
            partition_data: (&parts[..]).into(),
            ..Default::default()
        }];
        frame(
            Request::ProduceRequest(api::View::V13(ProduceRequest {
                acks: -1,
                transactional_id: None,
                timeout_ms: 1000,
                topic_data: (&topics[..]).into(),
                ..Default::default()
            })),
            version,
        )
    } else {
        use wire::produce_request::{self as api, v9::*};
        let parts = [PartitionProduceData {
            index: partition,
            records: Some(Records::Borrowed(records)),
            ..Default::default()
        }];
        let topics = [TopicProduceData {
            name: "events",
            partition_data: (&parts[..]).into(),
            ..Default::default()
        }];
        frame(
            Request::ProduceRequest(api::View::V9(ProduceRequest {
                acks: -1,
                transactional_id: None,
                timeout_ms: 1000,
                topic_data: (&topics[..]).into(),
                ..Default::default()
            })),
            version,
        )
    }
}
fn status(bytes: &[u8], version: i16) -> (i16, i64, i32) {
    let response =
        wire::frame::decode_response(bytes, 0, version, 73, DecodeLimits::default()).unwrap();
    match response.body {
        Response::ProduceResponse(wire::produce_response::View::V9(r)) => {
            let topic = r.responses.iter().next().unwrap().unwrap();
            let p = topic.partition_responses.iter().next().unwrap().unwrap();
            (p.error_code, p.base_offset, r.throttle_time_ms)
        }
        Response::ProduceResponse(wire::produce_response::View::V10(r)) => {
            let topic = r.responses.iter().next().unwrap().unwrap();
            let p = topic.partition_responses.iter().next().unwrap().unwrap();
            (p.error_code, p.base_offset, r.throttle_time_ms)
        }
        Response::ProduceResponse(wire::produce_response::View::V13(r)) => {
            let topic = r.responses.iter().next().unwrap().unwrap();
            let p = topic.partition_responses.iter().next().unwrap().unwrap();
            (p.error_code, p.base_offset, r.throttle_time_ms)
        }
        _ => unreachable!(),
    }
}
fn send(
    b: &mut BrokerModel,
    version: i16,
    topic: TopicId,
    identity: Identity,
    tokens: &[u64],
    fault: FaultPlan,
) -> (i16, i64, i32) {
    let req = produce(version, topic, 0, &batch(identity, tokens, true));
    let bytes = reply(b.handle_frame(0, &req, fault).unwrap());
    status(&bytes, version)
}
fn id(sequence: i32) -> Identity {
    Identity {
        producer_id: 1,
        producer_epoch: 0,
        base_sequence: sequence,
    }
}
type MetadataSummary = (i16, TopicId, Option<String>, Vec<(i32, i32, i32)>);
fn metadata(b: &mut BrokerModel, topic: TopicId, name: Option<&str>) -> MetadataSummary {
    use wire::metadata_request::{self as api, v12::*};
    let topics = [MetadataRequestTopic {
        topic_id: topic,
        name,
        ..Default::default()
    }];
    let req = frame(
        Request::MetadataRequest(api::View::V12(MetadataRequest {
            topics: Some((&topics[..]).into()),
            allow_auto_topic_creation: false,
            ..Default::default()
        })),
        12,
    );
    let bytes = reply(b.handle_frame(0, &req, FaultPlan::default()).unwrap());
    match wire::frame::decode_response(&bytes, 3, 12, 73, DecodeLimits::default())
        .unwrap()
        .body
    {
        Response::MetadataResponse(wire::metadata_response::View::V12(r)) => {
            let t = r.topics.iter().next().unwrap().unwrap();
            (
                t.error_code,
                t.topic_id,
                t.name.map(str::to_owned),
                t.partitions
                    .iter()
                    .map(|p| {
                        let p = p.unwrap();
                        (p.partition_index, p.leader_id, p.leader_epoch)
                    })
                    .collect(),
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn api_negotiation_and_nontransactional_identity_follow_real_kafka() {
    for max in [9, 13] {
        let (mut broker, _) = cluster(max);
        for version in [0, 3] {
            use wire::api_versions_request::{self as api};
            let request = if version == 0 {
                Request::ApiVersionsRequest(api::View::V0(Default::default()))
            } else {
                Request::ApiVersionsRequest(api::View::V3(Default::default()))
            };
            let bytes = reply(
                broker
                    .handle_frame(0, &frame(request, version), FaultPlan::default())
                    .unwrap(),
            );
            let response =
                wire::frame::decode_response(&bytes, 18, version, 73, DecodeLimits::default())
                    .unwrap();
            let ranges: Vec<_> = match response.body {
                Response::ApiVersionsResponse(wire::api_versions_response::View::V0(r)) => r
                    .api_keys
                    .iter()
                    .map(|a| {
                        let a = a.unwrap();
                        (a.api_key, a.min_version, a.max_version)
                    })
                    .collect(),
                Response::ApiVersionsResponse(wire::api_versions_response::View::V3(r)) => r
                    .api_keys
                    .iter()
                    .map(|a| {
                        let a = a.unwrap();
                        (a.api_key, a.min_version, a.max_version)
                    })
                    .collect(),
                _ => unreachable!(),
            };
            assert!(ranges.contains(&(0, 9, max)));
            assert!(ranges.contains(&(3, 12, 12)));
        }
        assert_eq!(init(&mut broker, None), (0, 1, 0));
        assert_eq!(init(&mut broker, Some(id(0))), (0, 2, 0));
    }
}
#[test]
fn real_frames_preserve_zstd_records_and_all_supported_response_layouts() {
    for version in 9..=13 {
        let (mut broker, topic) = cluster(13);
        assert_eq!(
            send(
                &mut broker,
                version,
                topic,
                id(0),
                &[7, 8],
                FaultPlan {
                    throttle_time_ms: 23,
                    ..Default::default()
                }
            ),
            (0, 0, 23)
        );
        let log = &broker.log()[0];
        assert_eq!(log.topic, topic);
        assert_eq!(log.records[0].value, Some(7u64.to_be_bytes().to_vec()));
        assert_eq!(log.records[1].offset, 1);
        assert_eq!(log.records[1].sequence, 1);
        assert_eq!(log.records[0].headers[0].key, "source");
    }
}
#[test]
fn five_batch_history_rejects_old_ranges_and_enforces_epoch_sequence_and_wrap() {
    for version in [9, 13] {
        let (mut b, topic) = cluster(version);
        for sequence in 0..6 {
            assert_eq!(
                send(
                    &mut b,
                    version,
                    topic,
                    id(sequence),
                    &[sequence as u64],
                    FaultPlan::default()
                )
                .0,
                0
            );
        }
        assert_eq!(
            send(
                &mut b,
                version,
                topic,
                id(1),
                &[1],
                FaultPlan {
                    duplicate_sequence_error: true,
                    ..Default::default()
                }
            )
            .0,
            code::DUPLICATE_SEQUENCE_NUMBER
        );
        assert_eq!(
            send(&mut b, version, topic, id(0), &[0], FaultPlan::default()).0,
            code::OUT_OF_ORDER_SEQUENCE_NUMBER
        );
        assert_eq!(b.log().len(), 6);
        assert_eq!(
            send(
                &mut b,
                version,
                topic,
                id(6),
                &[6],
                FaultPlan {
                    reject_before_commit: Some(code::UNKNOWN_PRODUCER_ID),
                    ..Default::default()
                }
            )
            .0,
            code::UNKNOWN_PRODUCER_ID
        );
        assert_eq!(b.log().len(), 6);
        assert_eq!(
            send(&mut b, version, topic, id(6), &[6], FaultPlan::default()).0,
            0
        );
        assert_eq!(
            send(
                &mut b,
                version,
                topic,
                Identity {
                    producer_epoch: 1,
                    ..id(1)
                },
                &[9],
                FaultPlan::default()
            )
            .0,
            code::OUT_OF_ORDER_SEQUENCE_NUMBER
        );
        assert_eq!(
            send(
                &mut b,
                version,
                topic,
                Identity {
                    producer_epoch: 1,
                    ..id(0)
                },
                &[9],
                FaultPlan::default()
            )
            .0,
            0
        );
        assert_eq!(
            send(&mut b, version, topic, id(7), &[7], FaultPlan::default()).0,
            code::INVALID_PRODUCER_EPOCH
        );
        let (mut b, topic) = cluster(version);
        assert_eq!(
            send(
                &mut b,
                version,
                topic,
                id(i32::MAX - 1),
                &[1, 2],
                FaultPlan::default()
            )
            .0,
            0
        );
        assert_eq!(
            send(&mut b, version, topic, id(0), &[3], FaultPlan::default()).0,
            0
        );
    }
}
#[test]
fn loss_at_four_fault_points_changes_commits_only_at_commit() {
    for version in [9, 13] {
        for point in 0..4 {
            let (mut b, topic) = cluster(version);
            let request = produce(version, topic, 0, &batch(id(0), &[1], true));
            let fault = match point {
                0 => FaultPlan {
                    drop_after_parse: true,
                    ..Default::default()
                },
                1 => FaultPlan {
                    reject_before_commit: Some(code::REQUEST_TIMED_OUT),
                    ..Default::default()
                },
                2 => FaultPlan {
                    drop_after_commit: true,
                    ..Default::default()
                },
                _ => FaultPlan {
                    disconnect_before_response: true,
                    ..Default::default()
                },
            };
            let action = b.handle_frame(0, &request, fault).unwrap();
            match point {
                0 => assert_eq!(action, BrokerAction::DropRequest),
                1 => assert_eq!(status(&reply(action), version).0, code::REQUEST_TIMED_OUT),
                2 => assert_eq!(
                    action,
                    BrokerAction::DropResponse {
                        committed_batches: 1
                    }
                ),
                _ => assert_eq!(
                    action,
                    BrokerAction::Disconnect {
                        committed_batches: 1
                    }
                ),
            };
            assert_eq!(b.log().len(), usize::from(point >= 2));
            let response = reply(
                b.handle_frame(
                    0,
                    &request,
                    FaultPlan {
                        duplicate_sequence_error: true,
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            assert_eq!(
                status(&response, version).0,
                if point >= 2 {
                    code::DUPLICATE_SEQUENCE_NUMBER
                } else {
                    0
                }
            );
            assert_eq!(b.log().len(), 1);
        }
    }
}
#[test]
fn metadata_identity_expansion_and_leader_hints_survive_changes() {
    let (mut b, topic) = cluster(13);
    let by_name = metadata(&mut b, [0; 16], Some("events"));
    assert_eq!(by_name.1, topic);
    assert_eq!(by_name.3, vec![(0, 0, 0), (1, 1, 0)]);
    assert_eq!(metadata(&mut b, topic, None).1, topic);
    b.add_partitions(topic, &[0]).unwrap();
    let request = produce(13, topic, 0, &batch(id(0), &[1], false));
    let response = reply(
        b.handle_frame(
            0,
            &request,
            FaultPlan {
                leader_move: Some(LeaderMove {
                    topic,
                    partition: 0,
                    broker: 1,
                }),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    assert_eq!(status(&response, 13).0, code::NOT_LEADER_OR_FOLLOWER);
    assert_eq!(
        metadata(&mut b, topic, None).3,
        vec![(0, 1, 1), (1, 1, 0), (2, 0, 0)]
    );
    let response = reply(b.handle_frame(1, &request, FaultPlan::default()).unwrap());
    assert_eq!(status(&response, 13).0, 0);
    assert_eq!(b.log()[0].topic, topic);
}
#[test]
fn adding_partitions_is_additive_and_preserves_existing_log_and_sequence_state() {
    let (mut broker, topic) = cluster(13);
    assert_eq!(
        send(
            &mut broker,
            13,
            topic,
            id(0),
            &[10, 11],
            FaultPlan::default()
        ),
        (code::NONE, 0, 0)
    );
    broker.move_leader(topic, 1, 0).unwrap();
    let original_log = broker.log().to_vec();

    // Two supplied leaders add two partitions; they are not a replacement
    // topology or a requested final partition count.
    broker.add_partitions(topic, &[1, 0]).unwrap();
    broker.add_partitions(topic, &[]).unwrap();
    assert_eq!(
        metadata(&mut broker, topic, None).3,
        vec![(0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 0, 0)]
    );
    assert_eq!(broker.log(), original_log);
    assert_eq!(broker.leader(topic, 4), Err(Error::UnknownPartition));

    // Growth must retain the old duplicate window and next sequence/offset.
    assert_eq!(
        send(
            &mut broker,
            13,
            topic,
            id(0),
            &[10, 11],
            FaultPlan::default()
        ),
        (code::NONE, 0, 0)
    );
    assert_eq!(broker.log(), original_log);
    assert_eq!(
        send(&mut broker, 13, topic, id(2), &[12], FaultPlan::default()),
        (code::NONE, 2, 0)
    );
    // The same producer starts independently at zero on a newly added partition.
    let request = produce(13, topic, 2, &batch(id(0), &[20], false));
    let response = reply(
        broker
            .handle_frame(1, &request, FaultPlan::default())
            .unwrap(),
    );
    assert_eq!(status(&response, 13), (code::NONE, 0, 0));
    assert_eq!(broker.log().last().unwrap().partition, 2);
    assert_eq!(broker.log().last().unwrap().records[0].sequence, 0);
}
#[test]
fn adding_partitions_validates_the_whole_growth_and_counts_deleted_capacity() {
    let mut broker = BrokerModel::new(BrokerConfig {
        max_partitions: 5,
        ..Default::default()
    })
    .unwrap();
    for id in 0..2 {
        broker
            .add_broker(BrokerEndpoint {
                id,
                host: format!("broker-{id}"),
                port: 9092,
            })
            .unwrap();
    }
    let topic = broker.create_topic("events", &[0, 1]).unwrap();
    let other = broker.create_topic("other", &[1]).unwrap();
    let before = metadata(&mut broker, topic, None);
    assert_eq!(
        broker.add_partitions(topic, &[0, 99]),
        Err(Error::UnknownBroker)
    );
    assert_eq!(metadata(&mut broker, topic, None), before);
    assert_eq!(
        broker.add_partitions(topic, &[0, 1, 0]),
        Err(Error::Limit("partitions"))
    );
    assert_eq!(metadata(&mut broker, topic, None), before);

    broker.add_partitions(topic, &[1, 0]).unwrap();
    assert_eq!(metadata(&mut broker, topic, None).3.len(), 4);
    broker.add_partitions(topic, &[]).unwrap();
    broker.delete_topic(other).unwrap();
    assert_eq!(
        broker.add_partitions(topic, &[0]),
        Err(Error::Limit("partitions"))
    );
    assert_eq!(broker.add_partitions(other, &[]), Err(Error::UnknownTopic));
    assert_eq!(
        broker.add_partitions([0; 16], &[]),
        Err(Error::UnknownTopic)
    );
    assert_eq!(
        metadata(&mut broker, topic, None).3,
        vec![(0, 0, 0), (1, 1, 0), (2, 1, 0), (3, 0, 0)]
    );
}
#[test]
fn recreate_demonstrates_id_safety_and_unavoidable_name_only_protocol_race() {
    for version in [9, 13] {
        let (mut b, old) = cluster(version);
        let pending = produce(version, old, 0, &batch(id(0), &[99], false));
        assert_eq!(metadata(&mut b, old, None).0, 0);
        b.delete_topic(old).unwrap();
        let successor = b.create_topic("events", &[0]).unwrap();
        assert_ne!(old, successor);
        assert_eq!(metadata(&mut b, old, None).0, code::UNKNOWN_TOPIC_ID);
        assert_eq!(metadata(&mut b, [0; 16], Some("events")).1, successor);
        let response = reply(b.handle_frame(0, &pending, FaultPlan::default()).unwrap());
        if version == 13 {
            assert_eq!(status(&response, version).0, code::UNKNOWN_TOPIC_ID);
            assert!(b.log().is_empty());
        } else {
            assert_eq!(status(&response, version).0, 0);
            assert_eq!(
                b.log()[0].topic,
                successor,
                "v9 carries no old ID; broker cannot enforce client cache identity"
            );
        }
    }
}
#[test]
fn corrupt_batches_and_capacity_limits_do_not_mutate_log_or_sequence_state() {
    let (mut b, topic) = cluster(13);
    let mut corrupt = batch(id(0), &[1], false);
    corrupt[61] ^= 1;
    let response = reply(
        b.handle_frame(0, &produce(13, topic, 0, &corrupt), FaultPlan::default())
            .unwrap(),
    );
    assert_eq!(status(&response, 13).0, code::CORRUPT_MESSAGE);
    assert!(b.log().is_empty());
    assert_eq!(
        send(&mut b, 13, topic, id(0), &[1], FaultPlan::default()).0,
        0
    );
    let mut b = BrokerModel::new(BrokerConfig {
        max_log_batches: 1,
        max_producers: 1,
        ..Default::default()
    })
    .unwrap();
    b.add_broker(BrokerEndpoint {
        id: 0,
        host: "broker".into(),
        port: 9092,
    })
    .unwrap();
    let topic = b.create_topic("events", &[0]).unwrap();
    assert_eq!(init(&mut b, None).0, 0);
    assert_eq!(init(&mut b, None).0, code::COORDINATOR_LOAD_IN_PROGRESS);
    assert_eq!(
        send(&mut b, 13, topic, id(0), &[1], FaultPlan::default()).0,
        0
    );
    assert_eq!(
        send(&mut b, 13, topic, id(1), &[2], FaultPlan::default()).0,
        code::KAFKA_STORAGE_ERROR
    );
    assert_eq!(b.log().len(), 1);
    assert_eq!(
        send(&mut b, 13, topic, id(0), &[1], FaultPlan::default()).0,
        0
    );
    assert_eq!(b.log().len(), 1);
}
#[test]
fn seeded_disconnect_campaign_has_identical_replay_and_independent_token_log() {
    fn run(seed: u64) -> (Vec<CommittedBatch>, BrokerStats) {
        let version = if seed.is_multiple_of(2) { 9 } else { 13 };
        let (mut b, topic) = cluster(version);
        let mut random = seed + 1;
        for sequence in 0..24 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let request = produce(
                version,
                topic,
                0,
                &batch(id(sequence), &[sequence as u64], random.is_multiple_of(2)),
            );
            let fault = if random.is_multiple_of(3) {
                FaultPlan {
                    drop_after_parse: true,
                    ..Default::default()
                }
            } else {
                FaultPlan {
                    drop_after_commit: true,
                    ..Default::default()
                }
            };
            let _ = b.handle_frame(0, &request, fault).unwrap();
            let bytes = reply(
                b.handle_frame(
                    0,
                    &request,
                    FaultPlan {
                        duplicate_sequence_error: true,
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            assert!([0, code::DUPLICATE_SEQUENCE_NUMBER].contains(&status(&bytes, version).0));
        }
        assert_eq!(b.log().len(), 24);
        for (expected, batch) in b.log().iter().enumerate() {
            assert_eq!(batch.records.len(), 1);
            assert_eq!(
                batch.records[0].value,
                Some((expected as u64).to_be_bytes().to_vec()),
                "seed={seed}"
            );
            assert_eq!(batch.records[0].offset, expected as i64);
        }
        (b.log().to_vec(), b.stats())
    }
    for seed in 0..32 {
        assert_eq!(run(seed), run(seed), "seed={seed}");
    }
}

#[test]
fn newer_api_versions_probes_fall_back_to_v0_without_accepting_malformed_headers() {
    use wire::api_versions_request::{self as api, v3::*};
    let (mut broker, _) = cluster(13);
    let supported = frame(
        Request::ApiVersionsRequest(api::View::V3(ApiVersionsRequest {
            client_software_name: "classic-client",
            client_software_version: "future",
            ..Default::default()
        })),
        3,
    );
    for version in [4i16, 5, i16::MAX] {
        let mut request = supported.clone();
        request[6..8].copy_from_slice(&version.to_be_bytes());
        // Unsupported bodies are deliberately not decoded. Even a future body
        // layout receives the defined negotiation response using its correlation.
        let response = reply(
            broker
                .handle_frame(0, &request, FaultPlan::default())
                .unwrap(),
        );
        let decoded =
            wire::frame::decode_response(&response, 18, 0, 73, DecodeLimits::default()).unwrap();
        let Response::ApiVersionsResponse(wire::api_versions_response::View::V0(response)) =
            decoded.body
        else {
            panic!("fallback must use response v0");
        };
        assert_eq!(response.error_code, code::UNSUPPORTED_VERSION);
        let versions = response
            .api_keys
            .iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let own = versions.iter().find(|v| v.api_key == 18).unwrap();
        assert_eq!((own.min_version, own.max_version), (0, 3));
        for cut in 0..15 {
            assert!(
                broker
                    .handle_frame(0, &request[..cut], FaultPlan::default())
                    .is_err()
            );
        }
        request[12..14].copy_from_slice(&i16::MAX.to_be_bytes());
        assert!(
            broker
                .handle_frame(0, &request, FaultPlan::default())
                .is_err()
        );
    }
    assert!(matches!(
        broker
            .handle_frame(0, &supported, FaultPlan::default())
            .unwrap(),
        BrokerAction::Reply(_)
    ));
}
