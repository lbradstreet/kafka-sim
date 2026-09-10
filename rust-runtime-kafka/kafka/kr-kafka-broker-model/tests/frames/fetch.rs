use super::*;

fn request(
    topic: TopicId,
    partition: i32,
    offset: i64,
    max_bytes: i32,
    partition_max_bytes: i32,
    epoch: i32,
) -> Vec<u8> {
    use wire::fetch_request::{self as api, v13::*};
    let partitions = [FetchPartition {
        partition,
        fetch_offset: offset,
        partition_max_bytes,
        current_leader_epoch: epoch,
        ..Default::default()
    }];
    let topics = [FetchTopic {
        topic_id: topic,
        partitions: partitions.as_slice().into(),
        ..Default::default()
    }];
    frame(
        Request::FetchRequest(api::View::V13(FetchRequest {
            max_bytes,
            topics: topics.as_slice().into(),
            ..Default::default()
        })),
        13,
    )
}
fn data(bytes: &[u8]) -> wire::fetch_response::v13::PartitionData<'_> {
    let response = wire::frame::decode_response(bytes, 1, 13, 73, DecodeLimits::default()).unwrap();
    let Response::FetchResponse(wire::fetch_response::View::V13(response)) = response.body else {
        panic!("expected Fetch13");
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.session_id, 0);
    assert_eq!(response.responses.len(), 1);
    let topic = response.responses.iter().next().unwrap().unwrap();
    assert_eq!(topic.partitions.len(), 1);
    topic.partitions.iter().next().unwrap().unwrap()
}
fn record_bytes(data: &wire::fetch_response::v13::PartitionData<'_>) -> Vec<u8> {
    match data.records {
        Some(Records::Borrowed(bytes)) => bytes.to_vec(),
        None => Vec::new(),
        _ => panic!("decoded response must borrow bytes"),
    }
}
fn limited(config: BrokerConfig) -> (BrokerModel, TopicId) {
    let mut model = BrokerModel::new(config).unwrap();
    model
        .add_broker(BrokerEndpoint {
            id: 0,
            host: "broker".into(),
            port: 9092,
        })
        .unwrap();
    let topic = model.create_topic("events", &[0]).unwrap();
    (model, topic)
}

#[test]
fn fetch_preserves_original_batches_and_patches_only_commit_header_fields() {
    for zstd in [false, true] {
        let (mut model, topic) = cluster(13);
        let mut expected = Vec::new();
        for (sequence, tokens) in [(0, vec![10, 11]), (2, vec![20, 21, 22]), (5, vec![30])] {
            model.move_leader(topic, 0, 0).unwrap();
            let original = batch(id(sequence), &tokens, zstd);
            let response = reply(
                model
                    .handle_frame(0, &produce(13, topic, 0, &original), FaultPlan::default())
                    .unwrap(),
            );
            assert_eq!(status(&response, 13).0, 0);
            let stored = model.wire_batch(expected.len()).unwrap();
            assert_eq!(&stored[8..12], &original[8..12]);
            assert_eq!(
                &stored[16..],
                &original[16..],
                "CRC and covered bytes must be unchanged"
            );
            let inspected =
                record::inspect_batch(stored, record::BatchDecodeLimits::default()).unwrap();
            assert_eq!(inspected.header.base_offset, i64::from(sequence));
            assert_eq!(inspected.header.leader_epoch, expected.len() as i32 + 1);
            expected.push(stored.to_vec());
        }
        let response = reply(
            model
                .handle_frame(
                    0,
                    &request(topic, 0, 0, 1_000_000, 1_000_000, -1),
                    FaultPlan::default(),
                )
                .unwrap(),
        );
        let partition = data(&response);
        assert_eq!(
            (
                partition.error_code,
                partition.high_watermark,
                partition.last_stable_offset,
                partition.log_start_offset
            ),
            (0, 6, 6, 0)
        );
        let records = record_bytes(&partition);
        assert_eq!(records, expected.concat());
        let stats = record::RecordSetIter::new(&records, record::RecordSetLimits::default())
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(
            (stats.batches, stats.records, stats.next_offset),
            (3, 6, Some(6))
        );
        for offset in [2, 3, 4] {
            let response = reply(
                model
                    .handle_frame(
                        0,
                        &request(topic, 0, offset, 1, 1, -1),
                        FaultPlan::default(),
                    )
                    .unwrap(),
            );
            assert_eq!(
                record_bytes(&data(&response)),
                expected[1],
                "middle offset returns its containing full batch"
            );
        }
        let response = reply(
            model
                .handle_frame(0, &request(topic, 0, 6, 1, 1, -1), FaultPlan::default())
                .unwrap(),
        );
        assert!(record_bytes(&data(&response)).is_empty());
        assert_eq!(model.stats().committed_batches, 3);
    }
}

#[test]
fn fetch_byte_limits_allow_the_first_batch_but_enforce_total_partition_and_hard_caps() {
    let (mut model, topic) = cluster(13);
    let bytes = batch(id(0), &[1, 2], false);
    let len = bytes.len();
    for sequence in [0, 2, 4] {
        let bytes = batch(id(sequence), &[1, 2], false);
        let response = reply(
            model
                .handle_frame(0, &produce(13, topic, 0, &bytes), FaultPlan::default())
                .unwrap(),
        );
        assert_eq!(status(&response, 13).0, 0);
    }
    for (global, partition, count) in [
        (1, 1, 1),
        (len as i32 * 2, i32::MAX, 2),
        (i32::MAX, len as i32 * 2 - 1, 1),
    ] {
        let response = reply(
            model
                .handle_frame(
                    0,
                    &request(topic, 0, 0, global, partition, -1),
                    FaultPlan::default(),
                )
                .unwrap(),
        );
        assert!(response.len() <= model.config().max_frame_bytes);
        let records = record_bytes(&data(&response));
        assert_eq!(
            record::RecordSetIter::new(&records, record::RecordSetLimits::default())
                .unwrap()
                .finish()
                .unwrap()
                .batches,
            count
        );
    }
    // Fetch's metadata is larger than this actual Produce request. Retaining a
    // valid record cannot make the model silently exceed its response hard cap.
    let hard = produce(13, topic, 0, &bytes).len();
    let (mut bounded, topic) = limited(BrokerConfig {
        max_frame_bytes: hard,
        ..Default::default()
    });
    let response = reply(
        bounded
            .handle_frame(0, &produce(13, topic, 0, &bytes), FaultPlan::default())
            .unwrap(),
    );
    assert_eq!(status(&response, 13).0, 0);
    assert!(matches!(
        bounded.handle_frame(0, &request(topic, 0, 0, 1, 1, -1), FaultPlan::default()),
        Err(Error::Limit("first Fetch batch exceeds hard frame limit"))
    ));
    assert_eq!(bounded.stats().committed_batches, 1);
}

#[test]
fn wire_and_decoded_retention_are_precharged_without_consuming_rejected_sequence_or_offset() {
    for zstd in [false, true] {
        let small = batch(id(0), &[1], zstd);
        let raw = record::inspect_batch(&small, record::BatchDecodeLimits::default())
            .unwrap()
            .raw_bytes()
            .len()
            + 61;
        let limit = 2 * (raw + small.len());
        let (mut model, topic) = limited(BrokerConfig {
            max_log_bytes: limit,
            ..Default::default()
        });
        let first = produce(13, topic, 0, &small);
        assert_eq!(
            status(
                &reply(model.handle_frame(0, &first, FaultPlan::default()).unwrap()),
                13
            )
            .0,
            0
        );
        let rejected = batch(id(1), &[2, 3], zstd);
        assert_eq!(
            status(
                &reply(
                    model
                        .handle_frame(0, &produce(13, topic, 0, &rejected), FaultPlan::default())
                        .unwrap()
                ),
                13
            )
            .0,
            code::KAFKA_STORAGE_ERROR
        );
        assert_eq!(model.stats().committed_records, 1);
        let retry = batch(id(1), &[2], zstd);
        assert_eq!(
            status(
                &reply(
                    model
                        .handle_frame(0, &produce(13, topic, 0, &retry), FaultPlan::default())
                        .unwrap()
                ),
                13
            )
            .1,
            1
        );
        assert_eq!(model.stats().log_bytes, limit);
        assert_eq!(model.stats().decoded_log_bytes, 2 * raw);
        assert_eq!(model.stats().wire_log_bytes, 2 * small.len());
        assert_eq!(
            status(
                &reply(model.handle_frame(0, &first, FaultPlan::default()).unwrap()),
                13
            )
            .1,
            0
        );
        assert_eq!(
            model.stats().committed_batches,
            2,
            "deduplication cannot retain another wire copy"
        );
        assert_eq!(model.stats().log_bytes, limit);
    }
}

#[test]
fn fetch_rejects_wrong_identity_route_epoch_and_range_and_isolates_recreation() {
    let (mut model, topic) = cluster(13);
    assert_eq!(
        send(&mut model, 13, topic, id(0), &[1, 2], FaultPlan::default()).0,
        0
    );
    model.move_leader(topic, 0, 1).unwrap();
    for (broker, requested, partition, offset, epoch, error) in [
        (1, [9; 16], 0, 0, -1, code::UNKNOWN_TOPIC_ID),
        (1, topic, 9, 0, -1, code::UNKNOWN_TOPIC_OR_PARTITION),
        (0, topic, 0, 0, -1, code::NOT_LEADER_OR_FOLLOWER),
        (1, topic, 0, -1, -1, code::OFFSET_OUT_OF_RANGE),
        (1, topic, 0, 3, -1, code::OFFSET_OUT_OF_RANGE),
        (1, topic, 0, 0, 0, code::FENCED_LEADER_EPOCH),
        (1, topic, 0, 0, 2, code::UNKNOWN_LEADER_EPOCH),
    ] {
        let response = reply(
            model
                .handle_frame(
                    broker,
                    &request(requested, partition, offset, 10000, 10000, epoch),
                    FaultPlan::default(),
                )
                .unwrap(),
        );
        let partition = data(&response);
        assert_eq!(partition.error_code, error);
        assert!(record_bytes(&partition).is_empty());
    }
    let old_wire = model.wire_batch(0).unwrap().to_vec();
    model.delete_topic(topic).unwrap();
    let replacement = model.create_topic("events", &[0]).unwrap();
    assert_ne!(replacement, topic);
    for (id, error) in [(topic, code::UNKNOWN_TOPIC_ID), (replacement, 0)] {
        let response = reply(
            model
                .handle_frame(0, &request(id, 0, 0, 1, 1, -1), FaultPlan::default())
                .unwrap(),
        );
        assert_eq!(data(&response).error_code, error);
        assert!(record_bytes(&data(&response)).is_empty());
    }
    assert_eq!(model.wire_batch(0).unwrap(), old_wire);
    assert_eq!(model.stats().committed_batches, 1);
}

#[test]
fn stateless_fetch_rejects_sessions_long_poll_and_multi_partition_requests() {
    use wire::fetch_request::{self as api, v13::*};
    let (mut model, topic) = cluster(13);
    let partitions = [FetchPartition {
        partition_max_bytes: 1024,
        ..Default::default()
    }; 1];
    let topics = [FetchTopic {
        topic_id: topic,
        partitions: partitions.as_slice().into(),
        ..Default::default()
    }];
    for (session_id, max_wait_ms, min_bytes, isolation_level) in
        [(1, 0, 0, 0), (0, 1, 0, 0), (0, 0, 1, 0), (0, 0, 0, 1)]
    {
        let frame = frame(
            Request::FetchRequest(api::View::V13(FetchRequest {
                session_id,
                max_wait_ms,
                min_bytes,
                isolation_level,
                topics: topics.as_slice().into(),
                ..Default::default()
            })),
            13,
        );
        assert!(matches!(
            model.handle_frame(0, &frame, FaultPlan::default()),
            Err(Error::InvalidRequest(_))
        ));
    }
    let duplicate = [
        FetchTopic {
            topic_id: topic,
            partitions: partitions.as_slice().into(),
            ..Default::default()
        },
        FetchTopic {
            topic_id: topic,
            partitions: partitions.as_slice().into(),
            ..Default::default()
        },
    ];
    let frame = frame(
        Request::FetchRequest(api::View::V13(FetchRequest {
            topics: duplicate.as_slice().into(),
            ..Default::default()
        })),
        13,
    );
    assert!(matches!(
        model.handle_frame(0, &frame, FaultPlan::default()),
        Err(Error::InvalidRequest(_))
    ));
    assert!(model.log().is_empty());
    assert_eq!(model.stats().log_bytes, 0);
}
