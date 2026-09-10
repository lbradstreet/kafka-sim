use kr_kafka_protocol::{
    Request, Response, api_versions_request, api_versions_response, frame, metadata_request,
    plan::{EncodeLimits, Records, SharedBytes},
    produce_request, produce_response, registry, request_header,
    wire::{DecodeLimits, Error, Sequence},
};
use std::sync::Arc;

#[test]
fn flexible_request_header_keeps_classic_client_id_and_response_exception() {
    let request = Request::ApiVersionsRequest(api_versions_request::View::V3(
        api_versions_request::v3::ApiVersionsRequest {
            client_software_name: "kr",
            client_software_version: "v0",
            ..Default::default()
        },
    ));
    let bytes = request
        .plan_frame(3, 0x01020304, Some("abc"), EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap();
    // Header after the frame prefix: key, version, correlation, classic i16
    // client-id length, UTF-8 client ID, flexible header tags.
    assert_eq!(
        &bytes[4..18],
        &[0, 18, 0, 3, 1, 2, 3, 4, 0, 3, b'a', b'b', b'c', 0]
    );
    assert_eq!(
        i32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
        bytes.len() - 4
    );
    let decoded = frame::decode_request(&bytes, DecodeLimits::default()).unwrap();
    assert_eq!(decoded.correlation_id, 0x01020304);
    assert_eq!(decoded.client_id, Some("abc"));
    assert!(matches!(decoded.header, request_header::View::V2(_)));

    let response =
        Response::ApiVersionsResponse(api_versions_response::View::V3(Default::default()));
    let bytes = response
        .plan_frame(3, 0x01020304, EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap();
    assert_eq!(&bytes[4..8], &[1, 2, 3, 4]);
    let body = api_versions_response::View::V3(Default::default())
        .plan(3, EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap();
    assert_eq!(&bytes[8..], body);
    assert!(frame::decode_response(&bytes, 18, 3, 0x01020304, DecodeLimits::default()).is_ok());
    assert!(frame::decode_response(&bytes, 18, 3, 999, DecodeLimits::default()).is_err());
}

#[test]
fn produce_chunk_plans_preserve_payload_ownership_and_topic_layouts() {
    use produce_request::v13::*;
    let chunk = SharedBytes::new(Arc::from(&b"opaque-record-batch"[..]));
    let chunks = [chunk.clone()];
    let partitions = [PartitionProduceData {
        index: 7,
        records: Some(Records::Chunks(&chunks)),
        ..Default::default()
    }];
    let topic_id = [0x73; 16];
    let topics = [TopicProduceData {
        topic_id,
        partition_data: Sequence::from_slice(&partitions),
        ..Default::default()
    }];
    let request = Request::ProduceRequest(produce_request::View::V13(ProduceRequest {
        acks: -1,
        timeout_ms: 5000,
        topic_data: Sequence::from_slice(&topics),
        ..Default::default()
    }));
    let plan = request
        .plan_frame(13, 41, None, EncodeLimits::default())
        .unwrap();
    assert_eq!(plan.shared_segments().count(), 1);
    assert!(
        plan.shared_segments()
            .next()
            .unwrap()
            .shares_allocation(&chunk)
    );
    assert!(plan.metadata_len() < plan.len());
    let bytes = plan.to_vec().unwrap();
    let frame = frame::decode_request(&bytes, DecodeLimits::default()).unwrap();
    let Request::ProduceRequest(produce_request::View::V13(view)) = frame.body else {
        panic!("wrong layout")
    };
    assert_eq!(view.acks, -1);
    assert_eq!(view.timeout_ms, 5000);
    let topic = view.topic_data.iter().next().unwrap().unwrap();
    assert_eq!(topic.topic_id, topic_id);
    let partition = topic.partition_data.iter().next().unwrap().unwrap();
    assert_eq!(partition.index, 7);
    let Some(Records::Borrowed(records)) = partition.records else {
        panic!("records must borrow input")
    };
    assert_eq!(records, chunk.as_slice());
    assert!(
        records.as_ptr() >= bytes.as_ptr()
            && records.as_ptr() < bytes.as_ptr().wrapping_add(bytes.len())
    );
    assert!(
        request
            .plan_frame(12, 41, None, EncodeLimits::default())
            .is_err()
    );

    let names = [produce_request::v9::TopicProduceData {
        name: "topic",
        ..Default::default()
    }];
    let request = Request::ProduceRequest(produce_request::View::V9(
        produce_request::v9::ProduceRequest {
            topic_data: Sequence::from_slice(&names),
            ..Default::default()
        },
    ));
    for version in 9..=12 {
        assert!(
            request
                .plan_frame(version, 41, None, EncodeLimits::default())
                .is_ok()
        );
    }
    assert!(
        request
            .plan_frame(13, 41, None, EncodeLimits::default())
            .is_err()
    );
}

#[test]
fn metadata_by_name_id_null_and_empty_are_distinct() {
    use metadata_request::v12::*;
    let by_name = [MetadataRequestTopic {
        name: Some("topic"),
        ..Default::default()
    }];
    let by_id = [MetadataRequestTopic {
        topic_id: [1; 16],
        name: None,
        ..Default::default()
    }];
    let make = |topics| {
        Request::MetadataRequest(metadata_request::View::V12(MetadataRequest {
            topics,
            allow_auto_topic_creation: false,
            ..Default::default()
        }))
    };
    let inputs = [
        Some(Sequence::from_slice(&by_name)),
        Some(Sequence::from_slice(&by_id)),
        None,
        Some(Sequence::from_slice(&[])),
    ];
    let mut encodings = Vec::new();
    for topics in inputs {
        let bytes = make(topics)
            .plan_frame(12, 9, Some("producer"), EncodeLimits::default())
            .unwrap()
            .to_vec()
            .unwrap();
        let frame = frame::decode_request(&bytes, DecodeLimits::default()).unwrap();
        assert_eq!(frame.version, 12);
        assert_eq!(frame.api_key, 3);
        encodings.push(bytes);
    }
    for i in 0..encodings.len() {
        for j in i + 1..encodings.len() {
            assert_ne!(encodings[i], encodings[j]);
        }
    }
}

#[test]
fn all_frame_failures_return_no_view() {
    let response = Response::ProduceResponse(produce_response::View::V13(Default::default()));
    let bytes = response
        .plan_frame(13, 42, EncodeLimits::default())
        .unwrap()
        .to_vec()
        .unwrap();
    for end in 0..bytes.len() {
        assert!(frame::decode_response(&bytes[..end], 0, 13, 42, DecodeLimits::default()).is_err());
    }
    for prefix in [-1_i32, 0, 1, i32::MAX] {
        let mut corrupt = bytes.clone();
        corrupt[..4].copy_from_slice(&prefix.to_be_bytes());
        assert!(frame::decode_response(&corrupt, 0, 13, 42, DecodeLimits::default()).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(frame::decode_response(&trailing, 0, 13, 42, DecodeLimits::default()).is_err());
    let length = trailing.len() - 4;
    trailing[..4].copy_from_slice(&(length as i32).to_be_bytes());
    assert!(frame::decode_response(&trailing, 0, 13, 42, DecodeLimits::default()).is_err());
    assert!(
        frame::decode_response(
            &bytes,
            0,
            13,
            42,
            DecodeLimits {
                max_bytes: bytes.len() - 1,
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[test]
fn negotiation_surface_excludes_unverified_new_schema_versions() {
    assert_eq!(
        registry::api_version(0, 13).unwrap().request_header_version,
        2
    );
    assert_eq!(
        registry::api_version(17, 1).unwrap().request_header_version,
        1
    );
    for version in [0, 3] {
        assert_eq!(
            registry::api_version(18, version)
                .unwrap()
                .response_header_version,
            0
        );
    }
    let fetch = registry::api_version(1, 13).unwrap();
    assert_eq!(
        (fetch.request_header_version, fetch.response_header_version),
        (2, 1)
    );
    for (api_key, version) in [
        (1, 12),
        (1, 14),
        (18, 5),
        (3, 13),
        (22, 6),
        (0, 8),
        (0, 14),
        (99, 0),
        (-1, 0),
        (0, -1),
    ] {
        assert_eq!(
            registry::api_version(api_key, version),
            Err(Error::UnsupportedVersion { api_key, version })
        );
    }
}
