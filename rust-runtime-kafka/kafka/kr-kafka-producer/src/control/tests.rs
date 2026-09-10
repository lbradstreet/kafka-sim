use super::*;
#[test]
fn shared_metadata_client_remains_usable_when_strict_producer_profile_is_unavailable() {
    use wire::api_versions_response::{self as api, v3::*};
    let ranges = [
        ApiVersion {
            api_key: 18,
            min_version: 0,
            max_version: 3,
            ..Default::default()
        },
        ApiVersion {
            api_key: 3,
            min_version: 0,
            max_version: 12,
            ..Default::default()
        },
    ];
    let response = Response::ApiVersionsResponse(api::View::V3(ApiVersionsResponse {
        api_keys: ranges.as_slice().into(),
        ..Default::default()
    }))
    .plan_frame(3, 9, Default::default())
    .unwrap()
    .to_vec()
    .unwrap();
    let producer = ControlCodec::new("wrapper".into(), false, ControlLimits::default()).unwrap();
    let common::Negotiation::Ready(advertised) = producer
        .shared()
        .parse_api_versions(&response, 9, Probe::V3)
        .unwrap()
    else {
        panic!("shared ready");
    };
    assert!(advertised.supports(3, 12));
    assert_eq!(
        Capabilities::from_advertised(&advertised, false),
        Err(ControlError::MissingCapability {
            api_key: 0,
            required: 13
        })
    );
    assert_eq!(
        producer.parse_api_versions(&response, 9, Probe::V3),
        Err(ControlError::MissingCapability {
            api_key: 0,
            required: 13
        })
    );
    let selectors = [kr_kafka_client::types::MetadataSelector::Name("events")];
    assert_eq!(
        producer.metadata_request(9, &selectors).unwrap(),
        producer.shared().metadata_request(9, &selectors).unwrap()
    );
}
