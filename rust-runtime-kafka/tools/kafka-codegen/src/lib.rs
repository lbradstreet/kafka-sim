//! Strict offline compiler for Apache Kafka's JSON protocol descriptions.
#![forbid(unsafe_code)]

pub mod schema;

pub mod emit;
pub mod fixtures;
pub mod registry;

/// The intentionally small, tested producer and stateless readback protocol surface.
/// This is independent of the schemas' maximum recognized versions.
pub const SELECTION: &[(&str, &[i16])] = &[
    ("ApiVersionsRequest", &[0, 3]),
    ("ApiVersionsResponse", &[0, 3]),
    ("MetadataRequest", &[12]),
    ("MetadataResponse", &[12]),
    ("FetchRequest", &[13]),
    ("FetchResponse", &[13]),
    ("ProduceRequest", &[9, 10, 11, 12, 13]),
    ("ProduceResponse", &[9, 10, 11, 12, 13]),
    ("InitProducerIdRequest", &[4]),
    ("InitProducerIdResponse", &[4]),
    ("SaslHandshakeRequest", &[1]),
    ("SaslHandshakeResponse", &[1]),
    ("SaslAuthenticateRequest", &[2]),
    ("SaslAuthenticateResponse", &[2]),
    ("RequestHeader", &[1, 2]),
    ("ResponseHeader", &[0, 1]),
];
