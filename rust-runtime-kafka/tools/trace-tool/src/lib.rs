//! Portable binary trace artifacts for [`kr-runtime`] simulations.
//!
//! Typed and byte-backed traces are exported as framed SBE artifacts. The
//! static runtime trace viewer validates and decodes those artifacts directly
//! in the browser. Export happens after the run so filesystem failures cannot
//! influence the simulated execution.

#![forbid(unsafe_code)]

use std::fmt;
use std::io;

use kr_runtime::trace::TRACE_SCHEMA_VERSION;

mod diff;
mod sbe_artifact;

#[cfg(test)]
mod browser_sbe_fixture;

pub use diff::{TraceArtifactDivergence, diff_sbe_trace_artifacts};
pub use sbe_artifact::{
    SBE_ARTIFACT_CONTAINER_VERSION, SBE_ARTIFACT_MAGIC, validate_sbe_trace_artifact,
    write_buffered_sbe_trace_artifact, write_sampled_buffered_sbe_trace_artifact,
    write_sampled_sbe_trace_artifact, write_sbe_trace_artifact,
};

/// Version of the binary artifact envelope.
///
/// This is intentionally independent from the diagnostic trace and runtime
/// reproduction schemas. Changing presentation metadata need not invalidate
/// either contract. Version 8 added the configured virtual start time to the
/// artifact header.
pub const TRACE_ARTIFACT_SCHEMA_VERSION: u32 = 8;

/// Diagnostic trace schema whose event variants this exporter understands.
///
/// Keep the literal explicit: changing [`TRACE_SCHEMA_VERSION`] must fail this
/// crate until every new or changed event has a stable binary rendering and
/// this value is deliberately advanced.
pub const SUPPORTED_TRACE_SCHEMA_VERSION: u32 = 5;

const _: () = assert!(SUPPORTED_TRACE_SCHEMA_VERSION == TRACE_SCHEMA_VERSION);

/// Harness metadata recorded in a trace artifact header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TraceArtifactMetadata<'a> {
    /// Stable harness name and version, such as `quarry/0.1.0`.
    pub driver: &'a str,
    /// Short terminal outcome, such as `completed`, `stalled`, or `failed`.
    pub outcome: &'a str,
}

impl<'a> TraceArtifactMetadata<'a> {
    /// Creates artifact metadata.
    #[must_use]
    pub const fn new(driver: &'a str, outcome: &'a str) -> Self {
        Self { driver, outcome }
    }
}

/// Failure while encoding, validating, or writing a binary trace artifact.
#[derive(Debug)]
#[non_exhaustive]
pub enum ExportError {
    /// Writing the artifact failed.
    Io(io::Error),
    /// Sampling metadata did not directly wrap the exported recorder.
    SamplingSinkMismatch,
    /// An input cannot be represented in the binary artifact schema.
    BinaryEncoding(&'static str),
    /// A runtime trace event cannot be represented in the binary schema.
    BinaryEventEncoding(String),
    /// A binary artifact is malformed or unsupported.
    InvalidBinaryArtifact(String),
    /// A byte-backed recorder omitted events that failed SBE encoding.
    BufferedTraceEncodingFailures(u64),
    /// Two artifacts do not describe reruns of the same experiment, so a
    /// divergence comparison would be meaningless.
    IncomparableArtifacts {
        /// The reproduction-affecting header field that differs.
        field: &'static str,
        /// The left artifact's value, rendered for diagnostics.
        left: String,
        /// The right artifact's value, rendered for diagnostics.
        right: String,
    },
}

impl fmt::Display for ExportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "could not write trace artifact: {error}"),
            Self::SamplingSinkMismatch => formatter
                .write_str("sampling metadata does not directly wrap the exported recording trace"),
            Self::BinaryEncoding(message) => {
                write!(
                    formatter,
                    "could not encode binary trace artifact: {message}"
                )
            }
            Self::BinaryEventEncoding(message) => {
                write!(formatter, "could not encode binary trace event: {message}")
            }
            Self::InvalidBinaryArtifact(message) => {
                write!(formatter, "invalid binary trace artifact: {message}")
            }
            Self::BufferedTraceEncodingFailures(count) => write!(
                formatter,
                "cannot export byte-backed trace with {count} SBE encoding failure(s)"
            ),
            Self::IncomparableArtifacts { field, left, right } => write!(
                formatter,
                "artifacts are not comparable: {field} differs (left {left}, right {right})"
            ),
        }
    }
}

impl std::error::Error for ExportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::SamplingSinkMismatch
            | Self::BinaryEncoding(_)
            | Self::BinaryEventEncoding(_)
            | Self::InvalidBinaryArtifact(_)
            | Self::BufferedTraceEncodingFailures(_)
            | Self::IncomparableArtifacts { .. } => None,
        }
    }
}
