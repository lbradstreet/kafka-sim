//! Linux production host for the provider-neutral disk-backed record ring.
//!
//! The exact [`kr_runtime_ring::file::FileRingDriver`] exercised against
//! deterministic storage runs here over `UringFile`. A dedicated thread drives
//! the ring state machine, while the file provider owns the kernel ring on its
//! own bounded actor thread. Kernel completions never enter a simulation
//! runtime.

#![forbid(unsafe_code)]

use std::fmt;
use std::time::Duration;

use kr_runtime_ring::RingLimits;
use kr_runtime_ring::file::FileRingConfig;
#[cfg(any(target_os = "linux", test))]
use kr_runtime_ring::file::FileRingOpenError;

#[cfg(target_os = "linux")]
mod driver;

#[cfg(target_os = "linux")]
pub use driver::{UringOperation, UringRing};

/// Fixed logical, physical, actor, and kernel-facing limits for one ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringRingConfig {
    /// Provider-neutral record, batch, read, and retained-resource bounds.
    pub limits: RingLimits,
    /// Logical bytes in the circular data area, excluding format metadata.
    pub data_capacity_bytes: u64,
    /// Maximum bytes placed in one lower-level positional I/O request.
    pub max_io_request_bytes: usize,
    /// Maximum commands admitted by each bounded actor.
    pub command_queue_capacity: usize,
    /// Submission/completion entries allocated for the file provider's ring.
    pub ring_entries: u32,
    /// Maximum bytes submitted in one read or write SQE.
    ///
    /// Small values are useful for exercising partial-I/O behavior.
    pub max_io_chunk_bytes: usize,
    /// Maximum wall-clock time allowed for host initialization and recovery.
    pub startup_timeout: Duration,
    /// Maximum wall-clock time an explicit final close waits for actor exit.
    ///
    /// Timing out detaches the host thread; it retains the file lock and owned
    /// buffers until the underlying kernel operation eventually terminalizes.
    pub shutdown_timeout: Duration,
}

impl Default for UringRingConfig {
    fn default() -> Self {
        let file = FileRingConfig::default();
        Self {
            limits: file.limits,
            data_capacity_bytes: file.data_capacity_bytes,
            max_io_request_bytes: file.max_io_request_bytes,
            command_queue_capacity: file.command_queue_capacity,
            ring_entries: 8,
            max_io_chunk_bytes: 64 * 1_024,
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

/// Failure to create, lock, validate, or recover an io_uring-hosted ring.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UringRingOpenError {
    /// The current compilation target is not Linux.
    UnsupportedPlatform,
    /// A fixed logical, format, actor, or kernel-facing bound is invalid.
    InvalidConfig {
        field: &'static str,
        message: String,
    },
    /// Another live file session holds the advisory writer lock.
    AlreadyLocked,
    /// An OS operation or io_uring setup failed.
    Io {
        action: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
    /// The exact-length file or one of its complete structures is invalid.
    Corrupt { offset: u64, message: String },
    /// The dedicated ring host terminated unexpectedly.
    DriverStopped,
    /// A bounded production-host lifecycle wait expired.
    TimedOut { phase: &'static str },
}

impl fmt::Display for UringRingOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => formatter.write_str("io_uring requires Linux"),
            Self::InvalidConfig { field, message } => {
                write!(formatter, "invalid io_uring ring {field}: {message}")
            }
            Self::AlreadyLocked => formatter.write_str("ring file already has a live writer"),
            Self::Io {
                action,
                raw_os_error,
                message,
            } => {
                write!(formatter, "could not {action}")?;
                if let Some(code) = raw_os_error {
                    write!(formatter, " (OS error {code})")?;
                }
                write!(formatter, ": {message}")
            }
            Self::Corrupt { offset, message } => {
                write!(formatter, "corrupt ring at byte {offset}: {message}")
            }
            Self::DriverStopped => formatter.write_str("io_uring ring driver stopped"),
            Self::TimedOut { phase } => write!(formatter, "io_uring ring {phase} timed out"),
        }
    }
}

impl std::error::Error for UringRingOpenError {}

#[cfg(target_os = "linux")]
impl UringRingOpenError {
    fn io(action: &'static str, error: std::io::Error) -> Self {
        Self::Io {
            action,
            raw_os_error: error.raw_os_error(),
            message: error.to_string(),
        }
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DerivedRingConfig {
    ring: FileRingConfig,
    physical_file_bytes: u64,
}

#[cfg(any(target_os = "linux", test))]
fn validate_and_derive_config(
    config: UringRingConfig,
) -> Result<DerivedRingConfig, UringRingOpenError> {
    let ring = FileRingConfig {
        limits: config.limits,
        data_capacity_bytes: config.data_capacity_bytes,
        max_io_request_bytes: config.max_io_request_bytes,
        command_queue_capacity: config.command_queue_capacity,
    };
    ring.validate().map_err(map_file_ring_open_error)?;
    let physical_file_bytes = ring
        .physical_file_bytes()
        .map_err(map_file_ring_open_error)?;
    if physical_file_bytes > i64::MAX as u64 {
        return invalid_config(
            "data_capacity_bytes",
            format!(
                "physical file length {physical_file_bytes} exceeds the kernel offset limit {}",
                i64::MAX
            ),
        );
    }
    if config.ring_entries < 4 || !config.ring_entries.is_power_of_two() {
        return invalid_config(
            "ring_entries",
            "must be a power of two and at least 4 for multiple queued I/O operations",
        );
    }
    if config.max_io_chunk_bytes == 0 || config.max_io_chunk_bytes > u32::MAX as usize {
        return invalid_config("max_io_chunk_bytes", format!("must be in 1..={}", u32::MAX));
    }
    if config.startup_timeout.is_zero() {
        return invalid_config("startup_timeout", "must be nonzero");
    }
    if config.shutdown_timeout.is_zero() {
        return invalid_config("shutdown_timeout", "must be nonzero");
    }
    Ok(DerivedRingConfig {
        ring,
        physical_file_bytes,
    })
}

#[cfg(any(target_os = "linux", test))]
fn invalid_config<T>(
    field: &'static str,
    message: impl Into<String>,
) -> Result<T, UringRingOpenError> {
    Err(UringRingOpenError::InvalidConfig {
        field,
        message: message.into(),
    })
}

#[cfg(any(target_os = "linux", test))]
fn map_file_ring_open_error(error: FileRingOpenError) -> UringRingOpenError {
    match error {
        FileRingOpenError::InvalidConfig { field, message } => {
            UringRingOpenError::InvalidConfig { field, message }
        }
        FileRingOpenError::Storage { action, error } => {
            #[cfg(target_os = "linux")]
            let raw_os_error = match &error {
                kr_runtime_io::StorageError::Backend { raw_os_error, .. } => *raw_os_error,
                _ => None,
            };
            #[cfg(not(target_os = "linux"))]
            let raw_os_error = None;
            UringRingOpenError::Io {
                action,
                raw_os_error,
                message: error.to_string(),
            }
        }
        FileRingOpenError::Corrupt { offset, message } => {
            UringRingOpenError::Corrupt { offset, message }
        }
        // Spawn failures and any future open-error variants mean the ring's
        // dedicated driver never became usable.
        _ => UringRingOpenError::DriverStopped,
    }
}

#[cfg(not(target_os = "linux"))]
mod unsupported {
    use std::future::{Ready, ready};
    use std::path::Path;

    use kr_runtime::{CompletionError, CompletionResult};
    use kr_runtime_ring::{
        AppendFailure, AppendRequest, AppendSuccess, ReadPage, ReadRequest, RingCursor, RingError,
        RingReader, RingStatus, RingWriter, SyncFailure, SyncSuccess, TrimSuccess,
    };

    use super::{UringRingConfig, UringRingOpenError};

    /// Completed placeholder operation used by the unsupported backend.
    pub type UringOperation<T> = Ready<T>;

    /// Unconstructable production-ring placeholder on non-Linux targets.
    #[derive(Clone)]
    pub struct UringRing {
        _private: (),
    }

    impl UringRing {
        /// Creates a ring; unavailable on this platform.
        ///
        /// # Errors
        ///
        /// Always returns [`UringRingOpenError::UnsupportedPlatform`] off Linux.
        pub fn create(
            _path: impl AsRef<Path>,
            _config: UringRingConfig,
        ) -> CompletionResult<Self, UringRingOpenError> {
            Err(CompletionError::not_applied(
                UringRingOpenError::UnsupportedPlatform,
            ))
        }

        /// Opens a ring; unavailable on this platform.
        ///
        /// # Errors
        ///
        /// Always returns [`UringRingOpenError::UnsupportedPlatform`] off Linux.
        pub fn open(
            _path: impl AsRef<Path>,
            _config: UringRingConfig,
        ) -> CompletionResult<Self, UringRingOpenError> {
            Err(CompletionError::not_applied(
                UringRingOpenError::UnsupportedPlatform,
            ))
        }

        /// Returns an unsupported-backend failure for API symmetry.
        pub fn status(&self) -> UringOperation<CompletionResult<RingStatus, RingError>> {
            RingReader::status(self)
        }

        /// Closes the ring; unavailable on this platform.
        ///
        /// # Errors
        ///
        /// Always returns [`UringRingOpenError::UnsupportedPlatform`] off Linux.
        pub fn close(self) -> Result<(), UringRingOpenError> {
            Err(UringRingOpenError::UnsupportedPlatform)
        }
    }

    impl RingReader for UringRing {
        type ReadFuture = UringOperation<CompletionResult<ReadPage, RingError>>;
        type StatusFuture = UringOperation<CompletionResult<RingStatus, RingError>>;

        fn read(&self, _request: ReadRequest) -> Self::ReadFuture {
            ready(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )))
        }

        fn status(&self) -> Self::StatusFuture {
            ready(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )))
        }
    }

    impl RingWriter for UringRing {
        type AppendFuture = UringOperation<CompletionResult<AppendSuccess, AppendFailure>>;
        type TrimFuture = UringOperation<CompletionResult<TrimSuccess, RingError>>;
        type SyncFuture = UringOperation<CompletionResult<SyncSuccess, SyncFailure>>;

        fn append(&self, request: AppendRequest) -> Self::AppendFuture {
            ready(Err(CompletionError::not_applied(AppendFailure {
                error: RingError::RecoveryRequired,
                records: request.records,
                accepted_range: None,
            })))
        }

        fn trim(&self, _before: RingCursor) -> Self::TrimFuture {
            ready(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )))
        }

        fn sync(&self) -> Self::SyncFuture {
            ready(Err(CompletionError::not_applied(SyncFailure {
                error: RingError::RecoveryRequired,
                checkpoint: None,
            })))
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub use unsupported::{UringOperation, UringRing};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_ring_shape_is_rejected_before_open() {
        let config = UringRingConfig {
            ring_entries: 3,
            ..UringRingConfig::default()
        };
        assert!(matches!(
            validate_and_derive_config(config),
            Err(UringRingOpenError::InvalidConfig {
                field: "ring_entries",
                ..
            })
        ));
    }

    #[test]
    fn zero_lifecycle_timeouts_are_rejected_before_open() {
        for (field, config) in [
            (
                "startup_timeout",
                UringRingConfig {
                    startup_timeout: Duration::ZERO,
                    ..UringRingConfig::default()
                },
            ),
            (
                "shutdown_timeout",
                UringRingConfig {
                    shutdown_timeout: Duration::ZERO,
                    ..UringRingConfig::default()
                },
            ),
        ] {
            assert!(matches!(
                validate_and_derive_config(config),
                Err(UringRingOpenError::InvalidConfig {
                    field: actual,
                    ..
                }) if actual == field
            ));
        }
    }

    #[test]
    fn physical_file_bound_includes_format_metadata() {
        let config = UringRingConfig {
            limits: RingLimits {
                max_record_bytes: 64,
                max_live_records: 4,
                max_live_payload_bytes: 128,
                max_read_records: 2,
                max_read_bytes: 64,
                max_batch_records: 2,
                max_batch_bytes: 64,
            },
            data_capacity_bytes: 256,
            ..UringRingConfig::default()
        };
        assert_eq!(
            validate_and_derive_config(config)
                .unwrap()
                .physical_file_bytes,
            8_192 + 256
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_create_and_open_are_explicitly_unsupported() {
        let config = UringRingConfig::default();
        assert!(matches!(
            UringRing::create("ignored", config),
            Err(error) if *error.error() == UringRingOpenError::UnsupportedPlatform
        ));
        assert!(matches!(
            UringRing::open("ignored", config),
            Err(error) if *error.error() == UringRingOpenError::UnsupportedPlatform
        ));
    }
}
