use std::fmt;
use std::future::{Ready, ready};
use std::sync::{Arc, Mutex};

use crate::completion::lock_unpoisoned;

use kr_runtime::{CompletionError, CompletionResult};

use super::{
    FileIoSubmit, FileLength, ReadAtFailure, ReadAtRequest, ReadAtSuccess, SetLenSuccess,
    StorageError, StorageOperation, SyncSuccess, WriteAtFailure, WriteAtRequest, WriteAtSuccess,
};

/// Bounds for the thread-safe deterministic in-memory file provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryFileConfig {
    pub max_file_bytes: usize,
    pub max_read_bytes: usize,
    pub max_write_bytes: usize,
    /// Maximum bytes transferred by one successful read.
    pub max_read_chunk: usize,
    /// Maximum bytes transferred by one successful write.
    pub max_write_chunk: usize,
}

impl Default for MemoryFileConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: 16 * 1_024 * 1_024,
            max_read_bytes: 128 * 1_024,
            max_write_bytes: 128 * 1_024,
            max_read_chunk: 128 * 1_024,
            max_write_chunk: 128 * 1_024,
        }
    }
}

/// Failure to construct a [`MemoryFile`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MemoryFileOpenError {
    InvalidConfig(&'static str),
    ExistingFileTooLarge { size: usize, limit: usize },
}

impl fmt::Display for MemoryFileOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(formatter, "invalid memory file config: {message}")
            }
            Self::ExistingFileTooLarge { size, limit } => {
                write!(formatter, "initial file size {size} exceeds limit {limit}")
            }
        }
    }
}

impl std::error::Error for MemoryFileOpenError {}

/// Passive deterministic file state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryFileStatus {
    pub accepted_len: u64,
    pub durable_len: u64,
    pub dirty: bool,
}

struct State {
    config: MemoryFileConfig,
    accepted: Vec<u8>,
    durable: Vec<u8>,
}

/// Cloneable deterministic file whose handle and operation futures are `Send`.
///
/// Operations take effect during method invocation, under one bounded mutex,
/// and return ready futures. Consequently dropping an admitted response does
/// not roll back its effect. Concurrent invocation order is the mutex
/// acquisition order and is intentionally unspecified. [`FileIoSubmit::submit_sync`](crate::FileIoSubmit::submit_sync) copies
/// the complete accepted image to the durable image.
#[derive(Clone)]
pub struct MemoryFile {
    state: Arc<Mutex<State>>,
}

impl MemoryFile {
    /// Creates an empty file.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryFileOpenError::InvalidConfig`] when a config bound is
    /// zero or inconsistent.
    pub fn new(config: MemoryFileConfig) -> Result<Self, MemoryFileOpenError> {
        Self::from_durable_bytes(config, Vec::new())
    }

    /// Creates a file from an already-durable byte image.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryFileOpenError::InvalidConfig`] when a config bound is
    /// zero or inconsistent, or [`MemoryFileOpenError::ExistingFileTooLarge`]
    /// when `bytes` exceeds `config.max_file_bytes`.
    pub fn from_durable_bytes(
        config: MemoryFileConfig,
        bytes: Vec<u8>,
    ) -> Result<Self, MemoryFileOpenError> {
        validate_config(config)?;
        if bytes.len() > config.max_file_bytes {
            return Err(MemoryFileOpenError::ExistingFileTooLarge {
                size: bytes.len(),
                limit: config.max_file_bytes,
            });
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                config,
                accepted: bytes.clone(),
                durable: bytes,
            })),
        })
    }

    /// Returns a bounded passive status snapshot.
    #[must_use]
    pub fn status(&self) -> MemoryFileStatus {
        let state = lock_unpoisoned(&self.state);
        MemoryFileStatus {
            accepted_len: state.accepted.len() as u64,
            durable_len: state.durable.len() as u64,
            dirty: state.accepted != state.durable,
        }
    }

    /// Returns a copy of the currently accepted image.
    #[must_use]
    pub fn accepted_bytes(&self) -> Vec<u8> {
        lock_unpoisoned(&self.state).accepted.clone()
    }

    /// Returns a copy of the durable image installed by the last sync.
    #[must_use]
    pub fn durable_bytes(&self) -> Vec<u8> {
        lock_unpoisoned(&self.state).durable.clone()
    }
}

// Completing synchronously under the state mutex is sound only because every
// future below is `Ready`: no waker or caller code can run while the lock is
// held. Any pending semantics added here must move to the shared
// `crate::completion` primitives and their deferred-wake rules instead.
impl FileIoSubmit for MemoryFile {
    type ReadAtResponse = Ready<CompletionResult<ReadAtSuccess, ReadAtFailure>>;
    type WriteAtResponse = Ready<CompletionResult<WriteAtSuccess, WriteAtFailure>>;
    type SetLenResponse = Ready<CompletionResult<SetLenSuccess, StorageError>>;
    type LenResponse = Ready<CompletionResult<FileLength, StorageError>>;
    type SyncResponse = Ready<CompletionResult<SyncSuccess, StorageError>>;

    fn submit_read_at(&self, mut request: ReadAtRequest) -> Self::ReadAtResponse {
        let state = lock_unpoisoned(&self.state);
        if request.buffer.len() > state.config.max_read_bytes {
            let requested = request.buffer.len();
            let limit = state.config.max_read_bytes;
            drop(state);
            return ready(Err(CompletionError::not_applied(ReadAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::ReadAt,
                    requested,
                    limit,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }

        let start = usize::try_from(request.offset)
            .ok()
            .filter(|start| *start < state.accepted.len());
        let bytes_read = start.map_or(0, |start| {
            let count = request
                .buffer
                .len()
                .min(state.config.max_read_chunk)
                .min(state.accepted.len() - start);
            request.buffer[..count].copy_from_slice(&state.accepted[start..start + count]);
            count
        });
        request.buffer.truncate(bytes_read);
        ready(Ok(ReadAtSuccess {
            buffer: request.buffer,
            bytes_read,
        }))
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        let mut state = lock_unpoisoned(&self.state);
        if request.buffer.len() > state.config.max_write_bytes {
            let requested = request.buffer.len();
            let limit = state.config.max_write_bytes;
            drop(state);
            return ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::WriteAt,
                    requested,
                    limit,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        let Some(end) = request.offset.checked_add(request.buffer.len() as u64) else {
            drop(state);
            return ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::OffsetOverflow,
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        };
        if end > state.config.max_file_bytes as u64 {
            let limit = state.config.max_file_bytes as u64;
            drop(state);
            return ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::FileTooLarge {
                    requested: end,
                    limit,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }

        let bytes_written = request.buffer.len().min(state.config.max_write_chunk);
        if bytes_written != 0 {
            let start = request.offset as usize;
            let written_end = start + bytes_written;
            if state.accepted.len() < written_end {
                state.accepted.resize(written_end, 0);
            }
            state.accepted[start..written_end].copy_from_slice(&request.buffer[..bytes_written]);
        }
        ready(Ok(WriteAtSuccess {
            bytes_written,
            buffer: request.buffer,
        }))
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        let mut state = lock_unpoisoned(&self.state);
        if len > state.config.max_file_bytes as u64 {
            let limit = state.config.max_file_bytes as u64;
            drop(state);
            return ready(Err(CompletionError::not_applied(
                StorageError::FileTooLarge {
                    requested: len,
                    limit,
                },
            )));
        }
        state.accepted.resize(len as usize, 0);
        ready(Ok(SetLenSuccess { len }))
    }

    fn submit_len(&self) -> Self::LenResponse {
        ready(Ok(FileLength {
            len: lock_unpoisoned(&self.state).accepted.len() as u64,
        }))
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        let mut state = lock_unpoisoned(&self.state);
        state.durable = state.accepted.clone();
        ready(Ok(SyncSuccess {
            durable_len: state.durable.len() as u64,
        }))
    }
}

fn validate_config(config: MemoryFileConfig) -> Result<(), MemoryFileOpenError> {
    if config.max_read_bytes > config.max_file_bytes {
        return Err(MemoryFileOpenError::InvalidConfig(
            "max_read_bytes exceeds max_file_bytes",
        ));
    }
    if config.max_write_bytes > config.max_file_bytes {
        return Err(MemoryFileOpenError::InvalidConfig(
            "max_write_bytes exceeds max_file_bytes",
        ));
    }
    if config.max_read_chunk == 0 {
        return Err(MemoryFileOpenError::InvalidConfig(
            "max_read_chunk must be nonzero",
        ));
    }
    if config.max_write_chunk == 0 {
        return Err(MemoryFileOpenError::InvalidConfig(
            "max_write_chunk must be nonzero",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SendFileIoSubmit;
    use crate::conformance::check_empty_file;
    use kr_runtime::SimRuntime;

    #[test]
    fn implements_send_contract_and_shared_file_conformance() {
        fn assert_send_file<T: SendFileIoSubmit>() {}
        assert_send_file::<MemoryFile>();

        let file = MemoryFile::new(MemoryFileConfig {
            max_file_bytes: 64,
            max_read_bytes: 16,
            max_write_bytes: 16,
            max_read_chunk: 2,
            max_write_chunk: 2,
        })
        .expect("memory file config is valid");
        let mut runtime = SimRuntime::default();
        runtime
            .block_on(check_empty_file(file))
            .expect("runtime drives conformance check")
            .expect("memory file satisfies shared conformance");
    }
}
