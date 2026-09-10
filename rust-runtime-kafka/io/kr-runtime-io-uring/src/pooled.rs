//! Shared-ring parallel file provider: many files, a fixed number of threads.
//!
//! This module is the first stage of the shared-budgeted-rings design
//! recorded in `DESIGN.md`, built as a parallel implementation beside the
//! per-file actor/reactor pair in [`crate::UringFile`]. Both are usable;
//! nothing selects one for you. The contract — [`FileIoSubmit`], owned
//! buffers, per-file FIFO, certainty-tagged failures — is identical, and
//! both run the same conformance suite.
//!
//! Mechanics: a [`UringIoPool`] owns one io_uring ring (whose reactor is the
//! kernel-facing thread), one coordinator thread, and the blocking workers
//! of its [`crate::UringEnv`] — private by default, shareable across
//! providers through [`UringIoPool::with_env`].
//! Submissions validate on the caller thread and enqueue a command; the
//! coordinator runs every file's state machine, submits routed SQEs to the
//! shared ring, and completes responses when routed completions arrive.
//! Reads, writes, fsync, and both length observations (`len` and the
//! post-fsync durable length) go through the ring — lengths as routed
//! `statx` SQEs, which is why statx joins the pool's probed kernel floor.
//! Only `set_len` runs on the blocking environment, so one file's `ftruncate`
//! cannot convoy every other file's completions.
//!
//! Each file admits up to `file_queue_capacity` commands and pipelines the
//! commuting prefix of its queue — consecutive reads, or writes with
//! non-overlapping ranges — onto the ring concurrently, the same batch
//! boundary the per-file actor draws; `sync`, `set_len`, `len`, and an
//! overlapping write act as fences. Completions arrive unordered, so a
//! per-file reorder buffer delivers responses in admission order, which
//! keeps FIFO observable and fencing exact. Files share the ring's
//! `max_in_flight` budget under strictly FIFO admission: ring-bound work
//! parks in a waiter queue when the budget refuses it or other files are
//! already parked, completions serve that queue front to back, and budget
//! the front waiter cannot yet use stays reserved for it rather than
//! passing to later arrivals — so one deep queue cannot starve the
//! others, and a two-slot sync cannot be starved by one-slot commands.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use kr_runtime::{CompletionError, CompletionResult};
use kr_runtime_io::{
    FileIoSubmit, FileLength, ReadAtFailure, ReadAtRequest, ReadAtSuccess, SetLenSuccess,
    StorageError, StorageOperation, SyncSuccess, WriteAtFailure, WriteAtRequest, WriteAtSuccess,
};

use crate::env::{BlockingJob, UringEnv, UringEnvConfig, UringEnvOpenError};
use crate::file::{
    backend, invalid_kernel_range, map_read_failure, map_write_failure, set_len_failure,
};
use crate::operation::{
    DriverStoppedCommand, FailStopOnPanic, Responder, TerminalCommand, UringOperation, operation,
    ready,
};
use crate::ring::{
    OwnedTransferFailure, Ring, RingCapacity, RoutedCompletion, RoutedResult, RoutedSubmission,
};
use crate::support::{Ingress, WAKE_TOKEN, join_if_other_thread, lock_unpoisoned};

type ReadCompletion = CompletionResult<ReadAtSuccess, ReadAtFailure>;
type WriteCompletion = CompletionResult<WriteAtSuccess, WriteAtFailure>;
type SetLenCompletion = CompletionResult<SetLenSuccess, StorageError>;
type LenCompletion = CompletionResult<FileLength, StorageError>;
type SyncCompletion = CompletionResult<SyncSuccess, StorageError>;

/// Fixed limits for one shared-ring file pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringPoolConfig {
    /// Maximum bytes accepted by one read request.
    pub max_read_bytes: usize,
    /// Maximum bytes accepted by one write request.
    pub max_write_bytes: usize,
    /// Maximum accepted physical file length.
    pub max_file_bytes: u64,
    /// Commands admitted per file, queued and in flight together.
    pub file_queue_capacity: usize,
    /// Submission-queue depth of the shared ring.
    pub ring_entries: u32,
    /// Ring operations in flight at once across every file in the pool.
    pub max_in_flight: usize,
    /// Threads serving blocking lifecycle syscalls when the pool builds
    /// its private environment ([`UringIoPool::new`]). A shared
    /// environment ([`UringIoPool::with_env`]) supplies its own workers,
    /// and this field is not consulted.
    pub blocking_threads: usize,
    /// Maximum bytes submitted by one SQE and therefore one completion.
    pub max_io_chunk_bytes: usize,
}

impl Default for UringPoolConfig {
    fn default() -> Self {
        Self {
            max_read_bytes: 128 * 1024,
            max_write_bytes: 128 * 1024,
            max_file_bytes: 1024 * 1024 * 1024,
            file_queue_capacity: 64,
            ring_entries: 8,
            max_in_flight: 64,
            blocking_threads: 2,
            max_io_chunk_bytes: 128 * 1024,
        }
    }
}

/// Failure to construct a pool or register a file with it.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UringPoolOpenError {
    /// A configuration field is outside its supported range.
    InvalidConfig { field: &'static str, reason: String },
    /// An operating-system interface failed.
    Io {
        action: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
}

impl fmt::Display for UringPoolOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(formatter, "invalid pool config {field}: {reason}")
            }
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
        }
    }
}

impl std::error::Error for UringPoolOpenError {}

fn open_io(action: &'static str, error: io::Error) -> UringPoolOpenError {
    UringPoolOpenError::Io {
        action,
        raw_os_error: error.raw_os_error(),
        message: error.to_string(),
    }
}

fn map_env_error(error: UringEnvOpenError) -> UringPoolOpenError {
    match error {
        UringEnvOpenError::InvalidConfig { field, reason } => {
            UringPoolOpenError::InvalidConfig { field, reason }
        }
        UringEnvOpenError::Io {
            action,
            raw_os_error,
            message,
        } => UringPoolOpenError::Io {
            action,
            raw_os_error,
            message,
        },
    }
}

fn invalid_config(
    field: &'static str,
    reason: impl Into<String>,
) -> Result<(), UringPoolOpenError> {
    Err(UringPoolOpenError::InvalidConfig {
        field,
        reason: reason.into(),
    })
}

fn validate_config(config: UringPoolConfig) -> Result<(), UringPoolOpenError> {
    if config.max_read_bytes == 0 {
        return invalid_config("max_read_bytes", "must be nonzero");
    }
    if config.max_write_bytes == 0 {
        return invalid_config("max_write_bytes", "must be nonzero");
    }
    if config.max_file_bytes == 0 || config.max_file_bytes > i64::MAX as u64 {
        return invalid_config("max_file_bytes", format!("must be in 1..={}", i64::MAX));
    }
    if config.file_queue_capacity == 0 {
        return invalid_config("file_queue_capacity", "must be nonzero");
    }
    if config.ring_entries < 4 || !config.ring_entries.is_power_of_two() {
        return invalid_config(
            "ring_entries",
            "must be a power of two and at least 4 for multiple queued I/O operations",
        );
    }
    if config.max_in_flight < 2 {
        return invalid_config(
            "max_in_flight",
            "must be at least 2 to hold a sync's linked fsync-statx pair",
        );
    }
    if config.blocking_threads == 0 {
        return invalid_config("blocking_threads", "must be nonzero");
    }
    if config.max_io_chunk_bytes == 0 || config.max_io_chunk_bytes > u32::MAX as usize {
        return invalid_config("max_io_chunk_bytes", format!("must be in 1..={}", u32::MAX));
    }
    Ok(())
}

enum Command {
    Read {
        request: ReadAtRequest,
        response: Responder<ReadCompletion>,
    },
    Write {
        request: WriteAtRequest,
        response: Responder<WriteCompletion>,
    },
    SetLen {
        len: u64,
        response: Responder<SetLenCompletion>,
    },
    Len {
        response: Responder<LenCompletion>,
    },
    Sync {
        response: Responder<SyncCompletion>,
    },
}

impl DriverStoppedCommand for Command {
    fn complete_driver_stopped(self) {
        match self {
            Self::Read { request, response } => {
                response.complete(Err(CompletionError::not_applied(ReadAtFailure {
                    error: StorageError::DriverStopped,
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })));
            }
            Self::Write { request, response } => {
                response.complete(Err(CompletionError::not_applied(WriteAtFailure {
                    error: StorageError::DriverStopped,
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })));
            }
            Self::SetLen { response, .. } => {
                response.complete(Err(CompletionError::not_applied(
                    StorageError::DriverStopped,
                )));
            }
            Self::Len { response } => {
                response.complete(Err(CompletionError::not_applied(
                    StorageError::DriverStopped,
                )));
            }
            Self::Sync { response } => {
                response.complete(Err(CompletionError::not_applied(
                    StorageError::DriverStopped,
                )));
            }
        }
    }
}

enum BlockingOutcome {
    SetLen { len: u64, result: io::Result<()> },
}

enum PoolMessage {
    Command {
        file: Arc<FileControl>,
        command: TerminalCommand<Command>,
    },
    BlockingDone {
        file: u64,
        outcome: BlockingOutcome,
    },
    FileClosed {
        file: u64,
    },
    Shutdown,
}

/// Reports a blocking set_len outcome to the coordinator exactly once.
///
/// The environment contains job panics, so terminal reporting rides a
/// guard: a job that unwinds before reporting still fences the file with
/// an error outcome instead of leaving its command in flight forever.
struct SetLenReport {
    file: u64,
    len: u64,
    ingress: Arc<Ingress<PoolMessage>>,
    reported: bool,
}

impl SetLenReport {
    fn report(&mut self, result: io::Result<()>) {
        if !self.reported {
            self.reported = true;
            self.ingress.push(PoolMessage::BlockingDone {
                file: self.file,
                outcome: BlockingOutcome::SetLen {
                    len: self.len,
                    result,
                },
            });
        }
    }
}

impl Drop for SetLenReport {
    fn drop(&mut self) {
        self.report(Err(io::Error::other(
            "blocking set_len job did not run to completion",
        )));
    }
}

/// Builds the environment job for one ftruncate: run the syscall, report
/// the outcome, and keep the file control alive until both have happened.
fn set_len_job(
    control: Arc<FileControl>,
    len: u64,
    ingress: Arc<Ingress<PoolMessage>>,
) -> BlockingJob {
    Box::new(move || {
        let mut report = SetLenReport {
            file: control.id,
            len,
            ingress,
            reported: false,
        };
        let result = control.file.set_len(len);
        report.report(result);
    })
}

/// Everything a registered file's commands need to reach the pool: kept
/// alive by every queued and active command, so the descriptor outlives all
/// kernel-visible references to it.
struct FileControl {
    id: u64,
    file: File,
    config: UringPoolConfig,
    /// Commands admitted and not yet completed, bounded by
    /// `file_queue_capacity`.
    in_flight: AtomicUsize,
    ingress: Arc<Ingress<PoolMessage>>,
}

impl FileControl {
    fn retire_command(&self) {
        let previous = self.in_flight.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "file command count underflowed");
    }
}

/// Marks the file closed for the coordinator when the last public handle —
/// as opposed to the last queued command — goes away.
struct HandleGuard {
    control: Arc<FileControl>,
    /// Keeps the pool's shutdown ordered after this file's close: the field
    /// drops after the close message is pushed.
    _shared: Arc<PoolShared>,
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        self.control.ingress.push(PoolMessage::FileClosed {
            file: self.control.id,
        });
    }
}

struct PoolShared {
    config: UringPoolConfig,
    ingress: Arc<Ingress<PoolMessage>>,
    coordinator: Mutex<Option<JoinHandle<()>>>,
    next_file: AtomicU64,
}

impl Drop for PoolShared {
    fn drop(&mut self) {
        // Every public handle is gone, so only queued and in-flight work
        // remains. The coordinator drains it — every response is completed,
        // normally or with DriverStopped — and releases its environment
        // clone as it exits, so a private environment stops after the pool.
        self.ingress.push(PoolMessage::Shutdown);
        join_if_other_thread(lock_unpoisoned(&self.coordinator).take());
    }
}

/// A shared-ring file pool: one ring, one coordinator, one blocking
/// environment.
///
/// Files registered with the pool implement the same [`FileIoSubmit`]
/// contract as [`crate::UringFile`], with the same per-file FIFO and
/// certainty semantics, while every file shares the pool's threads
/// instead of owning two of its own.
#[derive(Clone)]
pub struct UringIoPool {
    shared: Arc<PoolShared>,
}

impl UringIoPool {
    /// Builds the pool with a private environment: the shared ring and its
    /// reactor, the coordinator thread, and `blocking_threads` lifecycle
    /// workers of its own.
    ///
    /// # Errors
    ///
    /// Returns [`UringPoolOpenError`] when the configuration is invalid or
    /// the ring or a thread cannot be created.
    pub fn new(config: UringPoolConfig) -> Result<Self, UringPoolOpenError> {
        validate_config(config)?;
        let env = UringEnv::new(UringEnvConfig {
            blocking_threads: config.blocking_threads,
        })
        .map_err(map_env_error)?;
        Self::with_env(config, &env)
    }

    /// Builds the pool on a shared [`UringEnv`]: the environment supplies
    /// the blocking workers, so `blocking_threads` is not consulted, and
    /// the workers serve every provider sharing the environment.
    ///
    /// # Errors
    ///
    /// Returns [`UringPoolOpenError`] when the configuration is invalid or
    /// the ring or the coordinator thread cannot be created.
    pub fn with_env(config: UringPoolConfig, env: &UringEnv) -> Result<Self, UringPoolOpenError> {
        validate_config(config)?;
        let ring = Ring::for_pool(
            RingCapacity {
                entries: config.ring_entries,
                transient: config.max_in_flight,
                sustained: 0,
            },
            config.max_io_chunk_bytes,
        )
        .map_err(|error| open_io("create pooled io_uring", error))?;
        let (routed, completions) = mpsc::channel();
        let ingress = Arc::new(Ingress::new(routed.clone()));

        let coordinator_ingress = Arc::clone(&ingress);
        let coordinator_env = env.clone();
        let spawn_result = thread::Builder::new()
            .name("kr-runtime-io-uring-pool".to_owned())
            .spawn(move || {
                Coordinator {
                    ring,
                    routed,
                    completions,
                    ingress: coordinator_ingress,
                    env: coordinator_env,
                    files: HashMap::new(),
                    tokens: HashMap::new(),
                    next_token: WAKE_TOKEN + 1,
                    ring_in_flight: 0,
                    ring_waiters: VecDeque::new(),
                    outstanding_ops: 0,
                    max_in_flight: config.max_in_flight,
                    shutting_down: false,
                }
                .run();
            });
        let coordinator = match spawn_result {
            Ok(join) => join,
            Err(error) => return Err(open_io("spawn pool coordinator", error)),
        };

        Ok(Self {
            shared: Arc::new(PoolShared {
                config,
                ingress,
                coordinator: Mutex::new(Some(coordinator)),
                next_file: AtomicU64::new(1),
            }),
        })
    }

    /// Registers an already-open file with the pool.
    ///
    /// Like [`crate::UringFile::from_file`], the caller owns everything a
    /// path would have provided: advisory locking and the containing
    /// directory's durability fence for a newly created file.
    ///
    /// # Errors
    ///
    /// This method currently cannot fail; the `Result` reserves room for
    /// registration limits without breaking callers.
    pub fn register_file(&self, file: File) -> Result<PooledUringFile, UringPoolOpenError> {
        let id = self.shared.next_file.fetch_add(1, Ordering::Relaxed);
        let control = Arc::new(FileControl {
            id,
            file,
            config: self.shared.config,
            in_flight: AtomicUsize::new(0),
            ingress: Arc::clone(&self.shared.ingress),
        });
        Ok(PooledUringFile {
            handle: Arc::new(HandleGuard {
                control,
                _shared: Arc::clone(&self.shared),
            }),
        })
    }
}

/// One pooled file session implementing [`FileIoSubmit`].
///
/// Clones share the session. Dropping the last clone closes the file once
/// every admitted command has terminalized; dropping a response future
/// abandons only its response, never the admitted operation or its buffer.
#[derive(Clone)]
pub struct PooledUringFile {
    handle: Arc<HandleGuard>,
}

impl PooledUringFile {
    fn try_admit(&self) -> Result<(), StorageError> {
        let control = &self.handle.control;
        let capacity = control.config.file_queue_capacity;
        let mut current = control.in_flight.load(Ordering::Acquire);
        loop {
            if current >= capacity {
                return Err(StorageError::ResourceExhausted {
                    resource: "queued file commands",
                    limit: capacity,
                });
            }
            match control.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn send(&self, command: Command) {
        self.handle.control.ingress.push(PoolMessage::Command {
            file: Arc::clone(&self.handle.control),
            command: TerminalCommand::new(command),
        });
    }
}

impl FileIoSubmit for PooledUringFile {
    type ReadAtResponse = UringOperation<ReadCompletion>;
    type WriteAtResponse = UringOperation<WriteCompletion>;
    type SetLenResponse = UringOperation<SetLenCompletion>;
    type LenResponse = UringOperation<LenCompletion>;
    type SyncResponse = UringOperation<SyncCompletion>;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        let config = self.handle.control.config;
        if request.buffer.len() > config.max_read_bytes {
            let requested = request.buffer.len();
            return ready(Err(CompletionError::not_applied(ReadAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::ReadAt,
                    requested,
                    limit: config.max_read_bytes,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        if invalid_kernel_range(request.offset, request.buffer.len()) {
            return ready(Err(CompletionError::not_applied(ReadAtFailure {
                error: StorageError::OffsetOverflow,
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        if let Err(error) = self.try_admit() {
            return ready(Err(CompletionError::not_applied(ReadAtFailure {
                error,
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        let (future, response) = operation();
        self.send(Command::Read { request, response });
        future
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        let config = self.handle.control.config;
        if request.buffer.len() > config.max_write_bytes {
            let requested = request.buffer.len();
            return ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::WriteAt,
                    requested,
                    limit: config.max_write_bytes,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        match request.offset.checked_add(request.buffer.len() as u64) {
            Some(end) if end <= config.max_file_bytes && end <= i64::MAX as u64 => {}
            Some(end) if end > config.max_file_bytes => {
                return ready(Err(CompletionError::not_applied(WriteAtFailure {
                    error: StorageError::FileTooLarge {
                        requested: end,
                        limit: config.max_file_bytes,
                    },
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })));
            }
            _ => {
                return ready(Err(CompletionError::not_applied(WriteAtFailure {
                    error: StorageError::OffsetOverflow,
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })));
            }
        }
        if let Err(error) = self.try_admit() {
            return ready(Err(CompletionError::not_applied(WriteAtFailure {
                error,
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        let (future, response) = operation();
        self.send(Command::Write { request, response });
        future
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        let config = self.handle.control.config;
        if len > config.max_file_bytes {
            return ready(Err(CompletionError::not_applied(
                StorageError::FileTooLarge {
                    requested: len,
                    limit: config.max_file_bytes,
                },
            )));
        }
        if let Err(error) = self.try_admit() {
            return ready(Err(CompletionError::not_applied(error)));
        }
        let (future, response) = operation();
        self.send(Command::SetLen { len, response });
        future
    }

    fn submit_len(&self) -> Self::LenResponse {
        if let Err(error) = self.try_admit() {
            return ready(Err(CompletionError::not_applied(error)));
        }
        let (future, response) = operation();
        self.send(Command::Len { response });
        future
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        if let Err(error) = self.try_admit() {
            return ready(Err(CompletionError::not_applied(error)));
        }
        let (future, response) = operation();
        self.send(Command::Sync { response });
        future
    }
}

/// Whether two half-open write ranges intersect. Empty writes never overlap,
/// matching the per-file actor's batching predicate.
fn ranges_overlap(first: (u64, usize), second: (u64, usize)) -> bool {
    if first.1 == 0 || second.1 == 0 {
        return false;
    }
    // Submission validated offset + len <= i64::MAX, so these adds cannot wrap.
    first.0 < second.0 + second.1 as u64 && second.0 < first.0 + first.1 as u64
}

/// The commuting class an in-flight operation belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpKind {
    Read,
    Write,
    /// `sync`, `set_len`, and `len`: nothing commutes with them, so they run
    /// with the pipeline otherwise empty.
    Fence,
}

/// One admitted operation in a file's pipeline, in admission order.
struct InflightOp {
    /// The routed tokens while SQEs are outstanding for this operation: one
    /// for most stages, two while a linked fsync-statx pair is in flight.
    tokens: [Option<u64>; 2],
    kind: OpKind,
    /// A write's target range, retained through `Done` so a later
    /// overlapping write cannot start until this one's response has been
    /// delivered — the same boundary the per-file actor's batch collection
    /// draws.
    write_range: Option<(u64, usize)>,
    state: OpState,
}

enum OpState {
    Read {
        buffer: Option<Vec<u8>>,
        requested: usize,
        response: Responder<ReadCompletion>,
    },
    Write {
        buffer: Option<Vec<u8>>,
        requested: usize,
        response: Responder<WriteCompletion>,
    },
    /// A sync's linked fsync-statx pair: one submission, two completions
    /// joined here. The kernel writes into `statx`, boxed so the allocation
    /// stays put until its completion arrives.
    Sync {
        response: Responder<SyncCompletion>,
        statx: Box<libc::statx>,
        fsync_result: Option<RoutedResult>,
        statx_result: Option<RoutedResult>,
    },
    SetLen {
        response: Responder<SetLenCompletion>,
    },
    LenStatx {
        response: Responder<LenCompletion>,
        statx: Box<libc::statx>,
    },
    /// Terminal outcome awaiting its turn in the reorder buffer: responses
    /// complete in admission order, so a finished operation waits for every
    /// earlier one.
    Done(Finished),
    /// Transient marker while an event handler replaces the state within one
    /// call; never observable across events.
    Resolving,
}

/// A terminal outcome paired with its responder, completed front-to-back.
enum Finished {
    Read(Responder<ReadCompletion>, ReadCompletion),
    Write(Responder<WriteCompletion>, WriteCompletion),
    Sync(Responder<SyncCompletion>, SyncCompletion),
    SetLen(Responder<SetLenCompletion>, SetLenCompletion),
    Len(Responder<LenCompletion>, LenCompletion),
}

impl Finished {
    fn complete(self) {
        match self {
            Self::Read(response, outcome) => response.complete(outcome),
            Self::Write(response, outcome) => response.complete(outcome),
            Self::Sync(response, outcome) => response.complete(outcome),
            Self::SetLen(response, outcome) => response.complete(outcome),
            Self::Len(response, outcome) => response.complete(outcome),
        }
    }
}

/// Whether `command` may start executing while `inflight` is outstanding.
///
/// Reads run concurrently with reads; writes run concurrently with
/// non-overlapping writes; fences require an empty pipeline. `Done` entries
/// still count — their responses have not been delivered, and the per-file
/// actor likewise starts the next batch only after the previous one has
/// fully completed.
fn commutes(inflight: &VecDeque<InflightOp>, command: &Command) -> bool {
    if inflight.is_empty() {
        return true;
    }
    match command {
        Command::Read { .. } => inflight.iter().all(|op| op.kind == OpKind::Read),
        Command::Write { request, .. } => {
            let range = (request.offset, request.buffer.len());
            inflight.iter().all(|op| {
                op.kind == OpKind::Write
                    && !ranges_overlap(
                        op.write_range.expect("write operation carries its range"),
                        range,
                    )
            })
        }
        Command::SetLen { .. } | Command::Len { .. } | Command::Sync { .. } => false,
    }
}

/// Completes the contiguous finished prefix of the pipeline in admission
/// order, releasing each command's admission slot as it goes.
fn drain_finished(state: &mut FileState) {
    while matches!(
        state.inflight.front(),
        Some(InflightOp {
            state: OpState::Done(_),
            ..
        })
    ) {
        let op = state
            .inflight
            .pop_front()
            .expect("checked front operation is present");
        let OpState::Done(finished) = op.state else {
            unreachable!("checked front operation stopped being finished")
        };
        finished.complete();
        state.control.retire_command();
    }
}

struct FileState {
    control: Arc<FileControl>,
    queue: VecDeque<Command>,
    /// Admitted operations executing or awaiting ordered completion. All
    /// entries are one commuting class: reads, non-overlapping writes, or a
    /// single fence.
    inflight: VecDeque<InflightOp>,
    /// Set on any failure whose effect on the file is uncertain, or when the
    /// shared ring poisons; every later command on this file fails closed
    /// with `RecoveryRequired` once the in-flight pipeline has drained.
    recovery_required: bool,
    /// The last public handle is gone; the state is removed once drained.
    closing: bool,
    /// The file's id is in the coordinator's waiter FIFO. Kept in step with
    /// `ring_waiters` so a file is never queued twice.
    parked: bool,
}

/// Which admission path is pumping a file, deciding how ring-bound work
/// interacts with the shared budget's waiter FIFO.
#[derive(Clone, Copy)]
enum PumpSource {
    /// An ingress or completion event: ring-bound work parks behind every
    /// file already in the waiter FIFO, keeping budget admission strictly
    /// first-come-first-served — a later one-slot command cannot overtake a
    /// parked two-slot sync and starve it.
    Event,
    /// The front of the waiter FIFO, served by `pump_waiters`: only the
    /// budget itself can refuse the head, and a refusal re-parks the file
    /// at the front, reserving the free budget until completions make room.
    FrontWaiter,
}

struct Coordinator {
    ring: Ring,
    /// The submission-side sender for routed completions; cloned into every
    /// routed SQE. The paired receiver below is the coordinator's single
    /// blocking point.
    routed: Sender<RoutedCompletion>,
    completions: Receiver<RoutedCompletion>,
    ingress: Arc<Ingress<PoolMessage>>,
    env: UringEnv,
    files: HashMap<u64, FileState>,
    /// Routed token to file id, one entry per ring operation in flight.
    tokens: HashMap<u64, u64>,
    next_token: u64,
    ring_in_flight: usize,
    /// Files whose head command needs ring budget it cannot yet take —
    /// because the budget refused it or because other files were already
    /// parked. `pump_waiters` serves the queue strictly front to back.
    ring_waiters: VecDeque<u64>,
    /// Operations still awaiting an external event — a ring completion or a
    /// blocking-pool result. The shutdown gate waits for zero.
    outstanding_ops: usize,
    max_in_flight: usize,
    shutting_down: bool,
}

impl Coordinator {
    fn run(mut self) {
        // The coordinator owns kernel-visible buffers in its slots, so an
        // internal panic cannot honestly unwind past them.
        let _fail_stop = FailStopOnPanic;
        loop {
            if self.shutting_down && self.outstanding_ops == 0 {
                return;
            }
            let Ok(completion) = self.completions.recv() else {
                // Every sender is gone: no handle, no reactor, no ingress
                // producer. Nothing can arrive, so nothing is owed.
                return;
            };
            if completion.token == WAKE_TOKEN {
                self.drain_ingress();
            } else {
                self.finish_ring(completion);
            }
        }
    }

    fn allocate_token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token = token
            .checked_add(1)
            .expect("routed token space is practically inexhaustible");
        token
    }

    fn drain_ingress(&mut self) {
        loop {
            let message = self.ingress.pop();
            match message {
                None => return,
                Some(PoolMessage::Command { file, command }) => {
                    let id = file.id;
                    let state = self.files.entry(id).or_insert_with(|| FileState {
                        control: file,
                        queue: VecDeque::new(),
                        inflight: VecDeque::new(),
                        recovery_required: false,
                        closing: false,
                        parked: false,
                    });
                    state.queue.push_back(command.into_inner());
                    self.pump(id);
                }
                Some(PoolMessage::BlockingDone { file, outcome }) => {
                    self.finish_blocking(file, outcome);
                }
                Some(PoolMessage::FileClosed { file }) => {
                    if let Some(state) = self.files.get_mut(&file) {
                        state.closing = true;
                    }
                    self.remove_if_drained(file);
                }
                Some(PoolMessage::Shutdown) => {
                    self.shutting_down = true;
                    // Queued commands are refused with DriverStopped;
                    // in-flight operations finish on their own and are
                    // awaited by the main loop's exit gate.
                    let ids: Vec<u64> = self.files.keys().copied().collect();
                    for id in ids {
                        if let Some(state) = self.files.get_mut(&id) {
                            state.parked = false;
                            let control = Arc::clone(&state.control);
                            while let Some(command) = state.queue.pop_front() {
                                command.complete_driver_stopped();
                                control.retire_command();
                            }
                        }
                        self.remove_if_drained(id);
                    }
                    self.ring_waiters.clear();
                }
            }
        }
    }

    fn remove_if_drained(&mut self, id: u64) {
        if let Some(state) = self.files.get(&id)
            && (state.closing || self.shutting_down)
            && state.queue.is_empty()
            && state.inflight.is_empty()
        {
            self.files.remove(&id);
        }
    }

    /// Admits as much of the file's queue as currently commutes.
    ///
    /// Each iteration completes any finished prefix, then starts the head
    /// command if it commutes with the in-flight pipeline and, for
    /// ring-bound work, its turn at the shared budget has come — otherwise
    /// the file parks in the waiter FIFO and resumes when `pump_waiters`
    /// serves it. Consecutive reads and non-overlapping writes therefore
    /// pipeline onto the ring together, the shape the per-file actor gets
    /// from its commuting batches.
    fn pump(&mut self, id: u64) {
        self.pump_as(id, PumpSource::Event);
    }

    /// [`pump`](Self::pump) with an explicit admission path.
    ///
    /// Returns `true` only when a [`PumpSource::FrontWaiter`] head was
    /// refused by the budget: the file is back at the front of the FIFO
    /// with the free budget reserved for it, so waiter service must stop.
    fn pump_as(&mut self, id: u64, source: PumpSource) -> bool {
        loop {
            let Some(state) = self.files.get_mut(&id) else {
                return false;
            };
            drain_finished(state);
            if state.recovery_required {
                // Fencing waits for the pipeline so responses keep admission
                // order; nothing new is admitted meanwhile.
                if !state.inflight.is_empty() {
                    return false;
                }
                while let Some(command) = state.queue.pop_front() {
                    complete_recovery_required(command);
                    state.control.retire_command();
                }
                self.remove_if_drained(id);
                return false;
            }
            let Some(head) = state.queue.front() else {
                self.remove_if_drained(id);
                return false;
            };
            if !commutes(&state.inflight, head) {
                return false;
            }
            // A sync's linked fsync-statx pair holds two ring slots; every
            // other ring-bound command holds one.
            let ring_slots = match head {
                Command::Sync { .. } => 2,
                Command::Read { .. } | Command::Write { .. } | Command::Len { .. } => 1,
                Command::SetLen { .. } => 0,
            };
            if ring_slots > 0 {
                let budget_refuses = self.ring_in_flight + ring_slots > self.max_in_flight;
                match source {
                    PumpSource::FrontWaiter => {
                        if budget_refuses {
                            state.parked = true;
                            self.ring_waiters.push_front(id);
                            return true;
                        }
                    }
                    PumpSource::Event => {
                        if budget_refuses || !self.ring_waiters.is_empty() {
                            if !state.parked {
                                state.parked = true;
                                self.ring_waiters.push_back(id);
                            }
                            return false;
                        }
                    }
                }
            }
            let command = state
                .queue
                .pop_front()
                .expect("peeked head command is present");
            let control = Arc::clone(&state.control);
            match command {
                Command::Read { request, response } => {
                    let mut buffer = request.buffer;
                    let len = buffer.len();
                    let token = self.allocate_token();
                    match self.ring.start_routed_read_at(
                        &control.file,
                        request.offset,
                        &mut buffer,
                        len,
                        &self.routed,
                        token,
                    ) {
                        Ok(RoutedSubmission::Submitted { requested }) => {
                            self.tokens.insert(token, id);
                            self.ring_in_flight += 1;
                            self.outstanding_ops += 1;
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [Some(token), None],
                                    kind: OpKind::Read,
                                    write_range: None,
                                    state: OpState::Read {
                                        buffer: Some(buffer),
                                        requested,
                                        response,
                                    },
                                },
                            );
                        }
                        Ok(RoutedSubmission::Empty) => {
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [None, None],
                                    kind: OpKind::Read,
                                    write_range: None,
                                    state: OpState::Done(Finished::Read(
                                        response,
                                        Ok(ReadAtSuccess {
                                            buffer,
                                            bytes_read: 0,
                                        }),
                                    )),
                                },
                            );
                        }
                        Err(error) => {
                            self.observe_submit_failure(id);
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [None, None],
                                    kind: OpKind::Read,
                                    write_range: None,
                                    state: OpState::Done(Finished::Read(
                                        response,
                                        Err(map_read_failure(OwnedTransferFailure {
                                            buffer,
                                            error,
                                            may_have_applied: false,
                                        })),
                                    )),
                                },
                            );
                        }
                    }
                }
                Command::Write { request, response } => {
                    let buffer = request.buffer;
                    let len = buffer.len();
                    let range = Some((request.offset, len));
                    let token = self.allocate_token();
                    match self.ring.start_routed_write_at(
                        &control.file,
                        request.offset,
                        &buffer,
                        len,
                        &self.routed,
                        token,
                    ) {
                        Ok(RoutedSubmission::Submitted { requested }) => {
                            self.tokens.insert(token, id);
                            self.ring_in_flight += 1;
                            self.outstanding_ops += 1;
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [Some(token), None],
                                    kind: OpKind::Write,
                                    write_range: range,
                                    state: OpState::Write {
                                        buffer: Some(buffer),
                                        requested,
                                        response,
                                    },
                                },
                            );
                        }
                        Ok(RoutedSubmission::Empty) => {
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [None, None],
                                    kind: OpKind::Write,
                                    write_range: range,
                                    state: OpState::Done(Finished::Write(
                                        response,
                                        Ok(WriteAtSuccess {
                                            bytes_written: 0,
                                            buffer,
                                        }),
                                    )),
                                },
                            );
                        }
                        Err(error) => {
                            self.observe_submit_failure(id);
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [None, None],
                                    kind: OpKind::Write,
                                    write_range: range,
                                    state: OpState::Done(Finished::Write(
                                        response,
                                        Err(map_write_failure(OwnedTransferFailure {
                                            buffer,
                                            error,
                                            may_have_applied: false,
                                        })),
                                    )),
                                },
                            );
                        }
                    }
                }
                Command::Sync { response } => {
                    // One submission carries the fence and its durable-length
                    // observation: the fsync links to a statx, so sync costs
                    // one coordinator round trip like every other operation.
                    let mut statx = Ring::new_statx_buffer();
                    let fsync_token = self.allocate_token();
                    let statx_token = self.allocate_token();
                    match self.ring.start_routed_fsync_then_statx_size(
                        &control.file,
                        &mut statx,
                        &self.routed,
                        fsync_token,
                        statx_token,
                    ) {
                        Ok(()) => {
                            self.tokens.insert(fsync_token, id);
                            self.tokens.insert(statx_token, id);
                            self.ring_in_flight += 2;
                            self.outstanding_ops += 1;
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [Some(fsync_token), Some(statx_token)],
                                    kind: OpKind::Fence,
                                    write_range: None,
                                    state: OpState::Sync {
                                        response,
                                        statx,
                                        fsync_result: None,
                                        statx_result: None,
                                    },
                                },
                            );
                        }
                        Err(error) => {
                            // Mirrors `UringFile`: any sync failure fences
                            // the file and reports uncertainty, because a
                            // sync that did not provably run leaves
                            // durability unknown.
                            self.mark_recovery_required(id);
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [None, None],
                                    kind: OpKind::Fence,
                                    write_range: None,
                                    state: OpState::Done(Finished::Sync(
                                        response,
                                        Err(CompletionError::may_have_applied(backend(
                                            StorageOperation::Sync,
                                            error,
                                        ))),
                                    )),
                                },
                            );
                        }
                    }
                }
                Command::SetLen { len, response } => {
                    self.outstanding_ops += 1;
                    self.push_op(
                        id,
                        InflightOp {
                            tokens: [None, None],
                            kind: OpKind::Fence,
                            write_range: None,
                            state: OpState::SetLen { response },
                        },
                    );
                    self.env
                        .submit_blocking(set_len_job(control, len, Arc::clone(&self.ingress)));
                }
                Command::Len { response } => {
                    let mut statx = Ring::new_statx_buffer();
                    let token = self.allocate_token();
                    match self.ring.start_routed_statx_size(
                        &control.file,
                        &mut statx,
                        &self.routed,
                        token,
                    ) {
                        Ok(()) => {
                            self.tokens.insert(token, id);
                            self.ring_in_flight += 1;
                            self.outstanding_ops += 1;
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [Some(token), None],
                                    kind: OpKind::Fence,
                                    write_range: None,
                                    state: OpState::LenStatx { response, statx },
                                },
                            );
                        }
                        Err(error) => {
                            self.observe_submit_failure(id);
                            self.push_op(
                                id,
                                InflightOp {
                                    tokens: [None, None],
                                    kind: OpKind::Fence,
                                    write_range: None,
                                    state: OpState::Done(Finished::Len(
                                        response,
                                        Err(CompletionError::not_applied(backend(
                                            StorageOperation::Len,
                                            error,
                                        ))),
                                    )),
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    fn push_op(&mut self, id: u64, op: InflightOp) {
        self.files
            .get_mut(&id)
            .expect("pumped file state disappeared")
            .inflight
            .push_back(op);
    }

    fn mark_recovery_required(&mut self, id: u64) {
        if let Some(state) = self.files.get_mut(&id) {
            state.recovery_required = true;
        }
    }

    fn observe_submit_failure(&mut self, id: u64) {
        if self.ring.is_poisoned() {
            self.mark_recovery_required(id);
        }
    }

    fn finish_ring(&mut self, completion: RoutedCompletion) {
        let id = self
            .tokens
            .remove(&completion.token)
            .expect("routed completion carried an unknown token");
        self.ring_in_flight = self
            .ring_in_flight
            .checked_sub(1)
            .expect("ring in-flight count underflowed");
        let ring_poisoned = self.ring.is_poisoned();
        let state = self
            .files
            .get_mut(&id)
            .expect("completed file state disappeared");
        let index = state
            .inflight
            .iter()
            .position(|op| op.tokens.contains(&Some(completion.token)))
            .expect("routed completion matches an in-flight operation");
        let op = &mut state.inflight[index];
        let arrived_first = op.tokens[0] == Some(completion.token);
        for slot in &mut op.tokens {
            if *slot == Some(completion.token) {
                *slot = None;
            }
        }
        let previous = std::mem::replace(&mut op.state, OpState::Resolving);
        let finished = match previous {
            OpState::Read {
                buffer,
                requested,
                response,
            } => {
                let mut buffer = buffer.expect("active read buffer is available");
                match self
                    .ring
                    .finish_routed_transfer(requested, completion.result)
                {
                    Ok(transferred) => {
                        buffer.truncate(transferred);
                        Finished::Read(
                            response,
                            Ok(ReadAtSuccess {
                                buffer,
                                bytes_read: transferred,
                            }),
                        )
                    }
                    Err(failure) => {
                        if ring_poisoned {
                            state.recovery_required = true;
                        }
                        Finished::Read(
                            response,
                            Err(map_read_failure(OwnedTransferFailure {
                                buffer,
                                error: failure.error,
                                may_have_applied: failure.may_have_applied,
                            })),
                        )
                    }
                }
            }
            OpState::Write {
                buffer,
                requested,
                response,
            } => {
                let buffer = buffer.expect("active write buffer is available");
                match self
                    .ring
                    .finish_routed_transfer(requested, completion.result)
                {
                    Ok(transferred) => Finished::Write(
                        response,
                        Ok(WriteAtSuccess {
                            bytes_written: transferred,
                            buffer,
                        }),
                    ),
                    Err(failure) => {
                        if failure.may_have_applied || ring_poisoned {
                            state.recovery_required = true;
                        }
                        Finished::Write(
                            response,
                            Err(map_write_failure(OwnedTransferFailure {
                                buffer,
                                error: failure.error,
                                may_have_applied: failure.may_have_applied,
                            })),
                        )
                    }
                }
            }
            OpState::Sync {
                response,
                statx,
                mut fsync_result,
                mut statx_result,
            } => {
                if arrived_first {
                    fsync_result = Some(completion.result);
                } else {
                    statx_result = Some(completion.result);
                }
                if fsync_result.is_none() || statx_result.is_none() {
                    // Half the pair is still owed. Put the operation back;
                    // waiter service decides whether the slot this CQE freed
                    // resumes a parked file or stays reserved for one that
                    // needs more.
                    op.state = OpState::Sync {
                        response,
                        statx,
                        fsync_result,
                        statx_result,
                    };
                    self.pump_waiters();
                    return;
                }
                let fsync = fsync_result.take().expect("fsync result is present");
                let observed = statx_result.take().expect("statx result is present");
                match Ring::finish_routed_fsync(fsync) {
                    // The fence itself failed: durability is unknown and the
                    // linked statx was severed with it.
                    Err(error) => {
                        state.recovery_required = true;
                        Finished::Sync(
                            response,
                            Err(CompletionError::may_have_applied(backend(
                                StorageOperation::Sync,
                                error,
                            ))),
                        )
                    }
                    Ok(()) => match Ring::finish_routed_statx_size(observed, &statx) {
                        Ok(durable_len) => {
                            Finished::Sync(response, Ok(SyncSuccess { durable_len }))
                        }
                        // The fsync completed; only the length observation
                        // failed, so the effect is applied.
                        Err(error) => Finished::Sync(
                            response,
                            Err(CompletionError::applied(backend(
                                StorageOperation::Sync,
                                error,
                            ))),
                        ),
                    },
                }
            }
            OpState::LenStatx { response, statx } => {
                match Ring::finish_routed_statx_size(completion.result, &statx) {
                    Ok(len) => Finished::Len(response, Ok(FileLength { len })),
                    Err(error) => Finished::Len(
                        response,
                        Err(CompletionError::not_applied(backend(
                            StorageOperation::Len,
                            error,
                        ))),
                    ),
                }
            }
            OpState::SetLen { .. } | OpState::Done(_) | OpState::Resolving => {
                unreachable!("ring completion arrived for a non-ring stage")
            }
        };
        let op = &mut state.inflight[index];
        op.state = OpState::Done(finished);
        self.outstanding_ops -= 1;
        self.pump(id);
        self.pump_waiters();
    }

    fn finish_blocking(&mut self, id: u64, outcome: BlockingOutcome) {
        let Some(state) = self.files.get_mut(&id) else {
            unreachable!("blocking completion arrived for a removed file")
        };
        // Fences run with the pipeline otherwise empty, so the blocking
        // operation is the front entry.
        let op = state
            .inflight
            .front_mut()
            .expect("blocking completion arrived for an idle file");
        let previous = std::mem::replace(&mut op.state, OpState::Resolving);
        let finished = match (previous, outcome) {
            (OpState::SetLen { response }, BlockingOutcome::SetLen { len, result }) => match result
            {
                Ok(()) => Finished::SetLen(response, Ok(SetLenSuccess { len })),
                Err(error) => {
                    state.recovery_required = true;
                    Finished::SetLen(response, Err(set_len_failure(error)))
                }
            },
            _ => unreachable!("blocking completion did not match the active stage"),
        };
        let op = state
            .inflight
            .front_mut()
            .expect("blocking operation is still front");
        op.state = OpState::Done(finished);
        self.outstanding_ops -= 1;
        self.pump(id);
        self.pump_waiters();
    }

    /// Serves the waiter FIFO in order while the budget has room.
    ///
    /// Service stops at the first head the budget refuses: that file
    /// returns to the front and the free budget stays reserved for it, so
    /// a two-slot sync admits after at most the operations already in
    /// flight instead of being starved by later one-slot admissions.
    /// Every iteration either stops or permanently consumes a waiter, so
    /// service terminates even when nothing fits.
    fn pump_waiters(&mut self) {
        while self.ring_in_flight < self.max_in_flight {
            let Some(id) = self.ring_waiters.pop_front() else {
                return;
            };
            if let Some(state) = self.files.get_mut(&id) {
                state.parked = false;
            }
            if self.pump_as(id, PumpSource::FrontWaiter) {
                return;
            }
        }
    }
}

fn complete_recovery_required(command: Command) {
    match command {
        Command::Read { request, response } => {
            response.complete(Err(CompletionError::not_applied(ReadAtFailure {
                error: StorageError::RecoveryRequired,
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        Command::Write { request, response } => {
            response.complete(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::RecoveryRequired,
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        Command::SetLen { response, .. } => {
            response.complete(Err(CompletionError::not_applied(
                StorageError::RecoveryRequired,
            )));
        }
        Command::Len { response } => {
            response.complete(Err(CompletionError::not_applied(
                StorageError::RecoveryRequired,
            )));
        }
        Command::Sync { response } => {
            response.complete(Err(CompletionError::not_applied(
                StorageError::RecoveryRequired,
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::block_on;
    use kr_runtime::CompletionCertainty;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Waker};
    use std::time::Duration;

    fn is_pending<T>(future: &mut UringOperation<T>) -> bool {
        let mut context = Context::from_waker(Waker::noop());
        Pin::new(future).poll(&mut context).is_pending()
    }

    /// A coordinator whose thread is the test itself: commands and
    /// completions are delivered one event at a time, so admission decisions
    /// are asserted deterministically instead of raced against the kernel.
    struct Harness {
        coordinator: Coordinator,
        ingress: Arc<Ingress<PoolMessage>>,
        config: UringPoolConfig,
    }

    impl Harness {
        fn new(max_in_flight: usize) -> Self {
            let config = UringPoolConfig {
                max_read_bytes: 256,
                max_write_bytes: 256,
                max_file_bytes: 16 * 1024,
                file_queue_capacity: 8,
                ring_entries: 8,
                max_in_flight,
                blocking_threads: 1,
                max_io_chunk_bytes: 256,
            };
            let ring = Ring::for_pool(
                RingCapacity {
                    entries: config.ring_entries,
                    transient: config.max_in_flight,
                    sustained: 0,
                },
                config.max_io_chunk_bytes,
            )
            .expect("create test ring");
            let (routed, completions) = mpsc::channel();
            let ingress = Arc::new(Ingress::new(routed.clone()));
            Self {
                coordinator: Coordinator {
                    ring,
                    routed,
                    completions,
                    ingress: Arc::clone(&ingress),
                    env: UringEnv::new(UringEnvConfig {
                        blocking_threads: 1,
                    })
                    .expect("create test environment"),
                    files: HashMap::new(),
                    tokens: HashMap::new(),
                    next_token: WAKE_TOKEN + 1,
                    ring_in_flight: 0,
                    ring_waiters: VecDeque::new(),
                    outstanding_ops: 0,
                    max_in_flight,
                    shutting_down: false,
                },
                ingress,
                config,
            }
        }

        fn register(&self, id: u64, name: &str) -> Arc<FileControl> {
            let path = std::env::temp_dir().join(format!(
                "kr-runtime-io-uring-pooled-coordinator-{name}-{}",
                std::process::id()
            ));
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .expect("open backing file");
            // The descriptor keeps the unlinked file alive for the test.
            let _ = std::fs::remove_file(&path);
            Arc::new(FileControl {
                id,
                file,
                config: self.config,
                in_flight: AtomicUsize::new(0),
                ingress: Arc::clone(&self.ingress),
            })
        }

        fn submit(&mut self, control: &Arc<FileControl>, command: Command) {
            control.in_flight.fetch_add(1, Ordering::AcqRel);
            self.ingress.push(PoolMessage::Command {
                file: Arc::clone(control),
                command: TerminalCommand::new(command),
            });
            self.coordinator.drain_ingress();
        }

        /// Feeds the next real ring completion to the coordinator, skipping
        /// the wake sentinels `Ingress::push` interleaves on the channel.
        fn deliver_ring_completion(&mut self) {
            loop {
                let completion = self
                    .coordinator
                    .completions
                    .recv_timeout(Duration::from_secs(10))
                    .expect("ring completion arrives");
                if completion.token == WAKE_TOKEN {
                    continue;
                }
                self.coordinator.finish_ring(completion);
                return;
            }
        }

        fn waiters(&self) -> Vec<u64> {
            self.coordinator.ring_waiters.iter().copied().collect()
        }
    }

    #[test]
    fn a_second_sync_admits_once_the_first_pair_fully_completes() {
        let mut harness = Harness::new(2);
        let first = harness.register(1, "second-sync-first");
        let second = harness.register(2, "second-sync-second");

        let (first_sync, first_response) = operation();
        harness.submit(
            &first,
            Command::Sync {
                response: first_response,
            },
        );
        assert_eq!(
            harness.coordinator.ring_in_flight, 2,
            "the fsync-statx pair holds both slots"
        );
        assert!(harness.waiters().is_empty());

        let (mut second_sync, second_response) = operation();
        harness.submit(
            &second,
            Command::Sync {
                response: second_response,
            },
        );
        assert_eq!(harness.coordinator.ring_in_flight, 2);
        assert_eq!(harness.waiters(), [2], "the second sync parks");

        // Half of the first pair: one slot frees, but the parked sync needs
        // two, so the slot stays reserved and the coordinator must return to
        // its event loop for the second half instead of spinning on the
        // waiter it cannot serve.
        harness.deliver_ring_completion();
        assert_eq!(harness.coordinator.ring_in_flight, 1);
        assert_eq!(harness.waiters(), [2]);
        assert!(is_pending(&mut second_sync));

        harness.deliver_ring_completion();
        assert_eq!(
            block_on(first_sync)
                .expect("first sync terminalizes")
                .durable_len,
            0
        );
        assert_eq!(
            harness.coordinator.ring_in_flight, 2,
            "both slots freed, so the parked pair admitted"
        );
        assert!(harness.waiters().is_empty());

        harness.deliver_ring_completion();
        harness.deliver_ring_completion();
        assert_eq!(
            block_on(second_sync)
                .expect("second sync terminalizes")
                .durable_len,
            0
        );
        assert_eq!(harness.coordinator.ring_in_flight, 0);
        assert!(harness.coordinator.tokens.is_empty());
        assert_eq!(harness.coordinator.outstanding_ops, 0);
    }

    #[test]
    fn a_poisoned_ring_fences_every_pool_file_fail_closed() {
        let mut harness = Harness::new(2);
        let first = harness.register(1, "poison-first");
        let second = harness.register(2, "poison-second");
        harness.coordinator.ring.poison_for_test();

        // The first submission observes the poison at the submit boundary:
        // nothing was staged, so the failure is NotApplied with the buffer
        // returned, and the file fences.
        let (write, write_response) = operation();
        harness.submit(
            &first,
            Command::Write {
                request: WriteAtRequest::new(0, vec![7; 8]),
                response: write_response,
            },
        );
        let refused = block_on(write).expect_err("poisoned submission fails");
        assert_eq!(refused.certainty(), CompletionCertainty::NotApplied);
        assert!(matches!(
            refused.error().error,
            StorageError::Backend { .. }
        ));
        assert_eq!(refused.error().buffer, vec![7; 8], "the buffer came back");

        // Every later command on the fenced file fails RecoveryRequired
        // before any effect.
        let (read, read_response) = operation();
        harness.submit(
            &first,
            Command::Read {
                request: ReadAtRequest::new(0, vec![0; 8]),
                response: read_response,
            },
        );
        let fenced = block_on(read).expect_err("fenced file refuses later commands");
        assert_eq!(fenced.certainty(), CompletionCertainty::NotApplied);
        assert!(matches!(
            fenced.error().error,
            StorageError::RecoveryRequired
        ));
        assert_eq!(fenced.error().buffer, vec![0; 8]);

        // The blast radius is the whole provider: the second file observes
        // the poison on its own first submission — a sync, whose failure is
        // honest about unknown durability — and fences identically.
        let (sync, sync_response) = operation();
        harness.submit(
            &second,
            Command::Sync {
                response: sync_response,
            },
        );
        let uncertain = block_on(sync).expect_err("poisoned sync fails");
        assert_eq!(uncertain.certainty(), CompletionCertainty::MayHaveApplied);
        assert!(matches!(uncertain.error(), StorageError::Backend { .. }));
        let (second_read, second_read_response) = operation();
        harness.submit(
            &second,
            Command::Read {
                request: ReadAtRequest::new(0, vec![0; 4]),
                response: second_read_response,
            },
        );
        let second_fenced = block_on(second_read).expect_err("second file is fenced");
        assert!(matches!(
            second_fenced.error().error,
            StorageError::RecoveryRequired
        ));

        // Nothing reached the ring and nothing is owed: teardown is clean.
        assert_eq!(harness.coordinator.ring_in_flight, 0);
        assert_eq!(harness.coordinator.outstanding_ops, 0);
        assert!(harness.coordinator.tokens.is_empty());
    }

    #[test]
    fn a_parked_sync_is_served_before_later_single_slot_commands() {
        let mut harness = Harness::new(2);
        let busy = harness.register(1, "fifo-busy");
        let syncing = harness.register(2, "fifo-syncing");

        let (first_read, first_response) = operation();
        harness.submit(
            &busy,
            Command::Read {
                request: ReadAtRequest::new(0, vec![0; 8]),
                response: first_response,
            },
        );
        let (second_read, second_response) = operation();
        harness.submit(
            &busy,
            Command::Read {
                request: ReadAtRequest::new(0, vec![0; 8]),
                response: second_response,
            },
        );
        assert_eq!(harness.coordinator.ring_in_flight, 2);

        let (sync, sync_response) = operation();
        harness.submit(
            &syncing,
            Command::Sync {
                response: sync_response,
            },
        );
        assert_eq!(harness.waiters(), [2]);

        let (mut third_read, third_response) = operation();
        harness.submit(
            &busy,
            Command::Read {
                request: ReadAtRequest::new(0, vec![0; 8]),
                response: third_response,
            },
        );
        assert_eq!(
            harness.waiters(),
            [2, 1],
            "the sync got there first, so the later read queues behind it"
        );

        // One read completes: the freed slot is reserved for the front
        // sync, which still needs two — the parked read may not take it.
        harness.deliver_ring_completion();
        assert_eq!(harness.coordinator.ring_in_flight, 1);
        assert_eq!(harness.waiters(), [2, 1]);
        assert_eq!(
            harness.coordinator.tokens.len(),
            1,
            "no new admission consumed the reserved slot"
        );

        // The second read completes: both slots free, and FIFO order admits
        // the sync pair ahead of the parked read.
        harness.deliver_ring_completion();
        assert_eq!(harness.coordinator.ring_in_flight, 2);
        assert_eq!(harness.waiters(), [1]);
        assert!(
            harness.coordinator.tokens.values().all(|&file| file == 2),
            "only the sync pair is on the ring"
        );
        assert!(is_pending(&mut third_read));
        assert_eq!(
            block_on(first_read)
                .expect("first read terminalizes")
                .bytes_read,
            0
        );
        assert_eq!(
            block_on(second_read)
                .expect("second read terminalizes")
                .bytes_read,
            0
        );

        // Half of the sync pair completes: the sync already admitted, so
        // the reservation is over and the parked read takes the free slot.
        harness.deliver_ring_completion();
        assert_eq!(harness.coordinator.ring_in_flight, 2);
        assert!(harness.waiters().is_empty());

        harness.deliver_ring_completion();
        harness.deliver_ring_completion();
        assert_eq!(
            block_on(sync)
                .expect("parked sync terminalizes")
                .durable_len,
            0
        );
        assert_eq!(
            block_on(third_read)
                .expect("third read terminalizes")
                .bytes_read,
            0
        );
        assert_eq!(harness.coordinator.ring_in_flight, 0);
        assert!(harness.coordinator.tokens.is_empty());
        assert_eq!(harness.coordinator.outstanding_ops, 0);
    }
}
