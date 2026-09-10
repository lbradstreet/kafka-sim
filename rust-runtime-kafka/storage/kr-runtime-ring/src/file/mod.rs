//! Fixed-length, checksummed circular file ring over [`kr_runtime_io::FileIoSubmit`].

mod format;

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use kr_runtime::{
    CompletionCertainty, CompletionError, CompletionResult, Handle, SpawnError, contain_panic,
};
use kr_runtime_io::completion::{SyncOperation, SyncResponder};
use kr_runtime_io::{
    FileIoSubmit, ReadAtFailure, ReadAtRequest, StorageError, StorageOperation, WriteAtFailure,
    WriteAtRequest,
};

use self::format::{
    Checkpoint, DATA_OFFSET, FRAME_HEADER_LEN, FrameKind, Geometry, SUPERBLOCK_COUNT,
    SUPERBLOCK_LEN, Superblock, decode_data_frame, decode_frame_header, decode_padding_frame,
    decode_superblock, encode_data_frame, encode_padding_frame, encode_superblock,
};
use super::{
    AppendFailure, AppendRequest, AppendSuccess, ReadPage, ReadRequest, RingCursor, RingError,
    RingLimits, RingOperation, RingPhysicalStatus, RingPosition, RingReader, RingRecord,
    RingStatus, RingWriter, SyncFailure, SyncSuccess, TrimSuccess, apply_trim, assemble_read_page,
    check_append_admission, empty_page, lock_unpoisoned, plan_read_page, read_page_interval,
    reserve_read_page, validate_append_request, validate_read_request,
};

type AppendResult = CompletionResult<AppendSuccess, AppendFailure>;
type ReadResult = CompletionResult<ReadPage, RingError>;
type StatusResult = CompletionResult<RingStatus, RingError>;
type TrimResult = CompletionResult<TrimSuccess, RingError>;
type SyncResult = CompletionResult<SyncSuccess, SyncFailure>;

/// Fixed logical, physical, I/O, and actor limits for one file ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRingConfig {
    pub limits: RingLimits,
    /// Logical bytes in the circular data area.
    ///
    /// Creation currently uses `set_len`; it does not promise filesystem block
    /// reservation. A later append can still fail with a backend out-of-space
    /// error even while physical diagnostics report logical free bytes.
    pub data_capacity_bytes: u64,
    /// Maximum bytes placed in one lower-level read or write request.
    pub max_io_request_bytes: usize,
    /// Maximum admitted commands, including the command currently doing I/O.
    pub command_queue_capacity: usize,
}

impl Default for FileRingConfig {
    fn default() -> Self {
        Self {
            limits: RingLimits::default(),
            data_capacity_bytes: 64 * 1_024 * 1_024,
            max_io_request_bytes: 64 * 1_024,
            command_queue_capacity: 64,
        }
    }
}

impl FileRingConfig {
    /// Validates every provider-neutral format and actor bound.
    ///
    /// Production adapters can call this before opening a path so invalid
    /// configuration has no filesystem side effects.
    pub fn validate(self) -> Result<(), FileRingOpenError> {
        validate_config(self)
    }

    /// Returns the exact v1 file length, including both superblocks.
    ///
    /// This checked derivation is also the `FileIoSubmit` provider's required
    /// maximum file bound. It validates only the length arithmetic; call
    /// [`Self::validate`] to validate the complete configuration.
    pub fn physical_file_bytes(self) -> Result<u64, FileRingOpenError> {
        DATA_OFFSET
            .checked_add(self.data_capacity_bytes)
            .ok_or_else(|| FileRingOpenError::InvalidConfig {
                field: "data_capacity_bytes",
                message: "physical file length overflows u64".to_owned(),
            })
    }
}

/// Failure to initialize or recover a physical ring.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FileRingOpenError {
    InvalidConfig {
        field: &'static str,
        message: String,
    },
    Storage {
        action: &'static str,
        error: StorageError,
    },
    Corrupt {
        offset: u64,
        message: String,
    },
    Spawn(SpawnError),
}

impl fmt::Display for FileRingOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, message } => {
                write!(formatter, "invalid file ring {field}: {message}")
            }
            Self::Storage { action, error } => write!(formatter, "could not {action}: {error}"),
            Self::Corrupt { offset, message } => {
                write!(formatter, "corrupt ring at byte {offset}: {message}")
            }
            Self::Spawn(error) => write!(formatter, "could not spawn file ring actor: {error}"),
        }
    }
}

impl std::error::Error for FileRingOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage { error, .. } => Some(error),
            Self::Spawn(error) => Some(error),
            Self::InvalidConfig { .. } | Self::Corrupt { .. } => None,
        }
    }
}

/// Owned response future for a [`FileRing`] operation.
///
/// Dropping the response abandons only its delivery; it does not cancel an
/// admitted ring operation.
pub type FileRingOperation<T> = SyncOperation<T>;

enum Command {
    Append {
        request: AppendRequest,
        payload_bytes: usize,
        response: SyncResponder<AppendResult>,
    },
    Read {
        request: ReadRequest,
        response: SyncResponder<ReadResult>,
    },
    Status {
        response: SyncResponder<StatusResult>,
    },
    Trim {
        before: RingCursor,
        response: SyncResponder<TrimResult>,
    },
    Sync {
        response: SyncResponder<SyncResult>,
    },
}

struct ActorState {
    queue: VecDeque<Command>,
    in_flight: usize,
    external_handles: usize,
    accepting: bool,
    worker_waker: Option<Waker>,
}

/// Cloneable ring handle whose actor owns an arbitrary [`FileIoSubmit`] backend.
pub struct FileRing<F> {
    actor: Arc<Mutex<ActorState>>,
    config: FileRingConfig,
    marker: PhantomData<fn() -> F>,
}

impl<F> Clone for FileRing<F> {
    fn clone(&self) -> Self {
        lock_unpoisoned(&self.actor).external_handles += 1;
        Self {
            actor: Arc::clone(&self.actor),
            config: self.config,
            marker: PhantomData,
        }
    }
}

impl<F> Drop for FileRing<F> {
    fn drop(&mut self) {
        let waker = {
            let mut actor = lock_unpoisoned(&self.actor);
            actor.external_handles -= 1;
            if actor.external_handles == 0 {
                actor.accepting = false;
                actor.worker_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            // Last-handle destruction must not let an executor-provided waker
            // unwind through `Drop`, especially while the caller is already
            // unwinding. The actor will also observe closed admission when it
            // is next polled by a conforming executor.
            contain_panic(|| waker.wake());
        }
    }
}

impl<F> FileRing<F>
where
    F: FileIoSubmit,
{
    /// Initializes an empty, exact-length ring and starts its bounded actor.
    ///
    /// The ring must exclusively own the supplied file session. Retained clones
    /// may observe it, but issuing positional I/O through them while the ring is
    /// live violates the format protocol. Production openers enforce this by
    /// keeping the underlying file handle private.
    pub async fn create(
        handle: Handle,
        file: F,
        config: FileRingConfig,
    ) -> CompletionResult<Self, FileRingOpenError> {
        let driver = FileRingDriver::create(file, config).await?;
        Self::spawn_driver(handle, driver)
    }

    /// Recovers an existing exact-length ring and starts its bounded actor.
    ///
    /// The ring must exclusively own the supplied file session; see
    /// [`Self::create`].
    pub async fn open(
        handle: Handle,
        file: F,
        config: FileRingConfig,
    ) -> CompletionResult<Self, FileRingOpenError> {
        let driver = FileRingDriver::open(file, config).await?;
        Self::spawn_driver(handle, driver)
    }

    fn spawn_driver(
        handle: Handle,
        driver: FileRingDriver<F>,
    ) -> CompletionResult<Self, FileRingOpenError> {
        let (ring, actor) = driver.start();
        let _join = handle
            .spawn(actor)
            .map_err(|error| CompletionError::may_have_applied(FileRingOpenError::Spawn(error)))?;
        Ok(ring)
    }

    fn admit(&self, command: Command) -> Result<(), (Command, RingError)> {
        let limit = self.config.command_queue_capacity;
        let waker = {
            let mut actor = lock_unpoisoned(&self.actor);
            if !actor.accepting {
                return Err((command, RingError::RecoveryRequired));
            }
            if actor.in_flight == limit {
                return Err((command, RingError::Backpressure { limit }));
            }
            actor.in_flight += 1;
            actor.queue.push_back(command);
            actor.worker_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
}

impl<F> RingReader for FileRing<F>
where
    F: FileIoSubmit,
{
    type ReadFuture = FileRingOperation<ReadResult>;
    type StatusFuture = FileRingOperation<StatusResult>;

    fn read(&self, request: ReadRequest) -> Self::ReadFuture {
        if let Err(error) = validate_read_request(request, self.config.limits) {
            return FileRingOperation::ready(Err(CompletionError::not_applied(error)));
        }
        let (future, response) = FileRingOperation::channel();
        match self.admit(Command::Read { request, response }) {
            Ok(()) => future,
            Err((Command::Read { response, .. }, error)) => {
                response.complete(Err(CompletionError::not_applied(error)));
                future
            }
            Err(_) => unreachable!("admit returned a different command"),
        }
    }

    fn status(&self) -> Self::StatusFuture {
        let (future, response) = FileRingOperation::channel();
        match self.admit(Command::Status { response }) {
            Ok(()) => future,
            Err((Command::Status { response }, error)) => {
                response.complete(Err(CompletionError::not_applied(error)));
                future
            }
            Err(_) => unreachable!("admit returned a different command"),
        }
    }
}

impl<F> RingWriter for FileRing<F>
where
    F: FileIoSubmit,
{
    type AppendFuture = FileRingOperation<AppendResult>;
    type TrimFuture = FileRingOperation<TrimResult>;
    type SyncFuture = FileRingOperation<SyncResult>;

    fn append(&self, request: AppendRequest) -> Self::AppendFuture {
        let payload_bytes = match validate_append_request(&request, self.config.limits) {
            Ok(payload_bytes) => payload_bytes,
            Err(error) => {
                return FileRingOperation::ready(Err(CompletionError::not_applied(
                    AppendFailure {
                        error,
                        records: request.records,
                        accepted_range: None,
                    },
                )));
            }
        };
        let (future, response) = FileRingOperation::channel();
        match self.admit(Command::Append {
            request,
            payload_bytes,
            response,
        }) {
            Ok(()) => future,
            Err((
                Command::Append {
                    request, response, ..
                },
                error,
            )) => {
                response.complete(Err(CompletionError::not_applied(AppendFailure {
                    error,
                    records: request.records,
                    accepted_range: None,
                })));
                future
            }
            Err(_) => unreachable!("admit returned a different command"),
        }
    }

    fn trim(&self, before: RingCursor) -> Self::TrimFuture {
        let (future, response) = FileRingOperation::channel();
        match self.admit(Command::Trim { before, response }) {
            Ok(()) => future,
            Err((Command::Trim { response, .. }, error)) => {
                response.complete(Err(CompletionError::not_applied(error)));
                future
            }
            Err(_) => unreachable!("admit returned a different command"),
        }
    }

    fn sync(&self) -> Self::SyncFuture {
        let (future, response) = FileRingOperation::channel();
        match self.admit(Command::Sync { response }) {
            Ok(()) => future,
            Err((Command::Sync { response }, error)) => {
                response.complete(Err(CompletionError::not_applied(SyncFailure {
                    error,
                    checkpoint: None,
                })));
                future
            }
            Err(_) => unreachable!("admit returned a different command"),
        }
    }
}

struct NextCommand {
    actor: Arc<Mutex<ActorState>>,
}

impl Future for NextCommand {
    type Output = Option<Command>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut actor = lock_unpoisoned(&self.actor);
        if let Some(command) = actor.queue.pop_front() {
            Poll::Ready(Some(command))
        } else if actor.accepting {
            actor.worker_waker = Some(context.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

fn finish_command(actor: &Arc<Mutex<ActorState>>) {
    lock_unpoisoned(actor).in_flight -= 1;
}

#[derive(Clone, Copy)]
enum SyncCancellation {
    NotApplied,
    MayHaveApplied { checkpoint: SyncSuccess },
}

struct ActiveCommand {
    actor: Arc<Mutex<ActorState>>,
    command: Option<Command>,
    sync_cancellation: SyncCancellation,
}

impl ActiveCommand {
    fn new(actor: Arc<Mutex<ActorState>>, command: Command) -> Self {
        Self {
            actor,
            command: Some(command),
            sync_cancellation: SyncCancellation::NotApplied,
        }
    }

    fn finish(mut self) -> Command {
        let command = self.command.take().expect("active command exists");
        finish_command(&self.actor);
        command
    }
}

impl Drop for ActiveCommand {
    fn drop(&mut self) {
        let Some(command) = self.command.take() else {
            return;
        };
        finish_command(&self.actor);
        cancel_command(command, self.sync_cancellation);
    }
}

struct ActorTerminationGuard {
    actor: Arc<Mutex<ActorState>>,
}

impl ActorTerminationGuard {
    fn new(actor: Arc<Mutex<ActorState>>) -> Self {
        Self { actor }
    }
}

impl Drop for ActorTerminationGuard {
    fn drop(&mut self) {
        let (queued, worker_waker) = {
            let mut actor = lock_unpoisoned(&self.actor);
            actor.accepting = false;
            let queued = std::mem::take(&mut actor.queue);
            debug_assert!(actor.in_flight >= queued.len());
            actor.in_flight -= queued.len();
            (queued, actor.worker_waker.take())
        };
        for command in queued {
            cancel_command(command, SyncCancellation::NotApplied);
        }
        if let Some(waker) = worker_waker {
            contain_panic(|| waker.wake());
        }
    }
}

fn cancel_command(command: Command, sync_cancellation: SyncCancellation) {
    match command {
        Command::Append {
            request, response, ..
        } => {
            response.complete(Err(CompletionError::not_applied(AppendFailure {
                error: RingError::RecoveryRequired,
                records: request.records,
                accepted_range: None,
            })));
        }
        Command::Read { response, .. } => {
            response.complete(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )));
        }
        Command::Status { response } => {
            response.complete(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )));
        }
        Command::Trim { response, .. } => {
            response.complete(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )));
        }
        Command::Sync { response } => {
            let error = SyncFailure {
                error: RingError::RecoveryRequired,
                checkpoint: match sync_cancellation {
                    SyncCancellation::NotApplied => None,
                    SyncCancellation::MayHaveApplied { checkpoint } => Some(checkpoint),
                },
            };
            response.complete(Err(match sync_cancellation {
                SyncCancellation::NotApplied => CompletionError::not_applied(error),
                SyncCancellation::MayHaveApplied { .. } => CompletionError::may_have_applied(error),
            }));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecordLocation {
    sequence: u64,
    offset: u64,
    encoded_len: usize,
    allocation_start: u64,
    allocation_len: u64,
    payload_len: usize,
}

/// Executor-neutral state machine for a file-backed ring.
///
/// `create` and `open` perform all initialization/recovery I/O without
/// spawning. Call [`Self::start`] afterwards and drive the returned future on
/// any local executor. The supplied [`FileIoSubmit`] session is exclusively owned by
/// this driver for its lifetime; external I/O through a retained clone can
/// invalidate both its index and crash protocol.
pub struct FileRingDriver<F> {
    file: F,
    config: FileRingConfig,
    records: VecDeque<RecordLocation>,
    retained_payload_bytes: usize,
    protected_used_bytes: u64,
    allocator_tail_offset: u64,
    accepted_head: RingCursor,
    accepted_tail: RingCursor,
    durable_head: RingCursor,
    durable_tail: RingCursor,
    durable_head_offset: u64,
    durable_tail_offset: u64,
    active_slot: u8,
    generation: u64,
    recovery_required: bool,
}

impl<F> FileRingDriver<F>
where
    F: FileIoSubmit,
{
    /// Creates a new ring in a zero-length file.
    pub async fn create(
        file: F,
        config: FileRingConfig,
    ) -> CompletionResult<Self, FileRingOpenError> {
        config.validate().map_err(CompletionError::not_applied)?;
        let length = file
            .submit_len()
            .await
            .map_err(|error| map_open_storage("inspect new ring length", error))?
            .len;
        if length != 0 {
            return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                offset: 0,
                message: format!("create requires an empty file, found {length} bytes"),
            }));
        }

        let file_len = config
            .physical_file_bytes()
            .map_err(CompletionError::not_applied)?;
        if let Err(error) = confirm_set_len(&file, file_len).await {
            let certainty = error.certainty();
            let original = FileRingOpenError::Storage {
                action: "set new ring length",
                error: error.into_parts().1,
            };
            return if certainty == CompletionCertainty::NotApplied {
                Err(CompletionError::not_applied(original))
            } else {
                Err(rollback_create(&file, original).await)
            };
        }

        let superblock = Superblock {
            physical_slot: 0,
            generation: 1,
            geometry: geometry(config),
            checkpoint: empty_checkpoint(),
        };
        let encoded = encode_superblock(&superblock).map_err(|error| {
            CompletionError::not_applied(FileRingOpenError::InvalidConfig {
                field: "limits",
                message: error.to_string(),
            })
        })?;
        if let Err(failure) =
            write_all_at(&file, 0, encoded.to_vec(), config.max_io_request_bytes).await
        {
            let original = FileRingOpenError::Storage {
                action: "write initial ring superblock",
                error: failure.error,
            };
            return Err(rollback_create(&file, original).await);
        }
        if let Err(error) = confirm_sync(&file).await {
            let original = FileRingOpenError::Storage {
                action: "sync initialized ring",
                error: error.into_parts().1,
            };
            return Err(rollback_create(&file, original).await);
        }

        Ok(Self::empty(file, config))
    }

    /// Opens the highest complete checkpoint and rebuilds its bounded index.
    pub async fn open(
        file: F,
        config: FileRingConfig,
    ) -> CompletionResult<Self, FileRingOpenError> {
        config.validate().map_err(CompletionError::not_applied)?;
        let expected_len = config
            .physical_file_bytes()
            .map_err(CompletionError::not_applied)?;
        let actual_len = file
            .submit_len()
            .await
            .map_err(|error| map_open_storage("inspect ring length", error))?
            .len;
        if actual_len != expected_len {
            return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                offset: actual_len.min(expected_len),
                message: format!(
                    "file length {actual_len} does not match configured exact length {expected_len}"
                ),
            }));
        }

        let mut decoded = Vec::with_capacity(SUPERBLOCK_COUNT);
        let mut decode_errors = Vec::with_capacity(SUPERBLOCK_COUNT);
        for slot in 0..SUPERBLOCK_COUNT {
            let physical_offset =
                u64::try_from(slot * SUPERBLOCK_LEN).expect("superblock area fits in u64");
            let bytes = read_exact_at(
                &file,
                physical_offset,
                SUPERBLOCK_LEN,
                config.max_io_request_bytes,
            )
            .await
            .map_err(|error| map_open_storage("read ring superblock", error))?;
            match decode_superblock(&bytes, slot as u8) {
                Ok(superblock) => {
                    let expected_slot = ((superblock.generation - 1) & 1) as u8;
                    if superblock.physical_slot != expected_slot {
                        return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                            offset: physical_offset,
                            message: format!(
                                "generation {} is noncanonical in slot {}; expected slot {expected_slot}",
                                superblock.generation, superblock.physical_slot
                            ),
                        }));
                    }
                    decoded.push(superblock);
                }
                Err(error) => decode_errors.push((physical_offset, error.to_string())),
            }
        }
        if decoded.is_empty() {
            let message = decode_errors
                .into_iter()
                .map(|(offset, error)| format!("slot at {offset}: {error}"))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                offset: 0,
                message: format!("no valid superblock: {message}"),
            }));
        }
        if decoded.len() == 2 {
            if decoded[0].geometry != decoded[1].geometry {
                return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                    offset: 0,
                    message: "valid superblocks disagree on static geometry".to_owned(),
                }));
            }
            if decoded[0].generation == decoded[1].generation && decoded[0] != decoded[1] {
                return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                    offset: 0,
                    message: format!(
                        "superblocks have split-brain generation {}",
                        decoded[0].generation
                    ),
                }));
            }
            if decoded[0].generation.abs_diff(decoded[1].generation) != 1 {
                return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                    offset: 0,
                    message: format!(
                        "valid superblock generations {} and {} are not adjacent",
                        decoded[0].generation, decoded[1].generation
                    ),
                }));
            }
        }
        let selected = decoded
            .into_iter()
            .max_by_key(|superblock| superblock.generation)
            .expect("at least one decoded superblock");
        let expected_geometry = geometry(config);
        if selected.geometry != expected_geometry {
            return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
                offset: u64::from(selected.physical_slot) * SUPERBLOCK_LEN as u64,
                message: format!(
                    "persisted geometry {:?} does not match configured {:?}",
                    selected.geometry, expected_geometry
                ),
            }));
        }
        validate_checkpoint(&selected, config)?;
        let (records, retained_payload_bytes) =
            recover_records(&file, config, &selected.checkpoint).await?;

        // Reopen is a fence: a page-cache-visible checkpoint selected above is
        // made stable before its logical state is exposed.
        confirm_sync(&file)
            .await
            .map_err(|error| map_open_storage("fence recovered ring", error))?;

        let checkpoint = selected.checkpoint;
        Ok(Self {
            file,
            config,
            records,
            retained_payload_bytes,
            protected_used_bytes: checkpoint.used_bytes,
            allocator_tail_offset: checkpoint.tail_offset,
            accepted_head: RingCursor::new(checkpoint.head_seq),
            accepted_tail: RingCursor::new(checkpoint.tail_seq),
            durable_head: RingCursor::new(checkpoint.head_seq),
            durable_tail: RingCursor::new(checkpoint.tail_seq),
            durable_head_offset: checkpoint.head_offset,
            durable_tail_offset: checkpoint.tail_offset,
            active_slot: selected.physical_slot,
            generation: selected.generation,
            recovery_required: false,
        })
    }

    fn empty(file: F, config: FileRingConfig) -> Self {
        Self {
            file,
            config,
            records: VecDeque::new(),
            retained_payload_bytes: 0,
            protected_used_bytes: 0,
            allocator_tail_offset: 0,
            accepted_head: RingCursor::START,
            accepted_tail: RingCursor::START,
            durable_head: RingCursor::START,
            durable_tail: RingCursor::START,
            durable_head_offset: 0,
            durable_tail_offset: 0,
            active_slot: 0,
            generation: 1,
            recovery_required: false,
        }
    }

    /// Creates a handle and its executor-neutral actor future.
    ///
    /// The actor must be driven for as long as any handle is in use. Dropping
    /// it closes admission and completes every admitted operation with
    /// [`RingError::RecoveryRequired`]. An interrupted sync reports
    /// `MayHaveApplied` with its candidate checkpoint once checkpoint metadata
    /// may have reached the file; other interrupted operations are
    /// `NotApplied` at the logical ring boundary.
    pub fn start(self) -> (FileRing<F>, impl Future<Output = ()> + 'static) {
        self.start_actor()
    }

    fn start_actor(self) -> (FileRing<F>, impl Future<Output = ()> + 'static) {
        let config = self.config;
        let actor = Arc::new(Mutex::new(ActorState {
            queue: VecDeque::new(),
            in_flight: 0,
            external_handles: 1,
            accepting: true,
            worker_waker: None,
        }));
        let handle = FileRing {
            actor: Arc::clone(&actor),
            config,
            marker: PhantomData,
        };
        // Construct the guard outside the async body so dropping an actor
        // future before its first poll still closes admission and drains any
        // commands invoked through the returned handle.
        let termination = ActorTerminationGuard::new(Arc::clone(&actor));
        let future = async move {
            let _termination = termination;
            self.run(actor).await;
        };
        (handle, future)
    }

    async fn run(mut self, actor: Arc<Mutex<ActorState>>) {
        loop {
            let Some(command) = (NextCommand {
                actor: Arc::clone(&actor),
            })
            .await
            else {
                return;
            };
            let mut active = ActiveCommand::new(Arc::clone(&actor), command);
            match active.command.as_ref().expect("active command exists") {
                Command::Append {
                    request,
                    payload_bytes,
                    ..
                } => {
                    let output = self.append(request, *payload_bytes).await;
                    let Command::Append {
                        request, response, ..
                    } = active.finish()
                    else {
                        unreachable!("active command variant is stable")
                    };
                    response.complete(match output {
                        Ok(applied) => Ok(AppendSuccess {
                            first_position: applied.first_position,
                            next_cursor: applied.next_cursor,
                            records: request.records,
                        }),
                        Err(error) => Err(CompletionError::not_applied(AppendFailure {
                            error,
                            records: request.records,
                            accepted_range: None,
                        })),
                    });
                }
                Command::Read { request, .. } => {
                    let output = self.read(*request).await;
                    let Command::Read { response, .. } = active.finish() else {
                        unreachable!("active command variant is stable")
                    };
                    response.complete(output);
                }
                Command::Status { .. } => {
                    let output = self.status();
                    let Command::Status { response } = active.finish() else {
                        unreachable!("active command variant is stable")
                    };
                    response.complete(Ok(output));
                }
                Command::Trim { before, .. } => {
                    let output = self.trim(*before);
                    let Command::Trim { response, .. } = active.finish() else {
                        unreachable!("active command variant is stable")
                    };
                    response.complete(output);
                }
                Command::Sync { .. } => {
                    let output = self.sync(&mut active.sync_cancellation).await;
                    let Command::Sync { response } = active.finish() else {
                        unreachable!("active command variant is stable")
                    };
                    response.complete(output);
                }
            }
        }
    }
}

fn validate_checkpoint(
    selected: &Superblock,
    config: FileRingConfig,
) -> CompletionResult<(), FileRingOpenError> {
    if selected.generation == 0 {
        return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
            offset: u64::from(selected.physical_slot) * SUPERBLOCK_LEN as u64,
            message: "superblock generation must be nonzero".to_owned(),
        }));
    }
    let checkpoint = selected.checkpoint;
    if checkpoint.head_seq == checkpoint.tail_seq
        && (checkpoint.head_offset != 0 || checkpoint.tail_offset != 0)
    {
        return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
            offset: u64::from(selected.physical_slot) * SUPERBLOCK_LEN as u64,
            message: "empty checkpoint must use canonical zero offsets".to_owned(),
        }));
    }
    let count = checkpoint.tail_seq - checkpoint.head_seq;
    if count > config.limits.max_live_records as u64 {
        return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
            offset: u64::from(selected.physical_slot) * SUPERBLOCK_LEN as u64,
            message: format!(
                "checkpoint retains {count} records, exceeding configured {}",
                config.limits.max_live_records
            ),
        }));
    }
    Ok(())
}

struct RecoveryReadBuffer<'a, F> {
    file: &'a F,
    max_request_bytes: usize,
    relative_offset: u64,
    bytes: Vec<u8>,
    start: usize,
}

impl<'a, F> RecoveryReadBuffer<'a, F>
where
    F: FileIoSubmit,
{
    fn new(file: &'a F, max_request_bytes: usize, relative_offset: u64) -> Self {
        Self {
            file,
            max_request_bytes,
            relative_offset,
            bytes: Vec::new(),
            start: 0,
        }
    }

    async fn read_exact(
        &mut self,
        length: usize,
        contiguous_available: u64,
    ) -> CompletionResult<&[u8], FileRingOpenError> {
        let required = u64::try_from(length).map_err(|_| {
            corrupt_frame(
                self.relative_offset,
                "recovery read length does not fit u64".to_owned(),
            )
        })?;
        if required > contiguous_available {
            return Err(corrupt_frame(
                self.relative_offset,
                format!(
                    "recovery needs {required} contiguous bytes but the checkpoint exposes only {contiguous_available}"
                ),
            ));
        }

        while self.available() < length {
            self.compact();
            let buffered = u64::try_from(self.available()).map_err(|_| {
                corrupt_frame(
                    self.relative_offset,
                    "buffered recovery span length does not fit u64".to_owned(),
                )
            })?;
            let unbuffered = contiguous_available.checked_sub(buffered).ok_or_else(|| {
                corrupt_frame(
                    self.relative_offset,
                    "buffered recovery span exceeds the committed contiguous range".to_owned(),
                )
            })?;
            let request_len = usize::try_from(
                unbuffered.min(u64::try_from(self.max_request_bytes).unwrap_or(u64::MAX)),
            )
            .map_err(|_| {
                corrupt_frame(
                    self.relative_offset,
                    "recovery span request length does not fit usize".to_owned(),
                )
            })?;
            if request_len == 0 {
                return Err(corrupt_frame(
                    self.relative_offset,
                    "recovery span made no read progress".to_owned(),
                ));
            }
            self.bytes.try_reserve_exact(request_len).map_err(|error| {
                CompletionError::not_applied(FileRingOpenError::Storage {
                    action: "buffer committed ring recovery span",
                    error: StorageError::Backend {
                        operation: StorageOperation::ReadAt,
                        raw_os_error: None,
                        message: format!(
                            "could not reserve {request_len}-byte recovery span: {error}"
                        ),
                    },
                })
            })?;
            let relative_read_offset =
                self.relative_offset.checked_add(buffered).ok_or_else(|| {
                    corrupt_frame(
                        self.relative_offset,
                        "recovery span offset overflowed".to_owned(),
                    )
                })?;
            let physical_offset =
                DATA_OFFSET
                    .checked_add(relative_read_offset)
                    .ok_or_else(|| {
                        corrupt_frame(
                            self.relative_offset,
                            "physical recovery span offset overflowed".to_owned(),
                        )
                    })?;
            let chunk = read_exact_at(
                self.file,
                physical_offset,
                request_len,
                self.max_request_bytes,
            )
            .await
            .map_err(|error| map_open_storage("read committed ring recovery span", error))?;
            self.bytes.extend_from_slice(&chunk);
        }

        Ok(&self.bytes[self.start..self.start + length])
    }

    fn advance(&mut self, length: usize) -> CompletionResult<(), FileRingOpenError> {
        if length > self.available() {
            return Err(corrupt_frame(
                self.relative_offset,
                format!(
                    "recovery tried to consume {length} bytes from a {}-byte buffered span",
                    self.available()
                ),
            ));
        }
        self.start += length;
        self.relative_offset = self
            .relative_offset
            .checked_add(u64::try_from(length).map_err(|_| {
                corrupt_frame(
                    self.relative_offset,
                    "recovery advance length does not fit u64".to_owned(),
                )
            })?)
            .ok_or_else(|| {
                corrupt_frame(
                    self.relative_offset,
                    "recovery buffer offset overflowed".to_owned(),
                )
            })?;
        if self.start == self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
        }
        Ok(())
    }

    fn reset(&mut self, relative_offset: u64) {
        self.relative_offset = relative_offset;
        self.bytes.clear();
        self.start = 0;
    }

    fn available(&self) -> usize {
        self.bytes.len() - self.start
    }

    fn compact(&mut self) {
        if self.start == 0 {
            return;
        }
        let available = self.available();
        self.bytes.copy_within(self.start.., 0);
        self.bytes.truncate(available);
        self.start = 0;
    }
}

async fn recover_records<F>(
    file: &F,
    config: FileRingConfig,
    checkpoint: &Checkpoint,
) -> CompletionResult<(VecDeque<RecordLocation>, usize), FileRingOpenError>
where
    F: FileIoSubmit,
{
    let count = usize::try_from(checkpoint.tail_seq - checkpoint.head_seq).map_err(|_| {
        CompletionError::not_applied(FileRingOpenError::Corrupt {
            offset: 0,
            message: "checkpoint record count cannot be indexed on this platform".to_owned(),
        })
    })?;
    let mut records = VecDeque::new();
    records.try_reserve_exact(count).map_err(|error| {
        CompletionError::not_applied(FileRingOpenError::InvalidConfig {
            field: "limits.max_live_records",
            message: format!("could not reserve recovery index: {error}"),
        })
    })?;
    if count == 0 {
        return Ok((records, 0));
    }

    let capacity = config.data_capacity_bytes;
    let mut cursor = checkpoint.head_offset;
    let mut consumed = 0u64;
    let mut expected_sequence = checkpoint.head_seq;
    let mut retained_payload_bytes = 0usize;
    let mut reader =
        RecoveryReadBuffer::new(file, config.max_io_request_bytes, checkpoint.head_offset);
    while expected_sequence < checkpoint.tail_seq {
        let allocation_start = cursor;
        let remaining = capacity - cursor;
        let mut padding_bytes = 0u64;
        let mut decoded_data_header = None;
        if remaining < FRAME_HEADER_LEN as u64 {
            consumed = checked_recovery_consumed(consumed, remaining, checkpoint, cursor)?;
            padding_bytes = remaining;
            cursor = 0;
            reader.reset(0);
        } else {
            let committed_remaining =
                checkpoint.used_bytes.checked_sub(consumed).ok_or_else(|| {
                    corrupt_frame(cursor, "recovery consumed beyond checkpoint".to_owned())
                })?;
            let contiguous_available = remaining.min(committed_remaining);
            let encoded_header = reader
                .read_exact(FRAME_HEADER_LEN, contiguous_available)
                .await?;
            let header = decode_frame_header(encoded_header, config.limits.max_record_bytes)
                .map_err(|error| corrupt_frame(cursor, error.to_string()))?;
            if header.kind == FrameKind::Padding {
                if header.sequence != expected_sequence {
                    return Err(corrupt_frame(
                        cursor,
                        format!(
                            "padding sequence {} does not match expected {expected_sequence}",
                            header.sequence
                        ),
                    ));
                }
                decode_padding_frame(encoded_header, config.limits.max_record_bytes, remaining)
                    .map_err(|error| corrupt_frame(cursor, error.to_string()))?;
                consumed = checked_recovery_consumed(consumed, remaining, checkpoint, cursor)?;
                padding_bytes = remaining;
                cursor = 0;
                reader.reset(0);
            } else {
                decoded_data_header = Some(header);
            }
        }

        let data_remaining = capacity - cursor;
        if data_remaining < FRAME_HEADER_LEN as u64 {
            return Err(corrupt_frame(
                cursor,
                "padding did not leave room for a data frame".to_owned(),
            ));
        }
        let committed_remaining = checkpoint.used_bytes.checked_sub(consumed).ok_or_else(|| {
            corrupt_frame(cursor, "recovery consumed beyond checkpoint".to_owned())
        })?;
        let contiguous_available = data_remaining.min(committed_remaining);
        let header = match decoded_data_header {
            Some(header) => header,
            None => {
                let encoded_header = reader
                    .read_exact(FRAME_HEADER_LEN, contiguous_available)
                    .await?;
                decode_frame_header(encoded_header, config.limits.max_record_bytes)
                    .map_err(|error| corrupt_frame(cursor, error.to_string()))?
            }
        };
        if header.kind != FrameKind::Data || header.sequence != expected_sequence {
            return Err(corrupt_frame(
                cursor,
                format!("expected DATA sequence {expected_sequence}, decoded {header:?}"),
            ));
        }
        let encoded_len = header.encoded_len();
        let encoded_len_u64 = u64::try_from(encoded_len)
            .map_err(|_| corrupt_frame(cursor, "data frame length does not fit u64".to_owned()))?;
        if encoded_len_u64 > data_remaining {
            return Err(corrupt_frame(
                cursor,
                format!(
                    "data frame of {encoded_len} bytes crosses the physical end with only {data_remaining} bytes remaining"
                ),
            ));
        }
        let next_consumed =
            checked_recovery_consumed(consumed, encoded_len_u64, checkpoint, cursor)?;
        let encoded = reader.read_exact(encoded_len, contiguous_available).await?;
        let (decoded_header, payload) = decode_data_frame(encoded, config.limits.max_record_bytes)
            .map_err(|error| corrupt_frame(cursor, error.to_string()))?;
        if decoded_header.sequence != expected_sequence {
            return Err(corrupt_frame(
                cursor,
                format!(
                    "data sequence {} does not match expected {expected_sequence}",
                    decoded_header.sequence
                ),
            ));
        }
        retained_payload_bytes = retained_payload_bytes
            .checked_add(payload.len())
            .ok_or_else(|| {
                corrupt_frame(cursor, "retained payload byte count overflowed".to_owned())
            })?;
        records.push_back(RecordLocation {
            sequence: expected_sequence,
            offset: cursor,
            encoded_len,
            allocation_start,
            allocation_len: padding_bytes + encoded_len_u64,
            payload_len: payload.len(),
        });
        reader.advance(encoded_len)?;
        consumed = next_consumed;
        cursor += encoded_len_u64;
        if cursor == capacity {
            cursor = 0;
            reader.reset(0);
        }
        expected_sequence += 1;
    }

    if consumed != checkpoint.used_bytes {
        return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
            offset: DATA_OFFSET + cursor,
            message: format!(
                "checkpoint consumed {consumed} bytes, expected {}",
                checkpoint.used_bytes
            ),
        }));
    }
    if cursor != checkpoint.tail_offset {
        return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
            offset: DATA_OFFSET + cursor,
            message: format!(
                "recovered tail offset {cursor}, expected {}",
                checkpoint.tail_offset
            ),
        }));
    }
    if retained_payload_bytes as u64 != checkpoint.retained_payload_bytes {
        return Err(CompletionError::not_applied(FileRingOpenError::Corrupt {
            offset: DATA_OFFSET + checkpoint.head_offset,
            message: format!(
                "recovered {retained_payload_bytes} payload bytes, checkpoint declares {}",
                checkpoint.retained_payload_bytes
            ),
        }));
    }
    Ok((records, retained_payload_bytes))
}

fn checked_recovery_consumed(
    consumed: u64,
    additional: u64,
    checkpoint: &Checkpoint,
    relative_offset: u64,
) -> CompletionResult<u64, FileRingOpenError> {
    let next = consumed.checked_add(additional).ok_or_else(|| {
        corrupt_frame(
            relative_offset,
            "committed byte count overflowed".to_owned(),
        )
    })?;
    if next > checkpoint.used_bytes {
        return Err(corrupt_frame(
            relative_offset,
            format!(
                "frame consumes beyond checkpoint: {next} > {}",
                checkpoint.used_bytes
            ),
        ));
    }
    Ok(next)
}

fn corrupt_frame(relative_offset: u64, message: String) -> CompletionError<FileRingOpenError> {
    CompletionError::not_applied(FileRingOpenError::Corrupt {
        offset: DATA_OFFSET + relative_offset,
        message,
    })
}

fn validate_config(config: FileRingConfig) -> Result<(), FileRingOpenError> {
    config
        .limits
        .validate()
        .map_err(|error| FileRingOpenError::InvalidConfig {
            field: "limits",
            message: error.to_string(),
        })?;
    if config.limits.max_record_bytes > u32::MAX as usize {
        return invalid_config(
            "limits.max_record_bytes",
            format!("must be at most {}", u32::MAX),
        );
    }
    if config.limits.max_live_records > u32::MAX as usize {
        return invalid_config(
            "limits.max_live_records",
            format!("must be at most {}", u32::MAX),
        );
    }
    if config.max_io_request_bytes == 0 {
        return invalid_config("max_io_request_bytes", "must be nonzero");
    }
    if config.command_queue_capacity == 0 {
        return invalid_config("command_queue_capacity", "must be nonzero");
    }
    geometry(config)
        .validate()
        .map_err(|error| FileRingOpenError::InvalidConfig {
            field: "data_capacity_bytes",
            message: error.to_string(),
        })?;
    config.physical_file_bytes()?;
    Ok(())
}

fn invalid_config<T>(
    field: &'static str,
    message: impl Into<String>,
) -> Result<T, FileRingOpenError> {
    Err(FileRingOpenError::InvalidConfig {
        field,
        message: message.into(),
    })
}

struct WriteAllFailure {
    progressed: usize,
    certainty: CompletionCertainty,
    error: StorageError,
}

impl WriteAllFailure {
    const fn needs_rollback(&self) -> bool {
        self.progressed != 0 || !matches!(self.certainty, CompletionCertainty::NotApplied)
    }
}

async fn write_all_at<F>(
    file: &F,
    mut offset: u64,
    mut remaining: Vec<u8>,
    max_request_bytes: usize,
) -> Result<(), WriteAllFailure>
where
    F: FileIoSubmit,
{
    let mut progressed = 0usize;
    while !remaining.is_empty() {
        let tail = if remaining.len() > max_request_bytes {
            remaining.split_off(max_request_bytes)
        } else {
            Vec::new()
        };
        let requested = remaining.len();
        match file
            .submit_write_at(WriteAtRequest::new(offset, remaining))
            .await
        {
            Ok(success) => {
                if success.bytes_written == 0
                    || success.bytes_written > success.buffer.len()
                    || success.buffer.len() != requested
                {
                    return Err(WriteAllFailure {
                        progressed,
                        certainty: CompletionCertainty::MayHaveApplied,
                        error: StorageError::Backend {
                            operation: StorageOperation::WriteAt,
                            raw_os_error: None,
                            message: format!(
                                "backend reported {} bytes written and returned a {}-byte buffer for a {requested}-byte request",
                                success.bytes_written,
                                success.buffer.len()
                            ),
                        },
                    });
                }
                progressed += success.bytes_written;
                offset =
                    offset
                        .checked_add(success.bytes_written as u64)
                        .ok_or(WriteAllFailure {
                            progressed,
                            certainty: CompletionCertainty::MayHaveApplied,
                            error: StorageError::OffsetOverflow,
                        })?;
                let mut returned = success.buffer;
                returned.drain(..success.bytes_written);
                returned.extend(tail);
                remaining = returned;
            }
            Err(error) => {
                let (
                    certainty,
                    WriteAtFailure {
                        error,
                        buffer: _,
                        bytes_transferred,
                    },
                ) = error.into_parts();
                return Err(WriteAllFailure {
                    progressed: progressed.saturating_add(bytes_transferred),
                    certainty,
                    error,
                });
            }
        }
    }
    Ok(())
}

/// The aggregated failure of a pipelined plan write.
struct WritePlanFailure {
    /// The first settled failure in admission order.
    primary: WriteAllFailure,
    /// Whether any settled outcome requires recovery fencing, even when the
    /// primary failure alone would not.
    requires_recovery: bool,
}

fn requires_recovery_outcome(failure: &WriteAllFailure) -> bool {
    failure.certainty == CompletionCertainty::MayHaveApplied
        || storage_requires_recovery(&failure.error)
}

/// Splits planned writes into request-sized chunks with precomputed offsets.
///
/// Chunking happens before any submission so every arithmetic failure is a
/// clean `NotApplied` rejection: once [`write_plan`] starts admitting, the
/// only remaining outcomes are settled completions.
fn plan_write_chunks(
    writes: Vec<PlannedWrite>,
    max_request_bytes: usize,
) -> Result<Vec<PlannedWrite>, WriteAllFailure> {
    let not_applied = |error: StorageError| WriteAllFailure {
        progressed: 0,
        certainty: CompletionCertainty::NotApplied,
        error,
    };
    let mut chunks = Vec::new();
    let total = writes
        .iter()
        .map(|write| write.bytes.len().div_ceil(max_request_bytes.max(1)).max(1))
        .sum();
    chunks.try_reserve_exact(total).map_err(|error| {
        not_applied(StorageError::Backend {
            operation: StorageOperation::WriteAt,
            raw_os_error: None,
            message: format!("could not reserve chunked write plan: {error}"),
        })
    })?;
    for write in writes {
        if write.bytes.is_empty() {
            continue;
        }
        let mut offset = write.offset;
        let mut remaining = write.bytes;
        loop {
            let tail = if remaining.len() > max_request_bytes {
                remaining.split_off(max_request_bytes)
            } else {
                Vec::new()
            };
            let len = remaining.len();
            chunks.push(PlannedWrite {
                offset,
                bytes: remaining,
            });
            if tail.is_empty() {
                break;
            }
            offset = offset
                .checked_add(len as u64)
                .ok_or_else(|| not_applied(StorageError::OffsetOverflow))?;
            remaining = tail;
        }
    }
    Ok(chunks)
}

/// Why one settled chunk did not complete.
enum SettleError {
    /// A resumable bound refused the chunk's admission and returned its
    /// bytes intact. The chunk can be resubmitted after earlier chunks
    /// release their reservations.
    Exhausted {
        offset: u64,
        bytes: Vec<u8>,
        error: StorageError,
    },
    Failed(WriteAllFailure),
}

/// Settles one admitted chunk: validates the transfer, retries short writes
/// at their advanced offset, and normalizes failures like [`write_all_at`].
/// A retry targets only this chunk's remaining range, so it commutes with
/// every other chunk still in flight.
async fn settle_write<F>(
    file: &F,
    mut offset: u64,
    mut requested: usize,
    mut response: F::WriteAtResponse,
) -> Result<(), SettleError>
where
    F: FileIoSubmit,
{
    let mut progressed = 0usize;
    loop {
        match response.await {
            Ok(success) => {
                if success.bytes_written == 0
                    || success.bytes_written > success.buffer.len()
                    || success.buffer.len() != requested
                {
                    return Err(SettleError::Failed(WriteAllFailure {
                        progressed,
                        certainty: CompletionCertainty::MayHaveApplied,
                        error: StorageError::Backend {
                            operation: StorageOperation::WriteAt,
                            raw_os_error: None,
                            message: format!(
                                "backend reported {} bytes written and returned a {}-byte buffer for a {requested}-byte request",
                                success.bytes_written,
                                success.buffer.len()
                            ),
                        },
                    }));
                }
                progressed += success.bytes_written;
                if success.bytes_written == requested {
                    return Ok(());
                }
                offset =
                    offset
                        .checked_add(success.bytes_written as u64)
                        .ok_or(SettleError::Failed(WriteAllFailure {
                            progressed,
                            certainty: CompletionCertainty::MayHaveApplied,
                            error: StorageError::OffsetOverflow,
                        }))?;
                let mut remaining = success.buffer;
                remaining.drain(..success.bytes_written);
                requested = remaining.len();
                response = file.submit_write_at(WriteAtRequest::new(offset, remaining));
            }
            Err(error) => {
                let (
                    certainty,
                    WriteAtFailure {
                        error,
                        buffer,
                        bytes_transferred,
                    },
                ) = error.into_parts();
                if certainty == CompletionCertainty::NotApplied
                    && matches!(error, StorageError::ResourceExhausted { .. })
                {
                    return Err(SettleError::Exhausted {
                        offset,
                        bytes: buffer,
                        error,
                    });
                }
                return Err(SettleError::Failed(WriteAllFailure {
                    progressed: progressed.saturating_add(bytes_transferred),
                    certainty,
                    error,
                }));
            }
        }
    }
}

/// One chunk of a plan moving through eager admission and ordered settling.
enum PlanChunk<Response> {
    /// Submitted; awaiting its settled outcome.
    Admitted {
        offset: u64,
        requested: usize,
        response: Response,
    },
    /// Refused by a resumable bound at first admission. Resubmitted when it
    /// reaches the front of the queue, after every earlier chunk's
    /// reservation has been released; a second refusal means the bound is
    /// held outside this plan and is surfaced, exactly as the serial path
    /// would surface it.
    Deferred { offset: u64, bytes: Vec<u8> },
}

/// Writes one append plan with its chunks admitted eagerly, then settled in
/// admission order.
///
/// The plan's writes are pairwise non-overlapping, so they commute under the
/// [`FileIoSubmit`] contract and the provider may overlap them and complete
/// them in any order; settling sequentially is collection, not
/// serialization. A plan wider than the provider's admission bounds degrades
/// gracefully: refused chunks defer to the back of the queue and resubmit
/// once earlier reservations release, so any plan the serial path could
/// write still succeeds. Every admitted chunk is settled even after an
/// earlier failure so a later ambiguous outcome cannot go unobserved. A
/// failed plan needs no repair: the caller advances driver state only when
/// the whole plan succeeded, so a partially written region sits beyond the
/// accepted tail — never read, never recovered, and replanned over by the
/// next append.
async fn write_plan<F>(
    file: &F,
    writes: Vec<PlannedWrite>,
    max_request_bytes: usize,
) -> Result<(), WritePlanFailure>
where
    F: FileIoSubmit,
{
    let not_applied_plan_failure = |error: StorageError| WritePlanFailure {
        primary: WriteAllFailure {
            progressed: 0,
            certainty: CompletionCertainty::NotApplied,
            error,
        },
        requires_recovery: false,
    };
    let chunks =
        plan_write_chunks(writes, max_request_bytes).map_err(|primary| WritePlanFailure {
            requires_recovery: requires_recovery_outcome(&primary),
            primary,
        })?;
    let mut queue = VecDeque::new();
    if let Err(error) = queue.try_reserve_exact(chunks.len()) {
        return Err(not_applied_plan_failure(StorageError::Backend {
            operation: StorageOperation::WriteAt,
            raw_os_error: None,
            message: format!("could not reserve admitted write plan: {error}"),
        }));
    }
    for chunk in chunks {
        let requested = chunk.bytes.len();
        let response = file.submit_write_at(WriteAtRequest::new(chunk.offset, chunk.bytes));
        queue.push_back(PlanChunk::Admitted {
            offset: chunk.offset,
            requested,
            response,
        });
    }
    let mut primary: Option<WriteAllFailure> = None;
    let mut requires_recovery = false;
    while let Some(chunk) = queue.pop_front() {
        let settled = match chunk {
            PlanChunk::Admitted {
                offset,
                requested,
                response,
            } => match settle_write(file, offset, requested, response).await {
                Err(SettleError::Exhausted {
                    offset,
                    bytes,
                    error: _,
                }) => {
                    queue.push_back(PlanChunk::Deferred { offset, bytes });
                    continue;
                }
                settled => settled,
            },
            PlanChunk::Deferred { offset, bytes } => {
                // The plan already failed: nothing was admitted for this
                // chunk, so there is nothing to settle or resubmit.
                if primary.is_some() {
                    continue;
                }
                let requested = bytes.len();
                let response = file.submit_write_at(WriteAtRequest::new(offset, bytes));
                match settle_write(file, offset, requested, response).await {
                    Err(SettleError::Exhausted { error, .. }) => {
                        Err(SettleError::Failed(WriteAllFailure {
                            progressed: 0,
                            certainty: CompletionCertainty::NotApplied,
                            error,
                        }))
                    }
                    settled => settled,
                }
            }
        };
        if let Err(SettleError::Failed(failure)) = settled {
            requires_recovery |= requires_recovery_outcome(&failure);
            if primary.is_none() {
                primary = Some(failure);
            }
        }
    }
    match primary {
        None => Ok(()),
        Some(primary) => Err(WritePlanFailure {
            primary,
            requires_recovery,
        }),
    }
}

async fn read_exact_at<F>(
    file: &F,
    mut offset: u64,
    length: usize,
    max_request_bytes: usize,
) -> CompletionResult<Vec<u8>, StorageError>
where
    F: FileIoSubmit,
{
    let mut output = Vec::new();
    output.try_reserve_exact(length).map_err(|error| {
        CompletionError::not_applied(StorageError::Backend {
            operation: StorageOperation::ReadAt,
            raw_os_error: None,
            message: format!("could not reserve {length}-byte exact read: {error}"),
        })
    })?;
    while output.len() < length {
        let request_len = (length - output.len()).min(max_request_bytes);
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(request_len).map_err(|error| {
            CompletionError::not_applied(StorageError::Backend {
                operation: StorageOperation::ReadAt,
                raw_os_error: None,
                message: format!("could not reserve {request_len}-byte read buffer: {error}"),
            })
        })?;
        buffer.resize(request_len, 0);
        match file
            .submit_read_at(ReadAtRequest::new(offset, buffer))
            .await
        {
            Ok(success)
                if success.bytes_read != 0
                    && success.bytes_read == success.buffer.len()
                    && success.bytes_read <= request_len =>
            {
                offset = offset
                    .checked_add(success.bytes_read as u64)
                    .ok_or_else(|| CompletionError::not_applied(StorageError::OffsetOverflow))?;
                output.extend_from_slice(&success.buffer);
            }
            Ok(success) => {
                return Err(CompletionError::not_applied(StorageError::Backend {
                    operation: StorageOperation::ReadAt,
                    raw_os_error: None,
                    message: format!(
                        "backend made invalid exact-read progress: bytes_read={}, buffer_len={}, requested={request_len}",
                        success.bytes_read,
                        success.buffer.len()
                    ),
                }));
            }
            Err(error) => {
                let (
                    certainty,
                    ReadAtFailure {
                        error,
                        buffer: _,
                        bytes_transferred: _,
                    },
                ) = error.into_parts();
                return Err(CompletionError::new(certainty, error));
            }
        }
    }
    Ok(output)
}

async fn confirm_sync<F>(file: &F) -> CompletionResult<(), StorageError>
where
    F: FileIoSubmit,
{
    match file.submit_sync().await {
        Ok(_) => Ok(()),
        Err(error) if error.certainty() == CompletionCertainty::Applied => Ok(()),
        Err(error) => Err(error),
    }
}

async fn confirm_set_len<F>(file: &F, len: u64) -> CompletionResult<(), StorageError>
where
    F: FileIoSubmit,
{
    match file.submit_set_len(len).await {
        Ok(_) => Ok(()),
        Err(error) if error.certainty() == CompletionCertainty::Applied => Ok(()),
        Err(error) => Err(error),
    }
}

async fn rollback_create<F>(
    file: &F,
    original: FileRingOpenError,
) -> CompletionError<FileRingOpenError>
where
    F: FileIoSubmit,
{
    let rolled_back = match confirm_set_len(file, 0).await {
        Ok(()) => confirm_sync(file).await.is_ok(),
        Err(_) => false,
    };
    if rolled_back {
        CompletionError::not_applied(original)
    } else {
        CompletionError::may_have_applied(original)
    }
}

fn map_open_storage(
    action: &'static str,
    error: CompletionError<StorageError>,
) -> CompletionError<FileRingOpenError> {
    error.map(|error| FileRingOpenError::Storage { action, error })
}

fn storage_to_ring_error(
    operation: RingOperation,
    error: StorageError,
    context: &'static str,
) -> RingError {
    if storage_requires_recovery(&error) {
        return RingError::RecoveryRequired;
    }
    let (raw_os_error, detail) = match error {
        StorageError::Backend {
            raw_os_error,
            message,
            ..
        } => (raw_os_error, message),
        other => (None, other.to_string()),
    };
    RingError::BackendFailure {
        operation,
        raw_os_error,
        message: format!("{context}: {detail}"),
    }
}

fn uncertain_ring_error(original: RingError, rollback: StorageError) -> RingError {
    let original_code = match &original {
        RingError::BackendFailure { raw_os_error, .. } => *raw_os_error,
        _ => None,
    };
    RingError::BackendFailure {
        operation: RingOperation::Sync,
        raw_os_error: original_code.or_else(|| raw_os_error(&rollback)),
        message: format!(
            "checkpoint outcome is uncertain after {original}; inactive-slot invalidation could not be confirmed: {rollback}; close and reopen"
        ),
    }
}

fn raw_os_error(error: &StorageError) -> Option<i32> {
    match error {
        StorageError::Backend { raw_os_error, .. } => *raw_os_error,
        _ => None,
    }
}

fn storage_requires_recovery(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::Closed
            | StorageError::DriverStopped
            | StorageError::RecoveryRequired
            | StorageError::RuntimeStopped
    )
}

#[cfg(test)]
mod actor_tests;
#[cfg(test)]
mod model_tests;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
#[cfg(test)]
mod tests;

fn geometry(config: FileRingConfig) -> Geometry {
    Geometry {
        data_capacity: config.data_capacity_bytes,
        max_record_bytes: config.limits.max_record_bytes as u32,
        max_live_records: config.limits.max_live_records as u32,
        max_live_payload_bytes: config.limits.max_live_payload_bytes as u64,
    }
}

const fn empty_checkpoint() -> Checkpoint {
    Checkpoint {
        head_seq: 0,
        tail_seq: 0,
        head_offset: 0,
        tail_offset: 0,
        used_bytes: 0,
        retained_payload_bytes: 0,
    }
}

struct PlannedWrite {
    offset: u64,
    bytes: Vec<u8>,
}

struct AppendPlan {
    locations: Vec<RecordLocation>,
    writes: Vec<PlannedWrite>,
    allocation_bytes: u64,
    payload_bytes: usize,
    next_tail_offset: u64,
    next_cursor: RingCursor,
}

#[derive(Clone, Copy)]
struct AppendApplied {
    first_position: RingPosition,
    next_cursor: RingCursor,
}

impl<F> FileRingDriver<F>
where
    F: FileIoSubmit,
{
    /// Plans the physical frames for one logically admitted batch.
    ///
    /// `next_cursor` is the accepted tail after the whole batch, as proven by
    /// [`check_append_admission`].
    fn plan_append(
        &self,
        records: &[Vec<u8>],
        payload_bytes: usize,
        next_cursor: RingCursor,
    ) -> Result<AppendPlan, RingError> {
        let mut locations = Vec::new();
        locations
            .try_reserve_exact(records.len())
            .map_err(|error| RingError::BackendFailure {
                operation: RingOperation::Append,
                raw_os_error: None,
                message: format!("could not reserve append index plan: {error}"),
            })?;
        let mut writes = Vec::new();
        writes
            .try_reserve_exact(records.len().saturating_mul(2))
            .map_err(|error| RingError::BackendFailure {
                operation: RingOperation::Append,
                raw_os_error: None,
                message: format!("could not reserve physical append plan: {error}"),
            })?;

        let capacity = self.config.data_capacity_bytes;
        let mut tail = self.allocator_tail_offset;
        let mut allocation_bytes = 0u64;
        for (index, payload) in records.iter().enumerate() {
            let sequence = self.accepted_tail.get()
                + u64::try_from(index).expect("validated batch index fits in u64");
            let encoded = encode_data_frame(sequence, payload, self.config.limits.max_record_bytes)
                .map_err(|error| RingError::BackendFailure {
                    operation: RingOperation::Append,
                    raw_os_error: None,
                    message: format!("could not encode data frame: {error}"),
                })?;
            let encoded_len =
                u64::try_from(encoded.len()).map_err(|_| RingError::PhysicalCapacityReached {
                    protected: self.protected_used_bytes,
                    requested: u64::MAX,
                    limit: capacity,
                })?;
            let remaining = capacity - tail;
            let (allocation_start, allocation_len, data_offset) = if encoded_len <= remaining {
                (tail, encoded_len, tail)
            } else {
                if remaining >= FRAME_HEADER_LEN as u64 {
                    let padding = encode_padding_frame(sequence, remaining).map_err(|error| {
                        RingError::BackendFailure {
                            operation: RingOperation::Append,
                            raw_os_error: None,
                            message: format!("could not encode wrap padding: {error}"),
                        }
                    })?;
                    writes.push(PlannedWrite {
                        offset: DATA_OFFSET + tail,
                        bytes: padding.to_vec(),
                    });
                }
                (
                    tail,
                    remaining.checked_add(encoded_len).ok_or(
                        RingError::PhysicalCapacityReached {
                            protected: self.protected_used_bytes,
                            requested: u64::MAX,
                            limit: capacity,
                        },
                    )?,
                    0,
                )
            };
            allocation_bytes = allocation_bytes.checked_add(allocation_len).ok_or(
                RingError::PhysicalCapacityReached {
                    protected: self.protected_used_bytes,
                    requested: u64::MAX,
                    limit: capacity,
                },
            )?;
            let requested = self.protected_used_bytes.saturating_add(allocation_bytes);
            if requested > capacity {
                return Err(RingError::PhysicalCapacityReached {
                    protected: self.protected_used_bytes,
                    requested: allocation_bytes,
                    limit: capacity,
                });
            }
            writes.push(PlannedWrite {
                offset: DATA_OFFSET + data_offset,
                bytes: encoded,
            });
            locations.push(RecordLocation {
                sequence,
                offset: data_offset,
                encoded_len: encoded_len as usize,
                allocation_start,
                allocation_len,
                payload_len: payload.len(),
            });
            tail = data_offset + encoded_len;
            if tail == capacity {
                tail = 0;
            }
        }

        Ok(AppendPlan {
            locations,
            writes,
            allocation_bytes,
            payload_bytes,
            next_tail_offset: tail,
            next_cursor,
        })
    }

    async fn append(
        &mut self,
        request: &AppendRequest,
        payload_bytes: usize,
    ) -> Result<AppendApplied, RingError> {
        if self.recovery_required {
            return Err(RingError::RecoveryRequired);
        }
        let admission = check_append_admission(
            request.expected_accepted_tail,
            self.accepted_tail,
            &request.records,
            payload_bytes,
            self.records.len(),
            self.retained_payload_bytes,
            self.config.limits,
        )?;
        let AppendPlan {
            locations,
            writes,
            allocation_bytes,
            payload_bytes,
            next_tail_offset,
            next_cursor,
        } = self.plan_append(&request.records, payload_bytes, admission.next_cursor)?;
        if let Err(error) = self.records.try_reserve(locations.len()) {
            return Err(RingError::BackendFailure {
                operation: RingOperation::Append,
                raw_os_error: None,
                message: format!("could not reserve retained record index: {error}"),
            });
        }

        // The plan's frame writes commute, so they are admitted eagerly and
        // may complete out of order. State advances only when every write
        // settled successfully; a failed plan leaves every cursor unchanged
        // and its partially written region is inert beyond the accepted tail.
        if let Err(failure) = write_plan(&self.file, writes, self.config.max_io_request_bytes).await
        {
            if failure.requires_recovery {
                self.recovery_required = true;
            }
            return Err(storage_to_ring_error(
                RingOperation::Append,
                failure.primary.error,
                "write planned ring frame",
            ));
        }

        let first_position = RingPosition::new(self.accepted_tail.get());
        self.records.extend(locations);
        self.retained_payload_bytes += payload_bytes;
        self.protected_used_bytes += allocation_bytes;
        self.allocator_tail_offset = next_tail_offset;
        self.accepted_tail = next_cursor;
        Ok(AppendApplied {
            first_position,
            next_cursor,
        })
    }

    async fn read(&mut self, request: ReadRequest) -> ReadResult {
        if self.recovery_required {
            return Err(CompletionError::not_applied(RingError::RecoveryRequired));
        }
        let interval = read_page_interval(request.cursor, self.durable_head, self.durable_tail)
            .map_err(CompletionError::not_applied)?;
        let Some((start, durable_remaining)) = interval else {
            return Ok(empty_page(request.cursor));
        };

        let plan = plan_read_page(
            self.records
                .iter()
                .skip(start)
                .take(durable_remaining)
                .map(|location| location.payload_len),
            request.max_records,
            request.max_bytes,
        )
        .map_err(CompletionError::not_applied)?;
        let mut output = reserve_read_page(plan.take).map_err(CompletionError::not_applied)?;
        for index in start..start + plan.take {
            let location = self.records[index];
            let encoded = match read_exact_at(
                &self.file,
                DATA_OFFSET + location.offset,
                location.encoded_len,
                self.config.max_io_request_bytes,
            )
            .await
            {
                Ok(encoded) => encoded,
                Err(error) => {
                    let (_, storage_error) = error.into_parts();
                    if storage_requires_recovery(&storage_error) {
                        self.recovery_required = true;
                    }
                    return Err(CompletionError::not_applied(storage_to_ring_error(
                        RingOperation::Read,
                        storage_error,
                        "read indexed ring frame",
                    )));
                }
            };
            let (header, payload) =
                match decode_data_frame(&encoded, self.config.limits.max_record_bytes) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        self.recovery_required = true;
                        return Err(CompletionError::not_applied(RingError::CorruptStorage {
                            offset: DATA_OFFSET + location.offset,
                            message: error.to_string(),
                        }));
                    }
                };
            if header.kind != FrameKind::Data
                || header.sequence != location.sequence
                || header.encoded_len() != location.encoded_len
                || payload.len() != location.payload_len
            {
                self.recovery_required = true;
                return Err(CompletionError::not_applied(RingError::CorruptStorage {
                    offset: DATA_OFFSET + location.offset,
                    message: format!(
                        "indexed sequence {} decoded as header {header:?}",
                        location.sequence
                    ),
                }));
            }
            output.push(RingRecord {
                position: RingPosition::new(location.sequence),
                buffer: payload,
            });
        }
        Ok(assemble_read_page(
            output,
            request.cursor,
            self.durable_tail,
            plan.payload_bytes,
        ))
    }
}

struct CheckpointTarget {
    checkpoint: Checkpoint,
    pending_records: usize,
    pending_payload_bytes: usize,
    success: SyncSuccess,
}

impl<F> FileRingDriver<F>
where
    F: FileIoSubmit,
{
    fn trim(&mut self, before: RingCursor) -> TrimResult {
        if self.recovery_required {
            return Err(CompletionError::not_applied(RingError::RecoveryRequired));
        }
        apply_trim(before, self.durable_tail, &mut self.accepted_head)
    }

    /// Returns the record count and payload bytes covered by an accepted but
    /// not yet durable trim.
    fn pending_reclaim(&self) -> (usize, usize) {
        let pending_records = usize::try_from(self.accepted_head.get() - self.durable_head.get())
            .expect("pending trim fits bounded index");
        let pending_payload_bytes = self
            .records
            .iter()
            .take(pending_records)
            .map(|record| record.payload_len)
            .sum();
        (pending_records, pending_payload_bytes)
    }

    fn checkpoint_target(&self) -> CheckpointTarget {
        let (pending_records, pending_payload_bytes) = self.pending_reclaim();
        let retained_payload_bytes = self.retained_payload_bytes - pending_payload_bytes;
        let used_bytes: u64 = self
            .records
            .iter()
            .skip(pending_records)
            .map(|record| record.allocation_len)
            .sum();
        let empty = self.accepted_head == self.accepted_tail;
        let (head_offset, tail_offset, used_bytes) = if empty {
            (0, 0, 0)
        } else {
            (
                self.records[pending_records].allocation_start,
                self.allocator_tail_offset,
                used_bytes,
            )
        };
        CheckpointTarget {
            checkpoint: Checkpoint {
                head_seq: self.accepted_head.get(),
                tail_seq: self.accepted_tail.get(),
                head_offset,
                tail_offset,
                used_bytes,
                retained_payload_bytes: retained_payload_bytes as u64,
            },
            pending_records,
            pending_payload_bytes,
            success: SyncSuccess {
                durable_head: self.accepted_head,
                durable_tail: self.accepted_tail,
                reclaimed_records: pending_records,
                reclaimed_payload_bytes: pending_payload_bytes,
            },
        }
    }

    async fn sync(&mut self, cancellation: &mut SyncCancellation) -> SyncResult {
        let reject = |error| {
            CompletionError::not_applied(SyncFailure {
                error,
                checkpoint: None,
            })
        };
        if self.recovery_required {
            return Err(reject(RingError::RecoveryRequired));
        }
        if self.accepted_head == self.durable_head && self.accepted_tail == self.durable_tail {
            return Ok(SyncSuccess {
                durable_head: self.durable_head,
                durable_tail: self.durable_tail,
                reclaimed_records: 0,
                reclaimed_payload_bytes: 0,
            });
        }
        let Some(next_generation) = self.generation.checked_add(1) else {
            return Err(reject(RingError::MetadataGenerationExhausted));
        };
        let target = self.checkpoint_target();

        // The data fence is deliberately separate from the metadata fence.
        // Until a complete superblock becomes durable, no newly written frame
        // or accepted trim is part of the recoverable ring.
        if let Err(error) = self.file.submit_sync().await
            && error.certainty() != CompletionCertainty::Applied
        {
            let (certainty, storage_error) = error.into_parts();
            if certainty == CompletionCertainty::MayHaveApplied
                || storage_requires_recovery(&storage_error)
            {
                self.recovery_required = true;
            }
            return Err(reject(storage_to_ring_error(
                RingOperation::Sync,
                storage_error,
                "sync ring data before checkpoint",
            )));
        }

        let inactive_slot = 1 - self.active_slot;
        let superblock = Superblock {
            physical_slot: inactive_slot,
            generation: next_generation,
            geometry: geometry(self.config),
            checkpoint: target.checkpoint,
        };
        let encoded = match encode_superblock(&superblock) {
            Ok(encoded) => encoded,
            Err(error) => {
                return Err(reject(RingError::BackendFailure {
                    operation: RingOperation::Sync,
                    raw_os_error: None,
                    message: format!("could not encode checkpoint: {error}"),
                }));
            }
        };
        let metadata_offset = u64::from(inactive_slot) * SUPERBLOCK_LEN as u64;
        *cancellation = SyncCancellation::MayHaveApplied {
            checkpoint: target.success,
        };
        if let Err(failure) = write_all_at(
            &self.file,
            metadata_offset,
            encoded.to_vec(),
            self.config.max_io_request_bytes,
        )
        .await
        {
            let needs_rollback = failure.needs_rollback();
            let original = storage_to_ring_error(
                RingOperation::Sync,
                failure.error,
                "write inactive ring superblock",
            );
            if !needs_rollback {
                *cancellation = SyncCancellation::NotApplied;
                if matches!(original, RingError::RecoveryRequired) {
                    self.recovery_required = true;
                }
                return Err(reject(original));
            }
            return self
                .roll_back_inactive_slot(inactive_slot, original, target.success, cancellation)
                .await;
        }

        match self.file.submit_sync().await {
            Ok(_) => {
                self.publish_checkpoint(inactive_slot, next_generation, &target);
                Ok(target.success)
            }
            Err(error) if error.certainty() == CompletionCertainty::Applied => {
                let storage_error = error.into_parts().1;
                self.publish_checkpoint(inactive_slot, next_generation, &target);
                if storage_requires_recovery(&storage_error) {
                    self.recovery_required = true;
                }
                Err(CompletionError::applied(SyncFailure {
                    error: storage_to_ring_error(
                        RingOperation::Sync,
                        storage_error,
                        "sync ring checkpoint",
                    ),
                    checkpoint: Some(target.success),
                }))
            }
            Err(error) => {
                let original = storage_to_ring_error(
                    RingOperation::Sync,
                    error.into_parts().1,
                    "sync ring checkpoint",
                );
                self.roll_back_inactive_slot(inactive_slot, original, target.success, cancellation)
                    .await
            }
        }
    }

    /// Rolls back a possibly installed candidate checkpoint after `original`.
    ///
    /// Certainty truth table: a confirmed invalidation proves the candidate
    /// checkpoint cannot be recovered, so the failure is `NotApplied` with no
    /// checkpoint and the pending cancellation certainty is reset. A failed
    /// invalidation leaves both checkpoints possible: the ring poisons itself
    /// and reports `MayHaveApplied` carrying the candidate checkpoint.
    async fn roll_back_inactive_slot(
        &mut self,
        inactive_slot: u8,
        original: RingError,
        candidate: SyncSuccess,
        cancellation: &mut SyncCancellation,
    ) -> SyncResult {
        match self.invalidate_slot(inactive_slot).await {
            Ok(()) => {
                *cancellation = SyncCancellation::NotApplied;
                Err(CompletionError::not_applied(SyncFailure {
                    error: original,
                    checkpoint: None,
                }))
            }
            Err(rollback_error) => {
                self.recovery_required = true;
                Err(CompletionError::may_have_applied(SyncFailure {
                    error: uncertain_ring_error(original, rollback_error),
                    checkpoint: Some(candidate),
                }))
            }
        }
    }

    async fn invalidate_slot(&self, slot: u8) -> Result<(), StorageError> {
        let offset = u64::from(slot) * SUPERBLOCK_LEN as u64;
        write_all_at(
            &self.file,
            offset,
            vec![0; SUPERBLOCK_LEN],
            self.config.max_io_request_bytes,
        )
        .await
        .map_err(|failure| failure.error)?;
        confirm_sync(&self.file)
            .await
            .map_err(|error| error.into_parts().1)
    }

    fn publish_checkpoint(&mut self, slot: u8, generation: u64, target: &CheckpointTarget) {
        for _ in 0..target.pending_records {
            let removed = self.records.pop_front().expect("pending record exists");
            debug_assert!(removed.sequence < self.accepted_head.get());
        }
        self.retained_payload_bytes -= target.pending_payload_bytes;
        self.protected_used_bytes = target.checkpoint.used_bytes;
        self.durable_head = self.accepted_head;
        self.durable_tail = self.accepted_tail;
        self.durable_head_offset = target.checkpoint.head_offset;
        self.durable_tail_offset = target.checkpoint.tail_offset;
        self.active_slot = slot;
        self.generation = generation;
        if self.records.is_empty() {
            self.allocator_tail_offset = 0;
        }
    }

    fn status(&self) -> RingStatus {
        let (pending_records, pending_payload_bytes) = self.pending_reclaim();
        RingStatus {
            accepted_head: self.accepted_head,
            accepted_tail: self.accepted_tail,
            durable_head: self.durable_head,
            durable_tail: self.durable_tail,
            accepted_live_records: self.records.len() - pending_records,
            accepted_live_payload_bytes: self.retained_payload_bytes - pending_payload_bytes,
            retained_records: self.records.len(),
            retained_payload_bytes: self.retained_payload_bytes,
            pending_reclaim_records: pending_records,
            pending_reclaim_payload_bytes: pending_payload_bytes,
            max_live_records: self.config.limits.max_live_records,
            max_live_payload_bytes: self.config.limits.max_live_payload_bytes,
            physical: Some(RingPhysicalStatus {
                data_capacity_bytes: self.config.data_capacity_bytes,
                protected_bytes: self.protected_used_bytes,
                free_bytes: self.config.data_capacity_bytes - self.protected_used_bytes,
                durable_head_offset: self.durable_head_offset,
                durable_tail_offset: self.durable_tail_offset,
                accepted_tail_offset: self.allocator_tail_offset,
                metadata_generation: self.generation,
                recovery_required: self.recovery_required,
            }),
        }
    }
}
