//! Single-file [`FileIoSubmit`](kr_runtime_io::FileIoSubmit) provider backed by io_uring.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};

use kr_runtime::{CompletionCertainty, CompletionError, CompletionResult};
use kr_runtime_io::{
    FileIoSubmit, FileLength, ReadAtFailure, ReadAtRequest, ReadAtSuccess, SetLenSuccess,
    StorageError, StorageOperation, SyncSuccess, WriteAtFailure, WriteAtRequest, WriteAtSuccess,
};

use crate::operation::{
    ActiveResponder, DriverStoppedCommand, FailStopOnPanic, Responder, TerminalCommand,
    UringOperation, operation, ready,
};
use crate::ring::{OwnedTransferFailure, PendingTransfer, Ring};
use crate::support::{Rejection, finish_actor_start, join_if_other_thread, try_send_command};

type ReadCompletion = CompletionResult<ReadAtSuccess, ReadAtFailure>;
type WriteCompletion = CompletionResult<WriteAtSuccess, WriteAtFailure>;
type SetLenCompletion = CompletionResult<SetLenSuccess, StorageError>;
type LenCompletion = CompletionResult<FileLength, StorageError>;
type SyncCompletion = CompletionResult<SyncSuccess, StorageError>;

/// Fixed limits for one open io_uring file session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UringFileConfig {
    /// Maximum bytes accepted by one read request.
    pub max_read_bytes: usize,
    /// Maximum bytes accepted by one write request.
    pub max_write_bytes: usize,
    /// Maximum accepted physical file length.
    pub max_file_bytes: u64,
    /// Commands waiting behind operations admitted by the file actor.
    pub command_queue_capacity: usize,
    /// Maximum user SQEs kept in flight by the file reactor.
    /// Command notification uses a separate internal poll operation.
    pub ring_entries: u32,
    /// Maximum bytes submitted by one SQE and therefore one completion.
    pub max_io_chunk_bytes: usize,
}

impl Default for UringFileConfig {
    fn default() -> Self {
        Self {
            max_read_bytes: 128 * 1024,
            max_write_bytes: 128 * 1024,
            max_file_bytes: 1024 * 1024 * 1024,
            command_queue_capacity: 64,
            ring_entries: 8,
            max_io_chunk_bytes: 128 * 1024,
        }
    }
}

/// Failure to open, lock, or initialize an io_uring file session.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UringFileOpenError {
    /// A fixed resource bound is invalid.
    InvalidConfig {
        field: &'static str,
        message: String,
    },
    /// Another live session holds the advisory exclusive lock.
    AlreadyLocked,
    /// The supplied descriptor does not refer to a regular file.
    NotRegularFile,
    /// The file exceeds the configured physical length bound.
    ExistingFileTooLarge { size: u64, limit: u64 },
    /// File, lock, actor, or io_uring setup failed.
    Io {
        action: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
    /// The actor stopped before its readiness handshake.
    DriverStopped,
}

impl fmt::Display for UringFileOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, message } => {
                write!(formatter, "invalid io_uring file {field}: {message}")
            }
            Self::AlreadyLocked => formatter.write_str("file already has a live writer session"),
            Self::NotRegularFile => formatter.write_str("descriptor is not a regular file"),
            Self::ExistingFileTooLarge { size, limit } => {
                write!(
                    formatter,
                    "existing file length {size} exceeds limit {limit}"
                )
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
            Self::DriverStopped => formatter.write_str("io_uring file driver stopped"),
        }
    }
}

impl std::error::Error for UringFileOpenError {}

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
    #[cfg(test)]
    Panic {
        entered: SyncSender<()>,
        release: Receiver<()>,
    },
}

type ActorMessage = TerminalCommand<Command>;

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
            #[cfg(test)]
            Self::Panic { .. } => {}
        }
    }
}

struct FileHandle {
    sender: Option<SyncSender<ActorMessage>>,
    join: Option<JoinHandle<()>>,
    config: UringFileConfig,
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        self.sender.take();
        join_if_other_thread(self.join.take());
    }
}

/// Cloneable owned operations for one exclusively locked regular file.
///
/// Clones share a single FIFO actor. Dropping the last clone drains commands
/// already admitted to that actor before closing the descriptor and lock.
#[derive(Clone)]
pub struct UringFile {
    handle: Arc<FileHandle>,
}

/// Result of atomically opening or creating a file path.
pub struct UringFileOpenOutcome {
    file: UringFile,
    created: bool,
}

impl UringFileOpenOutcome {
    /// Splits the live file session from whether this call created its path.
    #[must_use]
    pub fn into_parts(self) -> (UringFile, bool) {
        (self.file, self.created)
    }
}

impl UringFile {
    /// Atomically opens or creates a regular file and reports that outcome.
    ///
    /// Failures carry completion certainty for the path creation side effect.
    ///
    /// # Errors
    ///
    /// Returns [`UringFileOpenError`] with `NotApplied` certainty when the
    /// config is invalid or the open itself fails, and with the
    /// path-creation certainty when locking, sizing, directory fencing, or
    /// session start fails after the file was opened.
    /// [`UringFileOpenError::ExistingFileTooLarge`] reports a file larger
    /// than `config.max_file_bytes`.
    pub fn open_with_outcome(
        path: impl AsRef<Path>,
        config: UringFileConfig,
    ) -> CompletionResult<UringFileOpenOutcome, UringFileOpenError> {
        validate_config(config).map_err(CompletionError::not_applied)?;
        let path = path.as_ref();
        let (file, created) = open_or_create(path)
            .map_err(|error| CompletionError::not_applied(open_io("open file", error)))?;
        lock_exclusively(&file).map_err(|error| open_completion_error(created, error))?;
        let size =
            regular_file_len(&file).map_err(|error| open_completion_error(created, error))?;
        sync_parent_directory(path).map_err(|error| open_completion_error(created, error))?;
        if size > config.max_file_bytes {
            return Err(open_completion_error(
                created,
                UringFileOpenError::ExistingFileTooLarge {
                    size,
                    limit: config.max_file_bytes,
                },
            ));
        }
        Self::from_locked_file(file, config)
            .map(|file| UringFileOpenOutcome { file, created })
            .map_err(|error| open_completion_error(created, error))
    }

    /// Starts a session from an already-open file after acquiring its lock.
    ///
    /// Unlike [`Self::open_with_outcome`], this method has no path from which
    /// to identify a containing directory. The caller remains responsible for
    /// fencing any directory entry whose creation or rename must survive a
    /// crash.
    ///
    /// # Errors
    ///
    /// Returns [`UringFileOpenError`] when the config is invalid, the
    /// exclusive lock cannot be acquired, the file is not a regular file,
    /// its size exceeds `config.max_file_bytes`, or the driver session
    /// cannot be started.
    pub fn from_file(file: File, config: UringFileConfig) -> Result<Self, UringFileOpenError> {
        validate_config(config)?;
        lock_exclusively(&file)?;
        let size = regular_file_len(&file)?;
        if size > config.max_file_bytes {
            return Err(UringFileOpenError::ExistingFileTooLarge {
                size,
                limit: config.max_file_bytes,
            });
        }
        Self::from_locked_file(file, config)
    }

    fn from_locked_file(file: File, config: UringFileConfig) -> Result<Self, UringFileOpenError> {
        let (sender, receiver) = mpsc::sync_channel(config.command_queue_capacity);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("kr-runtime-io-uring-file".to_owned())
            .spawn(
                move || match Ring::for_file(config.ring_entries, config.max_io_chunk_bytes) {
                    Ok(ring) => {
                        if ready_sender.send(Ok(())).is_ok() {
                            FileActor::new(ring, file).run(receiver);
                        }
                    }
                    Err(error) => {
                        let _ = ready_sender.send(Err(open_io("create file io_uring", error)));
                    }
                },
            )
            .map_err(|error| open_io("spawn file actor", error))?;

        let (sender, join) = finish_actor_start(
            sender,
            join,
            ready_receiver,
            UringFileOpenError::DriverStopped,
        )?;
        Ok(Self {
            handle: Arc::new(FileHandle {
                sender: Some(sender),
                join: Some(join),
                config,
            }),
        })
    }

    fn try_send(&self, command: Command) -> Result<(), (Command, Rejection)> {
        try_send_command(self.handle.sender.as_ref(), command)
    }
}

impl FileIoSubmit for UringFile {
    type ReadAtResponse = UringOperation<ReadCompletion>;
    type WriteAtResponse = UringOperation<WriteCompletion>;
    type SetLenResponse = UringOperation<SetLenCompletion>;
    type LenResponse = UringOperation<LenCompletion>;
    type SyncResponse = UringOperation<SyncCompletion>;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        if request.buffer.len() > self.handle.config.max_read_bytes {
            let requested = request.buffer.len();
            return ready(Err(CompletionError::not_applied(ReadAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::ReadAt,
                    requested,
                    limit: self.handle.config.max_read_bytes,
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
        let (future, response) = operation();
        match self.try_send(Command::Read { request, response }) {
            Ok(()) => future,
            Err((Command::Read { request, response }, rejection)) => {
                response.complete(Err(CompletionError::not_applied(ReadAtFailure {
                    error: rejection_error(rejection, self.handle.config),
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })));
                future
            }
            Err(_) => unreachable!("try_send changed a read command variant"),
        }
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        if request.buffer.len() > self.handle.config.max_write_bytes {
            let requested = request.buffer.len();
            return ready(Err(CompletionError::not_applied(WriteAtFailure {
                error: StorageError::RequestTooLarge {
                    operation: StorageOperation::WriteAt,
                    requested,
                    limit: self.handle.config.max_write_bytes,
                },
                buffer: request.buffer,
                bytes_transferred: 0,
            })));
        }
        let requested_len = match request.offset.checked_add(request.buffer.len() as u64) {
            Some(end) if end <= self.handle.config.max_file_bytes && end <= i64::MAX as u64 => end,
            Some(end) if end > self.handle.config.max_file_bytes => {
                return ready(Err(CompletionError::not_applied(WriteAtFailure {
                    error: StorageError::FileTooLarge {
                        requested: end,
                        limit: self.handle.config.max_file_bytes,
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
        };
        debug_assert!(requested_len <= self.handle.config.max_file_bytes);
        let (future, response) = operation();
        match self.try_send(Command::Write { request, response }) {
            Ok(()) => future,
            Err((Command::Write { request, response }, rejection)) => {
                response.complete(Err(CompletionError::not_applied(WriteAtFailure {
                    error: rejection_error(rejection, self.handle.config),
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })));
                future
            }
            Err(_) => unreachable!("try_send changed a write command variant"),
        }
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        if len > self.handle.config.max_file_bytes {
            return ready(Err(CompletionError::not_applied(
                StorageError::FileTooLarge {
                    requested: len,
                    limit: self.handle.config.max_file_bytes,
                },
            )));
        }
        let (future, response) = operation();
        match self.try_send(Command::SetLen { len, response }) {
            Ok(()) => future,
            Err((Command::SetLen { response, .. }, rejection)) => {
                response.complete(Err(CompletionError::not_applied(rejection_error(
                    rejection,
                    self.handle.config,
                ))));
                future
            }
            Err(_) => unreachable!("try_send changed a set-len command variant"),
        }
    }

    fn submit_len(&self) -> Self::LenResponse {
        let (future, response) = operation();
        match self.try_send(Command::Len { response }) {
            Ok(()) => future,
            Err((Command::Len { response }, rejection)) => {
                response.complete(Err(CompletionError::not_applied(rejection_error(
                    rejection,
                    self.handle.config,
                ))));
                future
            }
            Err(_) => unreachable!("try_send changed a len command variant"),
        }
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        let (future, response) = operation();
        match self.try_send(Command::Sync { response }) {
            Ok(()) => future,
            Err((Command::Sync { response }, rejection)) => {
                response.complete(Err(CompletionError::not_applied(rejection_error(
                    rejection,
                    self.handle.config,
                ))));
                future
            }
            Err(_) => unreachable!("try_send changed a sync command variant"),
        }
    }
}

fn rejection_error(rejection: Rejection, config: UringFileConfig) -> StorageError {
    match rejection {
        Rejection::Full => StorageError::ResourceExhausted {
            resource: "queued file commands",
            limit: config.command_queue_capacity,
        },
        Rejection::Stopped => StorageError::DriverStopped,
    }
}

struct FileActor {
    ring: Ring,
    file: File,
    recovery_required: bool,
}

struct ReadBatchCommand {
    request: ReadAtRequest,
    response: Responder<ReadCompletion>,
}

struct WriteBatchCommand {
    request: WriteAtRequest,
    response: Responder<WriteCompletion>,
}

struct ActiveRead {
    response: Responder<ReadCompletion>,
    state: ActiveReadState,
}

enum ActiveReadState {
    Pending(PendingTransfer),
    Ready(ReadCompletion),
}

struct ActiveWrite {
    response: Responder<WriteCompletion>,
    state: ActiveWriteState,
}

enum ActiveWriteState {
    Pending(PendingTransfer),
    Ready(WriteCompletion),
}

impl FileActor {
    fn new(ring: Ring, file: File) -> Self {
        Self {
            ring,
            file,
            recovery_required: false,
        }
    }

    fn run(&mut self, receiver: Receiver<ActorMessage>) {
        let mut deferred = None;
        loop {
            let mut message = match deferred.take() {
                Some(message) => message,
                None => match receiver.recv() {
                    Ok(message) => message,
                    Err(_) => return,
                },
            };
            let command = message.take();
            match command {
                Command::Read { request, response } => {
                    let mut batch = vec![ReadBatchCommand { request, response }];
                    self.collect_read_batch(&receiver, &mut deferred, &mut batch);
                    self.run_read_batch(batch);
                }
                Command::Write { request, response } => {
                    let mut batch = vec![WriteBatchCommand { request, response }];
                    self.collect_write_batch(&receiver, &mut deferred, &mut batch);
                    self.run_write_batch(batch);
                }
                Command::SetLen { len, response } => {
                    let active = ActiveResponder::new(response, active_mutation_driver_stopped);
                    active.complete(self.submit_set_len(len));
                }
                Command::Len { response } => {
                    let active = ActiveResponder::new(response, active_read_driver_stopped);
                    active.complete(self.submit_len());
                }
                Command::Sync { response } => {
                    let active = ActiveResponder::new(response, active_mutation_driver_stopped);
                    active.complete(self.submit_sync());
                }
                #[cfg(test)]
                Command::Panic { entered, release } => {
                    let _ = entered.send(());
                    let _ = release.recv();
                    panic!("injected file actor panic");
                }
            }
        }
    }

    fn collect_read_batch(
        &self,
        receiver: &Receiver<ActorMessage>,
        deferred: &mut Option<ActorMessage>,
        batch: &mut Vec<ReadBatchCommand>,
    ) {
        let limit = self.ring.max_active_operations();
        debug_assert_ne!(limit, 0);
        while batch.len() < limit {
            let mut message = match receiver.try_recv() {
                Ok(message) => message,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            };
            match message.take() {
                Command::Read { request, response } => {
                    batch.push(ReadBatchCommand { request, response });
                }
                command => {
                    *deferred = Some(TerminalCommand::new(command));
                    return;
                }
            }
        }
    }

    fn collect_write_batch(
        &self,
        receiver: &Receiver<ActorMessage>,
        deferred: &mut Option<ActorMessage>,
        batch: &mut Vec<WriteBatchCommand>,
    ) {
        let limit = self.ring.max_active_operations();
        debug_assert_ne!(limit, 0);
        while batch.len() < limit {
            let mut message = match receiver.try_recv() {
                Ok(message) => message,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            };
            match message.take() {
                Command::Write { request, response }
                    if batch
                        .iter()
                        .all(|active| !write_requests_overlap(&active.request, &request)) =>
                {
                    batch.push(WriteBatchCommand { request, response });
                }
                command => {
                    *deferred = Some(TerminalCommand::new(command));
                    return;
                }
            }
        }
    }

    fn run_read_batch(&mut self, batch: Vec<ReadBatchCommand>) {
        let _fail_stop = FailStopOnPanic;
        let mut active = Vec::with_capacity(batch.len());
        for ReadBatchCommand { request, response } in batch {
            let state = if self.recovery_required {
                ActiveReadState::Ready(Err(CompletionError::not_applied(ReadAtFailure {
                    error: StorageError::RecoveryRequired,
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })))
            } else {
                let len = request.buffer.len();
                match self
                    .ring
                    .start_read_at(&self.file, request.offset, request.buffer, 0, len)
                {
                    Ok(transfer) => ActiveReadState::Pending(transfer),
                    Err(failure) => {
                        if self.ring.is_poisoned() {
                            self.recovery_required = true;
                        }
                        ActiveReadState::Ready(Err(map_read_failure(failure)))
                    }
                }
            };
            active.push(ActiveRead { response, state });
        }

        for ActiveRead { response, state } in active {
            let output = match state {
                ActiveReadState::Pending(transfer) => self.finish_read(transfer),
                ActiveReadState::Ready(output) => output,
            };
            response.complete(output);
        }
    }

    fn run_write_batch(&mut self, batch: Vec<WriteBatchCommand>) {
        let _fail_stop = FailStopOnPanic;
        let mut active = Vec::with_capacity(batch.len());
        for WriteBatchCommand { request, response } in batch {
            let state = if self.recovery_required {
                ActiveWriteState::Ready(Err(CompletionError::not_applied(WriteAtFailure {
                    error: StorageError::RecoveryRequired,
                    buffer: request.buffer,
                    bytes_transferred: 0,
                })))
            } else {
                let len = request.buffer.len();
                match self
                    .ring
                    .start_write_at(&self.file, request.offset, request.buffer, 0, len)
                {
                    Ok(transfer) => ActiveWriteState::Pending(transfer),
                    Err(failure) => {
                        if failure.may_have_applied || self.ring.is_poisoned() {
                            self.recovery_required = true;
                        }
                        ActiveWriteState::Ready(Err(map_write_failure(failure)))
                    }
                }
            };
            active.push(ActiveWrite { response, state });
        }

        for ActiveWrite { response, state } in active {
            let output = match state {
                ActiveWriteState::Pending(transfer) => self.finish_write(transfer),
                ActiveWriteState::Ready(output) => output,
            };
            response.complete(output);
        }
    }

    fn finish_read(&mut self, transfer: PendingTransfer) -> ReadCompletion {
        match transfer.finish() {
            Ok(mut transfer) => {
                transfer.buffer.truncate(transfer.transferred);
                Ok(ReadAtSuccess {
                    buffer: transfer.buffer,
                    bytes_read: transfer.transferred,
                })
            }
            Err(failure) => {
                if self.ring.is_poisoned() {
                    self.recovery_required = true;
                }
                Err(map_read_failure(failure))
            }
        }
    }

    fn finish_write(&mut self, transfer: PendingTransfer) -> WriteCompletion {
        match transfer.finish() {
            Ok(transfer) => Ok(WriteAtSuccess {
                bytes_written: transfer.transferred,
                buffer: transfer.buffer,
            }),
            Err(failure) => {
                if failure.may_have_applied || self.ring.is_poisoned() {
                    self.recovery_required = true;
                }
                Err(map_write_failure(failure))
            }
        }
    }

    fn submit_set_len(&mut self, len: u64) -> SetLenCompletion {
        if self.recovery_required {
            return Err(CompletionError::not_applied(StorageError::RecoveryRequired));
        }
        match self.file.set_len(len) {
            Ok(()) => Ok(SetLenSuccess { len }),
            Err(error) => {
                self.recovery_required = true;
                Err(set_len_failure(error))
            }
        }
    }

    fn submit_len(&self) -> LenCompletion {
        if self.recovery_required {
            return Err(CompletionError::not_applied(StorageError::RecoveryRequired));
        }
        self.file
            .metadata()
            .map(|metadata| FileLength {
                len: metadata.len(),
            })
            .map_err(|error| CompletionError::not_applied(backend(StorageOperation::Len, error)))
    }

    fn submit_sync(&mut self) -> SyncCompletion {
        if self.recovery_required {
            return Err(CompletionError::not_applied(StorageError::RecoveryRequired));
        }
        if let Err(error) = self.ring.fsync(&self.file) {
            self.recovery_required = true;
            return Err(CompletionError::may_have_applied(backend(
                StorageOperation::Sync,
                error,
            )));
        }
        match self.file.metadata() {
            Ok(metadata) => Ok(SyncSuccess {
                durable_len: metadata.len(),
            }),
            Err(error) => Err(CompletionError::applied(backend(
                StorageOperation::Sync,
                error,
            ))),
        }
    }
}

fn active_read_driver_stopped<T>() -> CompletionResult<T, StorageError> {
    Err(CompletionError::not_applied(StorageError::DriverStopped))
}

fn active_mutation_driver_stopped<T>() -> CompletionResult<T, StorageError> {
    Err(CompletionError::may_have_applied(
        StorageError::DriverStopped,
    ))
}

pub(crate) fn set_len_failure(error: io::Error) -> CompletionError<StorageError> {
    // `ftruncate(2)` does not provide a portable not-applied guarantee for an
    // error reported after the syscall was entered. Fence the session until
    // reopen and force callers to reconcile the physical length.
    CompletionError::may_have_applied(backend(StorageOperation::SetLen, error))
}

pub(crate) fn map_read_failure(failure: OwnedTransferFailure) -> CompletionError<ReadAtFailure> {
    let output = ReadAtFailure {
        error: backend(StorageOperation::ReadAt, failure.error),
        buffer: failure.buffer,
        bytes_transferred: 0,
    };
    if failure.may_have_applied {
        CompletionError::may_have_applied(output)
    } else {
        CompletionError::not_applied(output)
    }
}

pub(crate) fn map_write_failure(failure: OwnedTransferFailure) -> CompletionError<WriteAtFailure> {
    let output = WriteAtFailure {
        error: backend(StorageOperation::WriteAt, failure.error),
        buffer: failure.buffer,
        bytes_transferred: 0,
    };
    if failure.may_have_applied {
        CompletionError::may_have_applied(output)
    } else {
        CompletionError::not_applied(output)
    }
}

pub(crate) fn backend(operation: StorageOperation, error: io::Error) -> StorageError {
    StorageError::Backend {
        operation,
        raw_os_error: error.raw_os_error(),
        message: error.to_string(),
    }
}

pub(crate) fn invalid_kernel_range(offset: u64, len: usize) -> bool {
    offset
        .checked_add(len as u64)
        .is_none_or(|end| end > i64::MAX as u64)
}

fn write_requests_overlap(first: &WriteAtRequest, second: &WriteAtRequest) -> bool {
    if first.buffer.is_empty() || second.buffer.is_empty() {
        return false;
    }
    let first_end = first
        .offset
        .checked_add(first.buffer.len() as u64)
        .expect("admitted write range was validated");
    let second_end = second
        .offset
        .checked_add(second.buffer.len() as u64)
        .expect("admitted write range was validated");
    first.offset < second_end && second.offset < first_end
}

fn validate_config(config: UringFileConfig) -> Result<(), UringFileOpenError> {
    if config.max_read_bytes == 0 {
        return invalid_config("max_read_bytes", "must be nonzero");
    }
    if config.max_write_bytes == 0 {
        return invalid_config("max_write_bytes", "must be nonzero");
    }
    if config.max_file_bytes == 0 || config.max_file_bytes > i64::MAX as u64 {
        return invalid_config("max_file_bytes", format!("must be in 1..={}", i64::MAX));
    }
    if config.command_queue_capacity == 0 {
        return invalid_config("command_queue_capacity", "must be nonzero");
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
    Ok(())
}

fn invalid_config<T>(
    field: &'static str,
    message: impl Into<String>,
) -> Result<T, UringFileOpenError> {
    Err(UringFileOpenError::InvalidConfig {
        field,
        message: message.into(),
    })
}

fn open_or_create(path: &Path) -> io::Result<(File, bool)> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Ok((file, true)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map(|file| (file, false)),
        Err(error) => Err(error),
    }
}

fn open_completion_error(
    created: bool,
    error: UringFileOpenError,
) -> CompletionError<UringFileOpenError> {
    CompletionError::new(
        if created {
            CompletionCertainty::MayHaveApplied
        } else {
            CompletionCertainty::NotApplied
        },
        error,
    )
}

fn lock_exclusively(file: &File) -> Result<(), UringFileOpenError> {
    loop {
        // SAFETY: `file` owns a valid descriptor for this call.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if matches!(error.raw_os_error(), Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN)
        {
            return Err(UringFileOpenError::AlreadyLocked);
        }
        return Err(open_io("lock file", error));
    }
}

fn regular_file_len(file: &File) -> Result<u64, UringFileOpenError> {
    let metadata = file
        .metadata()
        .map_err(|error| open_io("inspect file metadata", error))?;
    if !metadata.is_file() {
        return Err(UringFileOpenError::NotRegularFile);
    }
    Ok(metadata.len())
}

fn sync_parent_directory(path: &Path) -> Result<(), UringFileOpenError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| open_io("sync file parent directory", error))
}

fn open_io(action: &'static str, error: io::Error) -> UringFileOpenError {
    UringFileOpenError::Io {
        action,
        raw_os_error: error.raw_os_error(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::block_on;

    #[test]
    fn config_rejects_invalid_ring_shape() {
        let config = UringFileConfig {
            ring_entries: 3,
            ..UringFileConfig::default()
        };
        assert!(matches!(
            validate_config(config),
            Err(UringFileOpenError::InvalidConfig {
                field: "ring_entries",
                ..
            })
        ));
    }

    #[test]
    fn range_validation_checks_capacity_and_kernel_offset() {
        assert!(!invalid_kernel_range(3, 4));
        assert!(invalid_kernel_range(i64::MAX as u64, 1));
        assert!(invalid_kernel_range(u64::MAX, 1));
    }

    #[test]
    fn write_batch_overlap_uses_half_open_ranges() {
        let request = |offset, bytes: &[u8]| WriteAtRequest::new(offset, bytes.to_vec());

        assert!(!write_requests_overlap(
            &request(0, b"abc"),
            &request(3, b"def")
        ));
        assert!(write_requests_overlap(
            &request(0, b"abc"),
            &request(2, b"def")
        ));
        assert!(write_requests_overlap(
            &request(2, b"def"),
            &request(0, b"abc")
        ));
        assert!(!write_requests_overlap(
            &request(1, b""),
            &request(0, b"abc")
        ));
    }

    #[test]
    fn from_file_rejects_non_regular_descriptor() {
        let file = File::open("/dev/null").expect("open non-regular descriptor");
        let error = UringFile::from_file(file, UringFileConfig::default())
            .err()
            .expect("character device must be rejected");

        assert_eq!(error, UringFileOpenError::NotRegularFile);
    }

    #[test]
    fn bare_file_name_syncs_the_current_directory() {
        sync_parent_directory(Path::new("ring.bin"))
            .expect("current working directory supports a durability fence");
    }

    #[test]
    fn set_len_syscall_failure_is_ambiguous() {
        let failure = set_len_failure(io::Error::from_raw_os_error(libc::EIO));

        assert_eq!(
            failure.certainty(),
            kr_runtime::CompletionCertainty::MayHaveApplied
        );
        assert!(matches!(
            failure.error(),
            StorageError::Backend {
                operation: StorageOperation::SetLen,
                raw_os_error: Some(code),
                ..
            } if *code == libc::EIO
        ));
    }

    #[test]
    fn open_outcome_certainty_tracks_path_creation() {
        let failure = UringFileOpenError::DriverStopped;
        assert_eq!(
            open_completion_error(false, failure.clone()).certainty(),
            CompletionCertainty::NotApplied
        );
        assert_eq!(
            open_completion_error(true, failure).certainty(),
            CompletionCertainty::MayHaveApplied
        );
    }

    #[test]
    fn panicked_file_actor_terminalizes_queued_and_later_operations() {
        let path = std::env::temp_dir().join(format!(
            "kr-runtime-io-uring-file-actor-panic-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let file = UringFile::open_with_outcome(
            &path,
            UringFileConfig {
                max_read_bytes: 64,
                max_write_bytes: 64,
                max_file_bytes: 1_024,
                command_queue_capacity: 8,
                ring_entries: 4,
                max_io_chunk_bytes: 64,
            },
        )
        .expect("open io_uring file")
        .into_parts()
        .0;
        let (entered_sender, entered_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        assert!(
            file.handle
                .sender
                .as_ref()
                .expect("actor sender")
                .send(TerminalCommand::new(Command::Panic {
                    entered: entered_sender,
                    release: release_receiver,
                }))
                .is_ok()
        );
        entered_receiver.recv().expect("actor reaches panic gate");

        let mut read_buffer = Vec::with_capacity(32);
        read_buffer.resize(4, 0);
        let read_pointer = read_buffer.as_ptr();
        let read = file.submit_read_at(ReadAtRequest::new(0, read_buffer));
        let mut write_buffer = Vec::with_capacity(32);
        write_buffer.extend_from_slice(b"data");
        let write_pointer = write_buffer.as_ptr();
        let write = file.submit_write_at(WriteAtRequest::new(0, write_buffer));
        let set_len = file.submit_set_len(0);
        let len = file.submit_len();
        let sync = file.submit_sync();

        release_sender.send(()).expect("release actor panic");

        let read = block_on(read).expect_err("queued read must fail");
        assert_eq!(read.error().error, StorageError::DriverStopped);
        assert_eq!(read.error().buffer.as_ptr(), read_pointer);
        let write = block_on(write).expect_err("queued write must fail");
        assert_eq!(write.error().error, StorageError::DriverStopped);
        assert_eq!(write.error().buffer.as_ptr(), write_pointer);
        assert_eq!(
            *block_on(set_len)
                .expect_err("queued set-len must fail")
                .error(),
            StorageError::DriverStopped
        );
        assert_eq!(
            *block_on(len).expect_err("queued len must fail").error(),
            StorageError::DriverStopped
        );
        assert_eq!(
            *block_on(sync).expect_err("queued sync must fail").error(),
            StorageError::DriverStopped
        );

        let mut later_buffer = Vec::with_capacity(32);
        later_buffer.resize(1, 0);
        let later_pointer = later_buffer.as_ptr();
        let later = block_on(file.submit_read_at(ReadAtRequest::new(0, later_buffer)))
            .expect_err("stopped actor rejects later read");
        assert_eq!(later.error().error, StorageError::DriverStopped);
        assert_eq!(later.error().buffer.as_ptr(), later_pointer);
        drop(file);
        let _ = std::fs::remove_file(path);
    }
}
