//! Production thread host for the provider-neutral file-ring state machine.

use std::fs::OpenOptions;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Instant;

use kr_runtime::{CompletionError, CompletionResult};
use kr_runtime_io_uring::host::{ActorExit, ActorHost, ActorHostJoinError};
use kr_runtime_io_uring::{UringFile, UringFileConfig, UringFileOpenError};
use kr_runtime_ring::file::{FileRing, FileRingDriver, FileRingOperation};
use kr_runtime_ring::{
    AppendFailure, AppendRequest, AppendSuccess, ReadPage, ReadRequest, RingCursor, RingError,
    RingReader, RingStatus, RingWriter, SyncFailure, SyncSuccess, TrimSuccess,
};

use crate::{
    UringRingConfig, UringRingOpenError, map_file_ring_open_error, validate_and_derive_config,
};

type AppendResult = CompletionResult<AppendSuccess, AppendFailure>;
type ReadResult = CompletionResult<ReadPage, RingError>;
type StatusResult = CompletionResult<RingStatus, RingError>;
type TrimResult = CompletionResult<TrimSuccess, RingError>;
type SyncResult = CompletionResult<SyncSuccess, SyncFailure>;

/// Future returned by an io_uring-hosted ring operation.
pub type UringOperation<T> = FileRingOperation<T>;

/// Cloneable Linux host for the durable file-ring state machine.
///
/// One dedicated thread drives [`FileRingDriver`]. A separate bounded
/// [`UringFile`] actor owns the descriptor, exclusive advisory lock, and kernel
/// ring. Each clone owns a corresponding shared-engine handle. Dropping or
/// closing the last clone drains admitted commands, stops both actors, and
/// releases the file lock.
#[derive(Clone)]
pub struct UringRing {
    // Field order is part of teardown: the final FileRing handle must disappear
    // before ActorHost::drop detaches the actor thread.
    ring: FileRing<UringFile>,
    host: Arc<ActorHost>,
}

impl UringRing {
    /// Creates and durably initializes an empty exact-length ring.
    ///
    /// A missing file is created and its parent directory is fenced. An
    /// existing file must be empty; it is never silently truncated.
    pub fn create(
        path: impl AsRef<Path>,
        config: UringRingConfig,
    ) -> CompletionResult<Self, UringRingOpenError> {
        Self::start(path.as_ref(), config, OpenMode::Create)
    }

    /// Opens, locks, validates, and recovers an existing ring file.
    ///
    /// Opening does not create a missing file. Recovery selects the newest
    /// complete checkpoint and fences it before returning.
    pub fn open(
        path: impl AsRef<Path>,
        config: UringRingConfig,
    ) -> CompletionResult<Self, UringRingOpenError> {
        Self::start(path.as_ref(), config, OpenMode::Recover)
    }

    /// Returns a snapshot ordered with all other ring operations.
    pub fn status(&self) -> UringOperation<StatusResult> {
        self.ring.status()
    }

    /// Releases this clone and, when it is the last clone, waits for teardown.
    ///
    /// Other live clones keep the session open. Dropping a handle has the same
    /// lifecycle semantics but cannot report an actor panic.
    pub fn close(self) -> Result<(), UringRingOpenError> {
        let Self { ring, host } = self;
        drop(ring);
        join_host_if_last(host)
    }

    fn start(
        path: &Path,
        config: UringRingConfig,
        mode: OpenMode,
    ) -> CompletionResult<Self, UringRingOpenError> {
        // Validate every cross-layer derivation before opening or creating the
        // path, so invalid configuration has no filesystem side effect.
        let derived = validate_and_derive_config(config).map_err(CompletionError::not_applied)?;
        let ring_config = derived.ring;
        let file_config = UringFileConfig {
            max_read_bytes: config.max_io_request_bytes,
            max_write_bytes: config.max_io_request_bytes,
            max_file_bytes: derived.physical_file_bytes,
            command_queue_capacity: config.command_queue_capacity,
            ring_entries: config.ring_entries,
            max_io_chunk_bytes: config.max_io_chunk_bytes,
        };
        let path = PathBuf::from(path);

        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (exit_sender, exit_receiver) = mpsc::sync_channel(1);
        let startup_started = Instant::now();
        let join = thread::Builder::new()
            .name("kr-runtime-file-ring".to_owned())
            .spawn(move || {
                let _exit = ActorExit::new(exit_sender);
                let (file, created_path) = match open_file(&path, file_config, mode) {
                    Ok(opened) => opened,
                    Err(error) => {
                        let _ = ready_sender.send(Err(error));
                        return;
                    }
                };
                let recovered = block_on(async move {
                    match mode {
                        OpenMode::Create => FileRingDriver::create(file, ring_config).await,
                        OpenMode::Recover => FileRingDriver::open(file, ring_config).await,
                    }
                });
                match recovered {
                    Ok(driver) => {
                        let (ring, actor) = driver.start();
                        if ready_sender.send(Ok(ring)).is_ok() {
                            block_on(actor);
                        }
                    }
                    Err(error) => {
                        let error = error.map(map_file_ring_open_error);
                        let _ =
                            ready_sender.send(Err(promote_created_path_error(error, created_path)));
                    }
                }
            })
            .map_err(|error| {
                CompletionError::not_applied(UringRingOpenError::io("spawn file ring host", error))
            })?;

        let startup_remaining = config
            .startup_timeout
            .saturating_sub(startup_started.elapsed());
        match ready_receiver.recv_timeout(startup_remaining) {
            Ok(Ok(ring)) => Ok(Self {
                ring,
                host: Arc::new(ActorHost::new(join, exit_receiver, config.shutdown_timeout)),
            }),
            failure @ (Ok(Err(_)) | Err(mpsc::RecvTimeoutError::Disconnected)) => {
                let host = ActorHost::new(
                    join,
                    exit_receiver,
                    config
                        .startup_timeout
                        .saturating_sub(startup_started.elapsed()),
                );
                let join_result = host.join();
                match failure {
                    Ok(Err(error)) => Err(error),
                    _ => {
                        let error = match join_result {
                            Err(ActorHostJoinError::TimedOut) => {
                                UringRingOpenError::TimedOut { phase: "startup" }
                            }
                            // Any other join failure means the actor thread did
                            // not exit cleanly; report the stopped driver.
                            Ok(()) | Err(_) => UringRingOpenError::DriverStopped,
                        };
                        Err(CompletionError::may_have_applied(error))
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // The host retains every pointer target and descriptor. If it
                // eventually reaches the readiness send, the disconnected
                // receiver makes it tear the session down without entering the
                // long-lived actor loop.
                drop(ActorHost::new(join, exit_receiver, config.shutdown_timeout));
                Err(CompletionError::may_have_applied(
                    UringRingOpenError::TimedOut { phase: "startup" },
                ))
            }
        }
    }
}

fn join_host_if_last(host: Arc<ActorHost>) -> Result<(), UringRingOpenError> {
    // `strong_count` followed by a drop is racy: concurrent final closes can
    // both observe another owner and leave nobody to perform the checked join.
    // `try_unwrap(...).ok()` has the same race. `into_inner` atomically consumes
    // this owner and guarantees exactly one winner when every clone does so.
    let Some(host) = Arc::into_inner(host) else {
        return Ok(());
    };
    host.join().map_err(map_host_join_error)
}

fn map_host_join_error(error: ActorHostJoinError) -> UringRingOpenError {
    match error {
        ActorHostJoinError::TimedOut => UringRingOpenError::TimedOut { phase: "shutdown" },
        // Any other join failure means the actor thread did not exit cleanly;
        // report the stopped driver.
        _ => UringRingOpenError::DriverStopped,
    }
}

impl RingReader for UringRing {
    type ReadFuture = UringOperation<ReadResult>;
    type StatusFuture = UringOperation<StatusResult>;

    fn read(&self, request: ReadRequest) -> Self::ReadFuture {
        self.ring.read(request)
    }

    fn status(&self) -> Self::StatusFuture {
        self.ring.status()
    }
}

impl RingWriter for UringRing {
    type AppendFuture = UringOperation<AppendResult>;
    type TrimFuture = UringOperation<TrimResult>;
    type SyncFuture = UringOperation<SyncResult>;

    fn append(&self, request: AppendRequest) -> Self::AppendFuture {
        self.ring.append(request)
    }

    fn trim(&self, before: RingCursor) -> Self::TrimFuture {
        self.ring.trim(before)
    }

    fn sync(&self) -> Self::SyncFuture {
        self.ring.sync()
    }
}

#[derive(Clone, Copy)]
enum OpenMode {
    Create,
    Recover,
}

fn open_file(
    path: &Path,
    config: UringFileConfig,
    mode: OpenMode,
) -> CompletionResult<(UringFile, bool), UringRingOpenError> {
    match mode {
        OpenMode::Create => UringFile::open_with_outcome(path, config)
            .map(|outcome| outcome.into_parts())
            .map_err(|error| error.map(map_file_open_error)),
        OpenMode::Recover => {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|error| {
                    CompletionError::not_applied(UringRingOpenError::io(
                        "open existing ring file",
                        error,
                    ))
                })?;
            UringFile::from_file(file, config)
                .map(|file| (file, false))
                .map_err(|error| CompletionError::not_applied(map_file_open_error(error)))
        }
    }
}

fn promote_created_path_error(
    error: CompletionError<UringRingOpenError>,
    created_path: bool,
) -> CompletionError<UringRingOpenError> {
    if !created_path || error.certainty() != kr_runtime::CompletionCertainty::NotApplied {
        return error;
    }
    CompletionError::may_have_applied(error.into_parts().1)
}

fn map_file_open_error(error: UringFileOpenError) -> UringRingOpenError {
    match error {
        UringFileOpenError::InvalidConfig { field, message } => {
            UringRingOpenError::InvalidConfig { field, message }
        }
        UringFileOpenError::AlreadyLocked => UringRingOpenError::AlreadyLocked,
        UringFileOpenError::ExistingFileTooLarge { size, limit } => UringRingOpenError::Corrupt {
            offset: limit,
            message: format!("ring is {size} bytes, beyond configured limit {limit}"),
        },
        UringFileOpenError::Io {
            action,
            raw_os_error,
            message,
        } => UringRingOpenError::Io {
            action,
            raw_os_error,
            message,
        },
        // A stopped file driver and any future open-error variants mean the
        // ring's backing provider never became usable.
        _ => UringRingOpenError::DriverStopped,
    }
}

struct ThreadWake(Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::time::Duration;

    use super::*;

    #[test]
    fn only_a_created_path_promotes_a_not_applied_driver_failure() {
        let existing = promote_created_path_error(
            CompletionError::not_applied(UringRingOpenError::DriverStopped),
            false,
        );
        assert_eq!(
            existing.certainty(),
            kr_runtime::CompletionCertainty::NotApplied
        );

        let created = promote_created_path_error(
            CompletionError::not_applied(UringRingOpenError::DriverStopped),
            true,
        );
        assert_eq!(
            created.certainty(),
            kr_runtime::CompletionCertainty::MayHaveApplied
        );
    }

    #[test]
    fn concurrent_final_host_owners_elect_one_checked_joiner() {
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (exit_sender, exit_receiver) = mpsc::sync_channel(1);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let join = thread::spawn(move || {
            let _exit = ActorExit::new(exit_sender);
            let _ = release_receiver.recv();
            let _ = finished_sender.send(());
        });
        let first_host = Arc::new(ActorHost::new(
            join,
            exit_receiver,
            Duration::from_millis(1),
        ));
        let second_host = Arc::clone(&first_host);
        let start = Arc::new(Barrier::new(3));

        let first_start = Arc::clone(&start);
        let first = thread::spawn(move || {
            first_start.wait();
            join_host_if_last(first_host)
        });
        let second_start = Arc::clone(&start);
        let second = thread::spawn(move || {
            second_start.wait();
            join_host_if_last(second_host)
        });
        start.wait();

        let first = first.join().expect("first close thread did not panic");
        let second = second.join().expect("second close thread did not panic");
        let results = [&first, &second];
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "exactly one non-final owner should return without joining: {results:?}"
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    matches!(
                        result,
                        Err(UringRingOpenError::TimedOut { phase: "shutdown" })
                    )
                })
                .count(),
            1,
            "exactly one final owner should perform the bounded join: {results:?}"
        );

        release_sender.send(()).expect("release detached host");
        finished_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("detached host did not stop after release");
    }
}
