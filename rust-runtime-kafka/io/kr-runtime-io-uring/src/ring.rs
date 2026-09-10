//! Bounded shared io_uring mechanics for file and socket actors.
//!
//! Each [`Ring`] is a cloneable handle to one dedicated reactor thread. Calls
//! enqueue one SQE or one linked pair. Socket calls block until the matching
//! CQEs arrive; file batches retain that state in [`PendingTransfer`] values.
//! In both cases every descriptor, buffer, and pointer target stays alive and
//! unmoved while the shared reactor has the operation in flight. Independent
//! callers can therefore keep several operations kernel-visible at once
//! without sharing raw pointer state.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::mem::size_of;
use std::net::{
    Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, TcpListener, TcpStream, UdpSocket,
};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use io_uring::{IoUring, Probe, opcode, squeue, types};
use kr_runtime_io::network::{MAX_WRITE_SEGMENTS, VectoredWriteRequest, WriteSegment};

use crate::operation::FailStopOnPanic;
use crate::support::{join_if_other_thread, lock_unpoisoned};

const FIRST_USER_DATA: u64 = 1;
const WAKE_USER_DATA: u64 = 0;
const MAX_IO_BYTES: usize = u32::MAX as usize;
/// `IORING_MAX_CQ_ENTRIES`. A reactor that would need a larger completion queue
/// cannot be built, so its capacity is rejected at construction rather than
/// discovered as completion-queue overflow under load.
const MAX_CQ_ENTRIES: u32 = 65_536;
/// Consecutive `io_uring_enter` failures tolerated while a poisoned ring still
/// has work the kernel owes a CQE for.
///
/// Each attempt is separated by `RETRY_PAUSE`, so this is roughly a second of
/// trying to quiesce. Any failure that clears itself does so long before that;
/// past it the ring is permanently broken and there is no path back to safely
/// releasing caller memory.
const MAX_STALLED_ENTER_FAILURES: usize = 1_000;
const RETRY_PAUSE: Duration = Duration::from_millis(1);
const ACCEPT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(1);

#[derive(Debug)]
pub(crate) struct OwnedDatagramSend {
    pub(crate) buffer: Vec<u8>,
    pub(crate) transferred: usize,
}

#[derive(Debug)]
pub(crate) struct OwnedDatagramReceive {
    pub(crate) buffer: Vec<u8>,
    /// Payload bytes copied into the caller's receive range.
    pub(crate) transferred: usize,
    /// Full payload length reported by `recvmsg(MSG_TRUNC)`.
    pub(crate) datagram_len: usize,
    pub(crate) source: SocketAddr,
}

/// What the kernel may have done before a datagram operation failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatagramEffect {
    NotApplied,
    Applied,
    MayHaveApplied,
}

#[derive(Debug)]
pub(crate) struct OwnedDatagramFailure {
    pub(crate) buffer: Vec<u8>,
    pub(crate) error: io::Error,
    pub(crate) effect: DatagramEffect,
    /// A copied byte count, or an invalid nonnegative send CQE count.
    pub(crate) bytes_transferred: usize,
}

/// How a blocking, cancellable receive submits and waits.
enum DatagramReceiveWait<'a> {
    /// One `MSG_DONTWAIT` attempt reported through `submit_one`.
    Nonblocking,
    /// A kernel-blocking receive retired by cancellation.
    ///
    /// `arm` runs once, after the SQE is staged and before this thread blocks.
    /// Returning `false` means the caller is already closing, so the receive is
    /// cancelled immediately rather than waiting for a packet that may never
    /// arrive.
    Cancellable {
        deadline: Option<Instant>,
        arm: &'a mut dyn FnMut(CancelToken) -> bool,
    },
}

#[derive(Debug)]
pub(crate) enum DatagramReceiveAttempt {
    Received(OwnedDatagramReceive),
    /// A nonblocking receive observed no queued packet.
    WouldBlock {
        buffer: Vec<u8>,
    },
    /// A blocking receive was retired before any packet reached it.
    ///
    /// Nothing left the kernel's socket buffer, so this is always
    /// `NotApplied`: no datagram was consumed and none was lost.
    Cancelled {
        buffer: Vec<u8>,
    },
}

pub(crate) struct OwnedTransfer {
    pub(crate) buffer: Vec<u8>,
    pub(crate) transferred: usize,
}

pub(crate) struct OwnedTransferFailure {
    pub(crate) buffer: Vec<u8>,
    pub(crate) error: io::Error,
    /// A positive-CQE invariant failure can make the exact effect uncertain.
    pub(crate) may_have_applied: bool,
}

/// An admitted transfer whose buffer remains owned until its CQE is observed.
///
/// Dropping this value waits for terminal completion before releasing the
/// allocation, preserving the raw-pointer lifetime promised to io_uring.
pub(crate) struct PendingTransfer {
    buffer: Option<Vec<u8>>,
    requested: usize,
    completion: Option<Receiver<SingleResponse>>,
    host: Arc<ReactorHost>,
}

impl PendingTransfer {
    pub(crate) fn finish(mut self) -> Result<OwnedTransfer, OwnedTransferFailure> {
        let buffer = self
            .buffer
            .take()
            .expect("pending transfer buffer is available");
        let result = match self.completion.take() {
            Some(completion) => completion
                .recv()
                .unwrap_or_else(|_| Err(SubmissionFailure::known(driver_stopped_error()))),
            None => Ok(0),
        };
        let result = match result {
            Ok(result) => result,
            Err(failure) => {
                return Err(if failure.may_have_applied {
                    OwnedTransferFailure::uncertain(buffer, failure.error)
                } else {
                    OwnedTransferFailure::known(buffer, failure.error)
                });
            }
        };
        let transferred = match decode_transfer_result(result) {
            Ok(transferred) => transferred,
            Err(error) => return Err(OwnedTransferFailure::known(buffer, error)),
        };
        if transferred > self.requested {
            self.host.poison();
            return Err(OwnedTransferFailure::uncertain(
                buffer,
                invalid_completion("transfer", transferred, self.requested),
            ));
        }
        Ok(OwnedTransfer {
            buffer,
            transferred,
        })
    }
}

impl Drop for PendingTransfer {
    fn drop(&mut self) {
        if self.buffer.is_some()
            && let Some(completion) = self.completion.take()
        {
            // The result may be abandoned, but ownership cannot be: wait until
            // the reactor proves the kernel has stopped using the allocation.
            let _ = completion.recv();
        }
    }
}

#[derive(Debug)]
pub(crate) struct SubmissionFailure {
    error: io::Error,
    may_have_applied: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectDisposition {
    Connected,
    TimedOut,
    Failed(i32),
}

enum AcceptCompletion {
    Accepted(TcpStream),
    Failed(i32),
}

struct LinkedAcceptCompletions {
    accept: AcceptCompletion,
    timeout: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptDisposition {
    Accepted,
    NoConnection,
    Failed(i32),
}

#[derive(Clone)]
pub(crate) struct Ring {
    host: Arc<ReactorHost>,
    max_io_chunk_bytes: usize,
}

type SingleResponse = Result<i32, SubmissionFailure>;
type LinkedResponse = Result<[i32; 2], SubmissionFailure>;
type LinkedAcceptResponse = Result<LinkedAcceptCompletions, SubmissionFailure>;

/// A terminal completion delivered on a shared coordinator channel.
///
/// The routed submission path exists for event-driven callers that own many
/// operations at once: instead of parking a thread per operation on a private
/// channel, every terminal result arrives on one channel tagged with a
/// caller-chosen token and the caller demultiplexes. Delivery carries the
/// same guarantee as every other response: exactly one message per admitted
/// operation, sent on its terminal CQE or on rejection, including during
/// poison drain.
pub(crate) struct RoutedCompletion {
    pub(crate) token: u64,
    pub(crate) result: RoutedResult,
    /// Original owned scatter/gather segments, returned only once the reactor
    /// proves its native pointer metadata is no longer kernel-visible.
    pub(crate) segments: Option<Vec<WriteSegment>>,
}

/// Routed completion ownership, including queued requests. A permanent
/// pre-submit reactor failure drops the ring before its request/operation
/// fields, then this guard returns ownership with NotApplied certainty.
/// Keeping the completion obligation in the request also closes the enqueue
/// versus reactor-stop race: a successfully queued request cannot vanish.
struct RoutedTerminal {
    token: u64,
    response: Option<Sender<RoutedCompletion>>,
    segments: Option<Vec<WriteSegment>>,
}

impl RoutedTerminal {
    fn new(
        token: u64,
        response: Sender<RoutedCompletion>,
        segments: Option<Vec<WriteSegment>>,
    ) -> Self {
        Self {
            token,
            response: Some(response),
            segments,
        }
    }

    fn disarm(&mut self) {
        self.response.take();
    }

    fn complete(mut self, result: RoutedResult) {
        self.publish(result);
    }

    fn publish(&mut self, result: RoutedResult) {
        if let Some(response) = self.response.take() {
            let _ = response.send(RoutedCompletion {
                token: self.token,
                result,
                segments: self.segments.take(),
            });
        }
    }
}

impl Drop for RoutedTerminal {
    fn drop(&mut self) {
        if self.response.is_none() {
            return;
        }
        if std::thread::panicking() {
            // A panic gives no proof that an armed pointer has retired. Abort
            // before the guard can release ownership or publish false certainty.
            std::process::abort();
        }
        // Every ordinary in-flight drop happens after Reactor.ring is dropped
        // (declaration order), and run returns early only when no user SQE was
        // submitted. Ordinary queued drops never made a pointer kernel-visible.
        self.publish(Err(SubmissionFailure::known(driver_stopped_error())));
    }
}

/// The raw terminal result of one routed operation, retained by coordinators
/// that join multi-SQE operations.
pub(crate) type RoutedResult = SingleResponse;

/// A routed transfer failure, decoded without a buffer.
///
/// The routed caller owns the operation's buffer in its own slot, so unlike
/// [`OwnedTransferFailure`] only the error and its certainty travel back.
pub(crate) struct RoutedTransferFailure {
    pub(crate) error: io::Error,
    pub(crate) may_have_applied: bool,
}

/// Whether a routed submission put an SQE in flight.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RoutedSubmission {
    /// One SQE is in flight and a [`RoutedCompletion`] carrying this
    /// operation's token will arrive. `requested` is the admitted chunk
    /// length the completion must not exceed.
    Submitted { requested: usize },
    /// The validated range was empty: nothing was submitted, no completion
    /// will arrive, and the caller completes the operation itself.
    Empty,
}

/// Failed pre-SQE admission returning every original shared allocation.
#[derive(Debug)]
pub(crate) struct RoutedVectoredStartFailure {
    pub(crate) error: io::Error,
    pub(crate) segments: Vec<WriteSegment>,
}

/// Native scatter/gather metadata, constructed and retained on the ring thread.
/// The vectors never grow after their addresses become visible to the kernel.
struct VectoredSendStorage {
    _iovecs: Vec<libc::iovec>,
    message: Vec<libc::msghdr>,
}

impl VectoredSendStorage {
    fn new(segments: &[WriteSegment], requested: usize) -> io::Result<Self> {
        let mut iovecs = Vec::new();
        let mut message = Vec::new();
        if iovecs.try_reserve_exact(segments.len()).is_err()
            || message.try_reserve_exact(1).is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "vectored send metadata allocation failed",
            ));
        }
        let mut remaining = requested;
        for segment in segments {
            let Some(bytes) = segment
                .bytes
                .as_slice()
                .get(segment.range.start as usize..segment.range.end as usize)
            else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid vectored send range",
                ));
            };
            let chunk = std::io::IoSlice::new(&bytes[..bytes.len().min(remaining)]);
            if !chunk.is_empty() {
                iovecs.push(libc::iovec {
                    iov_base: chunk.as_ptr().cast_mut().cast(),
                    iov_len: chunk.len(),
                });
                remaining -= chunk.len();
            }
            if remaining == 0 {
                break;
            }
        }
        if remaining != 0 || requested == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid vectored send prefix",
            ));
        }
        // SAFETY: all-zero msghdr is valid empty metadata. Only msg_iov and
        // msg_iovlen are used for connected TCP; the kernel only reads them.
        let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
        header.msg_iov = iovecs.as_mut_ptr();
        header.msg_iovlen = iovecs.len();
        message.push(header);
        Ok(Self {
            _iovecs: iovecs,
            message,
        })
    }
}

/// Kernel-visible storage for one routed linked connect: the encoded peer
/// address and the linked timeout. Boxed so moves of the owner do not move
/// them; the caller must keep this value alive until both of the pair's
/// routed completions have arrived, exactly like a routed transfer buffer.
pub(crate) struct RoutedConnectStorage {
    address: Box<EncodedSocketAddr>,
    timeout: Box<types::Timespec>,
}

/// Kernel-visible storage for one routed accept attempt: its linked retry
/// timeout, carrying the same liveness contract as
/// [`RoutedConnectStorage`].
pub(crate) struct RoutedAcceptStorage {
    timeout: Box<types::Timespec>,
}

/// The decoded outcome of one joined routed accept pair.
pub(crate) enum RoutedAcceptOutcome {
    /// One connection was dequeued from the backlog.
    Accepted(TcpStream),
    /// The attempt's timeout expired first; the caller re-arms while it
    /// still has an accept to serve.
    NoConnection,
    /// The accept itself failed.
    Failed(io::Error),
}

struct ReactorHost {
    sender: Mutex<Option<SyncSender<ReactorRequest>>>,
    /// Cancels travel on their own unbounded channel rather than sharing the
    /// request queue. Sharing it would deadlock: a ring saturated with
    /// cancellable operations defers every further request, so a cancel queued
    /// behind them could never be staged, and nothing would complete to free
    /// the slot it was waiting for.
    cancel_sender: Mutex<Option<Sender<CancelRequest>>>,
    event_fd: OwnedFd,
    join: Mutex<Option<JoinHandle<()>>>,
    poisoned: Arc<AtomicBool>,
    #[cfg(test)]
    max_observed_active_transient_sqes: Arc<AtomicUsize>,
    #[cfg(test)]
    deferred_requests: Arc<AtomicUsize>,
    max_transient_sqes: usize,
}

/// Asks the kernel to retire an operation staged by a `Cancellable` request.
struct CancelRequest {
    target: u64,
    response: Sender<i32>,
}

/// Identifies one in-flight cancellable operation.
///
/// Identifiers are allocated monotonically and never reused, so holding a
/// token past its operation's completion is harmless: the cancel can only miss
/// with `-ENOENT`, never retire an unrelated newer operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CancelToken(u64);

/// A staged cancellable operation the kernel still owes a completion.
///
/// Every pointer target the submitted entry referenced must stay live until
/// [`Self::wait`] returns. Dropping this without waiting would free memory the
/// kernel may still write to, which is why the type is `#[must_use]` and
/// exposes no other way to be consumed.
#[must_use = "the kernel still owes this operation a completion"]
pub(crate) struct PendingCancellable {
    token: CancelToken,
    /// Taken by whichever of [`Self::wait`] or [`Self::wait_until`] consumes
    /// this, leaving `Drop` nothing to do. While it is still present, the
    /// kernel may still write to the entry's pointer targets.
    completion: Option<Receiver<SingleResponse>>,
    ring: Ring,
}

impl PendingCancellable {
    pub(crate) const fn token(&self) -> CancelToken {
        self.token
    }

    /// Blocks until the operation's terminal CQE is consumed.
    ///
    /// A cancelled operation reports `-ECANCELED` here; a cancel that lost the
    /// race reports the ordinary result, because the effect already happened.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionFailure`] when the reactor stopped before delivering
    /// the completion.
    fn wait(mut self) -> Result<i32, SubmissionFailure> {
        self.take_completion()
            .recv()
            .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?
    }

    fn take_completion(&mut self) -> Receiver<SingleResponse> {
        self.completion
            .take()
            .expect("a pending operation is consumed exactly once")
    }

    /// Blocks until the terminal CQE, cancelling the operation at `deadline`.
    ///
    /// The deadline path still waits for the terminal CQE after issuing the
    /// cancel. Returning at the deadline itself would free memory the kernel
    /// may still be writing to, so the timeout bounds when the cancel is
    /// *issued*, never when this returns.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionFailure`] when the reactor stopped before delivering
    /// the completion.
    fn wait_until(mut self, deadline: Option<Instant>) -> Result<i32, SubmissionFailure> {
        let Some(deadline) = deadline else {
            return self.wait();
        };
        let completion = self.take_completion();
        let remaining = deadline.saturating_duration_since(Instant::now());
        match completion.recv_timeout(remaining) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(SubmissionFailure::known(driver_stopped_error()))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.ring.request_cancel(self.token);
                completion
                    .recv()
                    .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?
            }
        }
    }
}

impl Drop for PendingCancellable {
    fn drop(&mut self) {
        let Some(completion) = self.completion.take() else {
            return;
        };
        // Reaching here means safe internal code abandoned the operation
        // without waiting — a drop on an early return, or an unwind. The result
        // may be abandoned; ownership may not, because the submitted entry
        // referenced memory the kernel can still write to. Retire the operation
        // and wait for the reactor to prove the kernel is finished with it.
        //
        // This is what makes the type's safety contract structural rather than
        // a convention `#[must_use]` merely encourages: the guarantee no longer
        // depends on every call site remembering to wait.
        self.ring.request_cancel(self.token);
        let _ = completion.recv();
    }
}

enum ReactorRequest {
    Single {
        entry: squeue::Entry,
        response: Sender<SingleResponse>,
    },
    /// A single SQE whose assigned `user_data` is reported on `token` as soon
    /// as it is staged, so another thread can cancel it while the submitting
    /// caller is still blocked on `response`.
    ///
    /// The response is still delivered only from the target's own terminal
    /// CQE. A cancel never short-circuits it, which is what preserves the
    /// invariant that the caller retains every pointer target until it is
    /// woken.
    Cancellable {
        entry: squeue::Entry,
        class: SqeClass,
        token: Sender<u64>,
        response: Sender<SingleResponse>,
    },
    /// A single SQE whose terminal result is delivered on a shared channel,
    /// tagged with a caller-chosen token, instead of a per-operation channel.
    ///
    /// The submitting caller retains every pointer target the entry
    /// references until the [`RoutedCompletion`] carrying `token` arrives.
    Routed {
        entry: squeue::Entry,
        class: SqeClass,
        terminal: RoutedTerminal,
    },
    /// Transfers shared byte ownership to the ring thread for a native sendmsg.
    RoutedVectored {
        socket: RawFd,
        requested: usize,
        terminal: RoutedTerminal,
    },
    /// Two routed SQEs pushed adjacently with the first linked to the
    /// second, so the kernel runs them in order and severs on failure. Each
    /// delivers its own [`RoutedCompletion`]; the caller joins them.
    RoutedPair {
        entries: [squeue::Entry; 2],
        class: SqeClass,
        terminals: [RoutedTerminal; 2],
    },
    Linked {
        entries: [squeue::Entry; 2],
        response: Sender<LinkedResponse>,
    },
    LinkedAccept {
        entries: [squeue::Entry; 2],
        response: Sender<LinkedAcceptResponse>,
    },
}

/// How an in-flight SQE is charged against reactor capacity.
///
/// Submission-queue depth and in-flight capacity are unrelated quantities: an
/// SQE slot is reclaimed the moment the kernel consumes it, so a small ring can
/// carry far more concurrent operations than it has entries. What must be
/// bounded is the number of operations awaiting a CQE, and that bound is only
/// meaningful per class, because the classes have different lifetimes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqeClass {
    /// Work that terminalizes on its own: sends, closes, connects, accepts,
    /// file I/O, and deadline-bounded receives. A budget here is real
    /// backpressure, because waiting for a slot is waiting for work that is
    /// already guaranteed to finish.
    Transient,
    /// Work that stays armed until a peer acts or something cancels it: a
    /// receive with no deadline. Charging these against the transient budget
    /// lets idle sockets starve every send on the ring, so they draw on a
    /// reserved budget instead, sized by [`RingCapacity::sustained`] to the
    /// maximum that can be armed at once.
    Sustained,
    /// Cancels. They retire work rather than adding it, so charging them would
    /// let a saturated ring refuse the very operation that frees it.
    Exempt,
}

/// In-flight capacity for one reactor, independent of submission depth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RingCapacity {
    /// Submission-queue depth. A batching parameter — how many SQEs may be
    /// staged before a submit — not a limit on concurrent operations.
    pub(crate) entries: u32,
    /// Maximum simultaneously in-flight [`SqeClass::Transient`] operations.
    pub(crate) transient: usize,
    /// Reserved slots for [`SqeClass::Sustained`] operations.
    ///
    /// This must cover the maximum number that can be armed at once — for the
    /// datagram provider, `max_sockets`, because `RECEIVE_LIMIT` allows one
    /// armed receive per socket. Provisioned that way a sustained request can
    /// never be refused a slot, which is what keeps it from ever occupying the
    /// reactor's single `deferred` slot and blocking transient work behind it.
    pub(crate) sustained: usize,
}

impl RingCapacity {
    /// Capacity for a ring that never arms a receive without a deadline.
    pub(crate) const fn transient_only(entries: u32) -> Self {
        Self {
            entries,
            transient: entries as usize,
            sustained: 0,
        }
    }
}

impl ReactorRequest {
    const fn sqe_count(&self) -> usize {
        match self {
            Self::Single { .. }
            | Self::Cancellable { .. }
            | Self::Routed { .. }
            | Self::RoutedVectored { .. } => 1,
            Self::RoutedPair { .. } | Self::Linked { .. } | Self::LinkedAccept { .. } => 2,
        }
    }

    const fn class(&self) -> SqeClass {
        match self {
            Self::Cancellable { class, .. }
            | Self::Routed { class, .. }
            | Self::RoutedPair { class, .. } => *class,
            Self::Single { .. } | Self::Linked { .. } | Self::LinkedAccept { .. } => {
                SqeClass::Transient
            }
            Self::RoutedVectored { .. } => SqeClass::Sustained,
        }
    }

    fn disarm_routed(&mut self) {
        match self {
            Self::Routed { terminal, .. } | Self::RoutedVectored { terminal, .. } => {
                terminal.disarm()
            }
            Self::RoutedPair { terminals, .. } => {
                for terminal in terminals {
                    terminal.disarm();
                }
            }
            _ => {}
        }
    }

    fn complete_not_applied(self, error: io::Error) {
        match self {
            Self::Single { response, .. } | Self::Cancellable { response, .. } => {
                let _ = response.send(Err(SubmissionFailure::known(error)));
            }
            Self::Routed { terminal, .. } | Self::RoutedVectored { terminal, .. } => {
                terminal.complete(Err(SubmissionFailure::known(error)));
            }
            Self::RoutedPair { terminals, .. } => {
                for terminal in terminals {
                    terminal.complete(Err(SubmissionFailure::known(io::Error::new(
                        error.kind(),
                        error.to_string(),
                    ))));
                }
            }
            Self::Linked { response, .. } => {
                let _ = response.send(Err(SubmissionFailure::known(error)));
            }
            Self::LinkedAccept { response, .. } => {
                let _ = response.send(Err(SubmissionFailure::known(error)));
            }
        }
    }
}

struct Reactor {
    ring: IoUring,
    receiver: Receiver<ReactorRequest>,
    cancel_receiver: Receiver<CancelRequest>,
    deferred: Option<ReactorRequest>,
    event_fd: RawFd,
    poisoned: Arc<AtomicBool>,
    /// Every SQE the kernel still owes a CQE for, cancels included. Drives
    /// completion accounting and the decision to enter a wait.
    active_user_sqes: usize,
    /// The subset of `active_user_sqes` in [`SqeClass::Transient`].
    active_transient_sqes: usize,
    /// The subset of `active_user_sqes` in [`SqeClass::Sustained`]. Kept apart
    /// so an idle armed receive can never consume a slot a send needs.
    active_sustained_sqes: usize,
    submitted_user_sqes: usize,
    #[cfg(test)]
    max_observed_active_transient_sqes: Arc<AtomicUsize>,
    /// Times a request was refused staging and parked in `deferred`.
    ///
    /// Coverage instrumentation, not accounting: deferral is timing-dependent
    /// in most shapes, so the tests that exist to exercise it assert this
    /// advanced rather than passing silently without ever taking the path.
    #[cfg(test)]
    deferred_requests: Arc<AtomicUsize>,
    max_transient_sqes: usize,
    max_sustained_sqes: usize,
    next_user_data: u64,
    inflight_slots: HashMap<u64, InflightSlot>,
    inflight_operations: HashMap<u64, InflightOperation>,
    wake_armed: bool,
    ingress_closed: bool,
    /// Targets still owed a poison-time cancel, drained as submission space
    /// allows and refilled from whatever remains in flight.
    poison_cancel_backlog: Vec<u64>,
    /// Consecutive enter failures with no reaping or submission in between.
    /// Reset by any evidence the ring is still making progress.
    stalled_enter_failures: usize,
}

#[derive(Clone, Copy)]
struct InflightSlot {
    operation: u64,
    part: usize,
    /// Recorded per SQE rather than derived from the logical operation, because
    /// the charge is per SQE: a completion must credit back exactly the budget
    /// its own staging debited.
    class: SqeClass,
}

enum InflightOperation {
    Single {
        response: Sender<SingleResponse>,
    },
    Routed {
        terminal: RoutedTerminal,
    },
    RoutedVectored {
        terminal: RoutedTerminal,
        storage: VectoredSendStorage,
    },
    Linked {
        results: [Option<i32>; 2],
        response: Sender<LinkedResponse>,
    },
    LinkedAccept {
        accept: Option<AcceptCompletion>,
        timeout: Option<i32>,
        response: Sender<LinkedAcceptResponse>,
    },
    /// An `AsyncCancel` SQE. Its CQE reports only whether the target was found
    /// (`0`, `-ENOENT`, or `-EALREADY`) and carries no caller buffer.
    Cancel {
        response: Sender<i32>,
    },
}

impl ReactorHost {
    fn enqueue(&self, mut request: ReactorRequest) -> Result<(), Box<ReactorRequest>> {
        if self.poisoned.load(Ordering::Acquire) {
            request.disarm_routed();
            return Err(Box::new(request));
        }
        let sender = lock_unpoisoned(&self.sender).as_ref().cloned();
        let Some(sender) = sender else {
            request.disarm_routed();
            return Err(Box::new(request));
        };
        match sender.send(request) {
            Ok(()) => {
                signal_event_fd(self.event_fd.as_raw_fd());
                Ok(())
            }
            Err(error) => {
                let mut request = error.0;
                request.disarm_routed();
                Err(Box::new(request))
            }
        }
    }

    /// Queues a cancel for an operation staged by a `Cancellable` request.
    ///
    /// A poisoned or stopped reactor reports failure rather than queueing, and
    /// the caller does not need to retry: the poisoned reactor cancels every
    /// in-flight operation itself in [`Reactor::cancel_poisoned_inflight`], so
    /// the target is already being retired. What makes refusing safe is that
    /// guarantee, not the request queue — before it existed, an armed receive
    /// refused a cancel here was simply left running forever.
    fn enqueue_cancel(&self, request: CancelRequest) -> Result<(), CancelRequest> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(request);
        }
        let sender = lock_unpoisoned(&self.cancel_sender).as_ref().cloned();
        let Some(sender) = sender else {
            return Err(request);
        };
        match sender.send(request) {
            Ok(()) => {
                signal_event_fd(self.event_fd.as_raw_fd());
                Ok(())
            }
            Err(error) => Err(error.0),
        }
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        signal_event_fd(self.event_fd.as_raw_fd());
    }
}

impl Drop for ReactorHost {
    fn drop(&mut self) {
        lock_unpoisoned(&self.sender).take();
        lock_unpoisoned(&self.cancel_sender).take();
        signal_event_fd(self.event_fd.as_raw_fd());
        join_if_other_thread(lock_unpoisoned(&self.join).take());
    }
}

impl Reactor {
    fn run(&mut self) {
        loop {
            self.collect_completions();
            if self.poisoned.load(Ordering::Acquire) {
                self.reject_waiting();
                self.cancel_poisoned_inflight();
            } else {
                self.stage_waiting();
            }

            if (self.ingress_closed || self.poisoned.load(Ordering::Acquire))
                && self.active_user_sqes == 0
                && self.deferred.is_none()
            {
                self.reject_waiting();
                return;
            }

            // Publish user work first. Once the kernel has consumed those SQEs,
            // the userspace SQ is empty again and the internal wake poll can be
            // added without stealing configured user queue depth.
            if !self.submit_all_staged(true) {
                return;
            }
            if !self.ingress_closed && !self.poisoned.load(Ordering::Acquire) {
                self.arm_wake_poll();
                if !self.submit_all_staged(false) {
                    return;
                }
            }

            // Poison can be published by a caller interpreting a malformed CQE
            // after the terminal check above. Never wait unless either user I/O
            // or the eventfd poll can produce a completion.
            if self.active_user_sqes == 0 && !self.wake_armed {
                continue;
            }

            // A poisoned reactor is trying to quiesce, and its only lever is
            // re-issuing cancels from the top of this loop. Blocking for a
            // completion that a missed cancel means may never arrive would
            // strand it there, so pause instead of waiting and come back around
            // to try again.
            if self.poisoned.load(Ordering::Acquire) {
                thread::park_timeout(RETRY_PAUSE);
                continue;
            }

            // Deferral is never a reason to wait. A request is deferred only
            // for submission-queue space or class budget, and the submits above
            // empty the submission queue — so by now the blocker is usually
            // already gone. Blocking here instead waits for a completion that
            // nothing in flight is obliged to produce: with every armed receive
            // waiting on a peer, the deferred request is the only thing that
            // could generate one, and it cannot until this loop comes back
            // around. Staging it first is what breaks that cycle.
            //
            // Guarded on the request actually being stageable rather than on
            // its mere presence, so a request still short of class budget parks
            // here as intended — a completion will free that budget — instead
            // of spinning.
            if self.deferred_is_stageable() {
                continue;
            }

            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(error) => match classify_enter(&error) {
                    // The top of this loop reaps before doing anything else,
                    // so both an interrupted call and a full completion queue
                    // recover by simply going around.
                    EnterRecovery::Retry | EnterRecovery::DrainCompletions => {}
                    EnterRecovery::Backoff => thread::park_timeout(RETRY_PAUSE),
                    EnterRecovery::Poison => self.poison_and_drain(&error),
                    EnterRecovery::LostCompletion => abort_on_lost_completion(&error),
                },
            }
        }
    }

    /// Publishes every userspace SQE before the caller enters a completion wait.
    ///
    /// Returns `false` only when a permanent enter failure occurred before any
    /// SQE in this batch crossed into the kernel and no older user pointer
    /// remains kernel-visible. Returning from the reactor then drops the ring
    /// before response senders, so userspace-only SQEs are discarded before
    /// their callers recover their buffers.
    fn submit_all_staged(&mut self, user_work: bool) -> bool {
        let mut made_progress = false;
        loop {
            let staged = {
                let mut submission = self.ring.submission();
                submission.sync();
                submission.len()
            };
            if staged == 0 {
                return true;
            }
            match self.ring.submit() {
                Ok(0) => thread::yield_now(),
                Ok(submitted) => {
                    made_progress = true;
                    self.stalled_enter_failures = 0;
                    if user_work {
                        self.submitted_user_sqes += submitted;
                        debug_assert!(self.submitted_user_sqes <= self.active_user_sqes);
                    }
                }
                Err(error) => match classify_enter(&error) {
                    EnterRecovery::Retry => {}
                    EnterRecovery::DrainCompletions => {
                        // Unlike the drive loop, this loop never reaps on its
                        // own, so the retry would resubmit into the same full
                        // queue forever. Reap here to make room. A reap that
                        // finds nothing means the condition is not one this
                        // thread can clear, so fall back to a backoff rather
                        // than spinning.
                        if self.collect_completions() == 0 {
                            thread::park_timeout(RETRY_PAUSE);
                        }
                    }
                    EnterRecovery::Backoff => thread::park_timeout(RETRY_PAUSE),
                    EnterRecovery::LostCompletion => abort_on_lost_completion(&error),
                    EnterRecovery::Poison => {
                        self.collect_completions();
                        if !made_progress && self.submitted_user_sqes == 0 {
                            // Nothing crossed into the kernel and no older user
                            // pointer is kernel-visible, so unwinding is still
                            // honest: poison, return, and let every caller
                            // recover its buffer.
                            self.poisoned.store(true, Ordering::Release);
                            return false;
                        }
                        // A user pointer may still be kernel-visible, so this
                        // thread cannot return. Poison and keep trying to
                        // quiesce; the attempt bound is what stops that from
                        // becoming the permanent hang it used to be.
                        self.poison_and_drain(&error);
                    }
                },
            }
        }
    }

    fn stage_waiting(&mut self) {
        self.stage_waiting_cancels();
        loop {
            let request = match self.deferred.take() {
                Some(request) => request,
                None => match self.receiver.try_recv() {
                    Ok(request) => request,
                    Err(TryRecvError::Empty) => return,
                    Err(TryRecvError::Disconnected) => {
                        self.ingress_closed = true;
                        return;
                    }
                },
            };
            if !self.can_stage(&request) {
                #[cfg(test)]
                self.deferred_requests.fetch_add(1, Ordering::AcqRel);
                self.deferred = Some(request);
                return;
            }
            self.stage(request);
        }
    }

    /// Whether a deferred request could be staged right now.
    ///
    /// Taken out and put back because [`Self::can_stage`] needs `&mut self` to
    /// sync the submission queue, and the answer must reflect the space freed
    /// by the submits that ran since the request was deferred.
    fn deferred_is_stageable(&mut self) -> bool {
        let Some(request) = self.deferred.take() else {
            return false;
        };
        let stageable = self.can_stage(&request);
        self.deferred = Some(request);
        stageable
    }

    /// Whether `request` fits both its class budget and real submission space.
    ///
    /// A sustained request must never be refused on budget: its capacity is
    /// provisioned to cover every operation that can be armed at once, so a
    /// shortfall means the provider's accounting is broken. Deferring it
    /// would park it in `deferred` and block transient work behind it until a
    /// peer happened to act — the exact starvation the class split exists to
    /// prevent — and nothing can prove how far the miscount reaches, so the
    /// reactor fails closed instead: it poisons itself, the next loop
    /// iteration rejects the refused request, and every in-flight operation
    /// is retired and its caller told.
    fn can_stage(&mut self, request: &ReactorRequest) -> bool {
        let count = request.sqe_count();
        let available = match request.class() {
            SqeClass::Transient => self.max_transient_sqes - self.active_transient_sqes,
            SqeClass::Sustained => {
                let available = self
                    .max_sustained_sqes
                    .saturating_sub(self.active_sustained_sqes);
                if count > available {
                    self.poisoned.store(true, Ordering::Release);
                    return false;
                }
                available
            }
            SqeClass::Exempt => count,
        };
        count <= available && self.submission_space() >= count
    }

    fn submission_space(&mut self) -> usize {
        let mut submission = self.ring.submission();
        submission.sync();
        submission.capacity() - submission.len()
    }

    /// Stages every queued cancel ahead of ordinary requests.
    ///
    /// Cancels bypass both the in-flight budget and the single `deferred` slot.
    /// Either one would reintroduce the deadlock the separate channel exists to
    /// avoid: a request parked in `deferred` blocks the head of the request
    /// queue, and the budget is exhausted precisely when a cancel is most
    /// needed. Only real submission-queue space can hold a cancel back, and
    /// that frees on the very next submit.
    fn stage_waiting_cancels(&mut self) {
        while self.submission_space() > 0 {
            let request = match self.cancel_receiver.try_recv() {
                Ok(request) => request,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            };
            self.stage_cancel(request);
        }
    }

    /// Retires every operation the kernel still owes a CQE for.
    ///
    /// A poisoned reactor stops staging new work, but it may not return while
    /// the kernel can still write to caller memory, so it must reach zero
    /// in-flight SQEs to exit. Transient work gets there on its own. An armed
    /// receive never does: it is waiting for a peer that may never send, and
    /// the poisoned path no longer accepts the cancel that would retire it.
    /// The reactor therefore issues those cancels itself. Without this the
    /// thread runs forever and everything joining it — socket drop, close
    /// waiters, the binding permit — hangs with it.
    ///
    /// Cancels are exempt: cancelling a cancel cannot help it finish, and a
    /// target that completed first simply misses with `-ENOENT`.
    fn cancel_poisoned_inflight(&mut self) {
        if self.poison_cancel_backlog.is_empty() {
            // Refilled from whatever is *still* in flight rather than collected
            // once. A cancel can miss — `-ENOENT` when it reaches the kernel
            // ahead of its own target — and a missed cancel under a one-shot
            // scheme is unrecoverable: that operation is never retired, the
            // in-flight count never reaches zero, and the reactor hangs exactly
            // as it did before it cancelled its own work. Re-issuing is safe
            // because a duplicate cancel is an ordinary miss, and it converges
            // because a target that is genuinely retired leaves `inflight_slots`
            // and stops being refilled.
            self.poison_cancel_backlog = self
                .inflight_slots
                .iter()
                .filter(|(_, slot)| !matches!(slot.class, SqeClass::Exempt))
                .map(|(user_data, _)| *user_data)
                .collect();
        }
        while let Some(&target) = self.poison_cancel_backlog.last() {
            if self.submission_space() == 0 {
                return;
            }
            self.poison_cancel_backlog.pop();
            // Nothing is waiting on the result: this cancel exists to retire a
            // target the caller can no longer reach, and the target's own
            // terminal CQE is what wakes that caller.
            let (response, discarded) = mpsc::channel();
            drop(discarded);
            self.stage_cancel(CancelRequest { target, response });
        }
    }

    fn stage_cancel(&mut self, request: CancelRequest) {
        let CancelRequest { target, response } = request;
        let user_data = self.allocate_user_data();
        // A cancel for an operation that already completed is not an error.
        // Identifiers are allocated monotonically and never reused, so a stale
        // target can only ever miss with -ENOENT; it can never name a
        // different, newer operation.
        let entry = opcode::AsyncCancel::new(target)
            .build()
            .user_data(user_data);
        assert!(
            self.inflight_slots
                .insert(
                    user_data,
                    InflightSlot {
                        operation: user_data,
                        part: 0,
                        class: SqeClass::Exempt,
                    },
                )
                .is_none(),
            "reactor reused active io_uring user_data"
        );
        assert!(
            self.inflight_operations
                .insert(user_data, InflightOperation::Cancel { response })
                .is_none(),
            "reactor reused active logical operation identifier"
        );
        let push = {
            let mut submission = self.ring.submission();
            // SAFETY: AsyncCancel stores only the target identifier. It
            // borrows no caller buffer and outlives no userspace allocation.
            unsafe { submission.push(&entry) }
        };
        assert!(push.is_ok(), "checked io_uring cancel space became full");
        self.add_active(1, SqeClass::Exempt);
    }

    fn stage(&mut self, request: ReactorRequest) {
        match request {
            ReactorRequest::Single { entry, response } => {
                self.stage_single(entry, SqeClass::Transient, response);
            }
            ReactorRequest::Routed {
                entry,
                class,
                terminal,
            } => {
                self.stage_one(entry, class, InflightOperation::Routed { terminal });
            }
            ReactorRequest::RoutedVectored {
                socket,
                requested,
                terminal,
            } => {
                let _fail_stop = FailStopOnPanic;
                match VectoredSendStorage::new(
                    terminal
                        .segments
                        .as_ref()
                        .expect("vectored request owns segments"),
                    requested,
                ) {
                    Ok(storage) => {
                        // SAFETY: native metadata and shared allocations are
                        // retained in the in-flight slot before submission. Moves
                        // do not change their heap addresses, which live to CQE.
                        let entry =
                            opcode::SendMsg::new(types::Fd(socket), storage.message.as_ptr())
                                .flags(libc::MSG_NOSIGNAL as u32)
                                .build();
                        self.stage_one(
                            entry,
                            SqeClass::Sustained,
                            InflightOperation::RoutedVectored { terminal, storage },
                        );
                    }
                    Err(error) => terminal.complete(Err(SubmissionFailure::known(error))),
                }
            }
            ReactorRequest::RoutedPair {
                entries,
                class,
                terminals,
            } => self.stage_routed_pair(entries, class, terminals),
            ReactorRequest::Cancellable {
                entry,
                class,
                token,
                response,
            } => {
                let user_data = self.stage_single(entry, class, response);
                // Publishing the identifier after staging keeps a cancel from
                // being submitted ahead of its own target, which would miss
                // with -ENOENT and leave the operation running uncancelled.
                let _ = token.send(user_data);
            }
            ReactorRequest::Linked { entries, response } => self.stage_linked(
                entries,
                InflightOperation::Linked {
                    results: [None, None],
                    response,
                },
            ),
            ReactorRequest::LinkedAccept { entries, response } => self.stage_linked(
                entries,
                InflightOperation::LinkedAccept {
                    accept: None,
                    timeout: None,
                    response,
                },
            ),
        }
    }

    /// Stages one user SQE, returning the identifier the kernel will echo back.
    fn stage_single(
        &mut self,
        entry: squeue::Entry,
        class: SqeClass,
        response: Sender<SingleResponse>,
    ) -> u64 {
        self.stage_one(entry, class, InflightOperation::Single { response })
    }

    /// Stages one user SQE with its logical operation record.
    fn stage_one(
        &mut self,
        mut entry: squeue::Entry,
        class: SqeClass,
        inflight: InflightOperation,
    ) -> u64 {
        let user_data = self.allocate_user_data();
        entry.set_user_data(user_data);
        let operation = user_data;
        assert!(
            self.inflight_slots
                .insert(
                    operation,
                    InflightSlot {
                        operation,
                        part: 0,
                        class,
                    }
                )
                .is_none(),
            "reactor reused active io_uring user_data"
        );
        assert!(
            self.inflight_operations
                .insert(operation, inflight)
                .is_none(),
            "reactor reused active logical operation identifier"
        );
        let push = {
            let mut submission = self.ring.submission();
            // SAFETY: the blocking caller retains every descriptor and
            // pointer target until this request's response is delivered.
            unsafe { submission.push(&entry) }
        };
        assert!(
            push.is_ok(),
            "checked io_uring submission space became full"
        );
        self.add_active(1, class);
        user_data
    }

    /// Stages two adjacent routed SQEs as independent operations.
    ///
    /// The caller pre-links the first entry to the second; adjacency in one
    /// push is what makes the kernel honor that link. Each operation keeps
    /// its own identifier and delivers its own routed completion.
    fn stage_routed_pair(
        &mut self,
        mut entries: [squeue::Entry; 2],
        class: SqeClass,
        terminals: [RoutedTerminal; 2],
    ) {
        for (entry, terminal) in entries.iter_mut().zip(terminals) {
            let user_data = self.allocate_user_data();
            entry.set_user_data(user_data);
            assert!(
                self.inflight_slots
                    .insert(
                        user_data,
                        InflightSlot {
                            operation: user_data,
                            part: 0,
                            class,
                        }
                    )
                    .is_none(),
                "reactor reused active io_uring user_data"
            );
            assert!(
                self.inflight_operations
                    .insert(user_data, InflightOperation::Routed { terminal })
                    .is_none(),
                "reactor reused active logical operation identifier"
            );
        }
        let push = {
            let mut submission = self.ring.submission();
            // SAFETY: the routed caller retains every pointer target both
            // entries reference until each terminal completion is received.
            unsafe { submission.push_multiple(&entries) }
        };
        assert!(
            push.is_ok(),
            "checked io_uring submission space became full"
        );
        self.add_active(2, class);
    }

    fn stage_linked(&mut self, mut entries: [squeue::Entry; 2], inflight: InflightOperation) {
        let first = self.allocate_user_data();
        let second = self.allocate_user_data();
        entries[0].set_user_data(first);
        entries[1].set_user_data(second);
        assert!(
            self.inflight_slots
                .insert(
                    first,
                    InflightSlot {
                        operation: first,
                        part: 0,
                        class: SqeClass::Transient,
                    },
                )
                .is_none(),
            "reactor reused active io_uring user_data"
        );
        assert!(
            self.inflight_slots
                .insert(
                    second,
                    InflightSlot {
                        operation: first,
                        part: 1,
                        class: SqeClass::Transient,
                    },
                )
                .is_none(),
            "reactor reused active io_uring user_data"
        );
        assert!(
            self.inflight_operations.insert(first, inflight).is_none(),
            "reactor reused active logical operation identifier"
        );
        let push = {
            let mut submission = self.ring.submission();
            // SAFETY: the caller retains all pointer targets until both linked
            // CQEs are delivered. A successful accept descriptor is taken into
            // reactor ownership as soon as its CQE is drained.
            unsafe { submission.push_multiple(&entries) }
        };
        assert!(
            push.is_ok(),
            "checked io_uring submission space became full"
        );
        self.add_active(2, SqeClass::Transient);
    }

    fn allocate_user_data(&mut self) -> u64 {
        let user_data = self.next_user_data;
        self.next_user_data = user_data.checked_add(1).unwrap_or_else(|| {
            self.poisoned.store(true, Ordering::Release);
            panic!("io_uring user_data identifier space is exhausted")
        });
        user_data
    }

    /// Poisons the ring and pauses, aborting once it is clearly unquiescable.
    ///
    /// The enter call was rejected but the ring is still coherent, so every
    /// in-flight operation is still owed a CQE and every caller can still be
    /// told. Poisoning starts that: the run loop retires the in-flight work and
    /// exits once the kernel owes nothing. Reaching zero requires submitting
    /// cancels, though, so an enter that never recovers leaves the ring
    /// permanently kernel-visible — bounded here rather than retried forever,
    /// because the unbounded retry is the hang this replaced.
    fn poison_and_drain(&mut self, error: &io::Error) {
        self.poisoned.store(true, Ordering::Release);
        self.stalled_enter_failures += 1;
        if self.stalled_enter_failures > MAX_STALLED_ENTER_FAILURES {
            abort_on_undrainable_ring(error, self.active_user_sqes);
        }
        thread::park_timeout(RETRY_PAUSE);
    }

    fn add_active(&mut self, count: usize, class: SqeClass) {
        self.active_user_sqes += count;
        match class {
            SqeClass::Transient => self.active_transient_sqes += count,
            SqeClass::Sustained => self.active_sustained_sqes += count,
            SqeClass::Exempt => {}
        }
        #[cfg(test)]
        self.max_observed_active_transient_sqes
            .fetch_max(self.active_transient_sqes, Ordering::AcqRel);
        debug_assert!(self.active_transient_sqes <= self.max_transient_sqes);
        debug_assert!(self.active_sustained_sqes <= self.max_sustained_sqes);
    }

    fn arm_wake_poll(&mut self) {
        if self.wake_armed || self.submission_space() == 0 {
            return;
        }
        let entry = opcode::PollAdd::new(types::Fd(self.event_fd), libc::POLLIN as u32)
            .build()
            .user_data(WAKE_USER_DATA);
        let push = {
            let mut submission = self.ring.submission();
            // SAFETY: PollAdd stores only the descriptor value. ReactorHost
            // keeps the eventfd open until this thread has joined.
            unsafe { submission.push(&entry) }
        };
        assert!(push.is_ok(), "checked io_uring wake-poll space became full");
        self.wake_armed = true;
    }

    /// Reaps every currently available CQE, returning how many were consumed.
    ///
    /// The count is what lets an `EBUSY` enter failure distinguish "the ring
    /// made room" from "reaping found nothing", so a full completion queue
    /// cannot become an unbounded retry loop.
    fn collect_completions(&mut self) -> usize {
        let completions = self
            .ring
            .completion()
            .map(|completion| {
                (
                    completion.user_data(),
                    completion.result(),
                    completion.flags(),
                )
            })
            .collect::<Vec<_>>();
        let consumed = completions.len();
        if consumed > 0 {
            // Evidence the ring is still moving, so any earlier enter failure
            // was not the permanent kind.
            self.stalled_enter_failures = 0;
        }
        for (user_data, result, _flags) in completions {
            if user_data == WAKE_USER_DATA {
                assert!(self.wake_armed, "io_uring returned a duplicate wake CQE");
                self.wake_armed = false;
                if result < 0 {
                    panic!("io_uring eventfd poll failed with CQE result {result}");
                }
                drain_event_fd(self.event_fd);
                continue;
            }

            let slot = self
                .inflight_slots
                .remove(&user_data)
                .unwrap_or_else(|| panic!("io_uring returned unknown user_data {user_data}"));
            self.active_user_sqes = self
                .active_user_sqes
                .checked_sub(1)
                .expect("user CQE underflowed the active SQE count");
            self.submitted_user_sqes = self
                .submitted_user_sqes
                .checked_sub(1)
                .expect("user CQE underflowed the submitted SQE count");
            match slot.class {
                SqeClass::Transient => {
                    self.active_transient_sqes = self
                        .active_transient_sqes
                        .checked_sub(1)
                        .expect("user CQE underflowed the transient SQE count");
                }
                SqeClass::Sustained => {
                    self.active_sustained_sqes = self
                        .active_sustained_sqes
                        .checked_sub(1)
                        .expect("user CQE underflowed the sustained SQE count");
                }
                SqeClass::Exempt => {}
            }
            let operation = self
                .inflight_operations
                .get_mut(&slot.operation)
                .expect("completion referenced a missing logical operation");
            match operation {
                InflightOperation::Cancel { .. } => {
                    // 0, -ENOENT (already completed), and -EALREADY (completing
                    // now) are all ordinary outcomes of a cancel that raced its
                    // target. The caller decides what to do with the result;
                    // none of them is a reactor fault.
                    let operation = self
                        .inflight_operations
                        .remove(&slot.operation)
                        .expect("cancel disappeared during completion");
                    let InflightOperation::Cancel { response } = operation else {
                        unreachable!("cancel completion changed operation kind")
                    };
                    let _ = response.send(result);
                }
                InflightOperation::Single { .. } => {
                    assert_eq!(slot.part, 0, "single operation used a nonzero CQE part");
                    let operation = self
                        .inflight_operations
                        .remove(&slot.operation)
                        .expect("single operation disappeared during completion");
                    let InflightOperation::Single { response } = operation else {
                        unreachable!("single completion changed operation kind")
                    };
                    let _ = response.send(Ok(result));
                }
                InflightOperation::Routed { .. } | InflightOperation::RoutedVectored { .. } => {
                    assert_eq!(slot.part, 0, "routed operation used a nonzero CQE part");
                    let operation = self
                        .inflight_operations
                        .remove(&slot.operation)
                        .expect("routed operation disappeared during completion");
                    let terminal = match operation {
                        InflightOperation::Routed { terminal } => terminal,
                        InflightOperation::RoutedVectored { terminal, storage } => {
                            // A terminal CQE proves metadata can be discarded;
                            // return shared ownership only after discarding it.
                            drop(storage);
                            terminal
                        }
                        _ => unreachable!("routed completion changed operation kind"),
                    };
                    terminal.complete(Ok(result));
                }
                InflightOperation::Linked { results, .. } => {
                    assert!(
                        results[slot.part].replace(result).is_none(),
                        "linked operation received a duplicate CQE part"
                    );
                    if results.iter().all(Option::is_some) {
                        let operation = self
                            .inflight_operations
                            .remove(&slot.operation)
                            .expect("linked operation disappeared during completion");
                        let InflightOperation::Linked { results, response } = operation else {
                            unreachable!("linked completion changed operation kind")
                        };
                        let _ = response.send(Ok([
                            results[0].expect("first linked result is present"),
                            results[1].expect("second linked result is present"),
                        ]));
                    }
                }
                InflightOperation::LinkedAccept {
                    accept, timeout, ..
                } => {
                    match slot.part {
                        0 => {
                            assert!(
                                accept.replace(own_accept_completion(result)).is_none(),
                                "linked accept received a duplicate accept CQE"
                            );
                        }
                        1 => {
                            assert!(
                                timeout.replace(result).is_none(),
                                "linked accept received a duplicate timeout CQE"
                            );
                        }
                        _ => unreachable!("linked accept used an invalid CQE part"),
                    }
                    if accept.is_some() && timeout.is_some() {
                        let operation = self
                            .inflight_operations
                            .remove(&slot.operation)
                            .expect("linked accept disappeared during completion");
                        let InflightOperation::LinkedAccept {
                            accept,
                            timeout,
                            response,
                        } = operation
                        else {
                            unreachable!("linked accept completion changed operation kind")
                        };
                        let _ = response.send(Ok(LinkedAcceptCompletions {
                            accept: accept.expect("accept result is present"),
                            timeout: timeout.expect("accept timeout result is present"),
                        }));
                    }
                }
            }
        }
        consumed
    }

    fn reject_waiting(&mut self) {
        if let Some(request) = self.deferred.take() {
            request.complete_not_applied(driver_stopped_error());
        }
        loop {
            match self.receiver.try_recv() {
                Ok(request) => request.complete_not_applied(driver_stopped_error()),
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.ingress_closed = true;
                    return;
                }
            }
        }
    }
}

impl Ring {
    pub(crate) fn for_file(entries: u32, max_io_chunk_bytes: usize) -> io::Result<Self> {
        Self::new(
            RingCapacity::transient_only(entries),
            max_io_chunk_bytes,
            &[
                ("read", opcode::Read::CODE),
                ("write", opcode::Write::CODE),
                ("fsync", opcode::Fsync::CODE),
            ],
        )
    }

    /// Builds the shared ring for a pooled file coordinator.
    ///
    /// Unlike [`Self::for_file`], in-flight capacity is chosen independently
    /// of submission depth: the pool multiplexes many files onto one ring,
    /// so its budget reflects the pool's admission, not the SQ size. The
    /// pool observes file lengths with statx SQEs instead of blocking
    /// metadata syscalls, so that opcode joins the probed floor.
    pub(crate) fn for_pool(capacity: RingCapacity, max_io_chunk_bytes: usize) -> io::Result<Self> {
        Self::new(
            capacity,
            max_io_chunk_bytes,
            &[
                ("read", opcode::Read::CODE),
                ("write", opcode::Write::CODE),
                ("fsync", opcode::Fsync::CODE),
                ("statx", opcode::Statx::CODE),
            ],
        )
    }

    /// Builds the data-plane ring for one connected stream.
    ///
    /// A blocking read stays armed until the peer sends, so by lifetime it is
    /// sustained work. It is nonetheless budgeted as transient here because
    /// this ring serves exactly one stream's read and write actors: the single
    /// armed read cannot starve anything but its own stream's writes, and the
    /// remaining depth is reserved for them. That isolation is bought with a
    /// ring and three threads per connection, which is why
    /// `UringNetworkProviderConfig::max_streams` is bounded to a stream count
    /// the host can actually carry.
    pub(crate) fn for_network(entries: u32, max_io_chunk_bytes: usize) -> io::Result<Self> {
        Self::new(
            RingCapacity::transient_only(entries),
            max_io_chunk_bytes,
            &[
                ("recv", opcode::Recv::CODE),
                ("send", opcode::Send::CODE),
                ("shutdown", opcode::Shutdown::CODE),
            ],
        )
    }

    /// Builds the shared ring for a pooled stream coordinator.
    ///
    /// Every connected stream shares this one ring, so both directions of
    /// stream I/O are peer-gated sustained work: an armed receive waits for
    /// the peer to send, and a send on a full socket buffer waits for the
    /// peer to read. `capacity.sustained` must therefore reserve one slot
    /// per direction for every stream that can be registered at once, so
    /// one stalled peer can never starve another stream's transfers.
    /// Control-plane pairs — connect and accept, each linked to a timeout —
    /// are deadline-bounded, so they run on the transient budget, sized to
    /// the maximum concurrent pairs. Probing the control opcodes here makes
    /// pool construction fail before a listener or connect would.
    pub(crate) fn for_stream_pool(
        capacity: RingCapacity,
        max_io_chunk_bytes: usize,
    ) -> io::Result<Self> {
        Self::new(
            capacity,
            max_io_chunk_bytes,
            &[
                ("recv", opcode::Recv::CODE),
                ("send", opcode::Send::CODE),
                ("sendmsg", opcode::SendMsg::CODE),
                ("connect", opcode::Connect::CODE),
                ("accept", opcode::Accept::CODE),
                ("link timeout", opcode::LinkTimeout::CODE),
            ],
        )
    }

    /// Builds a shared ring for atomic datagram sends and receives.
    ///
    /// Unlike streams, every socket shares this one ring, so an armed receive
    /// competes with unrelated sockets' sends. `capacity.sustained` must
    /// therefore reserve a slot for every receive that can be armed at once.
    pub(crate) fn for_datagram(capacity: RingCapacity) -> io::Result<Self> {
        Self::new(
            capacity,
            MAX_IO_BYTES,
            &[
                ("sendmsg", opcode::SendMsg::CODE),
                ("recvmsg", opcode::RecvMsg::CODE),
            ],
        )
    }

    /// Builds the provider control ring and proves all provider capabilities.
    ///
    /// Provider control and listeners share this ring. Probing the stream
    /// operations too makes provider construction fail before an accepted
    /// connection would need its own data-plane reactor.
    pub(crate) fn for_network_provider(
        entries: u32,
        max_io_chunk_bytes: usize,
    ) -> io::Result<Self> {
        Self::new(
            RingCapacity::transient_only(entries),
            max_io_chunk_bytes,
            &[
                ("connect", opcode::Connect::CODE),
                ("accept", opcode::Accept::CODE),
                ("link timeout", opcode::LinkTimeout::CODE),
                ("recv", opcode::Recv::CODE),
                ("send", opcode::Send::CODE),
                ("shutdown", opcode::Shutdown::CODE),
            ],
        )
    }

    fn new(
        capacity: RingCapacity,
        max_io_chunk_bytes: usize,
        required: &[(&str, u8)],
    ) -> io::Result<Self> {
        let RingCapacity {
            entries,
            transient,
            sustained,
        } = capacity;
        if entries < 4 || !entries.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("io_uring entries must be a power of two of at least 4, got {entries}"),
            ));
        }
        if transient == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring transient capacity must be nonzero",
            ));
        }
        if max_io_chunk_bytes == 0 || max_io_chunk_bytes > MAX_IO_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "maximum I/O chunk must be in 1..={MAX_IO_BYTES}, got {max_io_chunk_bytes}"
                ),
            ));
        }

        // The completion queue must hold every CQE the budgeted work can owe
        // at once. Sizing it from `entries` alone was sound only while
        // in-flight work could not outnumber submission slots; a reserved
        // sustained budget breaks that, and an undersized CQ would push the
        // kernel into its overflow list on every idle socket. The internal
        // wake poll owes a CQE no class budget accounts for, so it is added
        // here explicitly — otherwise a fully occupied power-of-two capacity
        // would overflow on the wake completion alone. This sizing is a
        // fast-path guarantee, not a hard bound: Exempt cancels also owe CQEs
        // and are deliberately unbudgeted, so a mass retirement can still
        // overflow the queue. That is safe only because construction requires
        // `IORING_FEAT_NODROP`: an overfull queue then refuses submission
        // with `EBUSY`, which the reactor recovers from by reaping.
        let max_in_flight = transient.checked_add(sustained).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring in-flight capacity overflowed",
            )
        })?;
        let owed_completions = max_in_flight.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring in-flight capacity overflowed",
            )
        })?;
        let cq_entries = u32::try_from(owed_completions.next_power_of_two())
            .ok()
            .filter(|&cq| cq <= MAX_CQ_ENTRIES)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "io_uring in-flight capacity {owed_completions} exceeds the \
                         {MAX_CQ_ENTRIES}-entry completion queue limit"
                    ),
                )
            })?
            .max(entries.saturating_mul(2));

        let mut ring = IoUring::builder()
            .dontfork()
            .setup_cqsize(cq_entries)
            .build(entries)?;
        // Completion-queue overflow recovery depends on the kernel refusing
        // to submit into a full queue rather than dropping CQEs. A dropped
        // CQE strands its operation forever: the reactor can never quiesce
        // and the caller's buffer can never be safely returned. Kernels
        // without the feature (io_uring before 5.5) fail closed here instead
        // of degrading into that silently under load.
        if !ring.params().is_feature_nodrop() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "running kernel does not report IORING_FEAT_NODROP; \
                 completion-queue overflow would drop completions",
            ));
        }
        probe_required_operations(&ring, required)?;
        probe_required_operations(
            &ring,
            &[
                ("poll", opcode::PollAdd::CODE),
                ("async cancel", opcode::AsyncCancel::CODE),
            ],
        )?;
        if ring.completion().next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "new io_uring unexpectedly contained a completion",
            ));
        }

        let event_fd = create_event_fd()?;
        // Sized to total in-flight capacity, not submission depth: every
        // admitted operation the reactor can legitimately stage must be able to
        // reach it without its caller blocking behind a full queue.
        let (sender, receiver) = mpsc::sync_channel(max_in_flight);
        // Unbounded: a closing thread must never block behind a full queue to
        // retire work, and live cancels are already bounded by the number of
        // in-flight cancellable operations.
        let (cancel_sender, cancel_receiver) = mpsc::channel();
        let poisoned = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        let max_observed_active_transient_sqes = Arc::new(AtomicUsize::new(0));
        #[cfg(test)]
        let deferred_requests = Arc::new(AtomicUsize::new(0));
        let reactor_poisoned = Arc::clone(&poisoned);
        #[cfg(test)]
        let reactor_max_observed = Arc::clone(&max_observed_active_transient_sqes);
        #[cfg(test)]
        let reactor_deferred_requests = Arc::clone(&deferred_requests);
        let event_raw_fd = event_fd.as_raw_fd();
        let join = thread::Builder::new()
            .name("kr-runtime-io-uring-reactor".to_owned())
            .spawn(move || {
                let _fail_stop = FailStopOnPanic;
                Reactor {
                    ring,
                    receiver,
                    cancel_receiver,
                    deferred: None,
                    event_fd: event_raw_fd,
                    poisoned: reactor_poisoned,
                    active_user_sqes: 0,
                    active_transient_sqes: 0,
                    active_sustained_sqes: 0,
                    submitted_user_sqes: 0,
                    #[cfg(test)]
                    max_observed_active_transient_sqes: reactor_max_observed,
                    #[cfg(test)]
                    deferred_requests: reactor_deferred_requests,
                    max_transient_sqes: transient,
                    max_sustained_sqes: sustained,
                    next_user_data: FIRST_USER_DATA,
                    inflight_slots: HashMap::new(),
                    inflight_operations: HashMap::new(),
                    wake_armed: false,
                    ingress_closed: false,
                    poison_cancel_backlog: Vec::new(),
                    stalled_enter_failures: 0,
                }
                .run();
            })?;
        Ok(Self {
            host: Arc::new(ReactorHost {
                sender: Mutex::new(Some(sender)),
                cancel_sender: Mutex::new(Some(cancel_sender)),
                event_fd,
                join: Mutex::new(Some(join)),
                poisoned,
                #[cfg(test)]
                max_observed_active_transient_sqes,
                #[cfg(test)]
                deferred_requests,
                max_transient_sqes: transient,
            }),
            max_io_chunk_bytes,
        })
    }

    pub(crate) fn start_read_at(
        &self,
        file: &File,
        file_offset: u64,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
    ) -> Result<PendingTransfer, OwnedTransferFailure> {
        self.start_transfer(buffer, buffer_offset, len, |pointer, chunk_len| {
            let offset = checked_kernel_offset(file_offset)?;
            Ok(
                opcode::Read::new(types::Fd(file.as_raw_fd()), pointer, chunk_len)
                    .offset(offset)
                    .build(),
            )
        })
    }

    pub(crate) fn start_write_at(
        &self,
        file: &File,
        file_offset: u64,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
    ) -> Result<PendingTransfer, OwnedTransferFailure> {
        self.start_transfer_const(buffer, buffer_offset, len, |pointer, chunk_len| {
            let offset = checked_kernel_offset(file_offset)?;
            Ok(
                opcode::Write::new(types::Fd(file.as_raw_fd()), pointer, chunk_len)
                    .offset(offset)
                    .build(),
            )
        })
    }

    /// Submits one positional read whose completion is routed.
    ///
    /// Not `unsafe`, but pointer-bearing: the SQE references `buffer`'s
    /// allocation, so the caller must keep that allocation alive and
    /// unshrunk — the `Vec` value may move, its allocation may not be freed
    /// or reallocated — until the [`RoutedCompletion`] carrying `token`
    /// arrives. The pooled coordinator satisfies this by holding the buffer
    /// in the operation's slot until the completion is consumed.
    pub(crate) fn start_routed_read_at(
        &self,
        file: &File,
        file_offset: u64,
        buffer: &mut Vec<u8>,
        len: usize,
        response: &Sender<RoutedCompletion>,
        token: u64,
    ) -> io::Result<RoutedSubmission> {
        let chunk_len = transfer_len(buffer, 0, len, self.max_io_chunk_bytes)?;
        self.ensure_healthy()?;
        if chunk_len == 0 {
            return Ok(RoutedSubmission::Empty);
        }
        let offset = checked_kernel_offset(file_offset)?;
        // SAFETY: transfer_len validated this initialized range, and the
        // routed contract above keeps the allocation live to the terminal
        // completion.
        let pointer = buffer.as_mut_ptr();
        let entry = opcode::Read::new(types::Fd(file.as_raw_fd()), pointer, chunk_len as u32)
            .offset(offset)
            .build();
        // File transfers terminalize on their own, so they charge the
        // transient budget.
        self.enqueue_routed(entry, SqeClass::Transient, response, token)?;
        Ok(RoutedSubmission::Submitted {
            requested: chunk_len,
        })
    }

    /// Submits one positional write whose completion is routed.
    ///
    /// Carries the same buffer-liveness contract as
    /// [`Self::start_routed_read_at`].
    pub(crate) fn start_routed_write_at(
        &self,
        file: &File,
        file_offset: u64,
        buffer: &[u8],
        len: usize,
        response: &Sender<RoutedCompletion>,
        token: u64,
    ) -> io::Result<RoutedSubmission> {
        let chunk_len = transfer_len(buffer, 0, len, self.max_io_chunk_bytes)?;
        self.ensure_healthy()?;
        if chunk_len == 0 {
            return Ok(RoutedSubmission::Empty);
        }
        let offset = checked_kernel_offset(file_offset)?;
        // SAFETY: transfer_len validated this initialized range, and the
        // routed contract keeps the allocation live to the terminal
        // completion. The kernel only reads through this pointer.
        let pointer = buffer.as_ptr();
        let entry = opcode::Write::new(types::Fd(file.as_raw_fd()), pointer, chunk_len as u32)
            .offset(offset)
            .build();
        self.enqueue_routed(entry, SqeClass::Transient, response, token)?;
        Ok(RoutedSubmission::Submitted {
            requested: chunk_len,
        })
    }

    /// Submits one socket receive whose completion is routed.
    ///
    /// Carries the same buffer-liveness contract as
    /// [`Self::start_routed_read_at`]. Submitted as [`SqeClass::Sustained`]:
    /// an armed stream receive waits for the peer to send — or for the
    /// socket to be shut down — so on a shared ring it must draw from the
    /// reserved per-stream budget rather than the transient one.
    pub(crate) fn start_routed_recv(
        &self,
        socket: &TcpStream,
        buffer: &mut Vec<u8>,
        buffer_offset: usize,
        len: usize,
        response: &Sender<RoutedCompletion>,
        token: u64,
    ) -> io::Result<RoutedSubmission> {
        let chunk_len = transfer_len(buffer, buffer_offset, len, self.max_io_chunk_bytes)?;
        self.ensure_healthy()?;
        if chunk_len == 0 {
            return Ok(RoutedSubmission::Empty);
        }
        // SAFETY: transfer_len validated this initialized range, and the
        // routed contract keeps the allocation live to the terminal
        // completion.
        let pointer = unsafe { buffer.as_mut_ptr().add(buffer_offset) };
        let entry =
            opcode::Recv::new(types::Fd(socket.as_raw_fd()), pointer, chunk_len as u32).build();
        self.enqueue_routed(entry, SqeClass::Sustained, response, token)?;
        Ok(RoutedSubmission::Submitted {
            requested: chunk_len,
        })
    }

    /// Submits one socket send whose completion is routed.
    ///
    /// Carries the same buffer-liveness contract as
    /// [`Self::start_routed_read_at`]; the kernel only reads through the
    /// pointer. Also [`SqeClass::Sustained`]: a send on a full socket buffer
    /// stays armed until the peer reads or the socket is shut down, so a
    /// stalled peer must consume its own stream's reserved slot, never the
    /// shared transient budget.
    pub(crate) fn start_routed_send(
        &self,
        socket: &TcpStream,
        buffer: &[u8],
        buffer_offset: usize,
        len: usize,
        response: &Sender<RoutedCompletion>,
        token: u64,
    ) -> io::Result<RoutedSubmission> {
        let chunk_len = transfer_len(buffer, buffer_offset, len, self.max_io_chunk_bytes)?;
        self.ensure_healthy()?;
        if chunk_len == 0 {
            return Ok(RoutedSubmission::Empty);
        }
        // SAFETY: transfer_len validated this initialized range, and the
        // routed contract keeps the allocation live to the terminal
        // completion. The kernel only reads through this pointer.
        let pointer = unsafe { buffer.as_ptr().add(buffer_offset) };
        let entry = opcode::Send::new(types::Fd(socket.as_raw_fd()), pointer, chunk_len as u32)
            .flags(libc::MSG_NOSIGNAL)
            .build();
        self.enqueue_routed(entry, SqeClass::Sustained, response, token)?;
        Ok(RoutedSubmission::Submitted {
            requested: chunk_len,
        })
    }

    /// Transfers original shared spans to the ring thread for a single native
    /// scatter/gather send. The socket stays owned by the coordinator until
    /// the routed completion returns its segments. No payload bytes are copied.
    pub(crate) fn start_routed_send_vectored(
        &self,
        socket: &TcpStream,
        request: VectoredWriteRequest,
        response: &Sender<RoutedCompletion>,
        token: u64,
    ) -> Result<usize, RoutedVectoredStartFailure> {
        let _fail_stop = FailStopOnPanic;
        let size = match request.validate(MAX_WRITE_SEGMENTS, usize::MAX) {
            Ok(size) => size,
            Err(error) => {
                return Err(RoutedVectoredStartFailure {
                    error: io::Error::new(io::ErrorKind::InvalidInput, error),
                    segments: request.segments,
                });
            }
        };
        if let Err(error) = self.ensure_healthy() {
            return Err(RoutedVectoredStartFailure {
                error,
                segments: request.segments,
            });
        }
        let requested = size.payload_bytes.min(self.max_io_chunk_bytes);
        let request = ReactorRequest::RoutedVectored {
            socket: socket.as_raw_fd(),
            requested,
            terminal: RoutedTerminal::new(token, response.clone(), Some(request.segments)),
        };
        if let Err(request) = self.host.enqueue(request) {
            let ReactorRequest::RoutedVectored { mut terminal, .. } = *request else {
                unreachable!("enqueue returns the submitted request")
            };
            return Err(RoutedVectoredStartFailure {
                error: driver_stopped_error(),
                segments: terminal
                    .segments
                    .take()
                    .expect("rejected request retains original segments"),
            });
        }
        Ok(requested)
    }

    /// Submits one connect linked to its establishment timeout, both routed.
    ///
    /// One submission, two completions joined by the caller through
    /// [`Self::finish_routed_connect`]. The kernel reads the encoded peer
    /// address and the timeout through pointers into the returned storage,
    /// which the caller must keep alive — boxed, so moves of the owner do
    /// not move it — until both routed completions have arrived. Transient:
    /// the linked timeout bounds how long the pair can stay armed.
    pub(crate) fn start_routed_connect(
        &self,
        socket: &TcpStream,
        address: SocketAddr,
        timeout: Duration,
        response: &Sender<RoutedCompletion>,
        connect_token: u64,
        timeout_token: u64,
    ) -> io::Result<RoutedConnectStorage> {
        self.ensure_healthy()?;
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring connect timeout must be nonzero",
            ));
        }
        let storage = RoutedConnectStorage {
            address: Box::new(EncodedSocketAddr::new(address)),
            timeout: Box::new(types::Timespec::from(timeout)),
        };
        let (address_pointer, address_len) = storage.address.as_ptr_len();
        let connect_entry =
            opcode::Connect::new(types::Fd(socket.as_raw_fd()), address_pointer, address_len)
                .build()
                .flags(squeue::Flags::IO_LINK);
        let timeout_entry = opcode::LinkTimeout::new(&*storage.timeout).build();
        let request = ReactorRequest::RoutedPair {
            entries: [connect_entry, timeout_entry],
            class: SqeClass::Transient,
            terminals: [
                RoutedTerminal::new(connect_token, response.clone(), None),
                RoutedTerminal::new(timeout_token, response.clone(), None),
            ],
        };
        self.host
            .enqueue(request)
            .map_err(|_| driver_stopped_error())?;
        Ok(storage)
    }

    /// Decodes a joined routed connect pair, mirroring [`Self::connect`].
    pub(crate) fn finish_routed_connect(
        &self,
        connect: RoutedResult,
        timeout: RoutedResult,
    ) -> Result<(), SubmissionFailure> {
        let connect_result = connect?;
        let timeout_result = timeout?;
        match classify_linked_connect(connect_result, timeout_result) {
            Ok(ConnectDisposition::Connected) => Ok(()),
            Ok(ConnectDisposition::TimedOut) => Err(SubmissionFailure::uncertain(
                io::Error::from_raw_os_error(libc::ETIMEDOUT),
            )),
            Ok(ConnectDisposition::Failed(result)) => {
                let error = decode_unit_result(result, "connect").unwrap_err();
                if connect_failure_is_known_not_applied(&error) {
                    Err(SubmissionFailure::known(error))
                } else {
                    Err(SubmissionFailure::uncertain(error))
                }
            }
            Err(error) => {
                self.host.poison();
                Err(SubmissionFailure::uncertain(error))
            }
        }
    }

    /// Submits one accept attempt linked to its retry timeout, both routed.
    ///
    /// One submission, two completions joined by the caller through
    /// [`Self::finish_routed_accept`]. The peer address is not captured, so
    /// the only kernel-owned storage is the returned timeout, which must
    /// stay alive until both routed completions have arrived. Transient:
    /// the linked timeout bounds how long the attempt can stay armed, and
    /// an expired attempt is re-armed by the caller while it still has an
    /// accept to serve.
    pub(crate) fn start_routed_accept(
        &self,
        listener: &TcpListener,
        timeout: Duration,
        response: &Sender<RoutedCompletion>,
        accept_token: u64,
        timeout_token: u64,
    ) -> io::Result<RoutedAcceptStorage> {
        self.ensure_healthy()?;
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring accept attempt timeout must be nonzero",
            ));
        }
        let storage = RoutedAcceptStorage {
            timeout: Box::new(types::Timespec::from(timeout)),
        };
        let accept_entry = opcode::Accept::new(
            types::Fd(listener.as_raw_fd()),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
        .flags(libc::SOCK_CLOEXEC)
        .build()
        .flags(squeue::Flags::IO_LINK);
        let timeout_entry = opcode::LinkTimeout::new(&*storage.timeout).build();
        let request = ReactorRequest::RoutedPair {
            entries: [accept_entry, timeout_entry],
            class: SqeClass::Transient,
            terminals: [
                RoutedTerminal::new(accept_token, response.clone(), None),
                RoutedTerminal::new(timeout_token, response.clone(), None),
            ],
        };
        self.host
            .enqueue(request)
            .map_err(|_| driver_stopped_error())?;
        Ok(storage)
    }

    /// Decodes a joined routed accept pair, mirroring [`Self::accept`].
    ///
    /// A successful descriptor is wrapped in `TcpStream` as soon as it is
    /// observed, so every later error path closes it by RAII.
    pub(crate) fn finish_routed_accept(
        &self,
        accept: RoutedResult,
        timeout: RoutedResult,
    ) -> io::Result<RoutedAcceptOutcome> {
        let accept_result = accept.map_err(|failure| failure.into_parts().0)?;
        let timeout_result = timeout.map_err(|failure| failure.into_parts().0)?;
        let stream = if accept_result >= 0 {
            // SAFETY: a nonnegative accept CQE result is a live descriptor
            // the kernel just returned and nothing else owns.
            Some(unsafe { TcpStream::from_raw_fd(accept_result) })
        } else {
            None
        };
        let disposition = match classify_linked_accept(accept_result, timeout_result) {
            Ok(disposition) => disposition,
            Err(error) => {
                self.host.poison();
                return Err(error);
            }
        };
        match (disposition, stream) {
            (AcceptDisposition::Accepted, Some(stream)) => {
                Ok(RoutedAcceptOutcome::Accepted(stream))
            }
            (AcceptDisposition::NoConnection, None) => Ok(RoutedAcceptOutcome::NoConnection),
            (AcceptDisposition::Failed(result), None) => match decode_transfer_result(result) {
                Err(error) => Ok(RoutedAcceptOutcome::Failed(error)),
                Ok(_) => {
                    self.host.poison();
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "io_uring classified a successful accept result as a failure",
                    ))
                }
            },
            _ => {
                self.host.poison();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "io_uring linked accept classification did not match its completion",
                ))
            }
        }
    }

    /// Submits one full-file sync linked to a size-only statx, both routed.
    ///
    /// One submission, two completions: the kernel runs the fsync, then the
    /// statx that observes the durable length, severing the statx with
    /// `-ECANCELED` if the fsync fails. The caller joins the two routed
    /// completions and carries the same buffer-liveness contract for `statx`
    /// as [`Self::start_routed_statx_size`].
    pub(crate) fn start_routed_fsync_then_statx_size(
        &self,
        file: &File,
        statx: &mut Box<libc::statx>,
        response: &Sender<RoutedCompletion>,
        fsync_token: u64,
        statx_token: u64,
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        // See `start_routed_statx_size` for the path's liveness argument.
        static EMPTY_PATH: [libc::c_char; 1] = [0];
        let fsync = opcode::Fsync::new(types::Fd(file.as_raw_fd()))
            .build()
            .flags(squeue::Flags::IO_LINK);
        let observe = opcode::Statx::new(
            types::Fd(file.as_raw_fd()),
            EMPTY_PATH.as_ptr(),
            std::ptr::from_mut::<libc::statx>(&mut **statx).cast(),
        )
        .flags(libc::AT_EMPTY_PATH)
        .mask(libc::STATX_SIZE)
        .build();
        let request = ReactorRequest::RoutedPair {
            entries: [fsync, observe],
            class: SqeClass::Transient,
            terminals: [
                RoutedTerminal::new(fsync_token, response.clone(), None),
                RoutedTerminal::new(statx_token, response.clone(), None),
            ],
        };
        self.host
            .enqueue(request)
            .map_err(|_| driver_stopped_error())
    }

    /// Allocates the kernel-writable result storage for a routed statx.
    pub(crate) fn new_statx_buffer() -> Box<libc::statx> {
        // SAFETY: `statx` is a plain-old-data kernel structure for which all
        // zero bytes are a valid (empty) value; the kernel overwrites the
        // fields it reports.
        Box::new(unsafe { std::mem::zeroed() })
    }

    /// Submits one size-only statx on the open descriptor, routed.
    ///
    /// The kernel writes into `statx`, so the caller must keep that
    /// allocation alive — boxed, so moves of the owner do not move it —
    /// until the [`RoutedCompletion`] carrying `token` arrives, exactly like
    /// a routed transfer buffer.
    pub(crate) fn start_routed_statx_size(
        &self,
        file: &File,
        statx: &mut Box<libc::statx>,
        response: &Sender<RoutedCompletion>,
        token: u64,
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        // `AT_EMPTY_PATH` statx targets the descriptor itself; the path must
        // still be a live empty C string for the kernel to read, and a
        // static is live for the whole program.
        static EMPTY_PATH: [libc::c_char; 1] = [0];
        let entry = opcode::Statx::new(
            types::Fd(file.as_raw_fd()),
            EMPTY_PATH.as_ptr(),
            std::ptr::from_mut::<libc::statx>(&mut **statx).cast(),
        )
        .flags(libc::AT_EMPTY_PATH)
        .mask(libc::STATX_SIZE)
        .build();
        self.enqueue_routed(entry, SqeClass::Transient, response, token)
    }

    /// Decodes a routed size-only statx completion.
    pub(crate) fn finish_routed_statx_size(
        result: SingleResponse,
        statx: &libc::statx,
    ) -> io::Result<u64> {
        match result {
            Ok(result) => {
                decode_unit_result(result, "statx")?;
                if statx.stx_mask & libc::STATX_SIZE == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "statx completed without reporting a size",
                    ));
                }
                Ok(statx.stx_size)
            }
            Err(failure) => Err(failure.error),
        }
    }

    fn enqueue_routed(
        &self,
        entry: squeue::Entry,
        class: SqeClass,
        response: &Sender<RoutedCompletion>,
        token: u64,
    ) -> io::Result<()> {
        let request = ReactorRequest::Routed {
            entry,
            class,
            terminal: RoutedTerminal::new(token, response.clone(), None),
        };
        self.host
            .enqueue(request)
            .map_err(|_| driver_stopped_error())
    }

    /// Decodes a routed transfer completion against its admitted length.
    ///
    /// The same validation [`PendingTransfer::finish`] performs, minus the
    /// buffer bookkeeping the routed caller owns: a completion larger than
    /// the admitted chunk poisons the ring and reports uncertainty, because
    /// the kernel's report can no longer be trusted.
    pub(crate) fn finish_routed_transfer(
        &self,
        requested: usize,
        result: SingleResponse,
    ) -> Result<usize, RoutedTransferFailure> {
        let result = match result {
            Ok(result) => result,
            Err(failure) => {
                return Err(RoutedTransferFailure {
                    error: failure.error,
                    may_have_applied: failure.may_have_applied,
                });
            }
        };
        let transferred = match decode_transfer_result(result) {
            Ok(transferred) => transferred,
            Err(error) => {
                return Err(RoutedTransferFailure {
                    error,
                    may_have_applied: false,
                });
            }
        };
        if transferred > requested {
            self.host.poison();
            return Err(RoutedTransferFailure {
                error: invalid_completion("transfer", transferred, requested),
                may_have_applied: true,
            });
        }
        Ok(transferred)
    }

    /// Decodes a routed fsync completion.
    pub(crate) fn finish_routed_fsync(result: SingleResponse) -> io::Result<()> {
        match result {
            Ok(result) => decode_unit_result(result, "fsync"),
            Err(failure) => Err(failure.error),
        }
    }

    pub(crate) fn recv(
        &mut self,
        stream: &TcpStream,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
    ) -> Result<OwnedTransfer, OwnedTransferFailure> {
        // Socket transfers block: admit the SQE and immediately wait for its
        // terminal CQE through the shared pending-transfer path.
        self.start_transfer(buffer, buffer_offset, len, |pointer, chunk_len| {
            Ok(opcode::Recv::new(types::Fd(stream.as_raw_fd()), pointer, chunk_len).build())
        })?
        .finish()
    }

    pub(crate) fn send(
        &mut self,
        stream: &TcpStream,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
    ) -> Result<OwnedTransfer, OwnedTransferFailure> {
        // Socket transfers block: admit the SQE and immediately wait for its
        // terminal CQE through the shared pending-transfer path.
        self.start_transfer_const(buffer, buffer_offset, len, |pointer, chunk_len| {
            Ok(
                opcode::Send::new(types::Fd(stream.as_raw_fd()), pointer, chunk_len)
                    .flags(libc::MSG_NOSIGNAL)
                    .build(),
            )
        })?
        .finish()
    }

    /// Sends exactly one UDP datagram with one `SendMsg` SQE.
    ///
    /// Empty buffers are submitted to the kernel rather than treated as an
    /// empty byte-stream transfer. A nonnegative CQE count other than the exact
    /// payload length poisons this ring and is returned as an uncertain effect.
    pub(crate) fn send_datagram(
        &mut self,
        socket: &UdpSocket,
        buffer: Vec<u8>,
        destination: SocketAddr,
    ) -> Result<OwnedDatagramSend, OwnedDatagramFailure> {
        if let Err(error) = self.ensure_healthy() {
            return Err(OwnedDatagramFailure::not_applied(buffer, error));
        }

        let address = EncodedSocketAddr::new(destination);
        let (address_pointer, address_len) = address.as_ptr_len();
        let mut iovec = libc::iovec {
            // The kernel does not dereference this pointer when `iov_len` is
            // zero. For a nonempty Vec it refers to its initialized payload.
            iov_base: buffer.as_ptr().cast_mut().cast(),
            iov_len: buffer.len(),
        };
        // SAFETY: zero is valid for all optional msghdr fields. Every pointer
        // used below is assigned together with its exact live length.
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_name = address_pointer.cast_mut().cast();
        message.msg_namelen = address_len;
        message.msg_iov = std::ptr::from_mut(&mut iovec);
        message.msg_iovlen = 1;
        let entry =
            opcode::SendMsg::new(types::Fd(socket.as_raw_fd()), std::ptr::from_ref(&message))
                .flags((libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) as u32)
                .build();
        let result = self.submit_one(&entry);
        // Keep every raw-pointer target visibly live until the terminal CQE has
        // been consumed, including on submission-error paths.
        std::hint::black_box((&address, &iovec, &message));

        let result = match result {
            Ok(result) => result,
            Err(failure) => {
                let (error, may_have_applied) = failure.into_parts();
                return Err(if may_have_applied {
                    OwnedDatagramFailure::may_have_applied(buffer, error, 0)
                } else {
                    OwnedDatagramFailure::not_applied(buffer, error)
                });
            }
        };
        if result == i32::MIN {
            self.poison();
            return Err(OwnedDatagramFailure::may_have_applied(
                buffer,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("io_uring returned invalid datagram-send result {result}"),
                ),
                0,
            ));
        }
        let transferred = match decode_transfer_result(result) {
            Ok(transferred) => transferred,
            Err(error) => return Err(OwnedDatagramFailure::not_applied(buffer, error)),
        };
        let requested = buffer.len();
        if transferred != requested {
            self.poison();
            return Err(OwnedDatagramFailure::may_have_applied(
                buffer,
                invalid_completion("datagram send", transferred, requested),
                transferred,
            ));
        }
        Ok(OwnedDatagramSend {
            buffer,
            transferred,
        })
    }

    /// Attempts one nonblocking UDP receive with `RecvMsg(MSG_TRUNC)`.
    ///
    /// `buffer_offset` must equal the incoming buffer length. Kernel writes go
    /// to separate scratch storage, so the caller buffer remains untouched on
    /// failure. A successful packet appends only the copied payload prefix.
    ///
    /// A negative `EAGAIN`/`EWOULDBLOCK` CQE is returned as a known-not-applied
    /// failure for the socket actor to map to its portable `WouldBlock` status.
    pub(crate) fn try_recv_datagram(
        &mut self,
        socket: &UdpSocket,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
    ) -> Result<OwnedDatagramReceive, OwnedDatagramFailure> {
        match self.recv_datagram(
            socket,
            buffer,
            buffer_offset,
            len,
            DatagramReceiveWait::Nonblocking,
        )? {
            DatagramReceiveAttempt::Received(receive) => Ok(receive),
            DatagramReceiveAttempt::WouldBlock { buffer } => {
                Err(OwnedDatagramFailure::not_applied(
                    buffer,
                    io::Error::from_raw_os_error(libc::EAGAIN),
                ))
            }
            DatagramReceiveAttempt::Cancelled { .. } => {
                unreachable!("a nonblocking receive is never cancellable")
            }
        }
    }

    /// Receives one UDP datagram, blocking in the kernel until a packet
    /// arrives, `deadline` elapses, or the caller cancels the operation.
    ///
    /// `buffer_offset` has the same preserved-prefix meaning as in
    /// [`Self::try_recv_datagram`]. `arm` runs once, after the receive is
    /// staged and before this thread blocks; it receives the token another
    /// thread needs to retire the receive and reports `false` when the socket
    /// is already closing.
    ///
    /// This method never leaves an armed receive in the kernel after
    /// returning: every path, including cancellation and the deadline,
    /// consumes the terminal CQE first.
    ///
    /// # Errors
    ///
    /// Returns [`OwnedDatagramFailure`] carrying the caller's buffer when the
    /// receive could not be staged or the kernel reported a failure.
    pub(crate) fn recv_datagram_blocking(
        &mut self,
        socket: &UdpSocket,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
        deadline: Option<Instant>,
        mut arm: impl FnMut(CancelToken) -> bool,
    ) -> Result<DatagramReceiveAttempt, OwnedDatagramFailure> {
        self.recv_datagram(
            socket,
            buffer,
            buffer_offset,
            len,
            DatagramReceiveWait::Cancellable {
                deadline,
                arm: &mut arm,
            },
        )
    }

    pub(crate) fn fsync(&mut self, file: &File) -> io::Result<()> {
        self.ensure_healthy()?;
        let entry = opcode::Fsync::new(types::Fd(file.as_raw_fd())).build();
        decode_unit_result(
            self.submit_one(&entry).map_err(|failure| failure.error)?,
            "fsync",
        )
    }

    pub(crate) fn shutdown(&mut self, stream: &TcpStream, how: i32) -> io::Result<()> {
        self.ensure_healthy()?;
        let entry = opcode::Shutdown::new(types::Fd(stream.as_raw_fd()), how).build();
        decode_unit_result(
            self.submit_one(&entry).map_err(|failure| failure.error)?,
            "shutdown",
        )
    }

    /// Connects an already-created socket to `address` within `timeout`.
    ///
    /// `socket` remains borrowed and the encoded sockaddr remains on this stack
    /// until both the Connect and LinkTimeout terminal CQEs are consumed.
    pub(crate) fn connect<S: AsRawFd>(
        &mut self,
        socket: &S,
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<(), SubmissionFailure> {
        self.ensure_healthy().map_err(SubmissionFailure::known)?;
        if timeout.is_zero() {
            return Err(SubmissionFailure::known(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring connect timeout must be nonzero",
            )));
        }
        let address = EncodedSocketAddr::new(address);
        let (address_pointer, address_len) = address.as_ptr_len();
        let timeout = types::Timespec::from(timeout);
        let connect_entry =
            opcode::Connect::new(types::Fd(socket.as_raw_fd()), address_pointer, address_len)
                .build()
                .flags(squeue::Flags::IO_LINK);
        let timeout_entry = opcode::LinkTimeout::new(&timeout).build();
        let entries = [connect_entry, timeout_entry];
        let completions = self.submit_linked_entries(&entries);
        // Moving the encoded address and timeout only after both terminal CQEs
        // makes their pointer lifetimes explicit on every return path.
        let _address_lifetime = address;
        std::hint::black_box(&timeout);
        let [connect_result, timeout_result] = completions?;
        match classify_linked_connect(connect_result, timeout_result) {
            Ok(ConnectDisposition::Connected) => Ok(()),
            Ok(ConnectDisposition::TimedOut) => Err(SubmissionFailure::uncertain(
                io::Error::from_raw_os_error(libc::ETIMEDOUT),
            )),
            Ok(ConnectDisposition::Failed(result)) => {
                let error = decode_unit_result(result, "connect").unwrap_err();
                if connect_failure_is_known_not_applied(&error) {
                    Err(SubmissionFailure::known(error))
                } else {
                    Err(SubmissionFailure::uncertain(error))
                }
            }
            Err(error) => {
                self.poison();
                Err(SubmissionFailure::uncertain(error))
            }
        }
    }

    /// Attempts to accept one TCP connection within a short bounded interval.
    ///
    /// The Accept SQE is linked to a timeout and this method consumes both CQEs
    /// before returning. A successful descriptor is wrapped in `TcpStream` as
    /// soon as its CQE is observed, so every later error path closes it by RAII.
    /// If no connection arrives, the returned error has kind `WouldBlock`.
    pub(crate) fn accept(&mut self, listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
        self.ensure_healthy()?;
        // The storage is fully initialized so inspecting it after a short or
        // malformed kernel-reported length never reads uninitialized bytes.
        // It stays on this stack until the matching terminal CQE is consumed.
        // SAFETY: `sockaddr_storage` is a plain-old-data type for which all
        // zero bytes are a valid (empty) value.
        let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut address_len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let timeout = types::Timespec::from(ACCEPT_ATTEMPT_TIMEOUT);
        let accept_entry = opcode::Accept::new(
            types::Fd(listener.as_raw_fd()),
            std::ptr::from_mut(&mut address).cast::<libc::sockaddr>(),
            &mut address_len,
        )
        .flags(libc::SOCK_CLOEXEC)
        .build()
        .flags(squeue::Flags::IO_LINK);
        let timeout_entry = opcode::LinkTimeout::new(&timeout).build();
        let entries = [accept_entry, timeout_entry];
        let completions = self.submit_linked_accept(&entries);
        // Keep the timeout storage explicitly live until both CQEs have been
        // consumed, including kernels predating stable submission state.
        std::hint::black_box(&timeout);
        let completions = completions?;
        let accept_result = match &completions.accept {
            AcceptCompletion::Accepted(stream) => stream.as_raw_fd(),
            AcceptCompletion::Failed(result) => *result,
        };
        let disposition = match classify_linked_accept(accept_result, completions.timeout) {
            Ok(disposition) => disposition,
            Err(error) => {
                self.poison();
                return Err(error);
            }
        };
        match (disposition, completions.accept) {
            (AcceptDisposition::Accepted, AcceptCompletion::Accepted(stream)) => {
                let peer = decode_socket_addr(&address, address_len)?;
                Ok((stream, peer))
            }
            (AcceptDisposition::NoConnection, AcceptCompletion::Failed(_)) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring accept attempt completed without a connection",
            )),
            (AcceptDisposition::Failed(result), AcceptCompletion::Failed(_)) => {
                match decode_transfer_result(result) {
                    Err(error) => Err(error),
                    Ok(_) => {
                        self.poison();
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "io_uring classified a successful accept result as a failure",
                        ))
                    }
                }
            }
            _ => {
                self.poison();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "io_uring linked accept classification did not match its completion",
                ))
            }
        }
    }

    fn recv_datagram(
        &mut self,
        socket: &UdpSocket,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
        wait: DatagramReceiveWait<'_>,
    ) -> Result<DatagramReceiveAttempt, OwnedDatagramFailure> {
        // One receive now issues one kernel operation, so the scratch layout is
        // built once per call rather than carried across retry attempts.
        let mut scratch = Vec::new();
        let scratch_offset = match prepare_datagram_receive_scratch(
            &buffer,
            buffer.capacity(),
            buffer_offset,
            len,
            &mut scratch,
        ) {
            Ok(offset) => offset,
            Err(error) => return Err(OwnedDatagramFailure::not_applied(buffer, error)),
        };
        if let Err(error) = self.ensure_healthy() {
            return Err(OwnedDatagramFailure::not_applied(buffer, error));
        }

        // The storage is initialized so malformed lengths or families can be
        // inspected without reading uninitialized bytes after a successful CQE.
        // SAFETY: `sockaddr_storage` is a plain-old-data type for which all
        // zero bytes are a valid (empty) value.
        let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        // The kernel writes only to provider-owned initialized scratch storage.
        // For an empty receive range this may be a dangling Vec pointer, but the
        // zero-length iovec is not dereferenced.
        // SAFETY: preparation initialized the complete scratch layout. The
        // offset is either zero or the preserved-prefix length and therefore
        // points at the start of a `len`-byte receive region.
        let target = unsafe { scratch.as_mut_ptr().add(scratch_offset) };
        let mut iovec = libc::iovec {
            iov_base: target.cast(),
            iov_len: len,
        };
        // SAFETY: zero is valid for all optional msghdr fields. Every live
        // pointer below is paired with the exact capacity available to Linux.
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_name = std::ptr::from_mut(&mut address).cast();
        message.msg_namelen = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        message.msg_iov = std::ptr::from_mut(&mut iovec);
        message.msg_iovlen = 1;
        // A cancellable receive blocks in the kernel until a packet arrives,
        // so it must not carry MSG_DONTWAIT. Its liveness comes from the
        // cancel token instead of from returning empty-handed.
        let flags = match wait {
            DatagramReceiveWait::Nonblocking => libc::MSG_DONTWAIT | libc::MSG_TRUNC,
            DatagramReceiveWait::Cancellable { .. } => libc::MSG_TRUNC,
        };
        let receive_entry = opcode::RecvMsg::new(
            types::Fd(socket.as_raw_fd()),
            std::ptr::from_mut(&mut message),
        )
        .flags(flags as u32)
        .build();
        let result = match wait {
            DatagramReceiveWait::Nonblocking => self.submit_one(&receive_entry),
            DatagramReceiveWait::Cancellable { deadline, arm } => {
                // Sustained regardless of deadline. A deadline bounds the wait
                // but does not make it short, and a receive that holds a
                // transient slot for its whole timeout starves sends just as
                // effectively as one with no deadline at all. Both modes are
                // capped at one per socket, so the reserved budget covers them.
                match self.start_cancellable(&receive_entry, SqeClass::Sustained) {
                    Ok(pending) => {
                        let token = pending.token();
                        // Arming publishes the token to the closing path. It
                        // reports false when the socket is already closing, in
                        // which case nothing else will ever issue the cancel.
                        if !arm(token) {
                            self.request_cancel(token);
                        }
                        pending.wait_until(deadline)
                    }
                    Err(failure) => Err(failure),
                }
            }
        };
        // Do not let any pointer-bearing value become dead until the one
        // terminal receive CQE has been consumed.
        std::hint::black_box((&scratch, &address, &iovec, &message));
        let result = match result {
            Ok(result) => result,
            Err(failure) => {
                let (error, may_have_applied) = failure.into_parts();
                return Err(if may_have_applied {
                    OwnedDatagramFailure::may_have_applied(buffer, error, 0)
                } else {
                    OwnedDatagramFailure::not_applied(buffer, error)
                });
            }
        };
        if result >= 0 {
            let datagram_len = result as usize;
            let transferred = datagram_len.min(len);
            let source = match decode_socket_addr(&address, message.msg_namelen) {
                Ok(source) => source,
                Err(error) => {
                    self.poison();
                    return Err(OwnedDatagramFailure::applied(buffer, error, transferred));
                }
            };
            return Ok(DatagramReceiveAttempt::Received(
                Self::finish_datagram_receive(
                    buffer,
                    scratch,
                    scratch_offset,
                    len,
                    datagram_len,
                    source,
                ),
            ));
        }
        if result == i32::MIN {
            self.poison();
            return Err(OwnedDatagramFailure::may_have_applied(
                buffer,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("io_uring returned invalid datagram-receive result {result}"),
                ),
                0,
            ));
        }
        if result == -libc::ECANCELED {
            // The kernel retired the receive before dequeuing anything. A
            // cancel that lost the race reports the packet above instead, so
            // reaching here always means no datagram was consumed.
            return Ok(DatagramReceiveAttempt::Cancelled { buffer });
        }
        let error = decode_transfer_result(result)
            .expect_err("negative datagram receive CQE must decode as an error");
        if error.kind() == io::ErrorKind::WouldBlock {
            Ok(DatagramReceiveAttempt::WouldBlock { buffer })
        } else {
            Err(OwnedDatagramFailure::not_applied(buffer, error))
        }
    }

    fn finish_datagram_receive(
        mut buffer: Vec<u8>,
        mut scratch: Vec<u8>,
        scratch_offset: usize,
        requested: usize,
        datagram_len: usize,
        source: SocketAddr,
    ) -> OwnedDatagramReceive {
        let transferred = datagram_len.min(requested);
        let caller_has_receive_capacity = buffer.capacity() - buffer.len() >= requested;
        let buffer = if caller_has_receive_capacity {
            debug_assert_eq!(scratch_offset, 0);
            buffer.extend_from_slice(&scratch[..transferred]);
            buffer
        } else {
            debug_assert_eq!(scratch_offset, buffer.len());
            debug_assert!(scratch.len() >= scratch_offset + requested);
            scratch.truncate(scratch_offset + transferred);
            scratch
        };
        OwnedDatagramReceive {
            buffer,
            transferred,
            datagram_len,
            source,
        }
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.host.poisoned.load(Ordering::Acquire)
    }

    /// Test-only: poisons the ring exactly as an inconsistent completion
    /// would, so provider suites can assert their fail-closed blast radius.
    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        self.host.poison();
    }

    /// The batch bound for work that terminalizes on its own.
    ///
    /// Sustained capacity is deliberately excluded: it is reserved for armed
    /// receives and is not available to a caller sizing a batch of transient
    /// operations.
    pub(crate) fn max_active_operations(&self) -> usize {
        self.host.max_transient_sqes
    }

    #[cfg(test)]
    fn max_observed_active_operations(&self) -> usize {
        self.host
            .max_observed_active_transient_sqes
            .load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn deferred_request_count(&self) -> usize {
        self.host.deferred_requests.load(Ordering::Acquire)
    }

    fn start_transfer(
        &self,
        mut buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
        build: impl FnOnce(*mut u8, u32) -> io::Result<squeue::Entry>,
    ) -> Result<PendingTransfer, OwnedTransferFailure> {
        let chunk_len = match transfer_len(&buffer, buffer_offset, len, self.max_io_chunk_bytes) {
            Ok(chunk_len) => chunk_len,
            Err(error) => return Err(OwnedTransferFailure::known(buffer, error)),
        };
        if let Err(error) = self.ensure_healthy() {
            return Err(OwnedTransferFailure::known(buffer, error));
        }
        if chunk_len == 0 {
            return Ok(PendingTransfer {
                buffer: Some(buffer),
                requested: 0,
                completion: None,
                host: Arc::clone(&self.host),
            });
        }
        // SAFETY: transfer_len validated this initialized range. Moving the Vec
        // does not move its allocation, and PendingTransfer retains it to CQE.
        let pointer = unsafe { buffer.as_mut_ptr().add(buffer_offset) };
        let entry = match build(pointer, chunk_len as u32) {
            Ok(entry) => entry,
            Err(error) => return Err(OwnedTransferFailure::known(buffer, error)),
        };
        self.enqueue_pending_transfer(buffer, chunk_len, entry)
    }

    fn start_transfer_const(
        &self,
        buffer: Vec<u8>,
        buffer_offset: usize,
        len: usize,
        build: impl FnOnce(*const u8, u32) -> io::Result<squeue::Entry>,
    ) -> Result<PendingTransfer, OwnedTransferFailure> {
        let chunk_len = match transfer_len(&buffer, buffer_offset, len, self.max_io_chunk_bytes) {
            Ok(chunk_len) => chunk_len,
            Err(error) => return Err(OwnedTransferFailure::known(buffer, error)),
        };
        if let Err(error) = self.ensure_healthy() {
            return Err(OwnedTransferFailure::known(buffer, error));
        }
        if chunk_len == 0 {
            return Ok(PendingTransfer {
                buffer: Some(buffer),
                requested: 0,
                completion: None,
                host: Arc::clone(&self.host),
            });
        }
        // SAFETY: transfer_len validated this initialized range. PendingTransfer
        // retains the Vec allocation without mutation until terminal CQE.
        let pointer = unsafe { buffer.as_ptr().add(buffer_offset) };
        let entry = match build(pointer, chunk_len as u32) {
            Ok(entry) => entry,
            Err(error) => return Err(OwnedTransferFailure::known(buffer, error)),
        };
        self.enqueue_pending_transfer(buffer, chunk_len, entry)
    }

    fn enqueue_pending_transfer(
        &self,
        buffer: Vec<u8>,
        requested: usize,
        entry: squeue::Entry,
    ) -> Result<PendingTransfer, OwnedTransferFailure> {
        let (response, completion) = mpsc::channel();
        let request = ReactorRequest::Single { entry, response };
        if self.host.enqueue(request).is_err() {
            return Err(OwnedTransferFailure::known(buffer, driver_stopped_error()));
        }
        Ok(PendingTransfer {
            buffer: Some(buffer),
            requested,
            completion: Some(completion),
            host: Arc::clone(&self.host),
        })
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        if self.is_poisoned() {
            Err(io::Error::other(
                "io_uring driver is unusable after a completion invariant failed",
            ))
        } else {
            Ok(())
        }
    }

    fn poison(&self) {
        self.host.poison();
    }

    fn submit_one(&mut self, entry: &squeue::Entry) -> Result<i32, SubmissionFailure> {
        self.ensure_healthy().map_err(SubmissionFailure::known)?;
        let (response, completion) = mpsc::channel();
        let request = ReactorRequest::Single {
            entry: entry.clone(),
            response,
        };
        self.host
            .enqueue(request)
            .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?;
        completion
            .recv()
            .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?
    }

    /// Stages one SQE that another holder of this `Ring` can retire early.
    ///
    /// Returns once the reactor has staged the SQE and published its
    /// identifier, which is well before the operation completes. The caller
    /// must then hold every pointer target the entry referenced until
    /// [`PendingCancellable::wait`] returns.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionFailure`] when the ring is already poisoned or the
    /// reactor stopped before staging the entry. In both cases the SQE never
    /// reached the kernel and no caller memory was exposed.
    fn start_cancellable(
        &mut self,
        entry: &squeue::Entry,
        class: SqeClass,
    ) -> Result<PendingCancellable, SubmissionFailure> {
        self.ensure_healthy().map_err(SubmissionFailure::known)?;
        let (response, completion) = mpsc::channel();
        let (token, token_receiver) = mpsc::channel();
        let request = ReactorRequest::Cancellable {
            entry: entry.clone(),
            class,
            token,
            response,
        };
        self.host
            .enqueue(request)
            .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?;
        // A dropped token sender means the reactor rejected the request
        // without staging it, so nothing is in flight to wait for.
        let target = token_receiver
            .recv()
            .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?;
        Ok(PendingCancellable {
            token: CancelToken(target),
            completion: Some(completion),
            ring: self.clone(),
        })
    }

    /// Asks the kernel to retire an in-flight cancellable operation.
    ///
    /// Takes `&self` so a thread other than the one blocked in
    /// [`PendingCancellable::wait`] can cancel through its own clone.
    ///
    /// Returns the raw cancel result: `0` when the target was found and
    /// cancelled, `-ENOENT` when it had already completed, and `-EALREADY`
    /// when it was already completing. `None` means the reactor is gone, in
    /// which case the target is being failed out by the shutdown path anyway.
    /// Queues a cancel and returns immediately.
    ///
    /// Unlike [`Self::cancel`], this never blocks on the reactor. Callers that
    /// only need the target retired must use it: waiting for the cancel's own
    /// CQE inside a submission path turns handing back a future into a
    /// synchronous round trip, which a caller cannot select on or time out.
    ///
    /// Dropping the result receiver is deliberate. The cancel's outcome says
    /// only whether the kernel found the target — `-ENOENT` when it had already
    /// completed — and never that the operation has terminalized. Terminalizing
    /// is proven by the target's own CQE, which is what every waiter is
    /// already parked on.
    pub(crate) fn request_cancel(&self, token: CancelToken) {
        let (response, discarded) = mpsc::channel();
        drop(discarded);
        let _ = self.host.enqueue_cancel(CancelRequest {
            target: token.0,
            response,
        });
    }

    #[cfg(test)]
    pub(crate) fn cancel(&self, token: CancelToken) -> Option<i32> {
        let (response, completion) = mpsc::channel();
        self.host
            .enqueue_cancel(CancelRequest {
                target: token.0,
                response,
            })
            .ok()?;
        completion.recv().ok()
    }

    fn submit_linked_entries(
        &mut self,
        entries: &[squeue::Entry; 2],
    ) -> Result<[i32; 2], SubmissionFailure> {
        self.ensure_healthy().map_err(SubmissionFailure::known)?;
        let (response, completion) = mpsc::channel();
        let request = ReactorRequest::Linked {
            entries: entries.clone(),
            response,
        };
        self.host
            .enqueue(request)
            .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?;
        completion
            .recv()
            .map_err(|_| SubmissionFailure::known(driver_stopped_error()))?
    }

    fn submit_linked_accept(
        &mut self,
        entries: &[squeue::Entry; 2],
    ) -> io::Result<LinkedAcceptCompletions> {
        self.ensure_healthy()?;
        let (response, completion) = mpsc::channel();
        let request = ReactorRequest::LinkedAccept {
            entries: entries.clone(),
            response,
        };
        self.host
            .enqueue(request)
            .map_err(|_| driver_stopped_error())?;
        completion
            .recv()
            .map_err(|_| driver_stopped_error())?
            .map_err(|failure| failure.error)
    }
}

impl SubmissionFailure {
    fn known(error: io::Error) -> Self {
        Self {
            error,
            may_have_applied: false,
        }
    }

    fn uncertain(error: io::Error) -> Self {
        Self {
            error,
            may_have_applied: true,
        }
    }

    pub(crate) fn into_parts(self) -> (io::Error, bool) {
        (self.error, self.may_have_applied)
    }
}

impl OwnedDatagramFailure {
    fn not_applied(buffer: Vec<u8>, error: io::Error) -> Self {
        Self {
            buffer,
            error,
            effect: DatagramEffect::NotApplied,
            bytes_transferred: 0,
        }
    }

    fn applied(buffer: Vec<u8>, error: io::Error, bytes_transferred: usize) -> Self {
        Self {
            buffer,
            error,
            effect: DatagramEffect::Applied,
            bytes_transferred,
        }
    }

    fn may_have_applied(buffer: Vec<u8>, error: io::Error, bytes_transferred: usize) -> Self {
        Self {
            buffer,
            error,
            effect: DatagramEffect::MayHaveApplied,
            bytes_transferred,
        }
    }
}

impl OwnedTransferFailure {
    fn known(buffer: Vec<u8>, error: io::Error) -> Self {
        Self {
            buffer,
            error,
            may_have_applied: false,
        }
    }

    fn uncertain(buffer: Vec<u8>, error: io::Error) -> Self {
        Self {
            buffer,
            error,
            may_have_applied: true,
        }
    }
}

fn probe_required_operations(ring: &IoUring, required: &[(&str, u8)]) -> io::Result<()> {
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe)?;
    for &(name, code) in required {
        if !probe.is_supported(code) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("running kernel does not support io_uring {name} opcode {code}"),
            ));
        }
    }
    Ok(())
}

fn transfer_len(
    buffer: &[u8],
    buffer_offset: usize,
    requested: usize,
    maximum: usize,
) -> io::Result<usize> {
    if buffer_offset > buffer.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "buffer offset {buffer_offset} is beyond buffer length {}",
                buffer.len()
            ),
        ));
    }
    let available = buffer.len() - buffer_offset;
    if requested > available {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("requested {requested} bytes but only {available} remain in the buffer"),
        ));
    }
    Ok(requested.min(maximum))
}

fn prepare_datagram_receive_scratch(
    buffer: &[u8],
    buffer_capacity: usize,
    buffer_offset: usize,
    requested: usize,
    scratch: &mut Vec<u8>,
) -> io::Result<usize> {
    if buffer_offset != buffer.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "datagram receive prefix length {buffer_offset} does not equal buffer length {}",
                buffer.len()
            ),
        ));
    }
    let result_len = buffer_offset.checked_add(requested).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "datagram receive buffer length overflowed usize",
        )
    })?;
    let spare_capacity = buffer_capacity.checked_sub(buffer.len()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "datagram receive buffer capacity is smaller than its length",
        )
    })?;
    let scratch_offset = if spare_capacity >= requested {
        0
    } else {
        buffer_offset
    };
    let scratch_len = if scratch_offset == 0 {
        requested
    } else {
        result_len
    };
    if scratch.len() < scratch_len {
        scratch
            .try_reserve_exact(scratch_len - scratch.len())
            .map_err(|error| io::Error::other(error.to_string()))?;
        scratch.resize(scratch_len, 0);
    } else {
        scratch.truncate(scratch_len);
    }
    if scratch_offset != 0 {
        scratch[..scratch_offset].copy_from_slice(buffer);
    }
    Ok(scratch_offset)
}

fn checked_kernel_offset(offset: u64) -> io::Result<u64> {
    if offset > i64::MAX as u64 {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("file offset {offset} exceeds the kernel signed-offset limit"),
        ))
    } else {
        Ok(offset)
    }
}

fn decode_transfer_result(result: i32) -> io::Result<usize> {
    if result >= 0 {
        Ok(result as usize)
    } else {
        let errno = result.checked_neg().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("io_uring returned invalid completion result {result}"),
            )
        })?;
        Err(io::Error::from_raw_os_error(errno))
    }
}

fn decode_unit_result(result: i32, operation: &str) -> io::Result<()> {
    let completed = decode_transfer_result(result)?;
    if completed == 0 {
        Ok(())
    } else {
        Err(invalid_completion(operation, completed, 0))
    }
}

fn own_accept_completion(result: i32) -> AcceptCompletion {
    if result >= 0 {
        // SAFETY: a successful single-shot IORING_OP_ACCEPT CQE transfers one
        // newly accepted descriptor to the application. Taking ownership at
        // CQE-drain time ensures every subsequent path closes it by RAII.
        AcceptCompletion::Accepted(unsafe { TcpStream::from_raw_fd(result) })
    } else {
        AcceptCompletion::Failed(result)
    }
}

fn classify_linked_connect(
    connect_result: i32,
    timeout_result: i32,
) -> io::Result<ConnectDisposition> {
    let timeout_cancelled = timeout_result == -libc::ECANCELED || timeout_result == -libc::ENOENT;
    let timeout_expired = timeout_result == -libc::ETIME;

    if connect_result > 0 {
        return Err(invalid_linked_connect_results(
            connect_result,
            timeout_result,
        ));
    }
    if connect_result == 0 {
        return if timeout_cancelled || timeout_expired {
            Ok(ConnectDisposition::Connected)
        } else {
            Err(invalid_linked_connect_results(
                connect_result,
                timeout_result,
            ))
        };
    }

    if timeout_expired && (connect_result == -libc::ECANCELED || connect_result == -libc::EINTR) {
        return Ok(ConnectDisposition::TimedOut);
    }
    if timeout_cancelled || timeout_expired {
        return Ok(ConnectDisposition::Failed(connect_result));
    }

    Err(invalid_linked_connect_results(
        connect_result,
        timeout_result,
    ))
}

fn invalid_linked_connect_results(connect_result: i32, timeout_result: i32) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "io_uring linked connect returned inconsistent CQEs: connect={connect_result}, timeout={timeout_result}"
        ),
    )
}

fn connect_failure_is_known_not_applied(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ECONNREFUSED | libc::EADDRINUSE)
    )
}

fn classify_linked_accept(
    accept_result: i32,
    timeout_result: i32,
) -> io::Result<AcceptDisposition> {
    let timeout_cancelled = timeout_result == -libc::ECANCELED || timeout_result == -libc::ENOENT;
    let timeout_expired = timeout_result == -libc::ETIME;

    if accept_result >= 0 {
        return if timeout_cancelled || timeout_expired {
            Ok(AcceptDisposition::Accepted)
        } else {
            Err(invalid_linked_accept_results(accept_result, timeout_result))
        };
    }

    if timeout_expired {
        return if accept_result == -libc::ECANCELED || accept_result == -libc::EINTR {
            Ok(AcceptDisposition::NoConnection)
        } else {
            Err(invalid_linked_accept_results(accept_result, timeout_result))
        };
    }

    if timeout_cancelled {
        return if accept_result == -libc::EAGAIN || accept_result == -libc::EWOULDBLOCK {
            Ok(AcceptDisposition::NoConnection)
        } else {
            Ok(AcceptDisposition::Failed(accept_result))
        };
    }

    Err(invalid_linked_accept_results(accept_result, timeout_result))
}

fn invalid_linked_accept_results(accept_result: i32, timeout_result: i32) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "io_uring linked accept returned inconsistent CQEs: accept={accept_result}, timeout={timeout_result}"
        ),
    )
}

pub(crate) enum EncodedSocketAddr {
    V4(libc::sockaddr_in),
    V6(libc::sockaddr_in6),
}

impl EncodedSocketAddr {
    pub(crate) fn new(address: SocketAddr) -> Self {
        match address {
            SocketAddr::V4(address) => {
                // SAFETY: zero is a valid value for the padding fields in a
                // sockaddr_in, which are not otherwise relevant to the socket
                // operation using this encoded address.
                let mut encoded: libc::sockaddr_in = unsafe { std::mem::zeroed() };
                encoded.sin_family = libc::AF_INET as libc::sa_family_t;
                encoded.sin_port = address.port().to_be();
                encoded.sin_addr = libc::in_addr {
                    s_addr: u32::from_ne_bytes(address.ip().octets()),
                };
                Self::V4(encoded)
            }
            SocketAddr::V6(address) => {
                // SAFETY: zero initializes the platform padding and any
                // implementation-specific fields not used by sockaddr_in6.
                let mut encoded: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
                encoded.sin6_family = libc::AF_INET6 as libc::sa_family_t;
                encoded.sin6_port = address.port().to_be();
                encoded.sin6_flowinfo = address.flowinfo();
                encoded.sin6_addr = libc::in6_addr {
                    s6_addr: address.ip().octets(),
                };
                encoded.sin6_scope_id = address.scope_id();
                Self::V6(encoded)
            }
        }
    }

    pub(crate) fn as_ptr_len(&self) -> (*const libc::sockaddr, libc::socklen_t) {
        match self {
            Self::V4(address) => (
                std::ptr::from_ref(address).cast::<libc::sockaddr>(),
                size_of::<libc::sockaddr_in>() as libc::socklen_t,
            ),
            Self::V6(address) => (
                std::ptr::from_ref(address).cast::<libc::sockaddr>(),
                size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            ),
        }
    }
}

fn decode_socket_addr(
    storage: &libc::sockaddr_storage,
    address_len: libc::socklen_t,
) -> io::Result<SocketAddr> {
    let address_len = usize::try_from(address_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "socket address length does not fit usize",
        )
    })?;
    if address_len > size_of::<libc::sockaddr_storage>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "socket address length {address_len} exceeds storage capacity {}",
                size_of::<libc::sockaddr_storage>()
            ),
        ));
    }
    if address_len < size_of::<libc::sa_family_t>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("socket address is too short: {address_len} bytes"),
        ));
    }

    match i32::from(storage.ss_family) {
        libc::AF_INET => {
            require_socket_addr_len(address_len, size_of::<libc::sockaddr_in>(), "IPv4")?;
            // SAFETY: sockaddr_storage has sufficient size and alignment for
            // sockaddr_in, and the validated kernel length covers the value.
            let address = unsafe {
                std::ptr::from_ref(storage)
                    .cast::<libc::sockaddr_in>()
                    .read()
            };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(address.sin_port),
            )))
        }
        libc::AF_INET6 => {
            require_socket_addr_len(address_len, size_of::<libc::sockaddr_in6>(), "IPv6")?;
            // SAFETY: sockaddr_storage has sufficient size and alignment for
            // sockaddr_in6, and the validated kernel length covers the value.
            let address = unsafe {
                std::ptr::from_ref(storage)
                    .cast::<libc::sockaddr_in6>()
                    .read()
            };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(address.sin6_addr.s6_addr),
                u16::from_be(address.sin6_port),
                address.sin6_flowinfo,
                address.sin6_scope_id,
            )))
        }
        family => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("socket address has unsupported family {family}"),
        )),
    }
}

fn require_socket_addr_len(actual: usize, required: usize, family: &str) -> io::Result<()> {
    if actual < required {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{family} socket address is {actual} bytes; expected at least {required}"),
        ))
    } else {
        Ok(())
    }
}

fn invalid_completion(operation: &str, completed: usize, requested: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "io_uring {operation} completion reported {completed} bytes for a {requested}-byte request"
        ),
    )
}

/// How the reactor recovers from a failed `io_uring_enter`.
///
/// The three recoverable failures need three different remedies, and applying
/// the wrong one is not merely slow: parking on `EBUSY` waits for a condition
/// only this thread can clear.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnterRecovery {
    /// A signal interrupted the call before it did any work. Retrying is free
    /// and immediate; pausing here would convert every profiler sample into a
    /// millisecond stall.
    Retry,
    /// The completion queue cannot absorb another CQE. Kernels reporting
    /// `IORING_FEAT_NODROP` — required at ring construction — refuse to
    /// submit rather than overflow, so the only remedy is to reap. The
    /// reactor is the sole consumer of this ring, so a pause cannot clear the
    /// condition — it can only defer the reap.
    DrainCompletions,
    /// Transient resource pressure with no userspace remedy. Back off.
    Backoff,
    /// The call cannot succeed as issued, but the ring stays coherent: a
    /// malformed submission, a bad descriptor, an unsupported opcode — this
    /// crate's own bug rather than a kernel fault. Every in-flight operation is
    /// still owed a CQE, so the ring can be drained and every caller can be
    /// told, which makes poisoning the honest response.
    Poison,
    /// A completion was dropped. The operation it belonged to can never be
    /// accounted for, so the ring can never be quiesced and no amount of
    /// retrying or draining reaches a state where returning is safe.
    LostCompletion,
}

/// Ends the process when the kernel reports a dropped completion.
///
/// `EBADR` says a CQE was lost. The operation it belonged to can never be
/// accounted for: its caller is parked forever on a response that will not
/// arrive, holding a buffer the kernel may already have written. Retrying
/// cannot reconstruct a CQE, and returning from this thread would release
/// memory the kernel may still own — so neither recovery nor honest unwinding
/// exists, exactly the condition [`FailStopOnPanic`] aborts for.
/// `io_uring_enter(2)` recommends termination unless the application can handle
/// CQE loss, and owned-completion I/O cannot: every operation having a terminal
/// CQE is the premise the contract is built on.
///
/// Retrying instead, as this used to, turned a reported kernel fault into an
/// unobservable permanent hang.
fn abort_on_lost_completion(error: &io::Error) -> ! {
    eprintln!(
        "kr-runtime-io-uring: the kernel dropped an io_uring completion ({error}). The operation it \
         belonged to cannot be accounted for and its buffer cannot be safely returned. Aborting."
    );
    std::process::abort()
}

/// Ends the process when a poisoned ring cannot be drained.
///
/// A poisoned reactor retires its own in-flight work and exits once the kernel
/// owes it nothing. That requires submitting cancels, so an `io_uring_enter`
/// that keeps failing leaves the ring permanently unquiescable: the operations
/// stay kernel-visible, and returning would free memory the kernel still owns.
/// Bounded rather than infinite, because retrying forever is the hang this
/// whole path exists to avoid.
fn abort_on_undrainable_ring(error: &io::Error, active_user_sqes: usize) -> ! {
    eprintln!(
        "kr-runtime-io-uring: a poisoned ring could not be drained after \
         {MAX_STALLED_ENTER_FAILURES} attempts ({error}); {active_user_sqes} operations remain \
         kernel-visible, so their memory cannot be released. Aborting."
    );
    std::process::abort()
}

fn classify_enter(error: &io::Error) -> EnterRecovery {
    if error.kind() == io::ErrorKind::Interrupted {
        return EnterRecovery::Retry;
    }
    match error.raw_os_error() {
        Some(code) if code == libc::EBUSY => EnterRecovery::DrainCompletions,
        Some(code) if code == libc::EAGAIN => EnterRecovery::Backoff,
        // The one errno that reports lost completions rather than a rejected
        // call. Everything else leaves the ring accountable, so it poisons.
        Some(code) if code == libc::EBADR => EnterRecovery::LostCompletion,
        _ => EnterRecovery::Poison,
    }
}

fn create_event_fd() -> io::Result<OwnedFd> {
    // SAFETY: eventfd returns a fresh descriptor or -1 without borrowing any
    // Rust storage. CLOEXEC prevents inheritance and NONBLOCK makes draining
    // race-safe after a one-shot PollAdd CQE.
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if raw < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: the successful eventfd call returned a fresh descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(raw) })
    }
}

fn signal_event_fd(fd: RawFd) {
    let value = 1_u64;
    loop {
        // SAFETY: `value` is an initialized native-endian u64 and ReactorHost
        // keeps `fd` open for this call.
        let written = unsafe {
            libc::write(
                fd,
                std::ptr::from_ref(&value).cast::<libc::c_void>(),
                size_of::<u64>(),
            )
        };
        if written == size_of::<u64>() as isize {
            return;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == io::ErrorKind::WouldBlock {
            // The counter is already saturated and therefore readable; an
            // armed or subsequently armed PollAdd cannot miss the wake.
            return;
        }
        // A request may already contain raw pointers and be resident in the
        // ingress channel. Unwinding its caller here could release those
        // targets before the reactor sees the request, so this invariant is
        // process-fatal rather than recoverable by panic.
        std::process::abort();
    }
}

fn drain_event_fd(fd: RawFd) {
    loop {
        let mut value = 0_u64;
        // SAFETY: `value` is writable u64 storage and the reactor owns a live
        // eventfd descriptor for the duration of this call.
        let read = unsafe {
            libc::read(
                fd,
                std::ptr::from_mut(&mut value).cast::<libc::c_void>(),
                size_of::<u64>(),
            )
        };
        if read == size_of::<u64>() as isize {
            continue;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == io::ErrorKind::WouldBlock {
            return;
        }
        panic!("could not drain io_uring reactor eventfd: {error}");
    }
}

fn driver_stopped_error() -> io::Error {
    io::Error::other("io_uring reactor stopped before operation completion")
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn encoded_storage(address: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
        let encoded = EncodedSocketAddr::new(address);
        let (source, len) = encoded.as_ptr_len();
        // SAFETY: sockaddr_storage is large enough and sufficiently aligned for
        // both supported sockaddr variants. The source points at `encoded` for
        // the duration of this copy, and the regions do not overlap.
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        unsafe {
            std::ptr::copy_nonoverlapping(
                source.cast::<u8>(),
                std::ptr::from_mut(&mut storage).cast::<u8>(),
                len as usize,
            );
        }
        (storage, len)
    }

    /// Builds a poll on a fresh eventfd nothing ever signals, so the operation
    /// stays in flight until something cancels it.
    fn never_ready_poll() -> (OwnedFd, squeue::Entry) {
        let event_fd = create_event_fd().expect("eventfd");
        let entry =
            opcode::PollAdd::new(types::Fd(event_fd.as_raw_fd()), libc::POLLIN as u32).build();
        (event_fd, entry)
    }

    /// A poll that is already satisfied, so it terminalizes without anything
    /// else having to complete first.
    fn always_ready_poll() -> (OwnedFd, squeue::Entry) {
        let (event_fd, entry) = never_ready_poll();
        signal_event_fd(event_fd.as_raw_fd());
        (event_fd, entry)
    }

    #[test]
    fn dropped_routed_requests_return_original_ownership_exactly_once() {
        let owner = kr_runtime_io::SharedBytes::from(vec![1, 2, 3]);
        let segments = vec![WriteSegment {
            bytes: owner.clone(),
            range: 0..3,
        }];
        let pointer = segments.as_ptr();
        let (sender, receiver) = mpsc::channel();
        let terminal = RoutedTerminal::new(42, sender.clone(), Some(segments));
        drop(ReactorRequest::RoutedVectored {
            socket: -1,
            requested: 3,
            terminal,
        });
        let completion = receiver
            .try_recv()
            .expect("queued drop publishes completion");
        assert_eq!(completion.token, 42);
        assert!(
            !completion
                .result
                .as_ref()
                .expect_err("unsubmitted drop fails")
                .may_have_applied
        );
        assert_eq!(
            completion
                .segments
                .as_ref()
                .expect("original ownership")
                .as_ptr(),
            pointer
        );
        drop(completion);
        assert_eq!(owner.strong_count(), 1);
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

        // A successful terminal result disarms fallback publication.
        RoutedTerminal::new(43, sender.clone(), None).complete(Ok(3));
        assert_eq!(receiver.try_recv().unwrap().result.unwrap(), 3);
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

        // Synchronous enqueue rejection is reported by the caller; it must not
        // also create a routed completion for a token never admitted there.
        let mut terminal = RoutedTerminal::new(44, sender, None);
        terminal.disarm();
        drop(terminal);
        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn vectored_metadata_points_at_the_original_allocations_and_caps_the_exact_prefix() {
        let owner = kr_runtime_io::SharedBytes::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        for prefix in 1..=6 {
            let segments = vec![
                WriteSegment {
                    bytes: owner.clone(),
                    range: 1..3,
                },
                WriteSegment {
                    bytes: owner.clone(),
                    range: 3..7,
                },
            ];
            let pointer = segments.as_ptr();
            let storage =
                VectoredSendStorage::new(&segments, prefix).expect("valid native metadata");
            assert_eq!(segments.as_ptr(), pointer);
            assert_eq!(
                storage._iovecs.iter().map(|iov| iov.iov_len).sum::<usize>(),
                prefix
            );
            assert_eq!(
                storage._iovecs[0].iov_base.cast_const(),
                owner.as_slice()[1..].as_ptr().cast()
            );
            if prefix > 2 {
                assert_eq!(
                    storage._iovecs[1].iov_base.cast_const(),
                    owner.as_slice()[3..].as_ptr().cast()
                );
                assert_eq!(storage._iovecs[1].iov_len, prefix - 2);
            }
            assert_eq!(
                storage.message[0].msg_iov.cast_const(),
                storage._iovecs.as_ptr()
            );
            assert_eq!(storage.message[0].msg_iovlen, storage._iovecs.len());
            assert_eq!(storage.message[0].msg_control, std::ptr::null_mut());
        }
        assert_eq!(owner.strong_count(), 1);
    }

    #[test]
    fn a_cancelled_operation_reports_ecanceled_to_its_blocked_caller() {
        let mut ring = Ring::for_datagram(RingCapacity::transient_only(8)).expect("ring");
        let (_event_fd, entry) = never_ready_poll();
        let pending = ring
            .start_cancellable(&entry, SqeClass::Transient)
            .expect("staged");
        let token = pending.token();

        let canceller = ring.clone();
        let cancelled = thread::spawn(move || canceller.cancel(token));

        assert_eq!(
            pending.wait().expect("completion"),
            -libc::ECANCELED,
            "a cancelled poll must report ECANCELED rather than succeeding"
        );
        assert_eq!(cancelled.join().expect("cancel thread"), Some(0));
    }

    #[test]
    fn cancelling_a_completed_operation_misses_instead_of_retiring_a_newer_one() {
        let mut ring = Ring::for_datagram(RingCapacity::transient_only(8)).expect("ring");
        let (event_fd, entry) = never_ready_poll();
        let pending = ring
            .start_cancellable(&entry, SqeClass::Transient)
            .expect("staged");
        let stale = pending.token();
        signal_event_fd(event_fd.as_raw_fd());
        assert!(
            pending.wait().expect("completion") >= 0,
            "poll became ready"
        );

        // Identifiers are never reused, so a second operation cannot inherit
        // the retired one's identity and the stale cancel must simply miss.
        let (_second_fd, second_entry) = never_ready_poll();
        let second = ring
            .start_cancellable(&second_entry, SqeClass::Transient)
            .expect("staged");
        assert_ne!(second.token(), stale, "identifiers must not be reused");
        assert_eq!(ring.cancel(stale), Some(-libc::ENOENT));

        let live = second.token();
        let canceller = ring.clone();
        let cancelled = thread::spawn(move || canceller.cancel(live));
        assert_eq!(second.wait().expect("completion"), -libc::ECANCELED);
        assert_eq!(cancelled.join().expect("cancel thread"), Some(0));
    }

    #[test]
    fn a_cancel_is_staged_even_when_every_budgeted_slot_is_occupied() {
        // Saturate the in-flight budget with operations that never complete on
        // their own. If cancels were charged against that budget, or queued
        // behind the deferred request slot, none of these could be retired and
        // the ring would deadlock.
        let entries = 8;
        let mut ring = Ring::for_datagram(RingCapacity::transient_only(entries)).expect("ring");
        let mut held = Vec::new();
        for _ in 0..entries {
            let (event_fd, entry) = never_ready_poll();
            let pending = ring
                .start_cancellable(&entry, SqeClass::Transient)
                .expect("staged");
            held.push((event_fd, pending));
        }

        for (_event_fd, pending) in held {
            let token = pending.token();
            let canceller = ring.clone();
            let cancelled = thread::spawn(move || canceller.cancel(token));
            assert_eq!(pending.wait().expect("completion"), -libc::ECANCELED);
            assert_eq!(cancelled.join().expect("cancel thread"), Some(0));
        }
    }

    #[test]
    fn armed_receives_beyond_ring_entries_cannot_delay_transient_work() {
        // The regression for the shared-ring wedge: sustained operations are
        // reserved capacity, so more of them than the ring has entries may be
        // armed at once without a later transient operation waiting on any of
        // them. Charged against one shared budget, the transient submit below
        // could not be staged until an armed receive happened to complete.
        let entries = 4;
        let armed = 32;
        let mut ring = Ring::for_datagram(RingCapacity {
            entries,
            transient: entries as usize,
            sustained: armed,
        })
        .expect("ring");

        let mut held = Vec::new();
        for _ in 0..armed {
            let (event_fd, entry) = never_ready_poll();
            let pending = ring
                .start_cancellable(&entry, SqeClass::Sustained)
                .expect("staged");
            held.push((event_fd, pending));
        }

        // Every sustained slot is occupied and none can complete on its own.
        // A transient operation must still reach the kernel and terminalize.
        let (ready_fd, ready_entry) = always_ready_poll();
        assert_eq!(ring.submit_one(&ready_entry).expect("completion"), 1);
        drop(ready_fd);

        for (_event_fd, pending) in held {
            let token = pending.token();
            let canceller = ring.clone();
            let cancelled = thread::spawn(move || canceller.cancel(token));
            assert_eq!(pending.wait().expect("completion"), -libc::ECANCELED);
            assert_eq!(cancelled.join().expect("cancel thread"), Some(0));
        }
    }

    #[test]
    fn dropping_a_pending_operation_retires_it_instead_of_abandoning_the_kernel_reference() {
        // `#[must_use]` discourages this but cannot prevent it, and an unwind
        // ignores it entirely. The submitted entry references memory the kernel
        // may still write to, so the drop must retire the operation and wait
        // for proof the kernel is done rather than just releasing the receiver.
        let mut ring = Ring::for_datagram(RingCapacity::transient_only(4)).expect("ring");
        let (_event_fd, entry) = never_ready_poll();
        let pending = ring
            .start_cancellable(&entry, SqeClass::Transient)
            .expect("staged");

        // Abandon it the way an early return or an unwind would.
        drop(pending);

        // Nothing else will ever signal that eventfd, so the reactor can only
        // reach zero in-flight SQEs if the drop retired the operation. Joining
        // its thread here is therefore the assertion: an abandoned reference
        // would hang this instead.
        drop(ring);
    }

    #[test]
    fn poisoning_retires_an_armed_operation_that_would_never_complete() {
        // Poisoning stops the reactor from accepting cancels, and its exit gate
        // waits for every in-flight SQE. An armed receive satisfies neither on
        // its own, so before the reactor cancelled its own work this hung the
        // reactor thread, its join, and every close waiter behind it.
        let mut ring = Ring::for_datagram(RingCapacity {
            entries: 4,
            transient: 4,
            sustained: 4,
        })
        .expect("ring");

        let (_event_fd, entry) = never_ready_poll();
        let pending = ring
            .start_cancellable(&entry, SqeClass::Sustained)
            .expect("staged");

        let waiter = thread::spawn(move || pending.wait());
        ring.host.poison();

        // The armed operation terminalizes because the reactor retired it, not
        // because the eventfd was ever signalled.
        let result = waiter.join().expect("waiter thread");
        assert_eq!(result.expect("completion"), -libc::ECANCELED);

        // The reactor can now reach zero in-flight SQEs and exit, so dropping
        // the last handle joins its thread instead of blocking forever.
        drop(ring);
    }

    #[test]
    fn a_transient_operation_refused_for_budget_is_deferred_then_staged() {
        // The deferral path is the one the shipped-defaults wedge lived in,
        // and in most shapes reaching it is timing-dependent: a test can pass
        // forever without ever taking it. Exhausting the transient budget
        // makes deferral mandatory, and the counter assertion makes silently
        // skipping the path a failure rather than a pass.
        let mut ring = Ring::for_datagram(RingCapacity {
            entries: 8,
            transient: 2,
            sustained: 0,
        })
        .expect("ring");

        let mut held = Vec::new();
        for _ in 0..2 {
            let (event_fd, entry) = never_ready_poll();
            let pending = ring
                .start_cancellable(&entry, SqeClass::Transient)
                .expect("staged");
            held.push((event_fd, pending));
        }

        // Every budgeted slot is occupied by an operation that never
        // completes on its own, so this submission cannot be staged eagerly.
        let (_ready_fd, ready_entry) = always_ready_poll();
        let mut submitter = ring.clone();
        let waiter = thread::spawn(move || submitter.submit_one(&ready_entry));

        let deadline = Instant::now() + Duration::from_secs(10);
        while ring.deferred_request_count() == 0 {
            assert!(
                Instant::now() < deadline,
                "the over-budget submission was never deferred"
            );
            thread::yield_now();
        }

        // Retiring one held operation frees the budget; the deferred
        // submission must then be staged and complete without any further
        // help.
        let (_event_fd, pending) = held.pop().expect("held operation");
        let token = pending.token();
        let canceller = ring.clone();
        let cancelled = thread::spawn(move || canceller.cancel(token));
        assert_eq!(pending.wait().expect("completion"), -libc::ECANCELED);
        assert_eq!(cancelled.join().expect("cancel thread"), Some(0));

        let completion = waiter.join().expect("submit thread");
        assert_eq!(completion.expect("completion"), 1);
    }

    #[test]
    fn a_sustained_budget_shortfall_poisons_the_ring_instead_of_wedging() {
        // Sustained capacity is provisioned so an armed receive is never
        // refused; a shortfall is an accounting bug. Deferring the refused
        // operation would park it ahead of every transient request until a
        // peer happened to act — the wedge the class split exists to prevent —
        // so the reactor must fail closed: refuse the operation, poison, and
        // retire its in-flight work rather than degrade silently.
        let mut ring = Ring::for_datagram(RingCapacity {
            entries: 4,
            transient: 4,
            sustained: 1,
        })
        .expect("ring");

        let (_event_fd, entry) = never_ready_poll();
        let pending = ring
            .start_cancellable(&entry, SqeClass::Sustained)
            .expect("staged");
        let waiter = thread::spawn(move || pending.wait());

        // A second armed operation overruns the provisioned budget. It must
        // be refused without ever being staged, not parked in the deferred
        // slot.
        let (_second_fd, second_entry) = never_ready_poll();
        let refused = ring.start_cancellable(&second_entry, SqeClass::Sustained);
        assert!(
            refused.is_err(),
            "an operation past the sustained budget must be refused, not deferred"
        );

        // The poisoned reactor retires the armed operation on its own, so the
        // waiter observes a terminal CQE instead of hanging.
        let result = waiter.join().expect("waiter thread");
        assert_eq!(result.expect("completion"), -libc::ECANCELED);

        // The reactor reaches zero in-flight SQEs with nothing deferred, so
        // dropping the last handle joins its thread instead of blocking.
        drop(ring);
    }

    #[test]
    fn a_poisoned_reactor_quiesces_across_arming_and_poison_interleavings() {
        // A single-shot poison test only covers one interleaving. The failure
        // mode that matters is a cancel reaching the kernel before its own
        // target, which misses with -ENOENT and strands the operation, so sweep
        // the two dimensions that decide whether that can happen: how many
        // operations are armed, and how long the reactor has had to submit them
        // before poison lands.
        for armed in 1..=6usize {
            for pause in [
                Duration::ZERO,
                Duration::from_micros(1),
                Duration::from_micros(50),
                Duration::from_micros(500),
                Duration::from_millis(2),
            ] {
                let mut ring = Ring::for_datagram(RingCapacity {
                    entries: 4,
                    transient: 4,
                    sustained: armed,
                })
                .expect("ring");

                let mut event_fds = Vec::new();
                let mut waiters = Vec::new();
                for _ in 0..armed {
                    let (event_fd, entry) = never_ready_poll();
                    let pending = ring
                        .start_cancellable(&entry, SqeClass::Sustained)
                        .expect("staged");
                    event_fds.push(event_fd);
                    waiters.push(thread::spawn(move || pending.wait()));
                }

                if !pause.is_zero() {
                    thread::sleep(pause);
                }
                ring.host.poison();

                for (index, waiter) in waiters.into_iter().enumerate() {
                    let result = waiter.join().expect("waiter thread");
                    assert_eq!(
                        result.expect("completion"),
                        -libc::ECANCELED,
                        "operation {index} of {armed} was not retired (poison pause {pause:?})"
                    );
                }

                // Nothing ever signals these eventfds, so the reactor reaches
                // zero in-flight only if every operation was retired. Joining
                // its thread is the assertion; a stranded entry hangs here.
                drop(ring);
                drop(event_fds);
            }
        }
    }

    #[test]
    fn a_full_completion_queue_is_recovered_by_reaping_not_by_waiting() {
        // EBUSY reports that the completion queue cannot absorb another CQE.
        // The reactor is the only consumer of its ring, so classifying this as
        // a backoff would wait for a condition nothing else can clear.
        assert_eq!(
            classify_enter(&io::Error::from_raw_os_error(libc::EBUSY)),
            EnterRecovery::DrainCompletions
        );
    }

    #[test]
    fn an_interrupted_enter_retries_without_pausing() {
        assert_eq!(
            classify_enter(&io::Error::from_raw_os_error(libc::EINTR)),
            EnterRecovery::Retry
        );
    }

    #[test]
    fn resource_pressure_backs_off_and_unrecoverable_errors_poison() {
        assert_eq!(
            classify_enter(&io::Error::from_raw_os_error(libc::EAGAIN)),
            EnterRecovery::Backoff
        );
        assert_eq!(
            classify_enter(&io::Error::from_raw_os_error(libc::EIO)),
            EnterRecovery::Poison
        );
        assert_eq!(
            classify_enter(&io::Error::from_raw_os_error(libc::EINVAL)),
            EnterRecovery::Poison
        );
    }

    #[test]
    fn only_a_dropped_completion_is_unsurvivable() {
        // The distinction this encodes: a rejected call leaves every operation
        // still owed a CQE, so the ring can be drained and every caller told.
        // A dropped completion does not, and nothing can recover the operation
        // it belonged to. Only the latter may end the process — classifying a
        // malformed submission alongside it would abort a production runtime
        // over this crate's own bug.
        assert_eq!(
            classify_enter(&io::Error::from_raw_os_error(libc::EBADR)),
            EnterRecovery::LostCompletion
        );
        for survivable in [libc::EINVAL, libc::EFAULT, libc::EBADF, libc::EOPNOTSUPP] {
            assert_eq!(
                classify_enter(&io::Error::from_raw_os_error(survivable)),
                EnterRecovery::Poison,
                "errno {survivable} must not be able to abort the process"
            );
        }
    }

    #[test]
    fn transfer_bounds_are_checked_and_capped() {
        assert_eq!(transfer_len(&[0; 8], 2, 6, 3).unwrap(), 3);
        assert_eq!(
            transfer_len(&[0; 8], 9, 0, 3).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            transfer_len(&[0; 8], 7, 2, 3).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn completion_result_maps_success_and_errno() {
        assert_eq!(decode_transfer_result(17).unwrap(), 17);
        assert_eq!(
            decode_transfer_result(-libc::EIO)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
    }

    #[test]
    fn kernel_offset_rejects_signed_overflow() {
        assert_eq!(
            checked_kernel_offset(i64::MAX as u64 + 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn socket_address_codec_round_trips_ipv4_and_ipv6() {
        let addresses = [
            SocketAddr::from(([127, 42, 9, 3], 54_321)),
            SocketAddr::V6(SocketAddrV6::new(
                "2001:db8::1234".parse().unwrap(),
                12_345,
                0x0102_0304,
                17,
            )),
        ];
        for address in addresses {
            let (storage, len) = encoded_storage(address);
            assert_eq!(decode_socket_addr(&storage, len).unwrap(), address);
        }
    }

    #[test]
    fn accepted_address_decoder_rejects_truncation_and_unknown_family() {
        let address = SocketAddr::from(([127, 0, 0, 1], 80));
        let (mut storage, _) = encoded_storage(address);
        assert_eq!(
            decode_socket_addr(
                &storage,
                (size_of::<libc::sockaddr_in>() - 1) as libc::socklen_t,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidData
        );

        storage.ss_family = libc::AF_UNIX as libc::sa_family_t;
        assert_eq!(
            decode_socket_addr(
                &storage,
                size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn datagram_ring_supports_minimum_queued_depth() {
        let ring = Ring::for_datagram(RingCapacity::transient_only(4)).unwrap();
        assert!(!ring.is_poisoned());
    }

    #[test]
    fn shared_reactor_dispatches_multiple_receives_by_user_data() {
        fn connected_pair() -> (TcpStream, TcpStream) {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (server, _) = listener.accept().unwrap();
            (client, server)
        }

        let ring = Ring::for_network(4, 1).unwrap();
        let (mut first_client, first_server) = connected_pair();
        let (mut second_client, second_server) = connected_pair();
        let mut first_ring = ring.clone();
        let mut second_ring = ring.clone();
        let (completed, completions) = mpsc::channel();
        let first_completed = completed.clone();
        let first = thread::spawn(move || {
            let result = first_ring.recv(&first_server, vec![0], 0, 1);
            let _ = first_completed.send((1, result));
        });

        let first_deadline = std::time::Instant::now() + Duration::from_secs(2);
        while ring.max_observed_active_operations() < 1
            && std::time::Instant::now() < first_deadline
        {
            thread::yield_now();
        }
        assert_eq!(ring.max_observed_active_operations(), 1);

        let second = thread::spawn(move || {
            let result = second_ring.recv(&second_server, vec![0], 0, 1);
            let _ = completed.send((2, result));
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while ring.max_observed_active_operations() < 2 && std::time::Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            ring.max_observed_active_operations() >= 2,
            "reactor never made both receives kernel-visible"
        );

        second_client.write_all(b"b").unwrap();
        let (which, second_result) = completions
            .recv_timeout(Duration::from_secs(2))
            .expect("second receive CQE was blocked behind the first");
        assert_eq!(which, 2, "the still-blocked first receive completed early");
        assert_eq!(
            second_result
                .unwrap_or_else(|failure| panic!("second receive failed: {}", failure.error))
                .transferred,
            1
        );

        first_client.write_all(b"a").unwrap();
        let (which, first_result) = completions
            .recv_timeout(Duration::from_secs(2))
            .expect("first receive did not complete");
        assert_eq!(which, 1);
        assert_eq!(
            first_result
                .unwrap_or_else(|failure| panic!("first receive failed: {}", failure.error))
                .transferred,
            1
        );
        first.join().expect("first receive thread panicked");
        second.join().expect("second receive thread panicked");
    }

    #[test]
    fn datagram_receive_scratch_preserves_caller_allocation_and_is_reusable() {
        let mut buffer = Vec::with_capacity(6);
        buffer.extend_from_slice(b"prefix");
        let buffer_pointer = buffer.as_ptr();
        let buffer_capacity = buffer.capacity();
        let mut scratch = Vec::new();
        assert_eq!(
            prepare_datagram_receive_scratch(&buffer, buffer.capacity(), 6, 3, &mut scratch)
                .unwrap(),
            6
        );
        assert_eq!(buffer, b"prefix");
        assert_eq!(buffer.as_ptr(), buffer_pointer);
        assert_eq!(buffer.capacity(), buffer_capacity);
        assert_eq!(scratch, b"prefix\0\0\0");

        let scratch_pointer = scratch.as_ptr();
        assert_eq!(
            prepare_datagram_receive_scratch(&buffer, buffer.capacity(), 6, 3, &mut scratch)
                .unwrap(),
            6
        );
        assert_eq!(scratch.as_ptr(), scratch_pointer);

        let mut spare = Vec::with_capacity(9);
        spare.extend_from_slice(b"prefix");
        assert_eq!(
            prepare_datagram_receive_scratch(&spare, spare.capacity(), 6, 3, &mut scratch).unwrap(),
            0
        );
        assert_eq!(scratch.len(), 3);

        let wrong_offset = b"prefix".to_vec();
        assert_eq!(
            prepare_datagram_receive_scratch(
                &wrong_offset,
                wrong_offset.capacity(),
                5,
                3,
                &mut scratch,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(wrong_offset, b"prefix");

        let empty_prefix = Vec::new();
        let receive_scratch = b"abc".to_vec();
        let receive_scratch_pointer = receive_scratch.as_ptr();
        let received = Ring::finish_datagram_receive(
            empty_prefix,
            receive_scratch,
            0,
            3,
            3,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 1)),
        );
        assert_eq!(received.buffer, b"abc");
        assert_eq!(received.buffer.as_ptr(), receive_scratch_pointer);
    }

    #[test]
    fn linked_connect_completion_matrix_covers_success_timeout_and_failure() {
        for timeout in [-libc::ECANCELED, -libc::ENOENT, -libc::ETIME] {
            assert_eq!(
                classify_linked_connect(0, timeout).unwrap(),
                ConnectDisposition::Connected
            );
        }
        for connect in [-libc::ECANCELED, -libc::EINTR] {
            assert_eq!(
                classify_linked_connect(connect, -libc::ETIME).unwrap(),
                ConnectDisposition::TimedOut
            );
        }
        assert_eq!(
            classify_linked_connect(-libc::ECONNREFUSED, -libc::ECANCELED).unwrap(),
            ConnectDisposition::Failed(-libc::ECONNREFUSED)
        );
        assert_eq!(
            classify_linked_connect(-libc::ECONNREFUSED, -libc::ETIME).unwrap(),
            ConnectDisposition::Failed(-libc::ECONNREFUSED)
        );
        assert_eq!(
            classify_linked_connect(1, -libc::ECANCELED)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            classify_linked_connect(-libc::ECANCELED, -libc::EINVAL)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn linked_connect_success_and_refusal_leave_ring_reusable() {
        use std::os::fd::OwnedFd;

        fn tcp_socket() -> OwnedFd {
            // SAFETY: socket returns a fresh descriptor or -1 and transfers no
            // Rust-owned resource on failure.
            let raw = unsafe {
                libc::socket(
                    libc::AF_INET,
                    libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                    libc::IPPROTO_TCP,
                )
            };
            assert!(
                raw >= 0,
                "create TCP socket: {}",
                io::Error::last_os_error()
            );
            // SAFETY: the successful socket call returned a fresh descriptor.
            unsafe { OwnedFd::from_raw_fd(raw) }
        }

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let mut ring = Ring::for_network_provider(4, 1).unwrap();
        let connected = tcp_socket();
        ring.connect(&connected, address, Duration::from_millis(100))
            .unwrap();
        let (accepted, _) = listener.accept().unwrap();
        drop(accepted);
        drop(connected);
        drop(listener);

        let refused = tcp_socket();
        let failure = ring
            .connect(&refused, address, Duration::from_millis(100))
            .unwrap_err();
        let (error, may_have_applied) = failure.into_parts();
        assert_eq!(error.raw_os_error(), Some(libc::ECONNREFUSED));
        assert!(!may_have_applied);
        assert!(!ring.is_poisoned());
    }

    #[test]
    fn linked_accept_completion_matrix_distinguishes_success_and_empty_poll() {
        assert_eq!(
            classify_linked_accept(42, -libc::ECANCELED).unwrap(),
            AcceptDisposition::Accepted
        );
        // A connection can win concurrently with timeout cancellation.
        assert_eq!(
            classify_linked_accept(42, -libc::ETIME).unwrap(),
            AcceptDisposition::Accepted
        );
        assert_eq!(
            classify_linked_accept(-libc::ECANCELED, -libc::ETIME).unwrap(),
            AcceptDisposition::NoConnection
        );
        assert_eq!(
            classify_linked_accept(-libc::EINTR, -libc::ETIME).unwrap(),
            AcceptDisposition::NoConnection
        );
        assert_eq!(
            classify_linked_accept(-libc::EAGAIN, -libc::ECANCELED).unwrap(),
            AcceptDisposition::NoConnection
        );
    }

    #[test]
    fn linked_accept_completion_matrix_preserves_accept_errors_and_rejects_bad_timeout() {
        assert_eq!(
            classify_linked_accept(-libc::EBADF, -libc::ECANCELED).unwrap(),
            AcceptDisposition::Failed(-libc::EBADF)
        );
        assert_eq!(
            classify_linked_accept(-libc::ECANCELED, -libc::EINVAL)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            classify_linked_accept(7, 0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn linked_accept_timeout_consumes_both_cqes_and_leaves_ring_reusable() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let mut ring = Ring::for_network_provider(4, 1).unwrap();

        for _ in 0..2 {
            assert_eq!(
                ring.accept(&listener).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        }

        let client = TcpStream::connect(address).unwrap();
        let client_address = client.local_addr().unwrap();
        let (_server, peer_address) = ring.accept(&listener).unwrap();
        assert_eq!(peer_address, client_address);
    }

    #[test]
    fn datagram_ring_preserves_empty_packets_truncation_and_reusability() {
        let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let sender_address = sender.local_addr().unwrap();
        let receiver_address = receiver.local_addr().unwrap();
        let mut ring = Ring::for_datagram(RingCapacity::transient_only(4)).unwrap();

        let sent = ring
            .send_datagram(&sender, Vec::new(), receiver_address)
            .unwrap();
        assert!(sent.buffer.is_empty());
        assert_eq!(sent.transferred, 0);
        let empty = match ring
            .recv_datagram(
                &receiver,
                b"prefix".to_vec(),
                6,
                0,
                DatagramReceiveWait::Nonblocking,
            )
            .unwrap()
        {
            DatagramReceiveAttempt::Received(receive) => receive,
            other => panic!("empty UDP packet was not received: {other:?}"),
        };
        assert_eq!(empty.buffer, b"prefix");
        assert_eq!(empty.transferred, 0);
        assert_eq!(empty.datagram_len, 0);
        assert_eq!(empty.source, sender_address);

        let mut unavailable_buffer = Vec::with_capacity(16);
        unavailable_buffer.extend_from_slice(b"again");
        let unavailable_pointer = unavailable_buffer.as_ptr();
        let unavailable = ring
            .try_recv_datagram(&receiver, unavailable_buffer, 5, 8)
            .unwrap_err();
        assert_eq!(unavailable.effect, DatagramEffect::NotApplied);
        assert_eq!(unavailable.buffer, b"again");
        assert_eq!(unavailable.buffer.as_ptr(), unavailable_pointer);
        assert_eq!(unavailable.error.kind(), io::ErrorKind::WouldBlock);

        let mut timeout_buffer = Vec::with_capacity(16);
        timeout_buffer.extend_from_slice(b"timeout");
        let timeout_pointer = timeout_buffer.as_ptr();
        let idle = ring
            .recv_datagram(
                &receiver,
                timeout_buffer,
                7,
                3,
                DatagramReceiveWait::Nonblocking,
            )
            .unwrap();
        match idle {
            DatagramReceiveAttempt::WouldBlock { buffer } => {
                assert_eq!(buffer, b"timeout");
                assert_eq!(buffer.as_ptr(), timeout_pointer);
            }
            other => panic!("unexpected UDP receive after queue drained: {other:?}"),
        }

        ring.send_datagram(&sender, b"truncate".to_vec(), receiver_address)
            .unwrap();
        let truncated = match ring
            .recv_datagram(
                &receiver,
                b"!".to_vec(),
                1,
                3,
                DatagramReceiveWait::Nonblocking,
            )
            .unwrap()
        {
            DatagramReceiveAttempt::Received(receive) => receive,
            other => panic!("UDP payload was not received: {other:?}"),
        };
        assert_eq!(truncated.buffer, b"!tru");
        assert_eq!(truncated.transferred, 3);
        assert_eq!(truncated.datagram_len, 8);
        assert_eq!(truncated.source, sender_address);
        assert!(!ring.is_poisoned());
    }
}
