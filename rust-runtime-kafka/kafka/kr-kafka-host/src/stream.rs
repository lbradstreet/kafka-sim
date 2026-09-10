//! Owned, passive TLS over the vectored byte-stream submission boundary.
//!
//! Submission reserves a bounded queue slot immediately; polling any response
//! drives the connection. There is no task or thread. Dropping a response only
//! abandons observation. Provider-facing ciphertext retains the original write
//! and its admission lease. Read/control completion cells retain the matching
//! plaintext ownership and connection lease through actual provider completion.
//! No self-reference or hidden executor is needed after all observers abandon.

use crate::{
    SecurityError, reserve,
    tls::{TlsClient, TlsProgress, VerifiedTls},
};
use kr_runtime::{CompletionCertainty, CompletionError, CompletionResult, contain_panic};
use kr_runtime_io::completion::CompletionGuard;
use kr_runtime_io::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, NetworkError, NetworkFailure, ReadRequest,
    ReadResult, SharedBytes, VectoredWriteFailure, VectoredWriteRequest, VectoredWriteResult,
    WriteRequest, WriteResult, WriteSegment,
};
use std::{
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

/// TLS requires actual provider completion guards for ciphertext reads and
/// controls. Plaintext ownership must survive abandonment of their responses.
pub trait TlsTransport:
    ByteStreamVectoredSubmit<ReadResponse: CompletionGuard, ControlResponse: CompletionGuard>
{
}
impl<S> TlsTransport for S where
    S: ByteStreamVectoredSubmit<ReadResponse: CompletionGuard, ControlResponse: CompletionGuard>
{
}

#[derive(Clone, Copy, Debug)]
pub struct TlsStreamLimits {
    pub read_operations: usize,
    pub write_operations: usize,
    pub control_operations: usize,
    pub read_bytes: usize,
    pub write_bytes: usize,
    pub operation_bytes: usize,
    pub max_segments: usize,
    /// Each provider-facing ciphertext backing allocation has this capacity.
    pub transport_bytes: usize,
    /// Maximum TLS/provider transitions during one response poll.
    pub transitions_per_poll: usize,
}
impl Default for TlsStreamLimits {
    fn default() -> Self {
        Self {
            read_operations: 2,
            write_operations: 2,
            control_operations: 2,
            read_bytes: 2 * 1024 * 1024,
            write_bytes: 2 * 1024 * 1024,
            operation_bytes: 1024 * 1024,
            max_segments: 256,
            transport_bytes: 16 * 1024,
            transitions_per_poll: 64,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsStreamStatus {
    pub verified: bool,
    pub failure: Option<SecurityError>,
    pub read_operations: usize,
    pub write_operations: usize,
    pub control_operations: usize,
    pub retained_read_bytes: usize,
    pub retained_write_bytes: usize,
    pub fixed_buffer_bytes: usize,
    pub underlying_read: bool,
    pub underlying_write: bool,
    pub underlying_control: bool,
    pub terminal_state_pinned: bool,
    pub ciphertext_copied: u64,
    pub plaintext_copied: u64,
}

#[derive(Default)]
struct Accounting {
    reads: AtomicUsize,
    writes: AtomicUsize,
    controls: AtomicUsize,
    read_bytes: AtomicUsize,
    write_bytes: AtomicUsize,
}
#[derive(Clone, Copy)]
enum Class {
    Read,
    Write,
    Control,
}
struct Lease {
    accounting: Arc<Accounting>,
    class: Class,
    bytes: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let a = &self.accounting;
        match self.class {
            Class::Read => {
                a.reads.fetch_sub(1, Ordering::Relaxed);
                a.read_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
            }
            Class::Write => {
                a.writes.fetch_sub(1, Ordering::Relaxed);
                a.write_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
            }
            Class::Control => {
                a.controls.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}
enum Input {
    Read(ReadRequest),
    Write(WriteRequest),
    Vectored(VectoredWriteRequest),
    Shutdown,
    Close,
}
enum Output {
    Read(CompletionResult<ReadResult, NetworkFailure>),
    Write(CompletionResult<WriteResult, NetworkFailure>),
    Vectored(CompletionResult<VectoredWriteResult, VectoredWriteFailure>),
    Control(CompletionResult<(), NetworkFailure>),
}
struct Payload {
    input: Mutex<Option<Input>>,
    _lease: Lease,
}
/// Holds no TLS state or stream handle, so provider completion breaks all
/// ownership chains even after both application response and stream are gone.
struct CompletionHold {
    _payload: Option<Arc<Payload>>,
    _lifetime: Option<Arc<dyn Send + Sync>>,
}
struct Job {
    id: u64,
    payload: Arc<Payload>,
    output: Option<Output>,
    abandoned: bool,
    read_may_have_applied: bool,
}
struct ActiveWrite {
    slot: usize,
    bytes: usize,
    submitted: bool,
    known_ciphertext: usize,
}
struct LowerWrite<F> {
    future: Pin<Box<F>>,
    requested: usize,
}
#[derive(Clone, Copy)]
enum ControlKind {
    Shutdown,
    Close,
}

/// A thread-safe waker fanout independent of the possibly owner-local stream.
/// Provider completions wake every live response, so one abandoned waiter cannot
/// strand the other direction's completion.
struct WakeSet {
    slots: Mutex<Vec<Option<Waker>>>,
}
impl Wake for WakeSet {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let count = lock(&self.slots).len();
        for index in 0..count {
            let waker = lock(&self.slots)[index].clone();
            if let Some(waker) = waker {
                contain_panic(|| waker.wake());
            }
        }
    }
}
impl WakeSet {
    fn set(&self, slot: usize, waker: &Waker) {
        let mut slots = lock(&self.slots);
        if slots[slot].as_ref().is_none_or(|old| !old.will_wake(waker)) {
            slots[slot] = Some(waker.clone());
        }
    }
    fn clear(&self, slot: usize) {
        lock(&self.slots)[slot] = None;
    }
}
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct Core<S: TlsTransport> {
    stream: S,
    client: TlsClient,
    limits: TlsStreamLimits,
    jobs: Vec<Option<Job>>,
    next_id: u64,
    accounting: Arc<Accounting>,
    wakes: Arc<WakeSet>,
    read: Option<Pin<Box<S::ReadResponse>>>,
    read_owner: Option<(usize, u64, bool)>,
    write: Option<LowerWrite<S::WriteVectoredResponse>>,
    control: Option<(ControlKind, Pin<Box<S::ControlResponse>>)>,
    rx: Option<Vec<u8>>,
    pending_rx: Option<(Vec<u8>, usize)>,
    rx_eof: bool,
    tx: SharedBytes,
    tx_segments: Vec<WriteSegment>,
    stage: Vec<u8>,
    active_write: Option<ActiveWrite>,
    handshake_started: bool,
    handshake_claimed: bool,
    verified: bool,
    failure: Option<SecurityError>,
    closing: bool,
    closed: bool,
    write_closed: bool,
    shutdown_notify: bool,
    shutdown_done: bool,
    close_result: Option<CompletionResult<(), NetworkFailure>>,
    fixed_bytes: usize,
    ciphertext_copied: u64,
    plaintext_copied: u64,
    telemetry: Option<Arc<kr_kafka_client::telemetry::TransportTelemetry>>,
    // Last field: backing buffers and provider operations drop before budget.
    lifetime: Option<Arc<dyn Send + Sync>>,
}

/// An exclusive TLS stream; operation futures retain its owner state. It is
/// Send + Sync exactly when the underlying stream and owned futures are Send.
pub struct TlsStream<S: TlsTransport> {
    core: Arc<Mutex<Core<S>>>,
}
pub struct VerifiedPeer<'a, S: TlsTransport> {
    core: MutexGuard<'a, Core<S>>,
}
impl<S: TlsTransport> VerifiedPeer<'_, S> {
    pub fn proof(&self) -> VerifiedTls<'_> {
        self.core
            .client
            .peer_verified()
            .expect("verified guard holds the connection lock")
    }
}

impl<S: TlsTransport> TlsStream<S> {
    pub fn new(
        stream: S,
        client: TlsClient,
        limits: TlsStreamLimits,
    ) -> Result<Self, SecurityError> {
        let slots = limits
            .read_operations
            .checked_add(limits.write_operations)
            .and_then(|n| n.checked_add(limits.control_operations))
            .filter(|n| *n <= 4096)
            .ok_or(SecurityError::InvalidConfig {
                field: "tls_operations",
            })?;
        if limits.read_operations == 0
            || limits.write_operations == 0
            || limits.control_operations == 0
            || limits.read_bytes == 0
            || limits.write_bytes == 0
            || limits.operation_bytes == 0
            || limits.max_segments == 0
            || !(1..=64 * 1024).contains(&limits.transport_bytes)
            || limits.transitions_per_poll == 0
            || stream.max_segments() == 0
        {
            return Err(SecurityError::InvalidConfig {
                field: "tls_stream_limits",
            });
        }
        let mut rx = Vec::new();
        reserve(&mut rx, limits.transport_bytes, "TLS transport receive")?;
        let mut stage = Vec::new();
        reserve(&mut stage, 16 * 1024, "TLS write staging")?;
        stage.resize(16 * 1024, 0);
        let tx = SharedBytes::from(Arc::<[u8]>::from_iter(std::iter::repeat_n(
            0,
            limits.transport_bytes,
        )));
        let mut jobs = Vec::new();
        jobs.try_reserve_exact(slots)
            .map_err(|_| SecurityError::ResourceExhausted {
                resource: "TLS operation slots",
                limit: slots,
            })?;
        jobs.resize_with(slots, || None);
        let mut wake_slots = Vec::new();
        wake_slots
            .try_reserve_exact(slots + 2)
            .map_err(|_| SecurityError::ResourceExhausted {
                resource: "TLS wakers",
                limit: slots + 2,
            })?;
        wake_slots.resize_with(slots + 2, || None);
        let wakes = Arc::new(WakeSet {
            slots: Mutex::new(wake_slots),
        });
        let accounting = Arc::new(Accounting::default());
        let fixed_bytes =
            client.retained_capacity() + rx.capacity() + stage.capacity() + tx.allocation_len();
        // Mutex preserves portable Send futures, while also accepting !Send SimStream.
        #[allow(clippy::arc_with_non_send_sync)]
        let core = Arc::new(Mutex::new(Core {
            stream,
            client,
            limits,
            jobs,
            next_id: 1,
            accounting,
            wakes,
            lifetime: None,
            read: None,
            read_owner: None,
            write: None,
            control: None,
            rx: Some(rx),
            pending_rx: None,
            rx_eof: false,
            tx,
            tx_segments: Vec::with_capacity(1),
            stage,
            active_write: None,
            handshake_started: false,
            handshake_claimed: false,
            verified: false,
            failure: None,
            closing: false,
            closed: false,
            write_closed: false,
            shutdown_notify: false,
            shutdown_done: false,
            close_result: None,
            fixed_bytes,
            ciphertext_copied: 0,
            plaintext_copied: 0,
            telemetry: None,
        }));
        Ok(Self { core })
    }
    /// The only pre-application operation. No Kafka or SASL plaintext is admitted
    /// until this future has completed certificate verification successfully.
    pub fn handshake(&self) -> TlsHandshake<S> {
        let mut core = lock(&self.core);
        let immediate = if core.verified {
            Some(Ok(()))
        } else if core.handshake_claimed || core.closing {
            Some(Err(SecurityError::InvalidState))
        } else {
            core.handshake_claimed = true;
            None
        };
        TlsHandshake {
            core: self.core.clone(),
            immediate,
            consumed: false,
        }
    }
    pub fn peer_verified(&self) -> Option<VerifiedPeer<'_, S>> {
        let core = lock(&self.core);
        if core.verified && !core.closing && core.client.peer_verified().is_some() {
            Some(VerifiedPeer { core })
        } else {
            None
        }
    }
    pub fn status(&self) -> TlsStreamStatus {
        lock(&self.core).status()
    }
    /// Installs cumulative producer diagnostics before any TLS admission.
    pub fn attach_telemetry(
        &self,
        metrics: Arc<kr_kafka_client::telemetry::TransportTelemetry>,
    ) -> Result<(), SecurityError> {
        let mut core = lock(&self.core);
        if core.telemetry.is_some()
            || core.handshake_started
            || core.jobs.iter().any(Option::is_some)
        {
            return Err(SecurityError::InvalidState);
        }
        metrics.instrument_tls();
        core.telemetry = Some(metrics);
        Ok(())
    }
    /// Retains the caller's connection-budget lease through actual retirement,
    /// including provider completion after owner abandonment.
    /// Attach once, before handshake or operation admission.
    pub fn attach_lifetime_guard(&self, guard: Arc<dyn Send + Sync>) -> Result<(), SecurityError> {
        let mut core = lock(&self.core);
        if core.lifetime.is_some()
            || core.handshake_started
            || core.jobs.iter().any(Option::is_some)
        {
            return Err(SecurityError::InvalidState);
        }
        core.lifetime = Some(guard);
        Ok(())
    }
    /// Retain and poll this ticket through completion when dropping a connection.
    /// It closes the lower stream and drains every admitted lower operation.
    pub fn retire(&self) -> TlsControlResponse<S> {
        self.submit_close()
    }
    /// Explicit owner progress for abandoned responses. Pending means the lower
    /// provider or unread plaintext still needs progress, not a hidden task.
    pub fn poll_progress(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut core = lock(&self.core);
        let index = core.jobs.len() + 1;
        core.wakes.set(index, cx.waker());
        core.pump();
        if core.read.is_none()
            && core.write.is_none()
            && core.control.is_none()
            && core.jobs.iter().flatten().all(|job| job.output.is_some())
        {
            core.wakes.clear(index);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
    fn submit(&self, input: Input) -> Ticket<S> {
        let mut core = lock(&self.core);
        match core.admit(input) {
            Ok((slot, id)) => Ticket {
                core: Some(self.core.clone()),
                slot,
                id,
                immediate: None,
                consumed: false,
            },
            Err(output) => Ticket {
                core: None,
                slot: 0,
                id: 0,
                immediate: Some(output),
                consumed: false,
            },
        }
    }
}

struct Ticket<S: TlsTransport> {
    core: Option<Arc<Mutex<Core<S>>>>,
    slot: usize,
    id: u64,
    immediate: Option<Output>,
    consumed: bool,
}
impl<S: TlsTransport> Ticket<S> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Output> {
        assert!(!self.consumed, "TLS operation polled after completion");
        if let Some(output) = self.immediate.take() {
            self.consumed = true;
            return Poll::Ready(output);
        }
        let mut core = lock(self.core.as_ref().unwrap());
        core.wakes.set(self.slot, cx.waker());
        core.pump();
        let job = core.jobs[self.slot]
            .as_mut()
            .expect("admitted TLS job missing");
        assert_eq!(job.id, self.id);
        if let Some(output) = job.output.take() {
            core.jobs[self.slot] = None;
            core.wakes.clear(self.slot);
            self.consumed = true;
            Poll::Ready(output)
        } else {
            Poll::Pending
        }
    }
}
impl<S: TlsTransport> Drop for Ticket<S> {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        if let Some(core) = &self.core {
            let mut core = lock(core);
            core.wakes.clear(self.slot);
            if let Some(job) = &mut core.jobs[self.slot]
                && job.id == self.id
            {
                job.abandoned = true;
                if job.output.is_some() {
                    core.jobs[self.slot] = None;
                }
            }
        }
    }
}
/// An owned response. Dropping it never cancels admitted TLS work.
pub struct TlsOperation<S: TlsTransport, T> {
    ticket: Ticket<S>,
    map: fn(Output) -> T,
    _output: PhantomData<fn() -> T>,
}
impl<S: TlsTransport, T> Unpin for TlsOperation<S, T> {}
impl<S: TlsTransport, T> Future for TlsOperation<S, T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let map = self.map;
        self.ticket.poll(cx).map(map)
    }
}
pub type TlsReadResponse<S> = TlsOperation<S, CompletionResult<ReadResult, NetworkFailure>>;
pub type TlsWriteResponse<S> = TlsOperation<S, CompletionResult<WriteResult, NetworkFailure>>;
pub type TlsVectoredWriteResponse<S> =
    TlsOperation<S, CompletionResult<VectoredWriteResult, VectoredWriteFailure>>;
pub type TlsControlResponse<S> = TlsOperation<S, CompletionResult<(), NetworkFailure>>;
impl<S: TlsTransport> ByteStreamSubmit for TlsStream<S> {
    type ReadResponse = TlsReadResponse<S>;
    type WriteResponse = TlsWriteResponse<S>;
    type ControlResponse = TlsControlResponse<S>;
    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        TlsOperation {
            ticket: self.submit(Input::Read(request)),
            map: |o| match o {
                Output::Read(r) => r,
                _ => unreachable!(),
            },
            _output: PhantomData,
        }
    }
    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        TlsOperation {
            ticket: self.submit(Input::Write(request)),
            map: |o| match o {
                Output::Write(r) => r,
                _ => unreachable!(),
            },
            _output: PhantomData,
        }
    }
    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        TlsOperation {
            ticket: self.submit(Input::Shutdown),
            map: |o| match o {
                Output::Control(r) => r,
                _ => unreachable!(),
            },
            _output: PhantomData,
        }
    }
    fn submit_close(&self) -> Self::ControlResponse {
        TlsOperation {
            ticket: self.submit(Input::Close),
            map: |o| match o {
                Output::Control(r) => r,
                _ => unreachable!(),
            },
            _output: PhantomData,
        }
    }
}
impl<S: TlsTransport> ByteStreamVectoredSubmit for TlsStream<S> {
    type WriteVectoredResponse = TlsVectoredWriteResponse<S>;
    fn max_segments(&self) -> usize {
        lock(&self.core).limits.max_segments
    }
    fn submit_write_vectored(&self, request: VectoredWriteRequest) -> Self::WriteVectoredResponse {
        TlsOperation {
            ticket: self.submit(Input::Vectored(request)),
            map: |o| match o {
                Output::Vectored(r) => r,
                _ => unreachable!(),
            },
            _output: PhantomData,
        }
    }
}

pub struct TlsHandshake<S: TlsTransport> {
    core: Arc<Mutex<Core<S>>>,
    immediate: Option<Result<(), SecurityError>>,
    consumed: bool,
}
impl<S: TlsTransport> Unpin for TlsHandshake<S> {}
impl<S: TlsTransport> Future for TlsHandshake<S> {
    type Output = Result<(), SecurityError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.consumed, "TLS handshake polled after completion");
        if let Some(result) = self.immediate.take() {
            self.consumed = true;
            return Poll::Ready(result);
        }
        let result = {
            let mut core = lock(&self.core);
            let slot = core.jobs.len();
            core.wakes.set(slot, cx.waker());
            core.handshake_started = true;
            core.pump();
            let result = if let Some(error) = &core.failure {
                Some(Err(error.clone()))
            } else if core.verified {
                Some(Ok(()))
            } else if core.closing {
                Some(Err(SecurityError::InvalidState))
            } else {
                None
            };
            if result.is_some() {
                core.wakes.clear(slot);
            }
            result
        };
        if let Some(result) = result {
            self.consumed = true;
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}
impl<S: TlsTransport> Drop for TlsHandshake<S> {
    fn drop(&mut self) {
        if !self.consumed && self.immediate.is_none() {
            let core = lock(&self.core);
            core.wakes.clear(core.jobs.len());
        }
    }
}

impl<S: TlsTransport> Core<S> {
    fn status(&self) -> TlsStreamStatus {
        let a = &self.accounting;
        TlsStreamStatus {
            verified: self.verified && self.failure.is_none() && !self.closing,
            failure: self.failure.clone(),
            read_operations: a.reads.load(Ordering::Relaxed),
            write_operations: a.writes.load(Ordering::Relaxed),
            control_operations: a.controls.load(Ordering::Relaxed),
            retained_read_bytes: a.read_bytes.load(Ordering::Relaxed),
            retained_write_bytes: a.write_bytes.load(Ordering::Relaxed),
            fixed_buffer_bytes: self.fixed_bytes,
            underlying_read: self.read.is_some(),
            underlying_write: self.write.is_some(),
            underlying_control: self.control.is_some(),
            terminal_state_pinned: false,
            ciphertext_copied: self.ciphertext_copied,
            plaintext_copied: self.plaintext_copied,
        }
    }
    fn admit(&mut self, mut input: Input) -> Result<(usize, u64), Output> {
        let reject = |input, error| rejected(input, error, CompletionCertainty::NotApplied);
        let (class, bytes, count, count_limit, used, byte_limit) = match &input {
            Input::Read(r) => {
                let Some(end) = r.buffer.len().checked_add(r.max_bytes) else {
                    return Err(reject(input, invalid("TLS read length overflow")));
                };
                let bytes = r.buffer.capacity().max(end);
                (
                    Class::Read,
                    bytes,
                    &self.accounting.reads,
                    self.limits.read_operations,
                    Some(&self.accounting.read_bytes),
                    self.limits.read_bytes,
                )
            }
            Input::Write(r) => (
                Class::Write,
                r.buffer.capacity(),
                &self.accounting.writes,
                self.limits.write_operations,
                Some(&self.accounting.write_bytes),
                self.limits.write_bytes,
            ),
            Input::Vectored(r) => {
                let size = match r.validate(self.limits.max_segments, self.limits.operation_bytes) {
                    Ok(size) => size,
                    Err(error) => return Err(reject(input, error)),
                };
                (
                    Class::Write,
                    size.retained_bytes,
                    &self.accounting.writes,
                    self.limits.write_operations,
                    Some(&self.accounting.write_bytes),
                    self.limits.write_bytes,
                )
            }
            Input::Shutdown | Input::Close => (
                Class::Control,
                0,
                &self.accounting.controls,
                self.limits.control_operations,
                None,
                usize::MAX,
            ),
        };
        if !matches!(class, Class::Control)
            && (!self.verified || self.closing || self.failure.is_some())
        {
            return Err(reject(input, NetworkError::ConnectionClosed));
        }
        if matches!(class, Class::Write) && self.write_closed {
            return Err(reject(input, NetworkError::WriteClosed));
        }
        if bytes > self.limits.operation_bytes {
            return Err(reject(
                input,
                invalid("TLS operation retained bytes exceed limit"),
            ));
        }
        if matches!(&input, Input::Read(r) if r.max_bytes == 0) {
            let Input::Read(r) = input else {
                unreachable!()
            };
            return Err(Output::Read(Ok(ReadResult {
                buffer: r.buffer,
                bytes_read: 0,
                end_of_stream: false,
            })));
        }
        if matches!(&input, Input::Write(r) if r.buffer.is_empty()) {
            let Input::Write(r) = input else {
                unreachable!()
            };
            return Err(Output::Write(Ok(WriteResult {
                buffer: r.buffer,
                bytes_written: 0,
            })));
        }
        if matches!(input, Input::Close)
            && self.closed
            && self.read.is_none()
            && self.write.is_none()
        {
            return Err(Output::Control(self.close_result.clone().unwrap_or(Ok(()))));
        }
        if matches!(input, Input::Shutdown) && self.shutdown_done {
            return Err(Output::Control(Ok(())));
        }
        if count.load(Ordering::Relaxed) >= count_limit
            || used
                .is_some_and(|used| bytes > byte_limit.saturating_sub(used.load(Ordering::Relaxed)))
        {
            return Err(reject(
                input,
                NetworkError::ResourceExhausted {
                    resource: "TLS admission",
                    limit: byte_limit,
                },
            ));
        }
        let Some(next_id) = self.next_id.checked_add(1) else {
            return Err(reject(input, NetworkError::IdentifierExhausted));
        };
        let Some(slot) = self.jobs.iter().position(Option::is_none) else {
            return Err(reject(
                input,
                NetworkError::ResourceExhausted {
                    resource: "TLS operation slots",
                    limit: self.jobs.len(),
                },
            ));
        };
        if let Input::Read(r) = &mut input {
            let needed = r.buffer.len() + r.max_bytes;
            if r.buffer
                .try_reserve_exact(needed.saturating_sub(r.buffer.len()))
                .is_err()
                || r.buffer.capacity() > bytes
            {
                return Err(reject(
                    input,
                    NetworkError::ResourceExhausted {
                        resource: "TLS read allocation",
                        limit: bytes,
                    },
                ));
            }
        }
        count.fetch_add(1, Ordering::Relaxed);
        if let Some(used) = used {
            used.fetch_add(bytes, Ordering::Relaxed);
        }
        match input {
            Input::Close => self.closing = true,
            Input::Shutdown => self.write_closed = true,
            _ => {}
        }
        let payload = Arc::new(Payload {
            input: Mutex::new(Some(input)),
            _lease: Lease {
                accounting: self.accounting.clone(),
                class,
                bytes,
            },
        });
        self.jobs[slot] = Some(Job {
            id: self.next_id,
            payload,
            output: None,
            abandoned: false,
            read_may_have_applied: false,
        });
        self.next_id = next_id;
        self.wakes.clone().wake_by_ref();
        Ok((slot, next_id - 1))
    }
    fn oldest(&self, predicate: impl Fn(&Input) -> bool) -> Option<usize> {
        self.jobs
            .iter()
            .enumerate()
            .filter_map(|(slot, job)| {
                let job = job.as_ref()?;
                if job.output.is_some() {
                    return None;
                }
                let input = lock(&job.payload.input);
                input
                    .as_ref()
                    .filter(|i| predicate(i))
                    .map(|_| (job.id, slot))
            })
            .min_by_key(|(id, _)| *id)
            .map(|(_, slot)| slot)
    }
    fn complete(&mut self, slot: usize, output: Output) {
        let job = self.jobs[slot].as_mut().unwrap();
        if job.abandoned {
            self.jobs[slot] = None;
            self.wakes.clear(slot);
        } else {
            job.output = Some(output);
        }
        self.wakes.clone().wake_by_ref();
    }
    fn take_input(&self, slot: usize) -> Input {
        lock(&self.jobs[slot].as_ref().unwrap().payload.input)
            .take()
            .unwrap()
    }
    fn fence(&mut self, error: SecurityError) {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
        self.client.transport_failed();
        self.verified = false;
        self.closing = true;
    }
    fn completion_hold(&self, slot: Option<usize>) -> Arc<dyn Send + Sync> {
        Arc::new(CompletionHold {
            _payload: slot.map(|slot| self.jobs[slot].as_ref().unwrap().payload.clone()),
            _lifetime: self.lifetime.clone(),
        })
    }
    fn pump(&mut self) {
        let waker = Waker::from(self.wakes.clone());
        let mut cx = Context::from_waker(&waker);
        for _ in 0..self.limits.transitions_per_poll {
            let mut progressed = self.poll_lower(&mut cx);
            if self.closing {
                if !self.closed && self.control.is_none() {
                    let guard = self.completion_hold(self.oldest(|i| matches!(i, Input::Close)));
                    let mut response = self.stream.submit_close();
                    response.attach_completion_guard(guard);
                    self.control = Some((ControlKind::Close, Box::pin(response)));
                    progressed = true;
                }
                if self.closed
                    && self.read.is_none()
                    && self.write.is_none()
                    && self.control.is_none()
                {
                    self.finish_closed();
                    return;
                }
            } else if self.handshake_started {
                progressed |= self.process_tls();
            }
            if !progressed {
                return;
            }
        }
        self.wakes.clone().wake_by_ref();
    }
    fn poll_lower(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        if let Some(read) = &mut self.read
            && let Poll::Ready(result) = read.as_mut().poll(cx)
        {
            self.read = None;
            let no_effect = match &result {
                Ok(result) => result.bytes_read == 0,
                Err(error) => {
                    error.certainty() == CompletionCertainty::NotApplied
                        && error.error().bytes_transferred() == 0
                }
            };
            if let Some((slot, id, prior)) = self.read_owner.take()
                && no_effect
                && let Some(job) = &mut self.jobs[slot]
                && job.id == id
            {
                job.read_may_have_applied = prior;
            }
            progressed = true;
            match result {
                Ok(result)
                    if result.bytes_read == result.buffer.len()
                        && result.bytes_read <= self.limits.transport_bytes
                        && (result.bytes_read != 0 || result.end_of_stream) =>
                {
                    self.rx_eof |= result.end_of_stream;
                    if result.end_of_stream && result.bytes_read != 0 {
                        self.fence(SecurityError::TlsFailed);
                    }
                    self.pending_rx = Some((result.buffer, 0));
                }
                Ok(mut result) => {
                    result.buffer.clear();
                    self.rx = Some(result.buffer);
                    self.fence(SecurityError::TlsFailed);
                }
                Err(error) => {
                    let failure = error.into_parts().1;
                    let cause = failure.error().clone();
                    self.rx = failure.into_buffer();
                    self.fence(SecurityError::Network(cause));
                }
            }
        }
        if let Some(write) = &mut self.write
            && let Poll::Ready(result) = write.future.as_mut().poll(cx)
        {
            let requested = write.requested;
            self.write = None;
            progressed = true;
            match result {
                Ok(result) if result.bytes_written != 0 && result.bytes_written <= requested => {
                    if let Some(metrics) = &self.telemetry {
                        metrics.tls_write(result.bytes_written);
                    }
                    if let Some(active) = &mut self.active_write {
                        active.known_ciphertext += result.bytes_written;
                    }
                    self.tx_segments = result.segments;
                    self.tx_segments.clear();
                    if !self.closing && self.client.consume_outbound(result.bytes_written).is_err()
                    {
                        self.fence(SecurityError::TlsFailed);
                    }
                }
                Ok(result) => {
                    self.tx_segments = result.segments;
                    self.tx_segments.clear();
                    self.fence(SecurityError::TlsFailed);
                }
                Err(error) => {
                    let (certainty, failure) = error.into_parts();
                    if let Some(metrics) = &self.telemetry {
                        metrics.tls_write(failure.bytes_transferred);
                    }
                    if let Some(active) = &mut self.active_write
                        && certainty == CompletionCertainty::NotApplied
                        && failure.bytes_transferred == 0
                        && active.known_ciphertext == 0
                    {
                        active.submitted = false;
                    }
                    self.tx_segments = failure.segments;
                    self.tx_segments.clear();
                    self.fence(SecurityError::Network(failure.error));
                }
            }
        }
        if let Some((kind, control)) = &mut self.control
            && let Poll::Ready(result) = control.as_mut().poll(cx)
        {
            let kind = *kind;
            self.control = None;
            progressed = true;
            match kind {
                ControlKind::Close => {
                    self.closed = true;
                    self.close_result = Some(result);
                }
                ControlKind::Shutdown => {
                    self.shutdown_done = true;
                    if let Err(error) = &result {
                        self.fence(SecurityError::Network(error.error().error().clone()));
                    }
                    while let Some(slot) = self.oldest(|i| matches!(i, Input::Shutdown)) {
                        let _ = self.take_input(slot);
                        self.complete(slot, Output::Control(result.clone()));
                    }
                }
            }
        }
        progressed
    }
    fn process_tls(&mut self) -> bool {
        let mut progressed = false;
        if let Some((bytes, offset)) = &mut self.pending_rx {
            match self.client.receive_ciphertext(&bytes[*offset..]) {
                Ok(count) => {
                    *offset += count;
                    self.ciphertext_copied = self.ciphertext_copied.saturating_add(count as u64);
                    if let Some(metrics) = &self.telemetry {
                        metrics.tls_copy(count, 0);
                    }
                    progressed |= count != 0;
                }
                Err(error) => {
                    self.fence(error);
                    return true;
                }
            }
            if *offset == bytes.len() {
                let (mut bytes, _) = self.pending_rx.take().unwrap();
                bytes.clear();
                self.rx = Some(bytes);
            }
        }
        if self.write.is_none()
            && self.client.outbound_ciphertext().is_empty()
            && let Some(active) = self.active_write.take()
        {
            let input = self.take_input(active.slot);
            let output = match input {
                Input::Write(r) => Output::Write(Ok(WriteResult {
                    buffer: r.buffer,
                    bytes_written: active.bytes,
                })),
                Input::Vectored(r) => Output::Vectored(Ok(VectoredWriteResult {
                    segments: r.segments,
                    bytes_written: active.bytes,
                })),
                _ => unreachable!(),
            };
            self.complete(active.slot, output);
            progressed = true;
        }
        let progress = match self.client.drive() {
            Ok(p) => p,
            Err(error) => {
                self.fence(error);
                return true;
            }
        };
        match progress {
            TlsProgress::Transmit => {
                if self.write.is_none() {
                    let bytes = self.client.outbound_ciphertext();
                    let count = bytes.len().min(self.limits.transport_bytes);
                    let Some(tx) = self.tx.try_as_mut() else {
                        self.fence(SecurityError::InvalidState);
                        return true;
                    };
                    tx[..count].copy_from_slice(&bytes[..count]);
                    self.ciphertext_copied = self.ciphertext_copied.saturating_add(count as u64);
                    if let Some(metrics) = &self.telemetry {
                        metrics.tls_copy(count, 0);
                    }
                    let slot = self
                        .active_write
                        .as_ref()
                        .map(|active| active.slot)
                        .or_else(|| {
                            self.shutdown_notify
                                .then(|| self.oldest(|i| matches!(i, Input::Shutdown)))
                                .flatten()
                        });
                    let bytes = self
                        .tx
                        .clone()
                        .attach_guard(self.completion_hold(slot))
                        .expect("transport backing has no prior guard");
                    if let Some(active) = &mut self.active_write {
                        active.submitted = true;
                    }
                    let mut segments = mem::take(&mut self.tx_segments);
                    segments.push(WriteSegment {
                        bytes,
                        range: 0..count as u32,
                    });
                    self.write = Some(LowerWrite {
                        future: Box::pin(
                            self.stream
                                .submit_write_vectored(VectoredWriteRequest { segments }),
                        ),
                        requested: count,
                    });
                    progressed = true;
                }
            }
            TlsProgress::Plaintext => {
                self.verified = true;
                if let Some(slot) = self.oldest(|i| matches!(i, Input::Read(_))) {
                    let Input::Read(mut request) = self.take_input(slot) else {
                        unreachable!()
                    };
                    let count = request.max_bytes.min(self.client.plaintext().len());
                    request
                        .buffer
                        .extend_from_slice(&self.client.plaintext()[..count]);
                    self.client
                        .consume_plaintext(count)
                        .expect("bounded plaintext prefix");
                    self.plaintext_copied = self.plaintext_copied.saturating_add(count as u64);
                    if let Some(metrics) = &self.telemetry {
                        metrics.tls_copy(0, count);
                    }
                    self.complete(
                        slot,
                        Output::Read(Ok(ReadResult {
                            buffer: request.buffer,
                            bytes_read: count,
                            end_of_stream: false,
                        })),
                    );
                    progressed = true;
                }
            }
            TlsProgress::Ready => {
                self.verified = true;
                if self.rx_eof {
                    self.fence(SecurityError::TruncatedTls);
                    return true;
                }
                if self.shutdown_notify {
                    if !self.shutdown_done && self.control.is_none() {
                        let guard =
                            self.completion_hold(self.oldest(|i| matches!(i, Input::Shutdown)));
                        let mut response = self.stream.submit_shutdown_write();
                        response.attach_completion_guard(guard);
                        self.control = Some((ControlKind::Shutdown, Box::pin(response)));
                        progressed = true;
                    }
                } else if let Some(slot) =
                    self.oldest(|i| matches!(i, Input::Write(_) | Input::Vectored(_)))
                {
                    let payload = self.jobs[slot].as_ref().unwrap().payload.clone();
                    let input = lock(&payload.input);
                    let count = match input.as_ref().unwrap() {
                        Input::Write(request) => {
                            let count = request.buffer.len().min(self.stage.len());
                            self.stage[..count].copy_from_slice(&request.buffer[..count]);
                            count
                        }
                        Input::Vectored(request) => {
                            let mut offset = 0;
                            for segment in &request.segments {
                                let bytes = &segment.bytes.as_slice()
                                    [segment.range.start as usize..segment.range.end as usize];
                                let count = bytes.len().min(self.stage.len() - offset);
                                self.stage[offset..offset + count].copy_from_slice(&bytes[..count]);
                                offset += count;
                                if offset == self.stage.len() {
                                    break;
                                }
                            }
                            offset
                        }
                        _ => unreachable!(),
                    };
                    drop(input);
                    if count == 0 {
                        let Input::Write(request) = self.take_input(slot) else {
                            unreachable!()
                        };
                        self.complete(
                            slot,
                            Output::Write(Ok(WriteResult {
                                buffer: request.buffer,
                                bytes_written: 0,
                            })),
                        );
                        return true;
                    }
                    self.plaintext_copied = self.plaintext_copied.saturating_add(count as u64);
                    if let Some(metrics) = &self.telemetry {
                        metrics.tls_copy(0, count);
                    }
                    let encrypted = self.client.encrypt(&self.stage[..count]);
                    self.stage[..count].fill(0);
                    match encrypted {
                        Ok(()) => {
                            self.active_write = Some(ActiveWrite {
                                slot,
                                bytes: count,
                                submitted: false,
                                known_ciphertext: 0,
                            })
                        }
                        Err(error) => self.fence(error),
                    }
                    progressed = true;
                } else if self.write_closed && !self.shutdown_done {
                    match self.client.close_notify() {
                        Ok(()) => self.shutdown_notify = true,
                        Err(error) => self.fence(error),
                    }
                    progressed = true;
                }
                if !self.closing
                    && self.read.is_none()
                    && self.pending_rx.is_none()
                    && self.oldest(|i| matches!(i, Input::Read(_))).is_some()
                {
                    progressed |= self.submit_lower_read();
                }
            }
            TlsProgress::Receive => {
                if self.rx_eof {
                    self.fence(SecurityError::TruncatedTls);
                    return true;
                }
                if self.read.is_none() && self.pending_rx.is_none() {
                    progressed |= self.submit_lower_read();
                }
            }
            TlsProgress::PeerClosed => {
                while let Some(slot) = self.oldest(|i| matches!(i, Input::Read(_))) {
                    let Input::Read(request) = self.take_input(slot) else {
                        unreachable!()
                    };
                    self.complete(
                        slot,
                        Output::Read(Ok(ReadResult {
                            buffer: request.buffer,
                            bytes_read: 0,
                            end_of_stream: request.max_bytes != 0,
                        })),
                    );
                }
                self.write_closed = true;
                self.shutdown_done = true;
                while let Some(slot) = self.oldest(|i| matches!(i, Input::Shutdown)) {
                    let _ = self.take_input(slot);
                    self.complete(slot, Output::Control(Ok(())));
                }
                self.closing = true;
                progressed = true;
            }
        }
        progressed
    }
    fn submit_lower_read(&mut self) -> bool {
        let Some(mut buffer) = self.rx.take() else {
            self.fence(SecurityError::InvalidState);
            return true;
        };
        buffer.clear();
        let slot = self.oldest(|i| matches!(i, Input::Read(_)));
        if let Some(slot) = slot {
            let job = self.jobs[slot].as_mut().unwrap();
            self.read_owner = Some((slot, job.id, job.read_may_have_applied));
            job.read_may_have_applied = true;
        }
        let guard = self.completion_hold(slot);
        let mut response = self.stream.submit_read(ReadRequest {
            buffer,
            max_bytes: self.limits.transport_bytes,
        });
        response.attach_completion_guard(guard);
        self.read = Some(Box::pin(response));
        true
    }
    fn finish_closed(&mut self) {
        let ambiguous_slot = self
            .active_write
            .take()
            .filter(|a| a.submitted || a.known_ciphertext != 0)
            .map(|a| a.slot);
        for slot in 0..self.jobs.len() {
            let Some(job) = &self.jobs[slot] else {
                continue;
            };
            if job.output.is_some() {
                continue;
            }
            let uncertain_read = job.read_may_have_applied;
            let input = self.take_input(slot);
            let output = if matches!(input, Input::Close) {
                Output::Control(self.close_result.clone().unwrap_or(Ok(())))
            } else {
                rejected(
                    input,
                    NetworkError::ConnectionClosed,
                    if ambiguous_slot == Some(slot) || uncertain_read {
                        CompletionCertainty::MayHaveApplied
                    } else {
                        CompletionCertainty::NotApplied
                    },
                )
            };
            self.complete(slot, output);
        }
    }
}
fn invalid(reason: &'static str) -> NetworkError {
    NetworkError::InvalidRequest { reason }
}
fn rejected(input: Input, error: NetworkError, certainty: CompletionCertainty) -> Output {
    match input {
        Input::Read(r) => Output::Read(Err(CompletionError::new(
            certainty,
            NetworkFailure::with_buffer(error, r.buffer, 0),
        ))),
        Input::Write(r) => Output::Write(Err(CompletionError::new(
            certainty,
            NetworkFailure::with_buffer(error, r.buffer, 0),
        ))),
        Input::Vectored(r) => Output::Vectored(Err(CompletionError::new(
            certainty,
            VectoredWriteFailure::new(error, r.segments, 0),
        ))),
        Input::Shutdown | Input::Close => Output::Control(Err(CompletionError::new(
            certainty,
            NetworkFailure::without_buffer(error),
        ))),
    }
}

#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;
