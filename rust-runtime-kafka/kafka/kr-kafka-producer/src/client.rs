//! Thread-safe bulk ingress. Only ownership reservation and bounded publication
//! happen on caller threads; Kafka state and routing policy remain actor-owned.
use crate::{
    admission::{Admission, AdmissionError, SubmissionBatch, Submitted},
    config::{ConfigError, ProducerConfig},
    credit::{Claim, CreditError, HeldCredits, Resource, SharedCredits},
    input::{InputBuffer, InputError, InputLeases, LeasedRecordDescriptor, ReleaseWakeDeferral},
    lifecycle::EventEnvelope,
    mailbox::{BoundedMailbox, MailboxConfig, MailboxError, QueueItem, SubmissionPolicy},
    topic::{TopicMetadata, TopicState},
    types::*,
};
use kr_runtime::{HostControl, RuntimeDuration, RuntimeHandle, RuntimeInstant};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    thread::{self, ThreadId},
};

#[cfg(feature = "binding-test-hooks")]
mod binding_test_hooks;

#[derive(Clone)]
pub enum ClientClock {
    Host(HostControl),
    Simulation,
}
impl ClientClock {
    fn now(
        &self,
        expected: Option<&kr_runtime::SimRuntimeIdentity>,
    ) -> Result<RuntimeInstant, ClientError> {
        match self {
            Self::Host(control) => Ok(control.now()),
            Self::Simulation => match RuntimeHandle::current() {
                Some(RuntimeHandle::Sim(handle))
                    if expected.is_none_or(|id| id.belongs_to(&handle)) =>
                {
                    Ok(handle.now())
                }
                _ => Err(ClientError::ClockUnavailable),
            },
        }
    }
    fn policy(&self) -> SubmissionPolicy {
        match self {
            Self::Host(_) => SubmissionPolicy::AnyThread,
            Self::Simulation => SubmissionPolicy::OwnerThreadOnly,
        }
    }
}
impl fmt::Debug for ClientClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Host(_) => "HostClock",
            Self::Simulation => "SimulationClock",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ClientError {
    MetricsSnapshot(crate::telemetry::metrics::SnapshotError),
    Config(ConfigError),
    Credit(CreditError),
    Mailbox(MailboxError),
    Input(InputError),
    Closed,
    TopicClosed,
    TopicAlreadyOpen,
    InvalidTopic,
    TopicLimit,
    NotReady,
    TokenExhausted,
    ClockUnavailable,
    TimeOverflow,
    AllocationFailed,
    /// The owner task failed. Synchronous `poll_events` still drains any late
    /// native input releases after providers finish their abandoned operations.
    OwnerStopped,
}
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "producer client error: {self:?}")
    }
}
impl std::error::Error for ClientError {}
impl From<CreditError> for ClientError {
    fn from(value: CreditError) -> Self {
        Self::Credit(value)
    }
}
impl From<InputError> for ClientError {
    fn from(value: InputError) -> Self {
        Self::Input(value)
    }
}
impl From<MailboxError> for ClientError {
    fn from(value: MailboxError) -> Self {
        Self::Mailbox(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientTopic {
    pub handle: TopicHandle,
    pub name: String,
    pub id: Option<TopicId>,
    pub state: TopicState,
    pub generation: u32,
    pub partitions: u32,
    pending: u32,
    pub closing: bool,
    pub snapshot: Option<Arc<crate::topic::MetadataSnapshot>>,
    refresh_pending: bool,
    pub failure_reason: u32,
    pub metadata_invalidated: bool,
}

/// Bounded shared admission and allocation diagnostics. Resource pools are one
/// atomic ledger snapshot; ingress and native ownership counts are sampled next.
#[derive(Clone, Debug)]
pub struct ClientStatus {
    pub accepted: RecordToken,
    pub accepting: bool,
    pub open_topics: usize,
    pub owner_aborted: bool,
    pub mailbox: crate::mailbox::MailboxStatus,
    pub inputs: crate::input::InputStatus,
    pub credits: [crate::credit::PoolStatus; Resource::COUNT],
    pub copied_input_bytes: u64,
    pub telemetry: crate::telemetry::TelemetrySnapshot,
}

#[derive(Debug)]
pub(crate) enum Command {
    #[cfg(feature = "binding-test-hooks")]
    TestMetadata {
        handle: TopicHandle,
        at: RuntimeInstant,
        update: crate::control::MetadataUpdate,
    },
    RefreshTopic {
        handle: TopicHandle,
        at: RuntimeInstant,
    },
    OpenTopic {
        handle: TopicHandle,
        name: String,
        at: RuntimeInstant,
        credit: HeldCredits,
    },
    CloseTopic {
        handle: TopicHandle,
        at: RuntimeInstant,
        watermark: RecordToken,
    },
    Submit(SubmissionBatch),
    Flush {
        token: FlushToken,
        at: RuntimeInstant,
        watermark: RecordToken,
        credit: HeldCredits,
    },
    Close {
        at: RuntimeInstant,
        deadline: RuntimeInstant,
        watermark: RecordToken,
    },
    Cancel {
        token: RecordToken,
    },
}
struct State {
    admission: Admission,
    topics: BTreeMap<TopicHandle, ClientTopic>,
    names: BTreeMap<String, TopicHandle>,
    next_topic: u32,
    next_flush: u64,
    closed: bool,
}
struct Shared {
    config: ProducerConfig,
    credits: SharedCredits,
    clock: ClientClock,
    simulation_runtime: Option<kr_runtime::SimRuntimeIdentity>,
    owner: ThreadId,
    state: Mutex<State>,
    mailbox: BoundedMailbox<Command>,
    events: BoundedMailbox<EventEnvelope>,
    inputs: InputLeases,
    owner_aborted: AtomicBool,
    owner_finished: AtomicBool,
    failure_requested: AtomicBool,
    telemetry: Arc<crate::telemetry::ProducerTelemetry>,
    #[cfg(feature = "binding-test-hooks")]
    test_publications: binding_test_hooks::Publications,
}
// Field order is the unlock-before-notify contract, including early return and
// unwind: the admission mutex drops before the release-wake deferral.
struct ClientStateGuard<'a> {
    state: MutexGuard<'a, State>,
    _release_wakes: ReleaseWakeDeferral,
}
impl core::ops::Deref for ClientStateGuard<'_> {
    type Target = State;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl core::ops::DerefMut for ClientStateGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
impl Shared {
    fn lock_state(&self) -> ClientStateGuard<'_> {
        // Suppress before locking: a provider can publish concurrently with the
        // acquisition itself. One shared count composes nested/waiting callers.
        let release_wakes = self.inputs.defer_release_wakes();
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        ClientStateGuard {
            state,
            _release_wakes: release_wakes,
        }
    }
}

/// Cloneable producer ingress, safe to share between host application threads.
/// Simulation methods reject foreign threads before reserving any resources.
#[derive(Clone)]
pub struct ProducerClient {
    shared: Arc<Shared>,
}
impl fmt::Debug for ProducerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerClient")
            .field("mailbox", &self.shared.mailbox.status())
            .field("inputs", &self.shared.inputs.status())
            .finish_non_exhaustive()
    }
}

pub(crate) struct ClientEndpoint {
    shared: Arc<Shared>,
}

/// Writable native memory with an owner notification on commit or drop. The
/// notification is separate from the allocation's passive release guard, so no
/// provider-owned byte destructor invokes application callbacks.
pub struct ClientBuffer {
    inner: Option<InputBuffer>,
    owner: ProducerClient,
}
impl fmt::Debug for ClientBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuffer")
            .field("capacity", &self.capacity())
            .finish()
    }
}
pub struct ClientCommitFailure {
    pub error: InputError,
    pub buffer: ClientBuffer,
}
impl fmt::Debug for ClientCommitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCommitFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl ClientBuffer {
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.as_ref().map_or(0, InputBuffer::capacity)
    }
    #[must_use]
    pub fn lease_id(&self) -> LeaseId {
        self.inner
            .as_ref()
            .expect("live writable buffer")
            .lease_id()
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.inner
            .as_mut()
            .expect("live writable buffer")
            .as_mut_slice()
    }
    /// # Errors
    /// An invalid used length returns the same writable allocation to its caller.
    pub fn commit(mut self, used: u32) -> Result<LeaseId, ClientCommitFailure> {
        match self
            .inner
            .take()
            .expect("live writable buffer")
            .commit(used)
        {
            Ok(lease) => Ok(lease),
            Err(failure) => {
                self.inner = Some(failure.buffer);
                Err(ClientCommitFailure {
                    error: failure.error,
                    buffer: self,
                })
            }
        }
    }
}
impl Drop for ClientBuffer {
    fn drop(&mut self) {
        drop(self.inner.take());
        let _ = self.owner.shared.mailbox.notify();
    }
}
impl ProducerClient {
    /// Independent distribution handle; does not acquire admission state or
    /// inspect histogram storage. Its passive requests do not wake the owner.
    pub fn metrics(&self) -> Result<crate::telemetry::metrics::MetricsReader, ClientError> {
        self.check_thread()?;
        Ok(self.shared.telemetry.metrics())
    }
    /// Explicit diagnostic work uses the existing owner notification. Merely
    /// enabling metrics does not add runtime wakeups or timers.
    pub fn request_metrics_snapshot(&self) -> Result<(), ClientError> {
        self.metrics()?
            .request_snapshot()
            .map_err(ClientError::MetricsSnapshot)?;
        self.shared.mailbox.notify()?;
        Ok(())
    }
    /// Fences ingress after a binding/runtime panic and requests producer failure
    /// without consuming ordinary mailbox or control-event capacity.
    pub fn fail_runtime(&self) -> Result<(), ClientError> {
        self.check_thread()?;
        {
            let mut state = self.shared.lock_state();
            state.closed = true;
            state.admission.close();
            self.shared.failure_requested.store(true, Ordering::Release);
        }
        self.shared.mailbox.notify()?;
        Ok(())
    }
    pub fn status(&self) -> Result<ClientStatus, ClientError> {
        self.check_thread()?;
        let state = self.shared.lock_state();
        Ok(ClientStatus {
            accepted: state.admission.last_token(),
            accepting: !state.closed,
            open_topics: state.topics.len(),
            owner_aborted: self.shared.owner_aborted.load(Ordering::Acquire),
            mailbox: self.shared.mailbox.status(),
            inputs: self.shared.inputs.status(),
            credits: self.shared.credits.snapshot(),
            copied_input_bytes: state.admission.copied_bytes(),
            telemetry: self.shared.telemetry.snapshot(),
        })
    }

    pub(crate) fn channel(
        config: ProducerConfig,
        credits: SharedCredits,
        clock: ClientClock,
        simulation_runtime: Option<kr_runtime::SimRuntimeIdentity>,
    ) -> Result<(Self, ClientEndpoint), ClientError> {
        let validated = config.validate().map_err(ClientError::Config)?;
        let policy = clock.policy();
        let mailbox = BoundedMailbox::new(MailboxConfig {
            data_capacity: config.mailbox_capacity as usize,
            control_capacity: config.max_open_topics as usize + 16,
            submission_policy: policy,
        })?;
        // All event kinds share one FIFO. Their independently reserved pools
        // guarantee control capacity without allowing control events to overtake.
        let capacity = [
            Resource::DeliveryEvents,
            Resource::ReleaseEvents,
            Resource::ControlEvents,
        ]
        .iter()
        .try_fold(0usize, |n, r| n.checked_add(validated.credits[*r as usize]))
        .ok_or(ClientError::AllocationFailed)?;
        let events = BoundedMailbox::new(MailboxConfig {
            data_capacity: capacity,
            control_capacity: 0,
            submission_policy: policy,
        })?;
        let inputs = InputLeases::new(&config, credits.clone())?;
        let admission = Admission::new(
            &config,
            credits.clone(),
            validated.effective_batch_payload_bytes,
        );
        let shared = Arc::new(Shared {
            config,
            credits,
            clock,
            simulation_runtime,
            owner: thread::current().id(),
            state: Mutex::new(State {
                admission,
                topics: BTreeMap::new(),
                names: BTreeMap::new(),
                next_topic: 1,
                next_flush: 1,
                closed: false,
            }),
            mailbox,
            events,
            inputs,
            owner_aborted: AtomicBool::new(false),
            owner_finished: AtomicBool::new(false),
            failure_requested: AtomicBool::new(false),
            telemetry: Arc::new(crate::telemetry::ProducerTelemetry::default()),
            #[cfg(feature = "binding-test-hooks")]
            test_publications: binding_test_hooks::Publications::default(),
        });
        Ok((
            Self {
                shared: shared.clone(),
            },
            ClientEndpoint { shared },
        ))
    }
    fn check_thread(&self) -> Result<(), ClientError> {
        if matches!(self.shared.clock, ClientClock::Simulation)
            && self.shared.owner != thread::current().id()
        {
            Err(MailboxError::ForeignThread.into())
        } else {
            Ok(())
        }
    }
    /// # Errors
    /// Rejects invalid/duplicate names, closed admission and bounded control capacity.
    pub fn open_topic(&self, name: &str) -> Result<TopicHandle, ClientError> {
        self.open_topic_at(
            name,
            self.shared
                .clock
                .now(self.shared.simulation_runtime.as_ref())?,
        )
    }
    /// Explicit-time ingress for passive simulation drivers outside a runtime task.
    /// # Errors
    /// Same admission guarantees as [`Self::open_topic`].
    pub fn open_topic_at(
        &self,
        name: &str,
        at: RuntimeInstant,
    ) -> Result<TopicHandle, ClientError> {
        self.check_thread()?;
        if name.is_empty()
            || name.len() > 249
            || name == "."
            || name == ".."
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(ClientError::InvalidTopic);
        }
        let (handle, wake) = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return Err(ClientError::Closed);
            }
            if state.names.contains_key(name) {
                return Err(ClientError::TopicAlreadyOpen);
            }
            if state.topics.len() >= self.shared.config.max_open_topics as usize {
                return Err(ClientError::TopicLimit);
            }
            let next = state
                .next_topic
                .checked_add(1)
                .ok_or(ClientError::TokenExhausted)?;
            let handle = TopicHandle(state.next_topic);
            let credit = self.shared.credits.reserve(&[Claim {
                resource: Resource::ControlEvents,
                amount: 1,
                lane: 0,
            }])?;
            let wake = self
                .shared
                .mailbox
                .push_deferred(
                    Command::OpenTopic {
                        handle,
                        name: name.into(),
                        at,
                        credit,
                    },
                    true,
                )
                .map_err(|r| ClientError::Mailbox(r.error))?;
            state.topics.insert(
                handle,
                ClientTopic {
                    handle,
                    name: name.into(),
                    id: None,
                    state: TopicState::Resolving,
                    generation: 0,
                    partitions: 0,
                    pending: 0,
                    closing: false,
                    snapshot: None,
                    refresh_pending: false,
                    failure_reason: 0,
                    metadata_invalidated: false,
                },
            );
            state.names.insert(name.into(), handle);
            state.next_topic = next;
            (handle, wake)
        };
        wake.wake();
        Ok(handle)
    }
    /// # Errors
    /// Rejects stale handles. A resolving topic returns `NotReady`.
    pub fn topic_id(&self, handle: TopicHandle) -> Result<TopicId, ClientError> {
        self.check_thread()?;
        let state = self.shared.lock_state();
        let topic = state.topics.get(&handle).ok_or(ClientError::TopicClosed)?;
        if topic.closing {
            return Err(ClientError::TopicClosed);
        }
        topic.id.ok_or(ClientError::NotReady)
    }
    #[cfg(feature = "binding-test-hooks")]
    pub fn test_metadata(
        &self,
        handle: TopicHandle,
        update: crate::control::MetadataUpdate,
    ) -> Result<(), ClientError> {
        self.check_thread()?;
        let at = self
            .shared
            .clock
            .now(self.shared.simulation_runtime.as_ref())?;
        let wake = {
            let state = self.shared.lock_state();
            if state.closed {
                return Err(ClientError::Closed);
            }
            if !state.topics.contains_key(&handle) {
                return Err(ClientError::TopicClosed);
            }
            self.shared
                .mailbox
                .push_deferred(Command::TestMetadata { handle, at, update }, true)
                .map_err(|rejected| ClientError::Mailbox(rejected.error))?
        };
        wake.wake();
        Ok(())
    }
    /// Returns None only for a previously allocated, completely retired handle.
    /// Handle IDs never repeat, so this requires no tombstone allocation.
    pub fn metadata_topic(&self, handle: TopicHandle) -> Result<Option<ClientTopic>, ClientError> {
        self.check_thread()?;
        let state = self.shared.lock_state();
        if handle.0 == 0 || handle.0 >= state.next_topic {
            return Err(ClientError::InvalidTopic);
        }
        Ok(state.topics.get(&handle).cloned())
    }
    /// 0: owner running; 1: normal closed publication complete; 2: aborted
    /// terminal publication complete. Provider retirement may still be pending.
    pub fn owner_status(&self) -> u32 {
        if !self.shared.owner_finished.load(Ordering::Acquire) {
            0
        } else if self.shared.owner_aborted.load(Ordering::Acquire) {
            2
        } else {
            1
        }
    }
    /// Schedules a refresh without waiting; duplicate unconsumed requests coalesce.
    pub fn refresh_topic(&self, handle: TopicHandle) -> Result<(), ClientError> {
        self.check_thread()?;
        let at = self
            .shared
            .clock
            .now(self.shared.simulation_runtime.as_ref())?;
        let wake = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return Err(ClientError::Closed);
            }
            let topic = state.topics.get(&handle).ok_or(ClientError::TopicClosed)?;
            if topic.closing || matches!(topic.state, TopicState::Deleted | TopicState::Failed) {
                return Err(ClientError::TopicClosed);
            }
            if topic.refresh_pending {
                return Ok(());
            }
            let wake = self
                .shared
                .mailbox
                .push_deferred(Command::RefreshTopic { handle, at }, true)
                .map_err(|r| ClientError::Mailbox(r.error))?;
            let topic = state.topics.get_mut(&handle).expect("checked topic");
            topic.refresh_pending = true;
            topic.metadata_invalidated = true;
            wake
        };
        wake.wake();
        Ok(())
    }
    /// # Errors
    /// Closing fences new admission immediately; the actor settles existing work.
    pub fn close_topic(&self, handle: TopicHandle) -> Result<(), ClientError> {
        self.check_thread()?;
        let at = self
            .shared
            .clock
            .now(self.shared.simulation_runtime.as_ref())?;
        let wake = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return Err(ClientError::Closed);
            }
            let topic = state.topics.get(&handle).ok_or(ClientError::TopicClosed)?;
            if topic.closing {
                return Err(ClientError::TopicClosed);
            }
            let watermark = state.admission.last_token();
            let wake = self
                .shared
                .mailbox
                .push_deferred(
                    Command::CloseTopic {
                        handle,
                        at,
                        watermark,
                    },
                    true,
                )
                .map_err(|r| ClientError::Mailbox(r.error))?;
            state
                .topics
                .get_mut(&handle)
                .expect("checked handle")
                .closing = true;
            wake
        };
        wake.wake();
        Ok(())
    }
    /// Copies only the accepted prefix. Completion and input capacity are reserved
    /// atomically before the mailbox publication becomes visible to the actor.
    /// A concurrent event poller can observe delivery once admission commits,
    /// including before this call returns. Serialize submission and polling if
    /// the application needs return-before-observation ordering.
    pub fn submit_copy(&self, records: &[RecordDescriptor<'_>]) -> Submitted {
        match self
            .shared
            .clock
            .now(self.shared.simulation_runtime.as_ref())
        {
            Ok(now) => self.submit_copy_at(now, records),
            Err(_) => rejected(AdmissionError::ClockUnavailable),
        }
    }
    pub fn submit_copy_at(
        &self,
        now: RuntimeInstant,
        records: &[RecordDescriptor<'_>],
    ) -> Submitted {
        if self.check_thread().is_err() {
            return rejected(AdmissionError::ForeignThread);
        }
        let (mut submitted, wake) = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return rejected(AdmissionError::Closed);
            }
            let n = records
                .len()
                .min(self.shared.config.max_submission_records as usize);
            let records = &records[..n];
            let valid = match self.validate(
                &state,
                records
                    .iter()
                    .map(|r| (r.topic, r.partition_hint, r.lane_hint)),
                n,
            ) {
                Ok(v) => v,
                Err(e) => return rejected(e),
            };
            let (submitted, batch) = state.admission.prepare_copy_routed(now, records, &valid);
            for record in records.iter().take(submitted.accepted as usize) {
                if let Some(topic) = state.topics.get_mut(&record.topic)
                    && topic.state == TopicState::Resolving
                {
                    topic.pending += 1;
                }
            }
            let wake = batch.map(|batch| {
                self.shared
                    .mailbox
                    .push_deferred(Command::Submit(batch), false)
                    .expect("single publisher lock and mailbox credit reserve")
            });
            (submitted, wake)
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        if submitted.error.is_none() && (submitted.accepted as usize) < records.len() {
            submitted.error = Some(AdmissionError::BulkLimit);
        }
        submitted
    }
    fn validate(
        &self,
        state: &State,
        records: impl Iterator<Item = (TopicHandle, Option<i32>, Option<u8>)>,
        count: usize,
    ) -> Result<Vec<Result<crate::admission::AdmissionRouting, AdmissionError>>, AdmissionError>
    {
        let mut valid = Vec::new();
        valid
            .try_reserve_exact(count)
            .map_err(|_| AdmissionError::AllocationFailed)?;
        let mut pending = BTreeMap::<TopicHandle, u32>::new();
        for (handle, partition, lane) in records {
            let result = (|| {
                let topic = state
                    .topics
                    .get(&handle)
                    .ok_or(AdmissionError::TopicClosed)?;
                if topic.closing || matches!(topic.state, TopicState::Deleted | TopicState::Failed)
                {
                    return Err(AdmissionError::TopicClosed);
                }
                if topic.state == TopicState::Resolving {
                    let delta = pending.entry(handle).or_default();
                    if topic.pending.saturating_add(*delta)
                        >= self.shared.config.pending_records_per_topic
                    {
                        return Err(AdmissionError::Credit(CreditError::ResourceExhausted {
                            resource: "pending topic records",
                            limit: self.shared.config.pending_records_per_topic as usize,
                        }));
                    }
                    *delta += 1;
                } else if partition.is_some_and(|p| p < 0 || p as u32 >= topic.partitions) {
                    return Err(AdmissionError::InvalidRecord);
                }
                let lane = lane.unwrap_or(0);
                if lane >= self.shared.config.lanes {
                    return Err(AdmissionError::InvalidLane);
                }
                Ok(crate::admission::AdmissionRouting {
                    lane,
                    ready: if topic.state == TopicState::Ready {
                        topic.id.map(|id| (id, topic.partitions))
                    } else {
                        None
                    },
                    builtin: matches!(
                        self.shared.config.partitioner,
                        crate::routing::PartitionerConfig::Builtin
                    ),
                })
            })();
            valid.push(result);
        }
        Ok(valid)
    }
    /// # Errors
    /// Native acquire reserves full retained capacity and its future release event.
    pub fn acquire(&self, bytes: u32, lane: u8) -> Result<ClientBuffer, ClientError> {
        self.check_thread()?;
        let state = self.shared.lock_state();
        if state.closed {
            return Err(ClientError::Closed);
        }
        Ok(ClientBuffer {
            inner: Some(self.shared.inputs.acquire(bytes, lane)?),
            owner: self.clone(),
        })
    }
    /// Registers an already immutable, uniquely owned allocation. Bindings must
    /// establish foreign pointer validity before constructing this safe owner.
    ///
    /// # Errors
    /// Rejects closure, nonunique/guarded owners and exhausted input/release pools.
    pub fn register_shared(
        &self,
        bytes: kr_shared_bytes::SharedBytes,
        lane: u8,
    ) -> Result<LeaseId, ClientError> {
        self.check_thread()?;
        let state = self.shared.lock_state();
        if state.closed {
            return Err(ClientError::Closed);
        }
        Ok(self.shared.inputs.register_shared(bytes, lane)?)
    }
    /// # Errors
    /// Fences future submissions; accepted views retain ownership until consumed.
    pub fn release(&self, lease: LeaseId) -> Result<(), ClientError> {
        self.check_thread()?;
        self.shared.inputs.release(lease)?;
        self.shared.mailbox.notify()?;
        Ok(())
    }
    pub fn submit_leased(
        &self,
        lease: LeaseId,
        records: &[LeasedRecordDescriptor<'_>],
    ) -> Submitted {
        match self
            .shared
            .clock
            .now(self.shared.simulation_runtime.as_ref())
        {
            Ok(now) => self.submit_leased_at(now, lease, records),
            Err(_) => rejected(AdmissionError::ClockUnavailable),
        }
    }
    pub fn submit_leased_at(
        &self,
        now: RuntimeInstant,
        lease: LeaseId,
        records: &[LeasedRecordDescriptor<'_>],
    ) -> Submitted {
        if self.check_thread().is_err() {
            return rejected(AdmissionError::ForeignThread);
        }
        let (mut submitted, wake) = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return rejected(AdmissionError::Closed);
            }
            let n = records
                .len()
                .min(self.shared.config.max_submission_records as usize);
            let records = &records[..n];
            let valid = match self.validate(
                &state,
                records
                    .iter()
                    .map(|r| (r.topic, r.partition_hint, r.lane_hint)),
                n,
            ) {
                Ok(v) => v,
                Err(e) => return rejected(e),
            };
            let (submitted, batch) = state.admission.prepare_leased_routed(
                now,
                &self.shared.inputs,
                lease,
                records,
                &valid,
            );
            for record in records.iter().take(submitted.accepted as usize) {
                if let Some(topic) = state.topics.get_mut(&record.topic)
                    && topic.state == TopicState::Resolving
                {
                    topic.pending += 1;
                }
            }
            let wake = batch.map(|batch| {
                self.shared
                    .mailbox
                    .push_deferred(Command::Submit(batch), false)
                    .expect("single publisher lock and mailbox credit reserve")
            });
            (submitted, wake)
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        if submitted.error.is_none() && (submitted.accepted as usize) < records.len() {
            submitted.error = Some(AdmissionError::BulkLimit);
        }
        submitted
    }
    /// # Errors
    /// A full fixed control reserve rejects flush before creating an obligation.
    pub fn flush(&self) -> Result<FlushToken, ClientError> {
        self.flush_at(
            self.shared
                .clock
                .now(self.shared.simulation_runtime.as_ref())?,
        )
    }
    pub fn flush_at(&self, at: RuntimeInstant) -> Result<FlushToken, ClientError> {
        self.check_thread()?;
        let (token, wake) = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return Err(ClientError::Closed);
            }
            let next = state
                .next_flush
                .checked_add(1)
                .ok_or(ClientError::TokenExhausted)?;
            let token = FlushToken(state.next_flush);
            let credit = self.shared.credits.reserve(&[Claim {
                resource: Resource::ControlEvents,
                amount: 1,
                lane: 0,
            }])?;
            let watermark = state.admission.last_token();
            let wake = self
                .shared
                .mailbox
                .push_deferred(
                    Command::Flush {
                        token,
                        at,
                        watermark,
                        credit,
                    },
                    true,
                )
                .map_err(|r| ClientError::Mailbox(r.error))?;
            state.next_flush = next;
            (token, wake)
        };
        wake.wake();
        Ok(token)
    }
    /// # Errors
    /// Close never waits for an ordinary mailbox/control slot. Repeated calls
    /// return `Closed`; the first call's deadline and watermark remain unchanged.
    pub fn close(&self, timeout: RuntimeDuration) -> Result<(), ClientError> {
        self.close_at(
            self.shared
                .clock
                .now(self.shared.simulation_runtime.as_ref())?,
            timeout,
        )
    }
    pub fn close_at(
        &self,
        at: RuntimeInstant,
        timeout: RuntimeDuration,
    ) -> Result<(), ClientError> {
        self.check_thread()?;
        let deadline = at.checked_add(timeout).ok_or(ClientError::TimeOverflow)?;
        let wake = {
            let mut state = self.shared.lock_state();
            if state.closed {
                return Err(ClientError::Closed);
            }
            let watermark = state.admission.last_token();
            let wake = self
                .shared
                .mailbox
                .close_deferred(Command::Close {
                    at,
                    deadline,
                    watermark,
                })
                .map_err(|r| ClientError::Mailbox(r.error))?;
            state.closed = true;
            state.admission.close();
            wake
        };
        wake.wake();
        Ok(())
    }
    /// # Errors
    /// Cancellation is ordered control work; the actor classifies whole-batch fate.
    pub fn cancel(&self, token: RecordToken) -> Result<(), ClientError> {
        self.check_thread()?;
        let wake = {
            let state = self.shared.lock_state();
            if state.closed {
                return Err(ClientError::Closed);
            }
            self.shared
                .mailbox
                .push_deferred(Command::Cancel { token }, true)
                .map_err(|r| ClientError::Mailbox(r.error))?
        };
        wake.wake();
        Ok(())
    }
    /// Drains the single FIFO event stream. A stopped consumer holds its reserved
    /// event credits, applying bounded backpressure to future submissions.
    /// Drains published events. Concurrent submission may still be returning
    /// when its delivery becomes observable; see [`Self::submit_copy`].
    pub fn poll_events(&self, out: &mut [Event]) -> usize {
        if self.check_thread().is_err() {
            return 0;
        }
        let mut count = 0;
        for slot in out {
            match self.shared.events.try_pop() {
                Ok(Some(QueueItem::Data(event))) => {
                    *slot = event.event;
                    count += 1;
                }
                _ => {
                    if self.shared.owner_aborted.load(Ordering::Acquire)
                        && let Some(event) = self.shared.inputs.pop_released()
                    {
                        *slot = event.event;
                        count += 1;
                    } else {
                        break;
                    }
                }
            }
        }
        if count > 0 {
            let _ = self.shared.mailbox.notify();
        }
        count
    }
    /// One application waiter; check/register/recheck mirrors the ingress queue.
    pub fn poll_event(&self, cx: &mut Context<'_>) -> Poll<Result<Option<Event>, ClientError>> {
        if let Err(e) = self.check_thread() {
            return Poll::Ready(Err(e));
        }
        match self.shared.events.poll_pop(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Some(QueueItem::Data(envelope)))) => {
                let event = envelope.event;
                // Return event capacity before waking a producer that may be
                // parked on those credits. The owner can run immediately.
                drop(envelope);
                let _ = self.shared.mailbox.notify();
                Poll::Ready(Ok(Some(event)))
            }
            Poll::Ready(Ok(None)) => {
                if self.shared.owner_aborted.load(Ordering::Acquire) {
                    if let Some(event) = self.shared.inputs.pop_released() {
                        return Poll::Ready(Ok(Some(event.event)));
                    }
                    if self.shared.inputs.status().live > 0 {
                        return Poll::Ready(Err(ClientError::OwnerStopped));
                    }
                }
                Poll::Ready(Ok(None))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(Some(_))) => unreachable!("single FIFO event lane"),
        }
    }
    #[must_use]
    pub fn credits(&self) -> SharedCredits {
        self.shared.credits.clone()
    }
}
fn rejected(error: AdmissionError) -> Submitted {
    Submitted {
        accepted: 0,
        first_token: None,
        error: Some(error),
    }
}

impl ClientEndpoint {
    pub(crate) fn attach_metrics(&self, reader: crate::telemetry::metrics::MetricsReader) {
        self.shared.telemetry.attach_metrics(reader);
    }
    pub(crate) fn telemetry(&self) -> Arc<crate::telemetry::ProducerTelemetry> {
        self.shared.telemetry.clone()
    }
    pub(crate) fn take_failure(&self) -> bool {
        self.shared.failure_requested.swap(false, Ordering::AcqRel)
    }
    pub(crate) fn aborted(&self) {
        self.shared.owner_aborted.store(true, Ordering::Release);
        self.finish();
    }
    pub(crate) fn queued_commands(&self) -> usize {
        let status = self.shared.mailbox.status();
        status.data_len + status.control_len + usize::from(status.close_pending)
    }
    pub(crate) fn fence(&self) {
        let mut state = self.shared.lock_state();
        state.closed = true;
        state.admission.close();
    }
    pub(crate) fn poll_command(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Command>, ClientError>> {
        match self.shared.mailbox.poll_pop(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => Poll::Ready(Ok(value.map(|item| match item {
                QueueItem::Data(c) | QueueItem::Control(c) | QueueItem::Close(c) => c,
            }))),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
        }
    }
    pub(crate) fn pop_command(&self) -> Result<Option<Command>, ClientError> {
        Ok(self.shared.mailbox.try_pop()?.map(|item| match item {
            QueueItem::Data(c) | QueueItem::Control(c) | QueueItem::Close(c) => c,
        }))
    }
    pub(crate) fn publish(&self, event: EventEnvelope) -> Result<(), ClientError> {
        #[cfg(feature = "binding-test-hooks")]
        let observed = event.event;
        if let Event::TopicFailed { topic, code } = event.event
            && let Some(topic) = self.shared.lock_state().topics.get_mut(&topic)
        {
            topic.state = if code == FailureReason::TopicDeleted as u32 && topic.id.is_some() {
                TopicState::Deleted
            } else {
                TopicState::Failed
            };
            topic.failure_reason = code;
        }
        self.shared
            .events
            .try_push(event)
            .map_err(|r| ClientError::Mailbox(r.error))?;
        #[cfg(feature = "binding-test-hooks")]
        self.shared.test_publications.published(observed);
        Ok(())
    }
    pub(crate) fn update_topic(&self, topic: &TopicMetadata) {
        let mut state = self.shared.lock_state();
        if let Some(client) = state.topics.get_mut(&topic.handle) {
            client.id = topic.id;
            client.state = topic.state;
            client.generation = topic.generation;
            client.partitions = topic.partitions.len() as u32;
            client.snapshot = topic.snapshot.clone();
            client.metadata_invalidated = topic
                .snapshot
                .as_ref()
                .is_none_or(|snapshot| snapshot.routing_generation != topic.generation);
            if topic.state != TopicState::Resolving {
                client.pending = 0;
            }
        }
    }
    pub(crate) fn refreshed(&self, handle: TopicHandle) {
        if let Some(topic) = self.shared.lock_state().topics.get_mut(&handle) {
            topic.refresh_pending = false;
        }
    }
    pub(crate) fn topic_closed(&self, handle: TopicHandle) {
        let mut state = self.shared.lock_state();
        if let Some(topic) = state.topics.remove(&handle) {
            state.names.remove(&topic.name);
        }
    }
    pub(crate) fn inputs(&self) -> &InputLeases {
        &self.shared.inputs
    }
    pub(crate) fn finish(&self) {
        self.fence();
        // Retained ABI snapshot clones have independent credit ownership.
        // The stopped owner must not pin its own cache through surviving clients.
        for topic in self.shared.lock_state().topics.values_mut() {
            topic.snapshot = None;
        }
        self.shared.owner_finished.store(true, Ordering::Release);
        let _ = self.shared.events.close();
    }
    pub(crate) fn watermark(&self) -> RecordToken {
        self.shared.lock_state().admission.last_token()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::{Wake, Waker},
    };
    fn channel() -> (ProducerClient, ClientEndpoint) {
        let config = ProducerConfig {
            record_descriptors: 16,
            delivery_event_capacity: 16,
            max_live_leases: 4,
            release_event_capacity: 4,
            max_open_topics: 4,
            pending_records_per_topic: 4,
            max_submission_records: 8,
            mailbox_capacity: 1,
            batch_target_bytes: 16,
            batch_hard_bytes: 1024,
            request_target_bytes: 1024,
            request_hard_bytes: 2048,
            output_chunk_bytes: 512,
            progressive_threshold: 8,
            input_bytes: 32 * 1024,
            compressed_bytes: 4096,
            ..Default::default()
        };
        let credits = SharedCredits::new(config.validate().unwrap().credits, config.lanes).unwrap();
        ProducerClient::channel(config, credits, ClientClock::Simulation, None).unwrap()
    }
    fn record(topic: TopicHandle) -> RecordDescriptor<'static> {
        RecordDescriptor {
            topic,
            partition_hint: None,
            lane_hint: None,
            key: None,
            value: Some(b"payload"),
            headers: &[],
            timestamp_ms: 0,
            user_token: 0,
            delivery_timeout: None,
        }
    }
    #[test]
    fn full_mailbox_rejects_before_token_assignment_and_close_keeps_accepted_work() {
        let (client, endpoint) = channel();
        let topic = client
            .open_topic_at("events", RuntimeInstant::ZERO)
            .unwrap();
        drop(endpoint.pop_command().unwrap());
        let first = client.submit_copy_at(RuntimeInstant::ZERO, &[record(topic)]);
        assert_eq!(first.accepted, 1);
        assert_eq!(first.first_token, Some(RecordToken(1)));
        let second = client.submit_copy_at(RuntimeInstant::ZERO, &[record(topic)]);
        assert_eq!(second.accepted, 0);
        client
            .close_at(RuntimeInstant::ZERO, RuntimeDuration::ZERO)
            .unwrap();
        assert_eq!(
            client
                .submit_copy_at(RuntimeInstant::ZERO, &[record(topic)])
                .accepted,
            0
        );
        match endpoint.pop_command().unwrap().unwrap() {
            Command::Close { watermark, .. } => assert_eq!(watermark, RecordToken(1)),
            other => panic!("expected emergency close: {other:?}"),
        }
        match endpoint.pop_command().unwrap().unwrap() {
            Command::Submit(batch) => assert_eq!(batch.records[0].token, RecordToken(1)),
            other => panic!("accepted batch lost: {other:?}"),
        }
        assert!(client.credits().is_empty());
    }
    #[test]
    fn unresolved_topic_quota_and_handle_identity_survive_concurrent_ingress_shape() {
        let (client, endpoint) = channel();
        let topic = client
            .open_topic_at("events", RuntimeInstant::ZERO)
            .unwrap();
        drop(endpoint.pop_command().unwrap());
        let records = [record(topic); 8];
        let first = client.submit_copy_at(RuntimeInstant::ZERO, &records);
        assert_eq!(first.accepted, 4);
        drop(endpoint.pop_command().unwrap());
        assert_eq!(
            client
                .submit_copy_at(RuntimeInstant::ZERO, &records)
                .accepted,
            0
        );
        endpoint.update_topic(&TopicMetadata {
            snapshot: None,
            handle: topic,
            name: "events".into(),
            id: Some(TopicId([1; 16])),
            state: TopicState::Ready,
            generation: 1,
            partitions: vec![crate::topic::PartitionMetadata {
                leader: 0,
                leader_epoch: 0,
            }],
            resolution_deadline: RuntimeInstant::ZERO,
            refresh_at: RuntimeInstant::ZERO,
        });
        assert_eq!(client.topic_id(topic), Ok(TopicId([1; 16])));
        assert_eq!(
            client
                .submit_copy_at(RuntimeInstant::ZERO, &[record(topic)])
                .first_token,
            Some(RecordToken(5))
        );
        drop(endpoint.pop_command().unwrap());
        endpoint.topic_closed(topic);
        let next = client
            .open_topic_at("events", RuntimeInstant::ZERO)
            .unwrap();
        assert_ne!(next, topic);
        assert_eq!(client.topic_id(topic), Err(ClientError::TopicClosed));
        drop(endpoint.pop_command().unwrap());
        assert!(client.credits().is_empty());
    }
    struct Reenter {
        client: ProducerClient,
        count: AtomicUsize,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            assert!(
                !matches!(
                    self.client.shared.state.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ),
                "admission mutex held during notification"
            );
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn retained_input_rollback_and_unwind_notify_after_admission_unlock() {
        for unwind in [false, true] {
            let (client, endpoint) = channel();
            let lease = client.acquire(16, 0).unwrap().commit(4).unwrap();
            let retained = endpoint.inputs().snapshot(lease).unwrap();
            client.release(lease).unwrap();
            let notify = Arc::new(Reenter {
                client: client.clone(),
                count: AtomicUsize::new(0),
            });
            endpoint
                .inputs()
                .register_release_waker(&Waker::from(notify.clone()));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut transaction = client.shared.lock_state();
                // The same retained-view destruction as a rejected leased
                // admission after a concurrent registry release. The real
                // admission rejects the now-stale generation without credit.
                let descriptors = [LeasedRecordDescriptor {
                    topic: TopicHandle(1),
                    partition_hint: None,
                    lane_hint: None,
                    key: None,
                    value: Some(0..4),
                    headers: &[],
                    timestamp_ms: 0,
                    user_token: 0,
                    delivery_timeout: None,
                }];
                let (submitted, batch) = transaction.admission.prepare_leased(
                    RuntimeInstant::ZERO,
                    endpoint.inputs(),
                    lease,
                    &descriptors,
                    &[Ok(0)],
                );
                assert_eq!(submitted.accepted, 0);
                assert_eq!(submitted.error, Some(AdmissionError::InvalidLease));
                assert!(batch.is_none());
                drop(retained);
                assert_eq!(notify.count.load(Ordering::SeqCst), 0);
                assert_eq!(endpoint.inputs().status().pending_release_events, 1);
                if unwind {
                    panic!("rollback unwind");
                }
            }));
            assert_eq!(result.is_err(), unwind);
            assert_eq!(notify.count.load(Ordering::SeqCst), 1);
            drop(endpoint.inputs().pop_released().unwrap());
            assert!(client.credits().is_empty());
        }
    }

    #[test]
    fn notification_runs_after_outer_admission_transaction_unlocks() {
        let (client, endpoint) = channel();
        let notify = Arc::new(Reenter {
            client: client.clone(),
            count: AtomicUsize::new(0),
        });
        let waker = Waker::from(notify.clone());
        assert!(
            endpoint
                .poll_command(&mut Context::from_waker(&waker))
                .is_pending()
        );
        client
            .open_topic_at("events", RuntimeInstant::ZERO)
            .unwrap();
        assert_eq!(notify.count.load(Ordering::SeqCst), 1);
        drop(endpoint.pop_command().unwrap());
        assert!(client.credits().is_empty());
    }
    #[test]
    fn hdr_recording_is_passive_and_only_explicit_snapshot_request_notifies() {
        use crate::telemetry::metrics::{
            Metric, MetricsConfig, MetricsRecorder, Scope, ScopeToken,
        };
        let (client, endpoint) = channel();
        let mut recorder = MetricsRecorder::new(MetricsConfig::default()).unwrap();
        endpoint.attach_metrics(recorder.reader());
        let reader = client.metrics().unwrap();
        let notify = Arc::new(Reenter {
            client: client.clone(),
            count: AtomicUsize::new(0),
        });
        let waker = Waker::from(notify.clone());
        assert!(
            endpoint
                .poll_command(&mut Context::from_waker(&waker))
                .is_pending()
        );
        recorder.record(Metric::BatchRawBytes, ScopeToken::GLOBAL, 8);
        assert_eq!(notify.count.load(Ordering::SeqCst), 0);
        client.status().unwrap();
        assert_eq!(notify.count.load(Ordering::SeqCst), 0);
        client.request_metrics_snapshot().unwrap();
        assert_eq!(notify.count.load(Ordering::SeqCst), 1);
        assert!(recorder.publish_at(RuntimeInstant::ZERO));
        let interval = reader.try_take_snapshot().unwrap();
        assert_eq!(
            interval
                .distribution(Scope::Global, Metric::BatchRawBytes)
                .unwrap()
                .count(),
            1
        );
        drop(interval);
        assert_eq!(notify.count.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn dropping_uncommitted_native_buffer_wakes_parked_owner_after_release() {
        let (client, endpoint) = channel();
        let buffer = client.acquire(16, 0).unwrap();
        let lease = buffer.lease_id();
        let notify = Arc::new(Reenter {
            client: client.clone(),
            count: AtomicUsize::new(0),
        });
        let waker = Waker::from(notify.clone());
        assert!(
            endpoint
                .poll_command(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(buffer);
        assert_eq!(notify.count.load(Ordering::SeqCst), 1);
        let event = endpoint.inputs().pop_released().unwrap();
        assert_eq!(event.event, Event::InputReleased { lease });
        endpoint.publish(event).unwrap();
        let mut events = [Event::Closed { unresolved: 0 }; 1];
        assert_eq!(client.poll_events(&mut events), 1);
        assert_eq!(events[0], Event::InputReleased { lease });
        assert!(client.credits().is_empty());
    }
    #[test]
    fn foreign_simulation_submission_cannot_reserve_or_wake() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<ProducerClient>();
        let (client, endpoint) = channel();
        let topic = client
            .open_topic_at("events", RuntimeInstant::ZERO)
            .unwrap();
        drop(endpoint.pop_command().unwrap());
        let foreign = client.clone();
        let result = std::thread::spawn(move || {
            foreign.submit_copy_at(RuntimeInstant::ZERO, &[record(topic)])
        })
        .join()
        .unwrap();
        assert_eq!(result.error, Some(AdmissionError::ForeignThread));
        assert!(client.credits().is_empty());
    }

    #[test]
    fn single_event_returns_credit_before_notifying_the_parked_owner() {
        struct InspectWake {
            credits: SharedCredits,
            held_at_wake: AtomicUsize,
        }
        impl Wake for InspectWake {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.held_at_wake.store(
                    self.credits.snapshot()[Resource::DeliveryEvents as usize].held,
                    Ordering::SeqCst,
                );
            }
        }
        let (client, endpoint) = channel();
        let credit = client
            .credits()
            .reserve(&[Claim {
                resource: Resource::DeliveryEvents,
                amount: 1,
                lane: 0,
            }])
            .unwrap();
        let event = Event::Delivery(DeliveryEvent {
            token: RecordToken(1),
            user_token: 7,
            topic: TopicHandle(1),
            partition: TopicPartition {
                topic: TopicId([1; 16]),
                partition: 0,
            },
            outcome: DeliveryOutcome::not_written(FailureReason::Cancelled),
            base_offset: None.into(),
            timestamp: None.into(),
            attempts: 0,
        });
        endpoint
            .publish(EventEnvelope::new(event, credit).unwrap())
            .unwrap();
        let wake = Arc::new(InspectWake {
            credits: client.credits(),
            held_at_wake: AtomicUsize::new(usize::MAX),
        });
        let waker = Waker::from(wake.clone());
        assert!(
            endpoint
                .poll_command(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            matches!(client.poll_event(&mut Context::from_waker(Waker::noop())), Poll::Ready(Ok(Some(actual))) if actual == event)
        );
        assert_eq!(wake.held_at_wake.load(Ordering::SeqCst), 0);
        assert!(client.credits().is_empty());
    }
}
