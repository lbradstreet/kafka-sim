//! Each C entry contains unwind panics before returning to its caller.
use crate::{config, memory, submission, types::*};
use kr_kafka_producer::{
    client::{ClientBuffer, ClientError, ProducerClient},
    config::ProducerConfig,
    credit::{Claim, HeldCredits, Resource},
    types::{Event, LeaseId, TopicHandle},
};
use kr_kafka_producer_host::producer::{HostError, HostProducer};
use kr_runtime::{RuntimeDuration, contain_panic};
use std::{
    mem::size_of,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicI32, Ordering},
    },
};

pub const KR_OK: i32 = 0;
pub const KR_ERR_INVALID: i32 = -1;
pub const KR_ERR_VERSION: i32 = -2;
pub const KR_ERR_EXHAUSTED: i32 = -3;
pub const KR_ERR_NOT_READY: i32 = -4;
pub const KR_ERR_CLOSED: i32 = -5;
pub const KR_ERR_UNSUPPORTED: i32 = -6;
pub const KR_ERR_FAILED: i32 = -7;
pub const KR_ERR_TIMEOUT: i32 = -8;

pub(crate) struct Lease {
    pub id: LeaseId,
    pub base: usize,
    pub used: u32,
    pub writable: Option<ClientBuffer>,
}
pub struct KrProducer {
    pub(crate) client: ProducerClient,
    owner: Mutex<Option<HostProducer>>,
    pub(crate) leases: Mutex<Vec<Option<Lease>>>,
    snapshots: Mutex<metadata::Snapshots>,
    pub(crate) config: ProducerConfig,
    failed: AtomicBool,
    last_error: AtomicI32,
    // Must follow the slot allocation, so metadata returns credit after its owner.
    _metadata: [HeldCredits; 2],
    #[cfg(any(test, feature = "binding-test-hooks"))]
    test_owner: Option<std::thread::JoinHandle<()>>,
    #[cfg(feature = "binding-test-hooks")]
    test_start: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    #[cfg(feature = "binding-test-hooks")]
    test_abort: std::sync::Arc<AtomicBool>,
    #[cfg(feature = "binding-test-hooks")]
    test_calls: binding_test_hooks::CallHooks,
}
impl KrProducer {
    pub(crate) fn new(
        client: ProducerClient,
        owner: Option<HostProducer>,
        config: ProducerConfig,
    ) -> Result<Self, i32> {
        let slots = config.max_live_leases as usize;
        let minimum = slots
            .checked_mul(size_of::<Option<Lease>>())
            .and_then(|n| {
                n.checked_add(metadata::Snapshots::storage_bytes(
                    config.max_open_topics as usize,
                ))
            })
            .ok_or(KR_ERR_EXHAUSTED)?;
        let metadata = client
            .credits()
            .reserve(&[Claim {
                resource: Resource::InputBytes,
                amount: minimum,
                lane: 0,
            }])
            .map_err(|_| KR_ERR_EXHAUSTED)?;
        let mut leases = Vec::new();
        leases
            .try_reserve_exact(slots)
            .map_err(|_| KR_ERR_EXHAUSTED)?;
        let actual = leases
            .capacity()
            .checked_mul(size_of::<Option<Lease>>())
            .and_then(|n| {
                n.checked_add(metadata::Snapshots::storage_bytes(
                    config.max_open_topics as usize,
                ))
            })
            .ok_or(KR_ERR_EXHAUSTED)?;
        let extra = actual.checked_sub(minimum).ok_or(KR_ERR_EXHAUSTED)?;
        let extra_claim = Claim {
            resource: Resource::InputBytes,
            amount: extra,
            lane: 0,
        };
        let slack = client
            .credits()
            .reserve(if extra == 0 {
                &[]
            } else {
                std::slice::from_ref(&extra_claim)
            })
            .map_err(|_| KR_ERR_EXHAUSTED)?;
        leases.resize_with(slots, || None);
        Ok(Self {
            client,
            owner: Mutex::new(owner),
            leases: Mutex::new(leases),
            snapshots: Mutex::new(metadata::Snapshots::new(config.max_open_topics as usize)?),
            config,
            failed: AtomicBool::new(false),
            last_error: AtomicI32::new(0),
            _metadata: [metadata, slack],
            #[cfg(any(test, feature = "binding-test-hooks"))]
            test_owner: None,
            #[cfg(feature = "binding-test-hooks")]
            test_start: Mutex::new(None),
            #[cfg(feature = "binding-test-hooks")]
            test_abort: std::sync::Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "binding-test-hooks")]
            test_calls: binding_test_hooks::CallHooks::default(),
        })
    }
    fn latch_failure(&self) {
        self.failed.store(true, Ordering::Release);
        self.last_error.store(KR_ERR_FAILED, Ordering::Release);
        contain_panic(|| {
            let _ = self.client.fail_runtime();
        });
    }
    pub(crate) fn scratch(&self, bytes: usize) -> Result<HeldCredits, i32> {
        if bytes == 0 {
            return self
                .client
                .credits()
                .reserve(&[])
                .map_err(|_| KR_ERR_EXHAUSTED);
        }
        self.client
            .credits()
            .reserve(&[Claim {
                resource: Resource::InputBytes,
                amount: bytes,
                lane: 0,
            }])
            .map_err(|_| KR_ERR_EXHAUSTED)
    }
    pub(crate) fn error(&self, error: i32) {
        self.last_error.store(error, Ordering::Release);
    }
}
fn run<T: Copy>(action: impl FnOnce() -> Result<T, i32>) -> Result<T, i32> {
    let mut result = Err(KR_ERR_FAILED);
    contain_panic(|| {
        result = action();
    });
    result
}
unsafe fn guarded<T: Copy>(
    producer: *mut KrProducer,
    allow_failed: bool,
    action: impl FnOnce(&KrProducer) -> Result<T, i32>,
) -> Result<T, i32> {
    memory::check(producer, 1)?;
    // SAFETY: ABI callers retain a live handle until the exclusive destroy call.
    let producer = unsafe { &*producer };
    if producer.failed.load(Ordering::Acquire) && !allow_failed {
        producer.error(KR_ERR_FAILED);
        return Err(KR_ERR_FAILED);
    }
    producer.error(KR_OK);
    let mut completed = false;
    let result = run(|| {
        let result = action(producer);
        completed = true;
        result
    });
    if !completed {
        producer.latch_failure();
    }
    if let Err(error) = result {
        producer.error(error);
    }
    result
}
macro_rules! with {
    ($producer:expr,$allow:expr,|$parameter:pat_param| $body:expr) => {{
        let action = |$parameter: &KrProducer| $body;
        // SAFETY: exported ABI functions forward their live-handle contract.
        unsafe { guarded($producer, $allow, action) }
    }};
}

mod metadata;
pub use metadata::*;

fn code(result: Result<(), i32>) -> i32 {
    result.err().unwrap_or(KR_OK)
}
pub(crate) fn client_error(error: ClientError) -> i32 {
    use kr_kafka_producer::mailbox::MailboxError;
    match error {
        ClientError::NotReady => KR_ERR_NOT_READY,
        ClientError::Closed | ClientError::TopicClosed => KR_ERR_CLOSED,
        ClientError::Credit(_) | ClientError::AllocationFailed | ClientError::TopicLimit => {
            KR_ERR_EXHAUSTED
        }
        ClientError::Mailbox(MailboxError::Full { .. } | MailboxError::AllocationFailed) => {
            KR_ERR_EXHAUSTED
        }
        ClientError::Mailbox(MailboxError::Closed) => KR_ERR_CLOSED,
        ClientError::Mailbox(MailboxError::WakerPanicked) => KR_ERR_FAILED,
        ClientError::Input(
            kr_kafka_producer::input::InputError::Credit(_)
            | kr_kafka_producer::input::InputError::AllocationFailed
            | kr_kafka_producer::input::InputError::ResourceExhausted { .. },
        ) => KR_ERR_EXHAUSTED,
        ClientError::Input(kr_kafka_producer::input::InputError::Closed) => KR_ERR_CLOSED,
        _ => KR_ERR_INVALID,
    }
}
fn host_error(error: HostError) -> i32 {
    match error {
        HostError::Connect(kr_kafka_producer::connector::ConnectError::TransportUnavailable) => {
            KR_ERR_UNSUPPORTED
        }
        HostError::Config(_)
        | HostError::Connect(kr_kafka_producer::connector::ConnectError::InvalidConfiguration) => {
            KR_ERR_INVALID
        }
        _ => KR_ERR_FAILED,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kr_abi_version() -> u32 {
    run(|| Ok(4)).unwrap_or(0)
}
/// # Safety
/// `out` is writable for exactly `size` bytes and cannot alias other inputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_producer_config_init(out: *mut KrProducerConfig, size: u32) -> i32 {
    code(run(|| {
        if size as usize != size_of::<KrProducerConfig>() {
            return Err(KR_ERR_VERSION);
        }
        // SAFETY: validated size and caller's writable output contract.
        unsafe { memory::write(out, KrProducerConfig::default()) }
    }))
}
/// # Safety
/// Config and all nested inputs are initialized and readable until return; out
/// is writable and disjoint. The returned handle needs one exclusive destroy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_producer_create(
    raw: *const KrProducerConfig,
    out: *mut *mut KrProducer,
) -> i32 {
    code(run(|| {
        // SAFETY: caller supplies the documented writable pointer output.
        unsafe { memory::write(out, std::ptr::null_mut()) }?;
        // SAFETY: version is checked before the complete config is copied.
        let raw = unsafe { memory::versioned(raw) }?;
        // SAFETY: nested config spans are valid for the complete create call.
        let config = unsafe { config::decode(raw) }?;
        let metadata = (config.max_live_leases as usize)
            .checked_mul(size_of::<Option<Lease>>())
            .and_then(|n| {
                n.checked_add(metadata::Snapshots::storage_bytes(
                    config.max_open_topics as usize,
                ))
            })
            .ok_or(KR_ERR_EXHAUSTED)?;
        if metadata >= config.input_bytes {
            return Err(KR_ERR_EXHAUSTED);
        }
        let owner = HostProducer::start(config.clone()).map_err(host_error)?;
        let client = owner.client();
        let producer = match KrProducer::new(client, None, config) {
            Ok(producer) => {
                *producer.owner.lock().unwrap_or_else(|p| p.into_inner()) = Some(owner);
                producer
            }
            Err(error) => {
                let _ = owner.close(RuntimeDuration::ZERO);
                let _ = owner.join();
                return Err(error);
            }
        };
        let producer = Box::into_raw(Box::new(producer));
        // SAFETY: output was checked before constructing any producer resource.
        unsafe { memory::write(out, producer) }
    }))
}
/// # Safety
/// Handle is live; name is readable for len bytes; out is writable and disjoint.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_topic_open(
    producer: *mut KrProducer,
    name: *const u8,
    len: u32,
    out: *mut u32,
) -> i32 {
    code(with!(producer, false, |p| {
        memory::check(out, 1)?;
        // SAFETY: the name span is immutable during this call and copied by open.
        let name = unsafe { memory::span(KrSpan { ptr: name, len }, 249) }?;
        let topic = p
            .client
            .open_topic(std::str::from_utf8(name).map_err(|_| KR_ERR_INVALID)?)
            .map_err(client_error)?;
        // SAFETY: checked output and caller's writable contract.
        unsafe { memory::write(out, topic.0) }
    }))
}
/// # Safety
/// Handle is live and id_out is writable for sixteen bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_topic_id(
    producer: *mut KrProducer,
    topic: u32,
    id_out: *mut u8,
) -> i32 {
    code(with!(producer, false, |p| {
        memory::check(id_out, 16)?;
        let id = p
            .client
            .topic_id(TopicHandle(topic))
            .map_err(client_error)?;
        // SAFETY: both arrays have sixteen bytes and cannot overlap.
        unsafe { std::ptr::copy_nonoverlapping(id.0.as_ptr(), id_out, 16) };
        Ok(())
    }))
}
/// # Safety
/// Handle remains live for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_topic_close(producer: *mut KrProducer, topic: u32) -> i32 {
    code(with!(producer, false, |p| p
        .client
        .close_topic(TopicHandle(topic))
        .map_err(client_error)))
}
/// # Safety
/// Handle is live and both outputs are writable and disjoint. The returned span
/// permits exclusive writes until commit/release; no writes may race those calls.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_buffer_acquire(
    producer: *mut KrProducer,
    bytes: u32,
    out: *mut *mut u8,
    lease_out: *mut u64,
) -> i32 {
    code(with!(producer, false, |p| {
        // SAFETY: caller supplies independent writable outputs.
        unsafe { memory::write(out, std::ptr::null_mut()) }?;
        // SAFETY: caller supplies the writable lease output.
        unsafe { memory::write(lease_out, 0) }?;
        let mut leases = p.leases.lock().unwrap_or_else(|e| e.into_inner());
        let slot = leases
            .iter_mut()
            .find(|s| s.is_none())
            .ok_or(KR_ERR_EXHAUSTED)?;
        let mut buffer = p.client.acquire(bytes, 0).map_err(client_error)?;
        let id = buffer.lease_id();
        let pointer = buffer.as_mut_slice().as_mut_ptr();
        *slot = Some(Lease {
            id,
            base: pointer as usize,
            used: 0,
            writable: Some(buffer),
        });
        // SAFETY: outputs checked before admission; ownership lives in the registry.
        unsafe { memory::write(out, pointer) }?;
        // SAFETY: the checked lease output receives its stable generation ID.
        unsafe { memory::write(lease_out, id.0) }?;
        Ok(())
    }))
}
/// # Safety
/// Handle is live; every foreign write into this acquired buffer has stopped.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_buffer_commit(producer: *mut KrProducer, lease: u64, used: u32) -> i32 {
    code(with!(producer, false, |p| {
        let mut leases = p.leases.lock().unwrap_or_else(|e| e.into_inner());
        let entry = leases
            .iter_mut()
            .flatten()
            .find(|e| e.id.0 == lease)
            .ok_or(KR_ERR_INVALID)?;
        let buffer = entry.writable.take().ok_or(KR_ERR_INVALID)?;
        match buffer.commit(used) {
            Ok(_) => {
                entry.used = used;
                Ok(())
            }
            Err(failure) => {
                entry.writable = Some(failure.buffer);
                Err(KR_ERR_INVALID)
            }
        }
    }))
}
/// # Safety
/// Handle is live; no caller still reads/writes the acquired mutable span.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_buffer_release(producer: *mut KrProducer, lease: u64) -> i32 {
    code(with!(producer, true, |p| {
        let entry = {
            let mut slots = p.leases.lock().unwrap_or_else(|e| e.into_inner());
            let slot = slots
                .iter_mut()
                .find(|e| e.as_ref().is_some_and(|e| e.id.0 == lease))
                .ok_or(KR_ERR_INVALID)?;
            slot.take().unwrap()
        };
        if entry.writable.is_none() {
            p.client.release(entry.id).map_err(client_error)?;
        }
        drop(entry);
        Ok(())
    }))
}
/// # Safety
/// Handle/output are live and disjoint. The complete foreign allocation is
/// readable and immutable across threads until InputReleased for this lease or
/// until destroy returns. On rejection no pointer is retained. Length describes
/// the full pinned allocation, including capacity slack kept alive by a binding.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_lease_register(
    producer: *mut KrProducer,
    pointer: *const u8,
    length: u64,
    out: *mut u64,
) -> i32 {
    code(with!(producer, false, |p| {
        // SAFETY: caller's independent lease output is writable.
        unsafe { memory::write(out, 0) }?;
        let used = u32::try_from(length).map_err(|_| KR_ERR_INVALID)?;
        if used == 0 {
            return Err(KR_ERR_INVALID);
        }
        memory::check(pointer, used as usize)?;
        let mut leases = p.leases.lock().unwrap_or_else(|e| e.into_inner());
        let slot = leases
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or(KR_ERR_EXHAUSTED)?;
        // SAFETY: the registration caller pins immutable initialized memory
        // through InputReleased; rejected registrations retain no owner.
        let owner = unsafe { crate::foreign::ForeignMemory::new(pointer, used as usize) };
        let bytes = kr_shared_bytes::SharedBytes::from_owner(std::sync::Arc::new(owner));
        let id = p.client.register_shared(bytes, 0).map_err(client_error)?;
        *slot = Some(Lease {
            id,
            base: pointer as usize,
            used,
            writable: None,
        });
        // SAFETY: output checked before admission, caller keeps it writable.
        unsafe { memory::write(out, id.0) }?;
        Ok(())
    }))
}
/// # Safety
/// Handle and versioned descriptor/header arrays are readable; all pointed copy
/// spans stay immutable/readable through return. Outputs never alias inputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_submitv_copy(
    producer: *mut KrProducer,
    records: *const KrRecord,
    count: u32,
) -> u32 {
    let accepted = with!(producer, false, |p| {
        // SAFETY: all borrowed C inputs are valid for this entire copy admission.
        unsafe { submission::copy(p, records, count) }
    })
    .unwrap_or(0);
    #[cfg(feature = "binding-test-hooks")]
    // SAFETY: the same call retains the live handle through the test barrier.
    unsafe {
        binding_test_hooks::after_call(producer, 1, accepted)
    };
    accepted
}
/// # Safety
/// Descriptor/header arrays are readable for the call. Payload spans address
/// immutable committed native memory from this producer and the named lease.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_submitv_leased(
    producer: *mut KrProducer,
    lease: u64,
    records: *const KrRecord,
    count: u32,
) -> u32 {
    with!(producer, false, |p| {
        // SAFETY: metadata arrays are valid; payload ranges are checked, not read.
        unsafe { submission::leased(p, LeaseId(lease), records, count) }
    })
    .unwrap_or(0)
}
/// # Safety
/// Handle is live; each output is writable, disjoint and has initialized struct_size.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_poll_events(
    producer: *mut KrProducer,
    out: *mut KrEvent,
    capacity: u32,
) -> u32 {
    with!(producer, true, |p| {
        let count = (capacity as usize)
            .min(p.config.max_completions_per_poll as usize)
            .min(1024);
        memory::check(out, count)?;
        for index in 0..count {
            // SAFETY: caller initializes every output version word before polling.
            let size = unsafe { out.wrapping_add(index).cast::<u32>().read() };
            if size as usize != size_of::<KrEvent>() {
                return Err(KR_ERR_VERSION);
            }
        }
        let mut written = 0;
        #[cfg(feature = "binding-test-hooks")]
        if count != 0
            && let Some(event) = p.test_calls.take_replay()
        {
            // SAFETY: the first output slot was size/capacity checked above.
            unsafe { memory::write(out, event) }?;
            written = 1;
        }
        let mut scratch = [Event::Closed { unresolved: 0 }; 64];
        while written < count {
            let batch = (count - written).min(scratch.len());
            let n = p.client.poll_events(&mut scratch[..batch]);
            for event in &scratch[..n] {
                let event = event_to_c(*event);
                #[cfg(feature = "binding-test-hooks")]
                p.test_calls.observe(event);
                // SAFETY: validated version/capacity; each disjoint slot is written once.
                unsafe { memory::write(out.wrapping_add(written), event) }?;
                written += 1;
            }
            if n < batch {
                break;
            }
        }
        p.error(KR_OK);
        Ok(written as u32)
    })
    .unwrap_or(0)
}
fn event_to_c(event: Event) -> KrEvent {
    let mut result = KrEvent::empty();
    match event {
        Event::Delivery(event) => {
            result.kind = 1;
            result.token = event.token.0;
            result.user_token = event.user_token;
            result.topic = event.topic.0;
            result.topic_id = event.partition.topic.0;
            result.partition = event.partition.partition;
            result.outcome = event.outcome.kind as u32;
            result.reason = event.outcome.reason as u32;
            result.base_offset = event.base_offset.value;
            result.base_offset_present = event.base_offset.present;
            result.timestamp_ms = event.timestamp.value;
            result.timestamp_present = event.timestamp.present;
            result.attempts = event.attempts;
        }
        Event::InputReleased { lease } => {
            result.kind = 2;
            result.token = lease.0;
        }
        Event::FlushDone { token } => {
            result.kind = 3;
            result.token = token.0;
        }
        Event::TopicReady {
            topic,
            id,
            partitions,
        } => {
            result.kind = 4;
            result.topic = topic.0;
            result.topic_id = id.0;
            result.count = partitions as u32;
        }
        Event::TopicFailed { topic, code } => {
            result.kind = 5;
            result.topic = topic.0;
            result.reason = code;
        }
        Event::Closed { unresolved } => {
            result.kind = 6;
            result.count = unresolved;
        }
        Event::Fatal { code } => {
            result.kind = 7;
            result.reason = code;
        }
    }
    result
}
/// # Safety
/// Handle is live and token_out is writable and disjoint.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_flush(producer: *mut KrProducer, token_out: *mut u64) -> i32 {
    let result = code(with!(producer, false, |p| {
        memory::check(token_out, 1)?;
        let token = p.client.flush().map_err(client_error)?;
        // SAFETY: output was validated before accepting the flush obligation.
        unsafe { memory::write(token_out, token.0) }
    }));
    #[cfg(feature = "binding-test-hooks")]
    // SAFETY: the same call retains the live handle through the test barrier.
    unsafe {
        binding_test_hooks::after_call(producer, 2, u32::from(result == KR_OK))
    };
    result
}
/// # Safety
/// Handle remains live for this call; timeout_ms is a relative close deadline.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_close(producer: *mut KrProducer, timeout_ms: u64) -> i32 {
    code(with!(producer, true, |p| {
        let nanos = timeout_ms.checked_mul(1_000_000).ok_or(KR_ERR_INVALID)?;
        match p.client.close(RuntimeDuration::from_nanos(nanos)) {
            Ok(()) | Err(ClientError::Closed) => Ok(()),
            Err(error) => Err(client_error(error)),
        }
    }))
}
/// # Safety
/// Handle remains live. Last-error is diagnostic and shared across calling threads.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_last_error(producer: *mut KrProducer) -> i32 {
    run(|| {
        memory::check(producer, 1)?;
        // SAFETY: caller retains the opaque handle until exclusive destruction.
        Ok(unsafe { &*producer }.last_error.load(Ordering::Acquire))
    })
    .unwrap_or_else(|error| error)
}
/// # Safety
/// Exclusive final call: producer was returned by create, is destroyed once,
/// and no concurrent call or access to its acquired buffers remains.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_destroy(producer: *mut KrProducer) {
    contain_panic(|| {
        if producer.is_null() {
            return;
        }
        if memory::check(producer, 1).is_err() {
            return;
        }
        // SAFETY: caller transfers the unique box from its single create call.
        let mut producer = unsafe { Box::from_raw(producer) };
        shutdown_step(&producer, || {
            let _ = producer.client.close(RuntimeDuration::ZERO);
        });
        let slots = producer
            .leases
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        for index in 0..slots {
            let entry = producer.leases.get_mut().unwrap_or_else(|e| e.into_inner())[index].take();
            if let Some(entry) = entry {
                shutdown_step(&producer, || {
                    if entry.writable.is_none() {
                        let _ = producer.client.release(entry.id);
                    }
                    drop(entry);
                });
            }
        }
        if let Some(owner) = producer
            .owner
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            shutdown_step(&producer, || {
                let _ = owner.join();
            });
        }
        #[cfg(feature = "binding-test-hooks")]
        if let Some(start) = producer
            .test_start
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = start.send(());
        }
        #[cfg(any(test, feature = "binding-test-hooks"))]
        if let Some(owner) = producer.test_owner.take() {
            shutdown_step(&producer, || {
                let _ = owner.join();
            });
        }
        // An aborted runtime can leave real provider completions retiring after
        // its owner thread. Only the last byte guard proves foreign reclamation.
        // No deadline or owner error may turn that obligation into an early free.
        let mut discarded = [Event::Closed { unresolved: 0 }; 64];
        loop {
            let mut retired = false;
            shutdown_step(&producer, || {
                retired = producer.client.status().is_ok_and(|status| {
                    status
                        .inputs
                        .allocation_bytes_by_lane
                        .iter()
                        .all(|bytes| *bytes == 0)
                });
                if !retired {
                    let _ = producer.client.poll_events(&mut discarded);
                }
            });
            if retired {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });
}

fn shutdown_step(producer: &KrProducer, action: impl FnOnce()) {
    let mut completed = false;
    contain_panic(|| {
        action();
        completed = true;
    });
    if !completed {
        producer.latch_failure();
    }
}

#[cfg(feature = "binding-test-hooks")]
mod binding_test_hooks;
#[cfg(test)]
mod tests;
