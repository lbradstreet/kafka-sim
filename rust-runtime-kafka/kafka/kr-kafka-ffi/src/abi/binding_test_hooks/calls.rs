//! Bounded, explicitly armed barriers at real ABI return points. Observers use
//! no guarded downcall and never change last_error, allowing diagnostics races
//! to be forced without introducing the overwrite being tested.
use super::*;
use kr_kafka_producer::types::RecordToken;
use std::sync::Condvar;
use std::time::Duration;

const ARMED: u32 = 1;
const ENTERED: u32 = 2;
const PUBLISHED: u32 = 4;
const RELEASED: u32 = 8;
const FINISHED: u32 = 16;
const TIMED_OUT: u32 = 32;
const CANCEL: u32 = 1;
const WAIT_PUBLICATION: u32 = 2;
const PAUSE: u32 = 4;
const WAIT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct State {
    operation: u32,
    flags: u32,
    phase: u32,
    before: u64,
    last_delivery: Option<KrEvent>,
    replay: Option<KrEvent>,
}
#[derive(Default)]
pub(in crate::abi) struct CallHooks {
    state: Mutex<State>,
    changed: Condvar,
}
impl CallHooks {
    pub(in crate::abi) fn observe(&self, event: KrEvent) {
        if event.kind == 1 {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_delivery = Some(event);
        }
    }
    pub(in crate::abi) fn take_replay(&self) -> Option<KrEvent> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replay
            .take()
    }
}

/// # Safety
/// The caller retains a live handle until every armed downcall has returned.
unsafe fn producer<'a>(raw: *mut KrProducer) -> Result<&'a KrProducer, i32> {
    memory::check(raw, 1)?;
    // SAFETY: test caller supplies the retained live opaque handle.
    Ok(unsafe { &*raw })
}

/// Arms one submit (1) or flush (2) return. Flags: cancel accepted records (1),
/// wait for real publication (2), and pause until explicitly released (4).
/// # Safety
/// Live test producer; no concurrent arm or destroy. Never resets diagnostics.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_arm_call(raw: *mut KrProducer, operation: u32, flags: u32) -> i32 {
    code(run(|| {
        // SAFETY: caller retains the test handle.
        let producer = unsafe { producer(raw) }?;
        if !matches!(operation, 1 | 2)
            || flags == 0
            || flags & !7 != 0
            || operation == 2 && flags & CANCEL != 0
        {
            return Err(KR_ERR_INVALID);
        }
        let mut state = producer
            .test_calls
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.operation != 0 || state.phase & ENTERED != 0 && state.phase & FINISHED == 0 {
            return Err(KR_ERR_EXHAUSTED);
        }
        state.operation = operation;
        state.flags = flags;
        state.phase = ARMED;
        state.before = producer
            .client
            .test_publication_count(if operation == 1 { 1 } else { 3 });
        Ok(())
    }))
}

/// Bitmask: armed=1, entered=2, published=4, released=8, finished=16,
/// timeout/progress failure=32. This observation never changes last_error.
/// # Safety
/// Live handle, retained through all observed calls.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_call_state(raw: *mut KrProducer) -> u32 {
    run(|| {
        // SAFETY: caller retains the test handle.
        let producer = unsafe { producer(raw) }?;
        Ok(producer
            .test_calls
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .phase)
    })
    .unwrap_or(TIMED_OUT)
}

/// Waits for an observable barrier state without polling or modifying diagnostics.
/// # Safety
/// Live handle; do not destroy until this call and the observed downcall return.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_wait_call(
    raw: *mut KrProducer,
    required: u32,
    timeout_ms: u32,
) -> i32 {
    code(run(|| {
        // SAFETY: caller retains the test handle.
        let producer = unsafe { producer(raw) }?;
        if required == 0 || required & !63 != 0 || timeout_ms > 10_000 {
            return Err(KR_ERR_INVALID);
        }
        let hooks = &producer.test_calls;
        let state = hooks
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (state, _) = hooks
            .changed
            .wait_timeout_while(
                state,
                Duration::from_millis(u64::from(timeout_ms)),
                |state| state.phase & required != required && state.phase & TIMED_OUT == 0,
            )
            .unwrap_or_else(|error| error.into_inner());
        if state.phase & TIMED_OUT != 0 {
            Err(KR_ERR_FAILED)
        } else if state.phase & required == required {
            Ok(())
        } else {
            Err(KR_ERR_TIMEOUT)
        }
    }))
}

/// Releases a paused return. It is safe to release before the owner publishes.
/// # Safety
/// Live handle; join the armed downcall before destroying it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_release_call(raw: *mut KrProducer) -> i32 {
    code(run(|| {
        // SAFETY: caller retains the test handle.
        let producer = unsafe { producer(raw) }?;
        let mut state = producer
            .test_calls
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.phase |= RELEASED;
        producer.test_calls.changed.notify_all();
        Ok(())
    }))
}

/// Replays the last actually drained delivery once. A zero user token preserves
/// its token; a nonzero override permits unknown/stale-generation probes.
/// # Safety
/// Live test handle. Deliberately corrupts the event protocol; never use in production.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_replay_delivery(raw: *mut KrProducer, user_token: u64) -> i32 {
    code(run(|| {
        // SAFETY: caller retains the test handle.
        let producer = unsafe { producer(raw) }?;
        let mut state = producer
            .test_calls
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.replay.is_some() {
            return Err(KR_ERR_EXHAUSTED);
        }
        let mut event = state.last_delivery.ok_or(KR_ERR_NOT_READY)?;
        if user_token != 0 {
            event.user_token = user_token;
        }
        state.replay = Some(event);
        Ok(())
    }))
}

/// Requests real owner cancellation for a bounded prefix after the supplied
/// native admission watermark. Partial progress is retained on control pressure.
/// Does not synthesize terminal events or alter producer-global diagnostics.
/// # Safety
/// Live test handle and initialized, writable, disjoint cursor for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_cancel_since(raw: *mut KrProducer, cursor: *mut u64) -> i32 {
    code(run(|| {
        // SAFETY: caller retains the live test handle throughout this call.
        let producer = unsafe { producer(raw) }?;
        memory::check(cursor, 1)?;
        // SAFETY: validated alignment and caller's initialized in/out contract.
        let mut next = unsafe { cursor.read() };
        let accepted = producer.client.status().map_err(client_error)?.accepted.0;
        if next > accepted {
            return Err(KR_ERR_INVALID);
        }
        let end = accepted.min(next.saturating_add(256));
        while next < end {
            producer
                .client
                .cancel(RecordToken(next + 1))
                .map_err(client_error)?;
            next += 1;
            // SAFETY: cursor is exclusively writable and remains live until return.
            unsafe { cursor.write(next) };
        }
        Ok(())
    }))
}

/// # Safety
/// Called within the lifetime of the ordinary downcall, after its diagnostic
/// result has been recorded. Invalid ordinary arguments must still return normally.
pub(in crate::abi) unsafe fn after_call(raw: *mut KrProducer, operation: u32, accepted: u32) {
    // SAFETY: ordinary ABI caller retains its handle through return.
    let Ok(producer) = (unsafe { producer(raw) }) else {
        return;
    };
    let (flags, before) = {
        let mut state = producer
            .test_calls
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.operation != operation {
            return;
        }
        state.operation = 0;
        state.phase |= ENTERED;
        producer.test_calls.changed.notify_all();
        (state.flags, state.before)
    };
    let mut progressed = true;
    if accepted != 0 && operation == 1 && flags & CANCEL != 0 {
        if let Ok(status) = producer.client.status() {
            let last = status.accepted.0;
            if let Some(first) = last.checked_sub(u64::from(accepted - 1)) {
                for token in first..=last {
                    progressed &= producer.client.cancel(RecordToken(token)).is_ok();
                }
            } else {
                progressed = false;
            }
        } else {
            progressed = false;
        }
    }
    let mut published = false;
    if progressed && accepted != 0 && flags & WAIT_PUBLICATION != 0 {
        published = producer.client.test_wait_publication(
            if operation == 1 { 1 } else { 3 },
            before.saturating_add(u64::from(accepted)),
            WAIT,
        );
        progressed = published;
    }
    let mut state = producer
        .test_calls
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if published {
        state.phase |= PUBLISHED;
    }
    if !progressed {
        state.phase |= TIMED_OUT;
    }
    producer.test_calls.changed.notify_all();
    if flags & PAUSE != 0 {
        let (next, result) = producer
            .test_calls
            .changed
            .wait_timeout_while(state, WAIT, |state| state.phase & RELEASED == 0)
            .unwrap_or_else(|error| error.into_inner());
        state = next;
        if result.timed_out() && state.phase & RELEASED == 0 {
            state.phase |= TIMED_OUT;
        }
    }
    state.phase |= FINISHED;
    producer.test_calls.changed.notify_all();
}

#[cfg(test)]
mod tests;
