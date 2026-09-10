// Each integration test compiles `support` independently; only durability
// tests exercise this helper.
#[allow(dead_code)]
pub(crate) mod fault_ring;

use std::future::Future;
use std::task::{Context, Poll, Waker};

use kr_runtime::{CompletionCertainty, RandomHandle, SimDuration, SimInstant};
use kr_runtime_ring::RingWriter;
use quarry::{
    DurableQueue, DurableQueueError, JobId, LeaseToken, RequestId, SubmitRequest, WorkerId,
};

/// Draws a workload value strictly below `upper`.
///
/// # Panics
///
/// Panics when `upper` is zero; campaign generators always draw from
/// nonzero bounds.
pub(crate) fn below(random: &RandomHandle, upper: u64) -> u64 {
    random
        .random_below(upper)
        .expect("campaign random bounds are nonzero")
}

/// Polls `future` once with a noop waker and requires it to complete.
///
/// # Panics
///
/// Panics when the future returns `Pending`; the durability tests drive only
/// synchronously completing memory-backed operations through this helper.
pub(crate) fn complete<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test future unexpectedly remained pending"),
    }
}

/// Asserts the complete poisoned-queue truth table on `queue`.
///
/// Every state operation must be fenced with `RecoveryRequired`: the
/// ephemeral operations (claim, renew, nack, expire, snapshot) as plain
/// errors, and the durable operations (submit, ack) as `NotApplied`
/// completion errors, proving the rejection happened before any effect.
///
/// # Panics
///
/// Panics when any operation is not fenced exactly as described.
pub(crate) fn assert_every_operation_is_fenced<R: RingWriter>(queue: &mut DurableQueue<R>) {
    let expected = DurableQueueError::RecoveryRequired;
    let token = LeaseToken::from_parts(queue.incarnation(), 1);
    assert_eq!(
        queue.claim(
            WorkerId::new(1),
            1,
            SimDuration::from_nanos(1),
            SimInstant::ZERO,
        ),
        Err(expected.clone())
    );
    assert_eq!(
        queue.renew(
            JobId::new(1),
            token,
            SimDuration::from_nanos(1),
            SimInstant::ZERO,
        ),
        Err(expected.clone())
    );
    assert_eq!(
        queue.nack(JobId::new(1), token, SimDuration::ZERO, SimInstant::ZERO),
        Err(expected.clone())
    );
    assert_eq!(
        queue.expire(JobId::new(1), token, SimInstant::ZERO),
        Err(expected.clone())
    );
    assert_eq!(queue.snapshot(SimInstant::ZERO), Err(expected.clone()));

    let request = SubmitRequest {
        request_id: RequestId::new(99),
        payload: b"fenced".to_vec(),
        not_before: SimInstant::ZERO,
    };
    let submit_error = complete(queue.submit(request, SimInstant::ZERO))
        .expect_err("poisoned queue must reject submit");
    assert_eq!(submit_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(submit_error.error(), &expected);

    let ack_error = complete(queue.ack(JobId::new(1), token, SimInstant::ZERO))
        .expect_err("poisoned queue must reject ack");
    assert_eq!(ack_error.certainty(), CompletionCertainty::NotApplied);
    assert_eq!(ack_error.error(), &expected);
}

/// Result of deterministic, attempt-bounded delta debugging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ShrinkResult<T> {
    pub(crate) minimized: Vec<T>,
    pub(crate) attempts: usize,
    pub(crate) attempt_limit_reached: bool,
}

/// Removes contiguous chunks while `fails` remains true.
///
/// The caller must provide an already-failing input. Invalid candidates are
/// skipped without consuming an execution attempt, and `fails` is invoked at
/// most `max_attempts` times.
pub(crate) fn bounded_ddmin<T: Clone>(
    input: &[T],
    max_attempts: usize,
    mut is_valid: impl FnMut(&[T]) -> bool,
    mut fails: impl FnMut(&[T]) -> bool,
) -> ShrinkResult<T> {
    let mut current = input.to_vec();
    let mut attempts = 0;
    let mut attempt_limit_reached = false;
    let mut granularity = 2;

    'shrink: while current.len() >= 2 {
        let chunk_len = current.len().div_ceil(granularity);
        let mut reduced = false;
        let mut start = 0;
        while start < current.len() {
            let end = (start + chunk_len).min(current.len());
            let mut candidate = Vec::with_capacity(current.len() - (end - start));
            candidate.extend_from_slice(&current[..start]);
            candidate.extend_from_slice(&current[end..]);
            if is_valid(&candidate) {
                if attempts == max_attempts {
                    attempt_limit_reached = true;
                    break 'shrink;
                }
                attempts += 1;
                if fails(&candidate) {
                    current = candidate;
                    granularity = granularity.saturating_sub(1).max(2);
                    reduced = true;
                    break;
                }
            }
            start = end;
        }

        if reduced {
            continue;
        }
        if granularity >= current.len() {
            break;
        }
        granularity = granularity.saturating_mul(2).min(current.len());
    }

    ShrinkResult {
        minimized: current,
        attempts,
        attempt_limit_reached,
    }
}
