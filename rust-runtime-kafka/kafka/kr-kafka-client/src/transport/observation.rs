//! Passive request boundaries, using the timestamp supplied to `poll_event`.
use super::{OwnedSendPlan, RetireReason};
use kr_runtime::{CompletionCertainty, RuntimeInstant};

/// A transition for one correlation's current request attempt on this driver.
/// Connection and globally unique request identities belong to the observer.
#[derive(Debug)]
pub enum RequestObservation<'a> {
    /// Immediately before the first warm write submission. The provider may
    /// still return NotApplied. The complete immutable plan is borrowed only
    /// during this callback; individual partial operations are not attempts.
    Dispatched {
        correlation: i32,
        plan: &'a OwnedSendPlan,
    },
    /// Every partial write completed successfully and the full plan is confirmed.
    WriteCompleted { correlation: i32 },
    /// A matching full response was presented, or cooperative retirement returned
    /// this request's last ownership. Unsent enqueued requests are distinguished
    /// by `dispatched == false` and must not be counted as send attempts.
    Finished {
        correlation: i32,
        dispatched: bool,
        confirmed: usize,
        certainty: CompletionCertainty,
        result: RequestFinish<'a>,
    },
}

#[derive(Debug)]
pub enum RequestFinish<'a> {
    Response,
    Retired(&'a RetireReason),
}

/// Diagnostics only: implementations must not panic, wake tasks, read clocks,
/// consume randomness, submit I/O, or alter producer state. Bounded sinks should
/// retain an overflow diagnostic rather than affect the transport's decisions.
/// The callback may copy observation data; no allocation occurs when absent.
pub trait RequestObserver: Send + Sync {
    fn observe(&self, now: RuntimeInstant, transition: RequestObservation<'_>);
}
