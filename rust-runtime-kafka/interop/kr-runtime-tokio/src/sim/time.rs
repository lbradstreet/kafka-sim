//! tokio::time-shaped virtual time over the ambient kr-runtime runtime.
//!
//! Every operation resolves the owning runtime through
//! [`RuntimeHandle::current`], so it must run inside a kr-runtime task (including
//! either runtime's `block_on` root). tokio's time API has no error channel;
//! conditions kr-runtime reports as typed [`kr_runtime::TimeError`]s surface here as
//! panics carrying the underlying error.

use kr_runtime::{RuntimeDuration, RuntimeHandle, RuntimeInstant};
use std::fmt;
use std::future::Future;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::pin::Pin;
use std::task::{Context, Poll};

pub use std::time::Duration;

fn current_handle(operation: &str) -> RuntimeHandle {
    RuntimeHandle::current().unwrap_or_else(|| {
        panic!("{operation} requires an ambient kr-runtime runtime: call it from inside a kr-runtime task")
    })
}

/// Saturates a std duration onto kr-runtime's `u64`-nanosecond timeline.
///
/// The cap is ~584 years; like tokio, absurdly distant deadlines behave as
/// "far future" rather than failing.
fn saturating_runtime_duration(duration: Duration) -> RuntimeDuration {
    u64::try_from(duration.as_nanos()).map_or(RuntimeDuration::MAX, RuntimeDuration::from_nanos)
}

fn saturating_deadline(handle: &RuntimeHandle, duration: Duration) -> RuntimeInstant {
    handle
        .now()
        .checked_add(saturating_runtime_duration(duration))
        .unwrap_or(RuntimeInstant::MAX)
}

/// A measurement of the owning runtime's timeline: virtual time under
/// simulation, monotonic elapsed time on a host runtime.
///
/// This is the facade's counterpart of `tokio::time::Instant`. It carries the
/// same numeric coordinate as [`RuntimeInstant`] and converts losslessly in
/// both directions for interop with kr-runtime-native code.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Instant(RuntimeInstant);

impl Instant {
    /// Returns the current instant on the ambient runtime's timeline.
    ///
    /// # Panics
    ///
    /// Panics when called outside a kr-runtime task.
    #[must_use]
    pub fn now() -> Self {
        Self(current_handle("Instant::now").now())
    }

    /// Wraps a kr-runtime instant, preserving its numeric coordinate.
    #[must_use]
    pub const fn from_runtime(instant: RuntimeInstant) -> Self {
        Self(instant)
    }

    /// Returns the wrapped kr-runtime instant.
    #[must_use]
    pub const fn into_runtime(self) -> RuntimeInstant {
        self.0
    }

    /// Returns the duration since `earlier`, saturating to zero.
    #[must_use]
    pub fn duration_since(&self, earlier: Self) -> Duration {
        self.saturating_duration_since(earlier)
    }

    /// Returns the duration since `earlier`, saturating to zero.
    #[must_use]
    pub fn saturating_duration_since(&self, earlier: Self) -> Duration {
        self.checked_duration_since(earlier)
            .unwrap_or(Duration::ZERO)
    }

    /// Returns the duration since `earlier`, or `None` if `earlier` is later.
    #[must_use]
    pub fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
        self.0
            .checked_duration_since(earlier.0)
            .map(|duration| Duration::from_nanos(duration.as_nanos()))
    }

    /// Returns how much runtime time elapsed since this instant.
    ///
    /// # Panics
    ///
    /// Panics when called outside a kr-runtime task.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        Self::now().saturating_duration_since(*self)
    }

    /// Adds a duration, or returns `None` on timeline overflow.
    #[must_use]
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        let duration = u64::try_from(duration.as_nanos()).ok()?;
        self.0
            .checked_add(RuntimeDuration::from_nanos(duration))
            .map(Self)
    }

    /// Subtracts a duration, or returns `None` when it precedes the epoch.
    #[must_use]
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        let duration = u64::try_from(duration.as_nanos()).ok()?;
        self.0
            .as_nanos()
            .checked_sub(duration)
            .map(|nanos| Self(RuntimeInstant::from_nanos(nanos)))
    }
}

impl Add<Duration> for Instant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self {
        self.checked_add(duration)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, duration: Duration) {
        *self = *self + duration;
    }
}

impl Sub<Duration> for Instant {
    type Output = Self;

    fn sub(self, duration: Duration) -> Self {
        self.checked_sub(duration)
            .expect("overflow when subtracting duration from instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, duration: Duration) {
        *self = *self - duration;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, earlier: Self) -> Duration {
        self.saturating_duration_since(earlier)
    }
}

/// Sleeps for `duration` on the ambient runtime's timeline.
///
/// # Panics
///
/// Panics when called outside a kr-runtime task.
#[must_use]
pub fn sleep(duration: Duration) -> Sleep {
    let handle = current_handle("time::sleep");
    let deadline = saturating_deadline(&handle, duration);
    Sleep {
        inner: handle.sleep_until(deadline),
        deadline: Instant(deadline),
    }
}

/// Sleeps until `deadline` on the ambient runtime's timeline.
///
/// # Panics
///
/// Panics when called outside a kr-runtime task.
#[must_use]
pub fn sleep_until(deadline: Instant) -> Sleep {
    let handle = current_handle("time::sleep_until");
    Sleep {
        inner: handle.sleep_until(deadline.0),
        deadline,
    }
}

/// The future returned by [`sleep`] and [`sleep_until`].
///
/// Unlike tokio's `Sleep` this future is `Unpin`, which is strictly more
/// permissive: tokio-compatible callers already pin it.
///
/// # Panics
///
/// Polling panics when the underlying kr-runtime timer reports a typed error —
/// registration capacity, identifier exhaustion, polling from a task owned by
/// another runtime, or runtime shutdown — because tokio's `Sleep` resolves to
/// `()` and has no error channel.
pub struct Sleep {
    inner: kr_runtime::Sleep,
    deadline: Instant,
}

impl Sleep {
    /// Returns the absolute deadline this sleep resolves at.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(context) {
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(error)) => {
                panic!(
                    "kr-runtime-tokio sleep failed: {error}; tokio's time API has no error channel"
                )
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The error returned by [`timeout`] when the deadline elapses first.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Elapsed(());

impl fmt::Display for Elapsed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Requires `future` to complete within `duration` of runtime time.
///
/// The inner future is polled before the deadline is checked, so completion
/// wins a same-instant tie, matching tokio.
///
/// # Panics
///
/// Panics when called outside a kr-runtime task.
#[must_use]
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future: Box::pin(future),
        sleep: sleep(duration),
    }
}

/// Requires `future` to complete before `deadline` on the runtime timeline.
///
/// # Panics
///
/// Panics when called outside a kr-runtime task.
#[must_use]
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    Timeout {
        future: Box::pin(future),
        sleep: sleep_until(deadline),
    }
}

/// The future returned by [`timeout`] and [`timeout_at`].
///
/// The inner future is boxed because this crate forbids `unsafe` and
/// therefore does not hand-roll structural pinning.
pub struct Timeout<F: Future> {
    future: Pin<Box<F>>,
    sleep: Sleep,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(output) = self.future.as_mut().poll(context) {
            return Poll::Ready(Ok(output));
        }
        match Pin::new(&mut self.sleep).poll(context) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed(()))),
            Poll::Pending => Poll::Pending,
        }
    }
}
