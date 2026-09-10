use std::fmt;

/// A duration on the runtime's integer nanosecond timeline.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeDuration(u64);

impl RuntimeDuration {
    /// A zero-length duration.
    pub const ZERO: Self = Self(0);

    /// The largest representable duration.
    pub const MAX: Self = Self(u64::MAX);

    /// Constructs a duration from nanoseconds.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Constructs a duration from microseconds, returning `None` on overflow.
    #[must_use]
    pub const fn from_micros(micros: u64) -> Option<Self> {
        match micros.checked_mul(1_000) {
            Some(nanos) => Some(Self(nanos)),
            None => None,
        }
    }

    /// Constructs a duration from milliseconds, returning `None` on overflow.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Option<Self> {
        match millis.checked_mul(1_000_000) {
            Some(nanos) => Some(Self(nanos)),
            None => None,
        }
    }

    /// Constructs a duration from seconds, returning `None` on overflow.
    #[must_use]
    pub const fn from_secs(seconds: u64) -> Option<Self> {
        match seconds.checked_mul(1_000_000_000) {
            Some(nanos) => Some(Self(nanos)),
            None => None,
        }
    }

    /// Returns the duration as nanoseconds.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Adds two durations, returning `None` on overflow.
    #[must_use]
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.0.checked_add(other.0) {
            Some(nanos) => Some(Self(nanos)),
            None => None,
        }
    }
}

impl fmt::Display for RuntimeDuration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}ns", self.0)
    }
}

/// A numeric coordinate on a runtime-relative, integer nanosecond timeline.
///
/// `RuntimeInstant` carries no runtime identity. Host execution begins at zero
/// and measures monotonic elapsed time since runtime construction; simulation
/// begins at its configured start time (zero by default) and advances the
/// coordinate virtually. Passing an instant between
/// runtimes deliberately preserves only its numeric coordinate. Timer futures
/// retain their originating runtime separately and reject polling from another
/// runtime.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeInstant(u64);

impl RuntimeInstant {
    /// The beginning of runtime-relative time.
    pub const ZERO: Self = Self(0);

    /// The largest representable instant.
    pub const MAX: Self = Self(u64::MAX);

    /// Constructs an instant from nanoseconds since the runtime's epoch.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Returns nanoseconds since the runtime's epoch.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Adds a duration, returning `None` on overflow.
    #[must_use]
    pub const fn checked_add(self, duration: RuntimeDuration) -> Option<Self> {
        match self.0.checked_add(duration.as_nanos()) {
            Some(nanos) => Some(Self(nanos)),
            None => None,
        }
    }

    /// Returns the elapsed duration since `earlier`, or `None` when it is later.
    #[must_use]
    pub const fn checked_duration_since(self, earlier: Self) -> Option<RuntimeDuration> {
        match self.0.checked_sub(earlier.0) {
            Some(nanos) => Some(RuntimeDuration::from_nanos(nanos)),
            None => None,
        }
    }
}

impl fmt::Display for RuntimeInstant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}ns", self.0)
    }
}

/// Simulation-oriented name for [`RuntimeDuration`].
///
/// This is an exact type alias; no conversion is required when sharing a
/// duration between simulation and host runtime APIs.
pub type SimDuration = RuntimeDuration;

/// Simulation-oriented name for [`RuntimeInstant`].
///
/// This is an exact type alias; no conversion is required when sharing an
/// instant between simulation and host runtime APIs.
pub type SimInstant = RuntimeInstant;

/// An error produced by a runtime timer future.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TimeError {
    /// Adding the requested duration to the current instant overflowed.
    DeadlineOverflow,
    /// The runtime's configured live-timer limit was reached.
    ResourceExhausted {
        /// The exhausted resource; always live timers for this error.
        resource: &'static str,
        /// The configured bound that was reached.
        limit: usize,
    },
    /// The runtime exhausted its timer identifier space.
    TimerIdentifierExhausted,
    /// The runtime exhausted its timer registration-order sequence space.
    TimerRegistrationSequenceExhausted,
    /// An active timer future was polled outside its originating runtime.
    WrongRuntime,
    /// The runtime was stopped before the timer became eligible to complete.
    RuntimeStopped,
}

impl fmt::Display for TimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineOverflow => formatter.write_str("runtime deadline overflowed"),
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} limit of {limit} is exhausted")
            }
            Self::TimerIdentifierExhausted => {
                formatter.write_str("timer identifier space exhausted")
            }
            Self::TimerRegistrationSequenceExhausted => {
                formatter.write_str("timer registration sequence space exhausted")
            }
            Self::WrongRuntime => formatter.write_str("timer was polled on the wrong runtime"),
            Self::RuntimeStopped => formatter.write_str("runtime is stopped"),
        }
    }
}

impl std::error::Error for TimeError {}
