//! Multi-seed sweeps for ordinary randomized tests.
//!
//! Quarry's campaign harness sweeps seeds with coverage gating, but that
//! rigor should not be the only place a randomized test runs more than one
//! seed. [`run_seed_sweep`] (usually via the [`seed_sweep!`](macro@crate::seed_sweep) macro) gives any
//! unit or integration test the campaign's core reproduction conventions for
//! the price of one call: a default seed range, environment overrides, and a
//! failure report that names the seed and prints a copy-pasteable repro
//! command.
//!
//! Environment contract:
//!
//! - `KR_RUNTIME_SEED=<u64>` runs exactly that seed, ignoring the sweep width.
//!   This is the reproduction mode the failure report prints.
//! - `KR_RUNTIME_SEEDS=<u64>` overrides how many seeds the sweep runs.
//! - Otherwise the sweep runs seeds `0..default_seed_count`.
//! - `KR_RUNTIME_SEED_TIMEOUT_SECS=<u64>` overrides the per-seed watchdog budget;
//!   `0` disables it, which is what an interactive debugging session wants.
//!
//! A sweep of zero seeds is rejected rather than silently passing: a test
//! that exercises nothing must fail, not succeed.
//!
//! A seed that hangs is reported rather than left to the harness. A liveness
//! bug does not fail a seed, it stops returning, and a test-harness timeout
//! reports only that the binary was killed — losing the seed exactly when it
//! is most needed. Each seed therefore runs under a wall-clock watchdog that
//! prints the same seed and repro command a failure would, then aborts. It is
//! host-side test scaffolding and observes only wall time, so it cannot
//! perturb a simulated run.
//!
//! ```
//! use kr_runtime::{RuntimeConfig, SimRuntime};
//!
//! kr_runtime::seed_sweep!(8, |seed| {
//!     let config = RuntimeConfig {
//!         seed,
//!         start_time: RuntimeConfig::derived_start_time(seed),
//!         ..RuntimeConfig::default()
//!     };
//!     let checkpoint = |config: &RuntimeConfig| {
//!         let mut runtime = SimRuntime::new(config.clone());
//!         runtime.block_on(async {}).expect("root completes");
//!         runtime.snapshot().determinism_checkpoint()
//!     };
//!     assert_eq!(checkpoint(&config), checkpoint(&config));
//! });
//! ```

use std::io::Write as _;
use std::ops::{Range, RangeInclusive};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Environment variable that pins a sweep to one exact seed.
pub const SEED_ENV: &str = "KR_RUNTIME_SEED";

/// Environment variable that overrides how many seeds a sweep runs.
pub const SEED_COUNT_ENV: &str = "KR_RUNTIME_SEEDS";

/// Environment variable that overrides the per-seed watchdog timeout.
///
/// The value is in whole seconds; `0` disables the watchdog, which is what a
/// debugging session wants.
pub const SEED_TIMEOUT_ENV: &str = "KR_RUNTIME_SEED_TIMEOUT_SECS";

/// Wall-clock budget one seed may take before the watchdog reports a hang.
///
/// This is deliberately far above any healthy seed. It exists to convert a
/// silent hang into a named, reproducible failure, not to police performance.
pub const DEFAULT_SEED_TIMEOUT: Duration = Duration::from_secs(60);

/// The seeds one sweep invocation will run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SeedPlan {
    /// Exactly one seed, pinned by [`SEED_ENV`].
    Single(u64),
    /// Seeds `0..count`.
    Sweep(u64),
}

impl SeedPlan {
    fn seeds(self) -> RangeInclusive<u64> {
        match self {
            Self::Single(seed) => seed..=seed,
            Self::Sweep(count) => 0..=count - 1,
        }
    }
}

/// Resolves the sweep plan from the two environment values.
///
/// [`SEED_ENV`] takes precedence over [`SEED_COUNT_ENV`]; both must parse as
/// `u64` when present, and the resulting sweep must run at least one seed.
fn resolve_seed_plan(
    single: Option<&str>,
    count: Option<&str>,
    default_seed_count: u64,
) -> Result<SeedPlan, String> {
    if let Some(value) = single {
        let seed = value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{SEED_ENV} value {value:?} is not a u64 seed"))?;
        return Ok(SeedPlan::Single(seed));
    }
    let count = match count {
        Some(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{SEED_COUNT_ENV} value {value:?} is not a u64 seed count"))?,
        None => default_seed_count,
    };
    if count == 0 {
        return Err("a sweep of zero seeds exercises nothing and must not pass".to_owned());
    }
    Ok(SeedPlan::Sweep(count))
}

/// Reads the sharded seed range for one coverage campaign.
///
/// Campaign suites derive their seed range from two environment values named
/// after `env_prefix`:
///
/// - `<PREFIX>_SEED_OFFSET` shifts the range start (default 0).
/// - `<PREFIX>_SEED_MULTIPLIER` scales the seed count (default 1).
///
/// The returned range is `offset..offset + default_seed_count * multiplier`,
/// so parallel shards can partition disjoint seed ranges and a long soak can
/// widen the same campaign without editing the test.
///
/// # Panics
///
/// Panics when either value is present but not a decimal `u64`, when the
/// multiplier is zero (a campaign of zero seeds exercises nothing), or when
/// the scaled range overflows `u64`.
#[must_use]
pub fn campaign_seed_range(env_prefix: &str, default_seed_count: u64) -> Range<u64> {
    let offset_var = format!("{env_prefix}_SEED_OFFSET");
    let multiplier_var = format!("{env_prefix}_SEED_MULTIPLIER");
    resolve_campaign_seed_range(
        &offset_var,
        std::env::var(&offset_var).ok().as_deref(),
        &multiplier_var,
        std::env::var(&multiplier_var).ok().as_deref(),
        default_seed_count,
    )
    .unwrap_or_else(|message| panic!("invalid campaign seed range: {message}"))
}

/// Resolves a campaign seed range from the two environment values.
fn resolve_campaign_seed_range(
    offset_var: &str,
    offset: Option<&str>,
    multiplier_var: &str,
    multiplier: Option<&str>,
    default_seed_count: u64,
) -> Result<Range<u64>, String> {
    let offset = parse_env_u64(offset_var, offset, 0)?;
    let multiplier = parse_env_u64(multiplier_var, multiplier, 1)?;
    if multiplier == 0 {
        return Err(format!("{multiplier_var} must be nonzero"));
    }
    let count = default_seed_count
        .checked_mul(multiplier)
        .ok_or_else(|| format!("{multiplier_var} value {multiplier} overflows the seed count"))?;
    let end = offset
        .checked_add(count)
        .ok_or_else(|| format!("{offset_var} value {offset} overflows the seed range"))?;
    Ok(offset..end)
}

fn parse_env_u64(name: &str, value: Option<&str>, default: u64) -> Result<u64, String> {
    match value {
        Some(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{name} value {value:?} is not a decimal u64")),
        None => Ok(default),
    }
}

/// Runs `case` once per sweep seed with the standard reproduction report.
///
/// Prefer the [`seed_sweep!`](macro@crate::seed_sweep) macro, which fills in `package` from the
/// calling crate so the printed repro command is correct.
///
/// The case runs under `catch_unwind` purely so a failing seed can be
/// reported before the original panic is resumed unchanged; sweeps stop at
/// the first failing seed. The repro command names the current test thread,
/// which is the test's full path under the default harness; running with
/// `--test-threads=1` (where tests share the unnamed main thread) degrades
/// the command to a placeholder filter.
///
/// Each seed also runs under a wall-clock watchdog. See the module
/// documentation for its environment contract.
///
/// # Panics
///
/// Panics if the environment overrides do not parse, if the resolved sweep
/// would run zero seeds, or by resuming the first failing case's panic.
///
/// # Aborts
///
/// Aborts the process when one seed exceeds the watchdog budget, after
/// printing that seed and its repro command to stderr. Unwinding is not an
/// option there: the hung case owns the test thread and is not returning.
pub fn run_seed_sweep<F>(package: &str, default_seed_count: u64, mut case: F)
where
    F: FnMut(u64),
{
    let plan = resolve_seed_plan(
        std::env::var(SEED_ENV).ok().as_deref(),
        std::env::var(SEED_COUNT_ENV).ok().as_deref(),
        default_seed_count,
    )
    .unwrap_or_else(|message| panic!("invalid seed sweep: {message}"));
    let timeout = resolve_seed_timeout(std::env::var(SEED_TIMEOUT_ENV).ok().as_deref())
        .unwrap_or_else(|message| panic!("invalid seed sweep: {message}"));
    sweep(package, plan, timeout, &mut case);
}

/// Resolves the per-seed watchdog budget, where `Ok(None)` disables it.
fn resolve_seed_timeout(value: Option<&str>) -> Result<Option<Duration>, String> {
    let Some(value) = value else {
        return Ok(Some(DEFAULT_SEED_TIMEOUT));
    };
    let seconds = value.trim().parse::<u64>().map_err(|_| {
        format!("{SEED_TIMEOUT_ENV} value {value:?} is not a whole number of seconds")
    })?;
    if seconds == 0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_secs(seconds)))
}

fn sweep<F>(package: &str, plan: SeedPlan, timeout: Option<Duration>, case: &mut F)
where
    F: FnMut(u64),
{
    let watchdog = timeout.map(|timeout| Watchdog::start(package.to_owned(), timeout));
    for seed in plan.seeds() {
        if let Some(watchdog) = &watchdog {
            watchdog.arm(seed);
        }
        let outcome = catch_unwind(AssertUnwindSafe(|| case(seed)));
        if let Some(watchdog) = &watchdog {
            watchdog.disarm();
        }
        if let Err(payload) = outcome {
            eprintln!("{}", failure_report(package, seed));
            resume_unwind(payload);
        }
    }
}

/// Returns the running test's name, or a placeholder when it has none.
///
/// Under the default harness a test thread is named for its full path. Running
/// with `--test-threads=1` shares the unnamed main thread, which degrades the
/// repro command to a filter placeholder.
fn test_name() -> String {
    let current = thread::current();
    match current.name() {
        Some(name) if name != "main" => name.to_owned(),
        _ => "<test-name>".to_owned(),
    }
}

/// Builds the failing-seed report, including a copy-pasteable repro command.
fn failure_report(package: &str, seed: u64) -> String {
    report(package, &test_name(), seed, "failed at")
}

fn report(package: &str, test: &str, seed: u64, what: &str) -> String {
    format!(
        "seed sweep {what} seed {seed}\n\
         reproduce: {SEED_ENV}={seed} cargo test -p {package} {test} -- --exact --nocapture"
    )
}

/// Reports the seed a sweep hung on instead of letting CI time out silently.
///
/// A hung seed is the case where a repro command matters most and is hardest to
/// recover: a liveness bug surfaces as a test that never returns, and the
/// harness's own timeout reports only that the binary was killed, taking the
/// seed with it. The watchdog observes from its own thread because a sweep case
/// owns `!Send` runtime state and cannot be moved off the test thread.
struct Watchdog {
    shared: Arc<(Mutex<WatchdogState>, Condvar)>,
    thread: Option<thread::JoinHandle<()>>,
}

struct WatchdogState {
    /// The running seed and the instant it must finish by.
    running: Option<(u64, Instant)>,
    /// Set when the sweep is over so the watchdog thread can exit.
    finished: bool,
}

impl Watchdog {
    fn start(package: String, timeout: Duration) -> Self {
        let shared = Arc::new((
            Mutex::new(WatchdogState {
                running: None,
                finished: false,
            }),
            Condvar::new(),
        ));
        let test = test_name();
        let watched = Arc::clone(&shared);
        let thread = thread::Builder::new()
            .name("kr-runtime-seed-watchdog".to_owned())
            .spawn(move || watch(&watched, &package, &test, timeout))
            .expect("spawn the seed-sweep watchdog");
        Self {
            shared,
            thread: Some(thread),
        }
    }

    fn arm(&self, seed: u64) {
        let (mutex, condvar) = &*self.shared;
        let mut state = lock(mutex);
        state.running = Some((seed, Instant::now()));
        condvar.notify_all();
    }

    fn disarm(&self) {
        let (mutex, condvar) = &*self.shared;
        let mut state = lock(mutex);
        state.running = None;
        condvar.notify_all();
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        {
            let (mutex, condvar) = &*self.shared;
            let mut state = lock(mutex);
            state.finished = true;
            state.running = None;
            condvar.notify_all();
        }
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

/// Waits for the running seed to exceed its budget, then aborts with a report.
fn watch(shared: &(Mutex<WatchdogState>, Condvar), package: &str, test: &str, timeout: Duration) {
    let (mutex, condvar) = shared;
    let mut state = lock(mutex);
    loop {
        if state.finished {
            return;
        }
        let Some((seed, started)) = state.running else {
            state = condvar
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continue;
        };
        let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
            // The test thread owns state this thread cannot safely unwind, and
            // it is by definition not returning. Abort is the only honest way
            // to end the run, so flush the repro line first.
            let mut stderr = std::io::stderr().lock();
            let _written = writeln!(
                stderr,
                "{}\nseed {seed} exceeded the {} second watchdog budget; set {SEED_TIMEOUT_ENV}=0 to disable",
                report(package, test, seed, "hung at"),
                timeout.as_secs(),
            );
            let _flushed = stderr.flush();
            std::process::abort();
        };
        let (guard, _timed_out) = condvar
            .wait_timeout(state, remaining)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state = guard;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Runs a test case once per sweep seed.
///
/// Expands to [`test_support::run_seed_sweep`](run_seed_sweep) with the
/// calling crate's package name, so the failure report's repro command
/// targets the right crate. See the module documentation for the
/// environment contract.
#[macro_export]
macro_rules! seed_sweep {
    ($default_seed_count:expr, $case:expr) => {
        $crate::test_support::run_seed_sweep(env!("CARGO_PKG_NAME"), $default_seed_count, $case)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn campaign_seed_ranges_default_shift_and_scale() {
        let resolve = |offset: Option<&str>, multiplier: Option<&str>, count| {
            resolve_campaign_seed_range(
                "X_SEED_OFFSET",
                offset,
                "X_SEED_MULTIPLIER",
                multiplier,
                count,
            )
        };
        assert_eq!(resolve(None, None, 16), Ok(0..16));
        assert_eq!(resolve(Some("32"), None, 16), Ok(32..48));
        assert_eq!(resolve(None, Some("3"), 16), Ok(0..48));
        assert_eq!(resolve(Some("8"), Some("2"), 16), Ok(8..40));
    }

    #[test]
    fn campaign_seed_ranges_reject_junk_zero_and_overflow() {
        let resolve = |offset: Option<&str>, multiplier: Option<&str>, count| {
            resolve_campaign_seed_range(
                "X_SEED_OFFSET",
                offset,
                "X_SEED_MULTIPLIER",
                multiplier,
                count,
            )
        };
        assert!(resolve(Some("many"), None, 16).is_err());
        assert!(resolve(None, Some("wide"), 16).is_err());
        assert!(resolve(None, Some("0"), 16).is_err());
        assert!(resolve(None, Some("2"), u64::MAX).is_err());
        assert!(resolve(Some(&u64::MAX.to_string()), None, 16).is_err());
    }

    #[test]
    fn sweeps_default_to_ascending_seeds_from_zero() {
        let plan = resolve_seed_plan(None, None, 4).expect("default plan resolves");
        assert_eq!(plan, SeedPlan::Sweep(4));
        assert_eq!(plan.seeds().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn seed_count_override_replaces_the_default_width() {
        let plan = resolve_seed_plan(None, Some("2"), 64).expect("override resolves");
        assert_eq!(plan.seeds().collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn an_exact_seed_wins_over_the_count_override() {
        let plan = resolve_seed_plan(Some("17"), Some("64"), 4).expect("exact seed resolves");
        assert_eq!(plan.seeds().collect::<Vec<_>>(), vec![17]);
    }

    #[test]
    fn unparseable_and_empty_sweeps_are_rejected() {
        assert!(resolve_seed_plan(Some("seed"), None, 4).is_err());
        assert!(resolve_seed_plan(None, Some("many"), 4).is_err());
        assert!(resolve_seed_plan(None, Some("0"), 4).is_err());
        assert!(resolve_seed_plan(None, None, 0).is_err());
    }

    #[test]
    fn the_largest_seed_and_count_are_representable() {
        let plan =
            resolve_seed_plan(Some(&u64::MAX.to_string()), None, 1).expect("max seed resolves");
        assert_eq!(plan.seeds().collect::<Vec<_>>(), vec![u64::MAX]);
        assert!(resolve_seed_plan(None, Some(&u64::MAX.to_string()), 1).is_ok());
    }

    #[test]
    fn a_sweep_runs_every_planned_seed_in_order() {
        let mut observed = Vec::new();
        sweep("kr-runtime", SeedPlan::Sweep(3), None, &mut |seed| {
            observed.push(seed)
        });
        assert_eq!(observed, vec![0, 1, 2]);
    }

    #[test]
    fn a_failing_seed_stops_the_sweep_and_propagates_its_panic() {
        let mut observed = Vec::new();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            sweep("kr-runtime", SeedPlan::Sweep(5), None, &mut |seed| {
                observed.push(seed);
                assert_ne!(seed, 2, "seed 2 is the planted failure");
            });
        }));
        let payload = outcome.expect_err("the planted failure must propagate");
        let message = payload
            .downcast_ref::<String>()
            .expect("assert_ne panics with a String payload");
        assert!(message.contains("planted failure"));
        assert_eq!(observed, vec![0, 1, 2], "later seeds must not run");
    }

    #[test]
    fn the_watchdog_budget_defaults_and_can_be_overridden_or_disabled() {
        assert_eq!(resolve_seed_timeout(None), Ok(Some(DEFAULT_SEED_TIMEOUT)));
        assert_eq!(
            resolve_seed_timeout(Some("5")),
            Ok(Some(Duration::from_secs(5))),
        );
        assert_eq!(
            resolve_seed_timeout(Some(" 5 ")),
            Ok(Some(Duration::from_secs(5))),
            "surrounding whitespace is tolerated like the other knobs",
        );
        assert_eq!(
            resolve_seed_timeout(Some("0")),
            Ok(None),
            "zero disables the watchdog for a debugging session",
        );
    }

    #[test]
    fn an_unparseable_watchdog_budget_is_rejected() {
        assert!(resolve_seed_timeout(Some("soon")).is_err());
        assert!(resolve_seed_timeout(Some("-1")).is_err());
    }

    #[test]
    fn a_sweep_under_a_watchdog_runs_every_seed_and_stops_the_watchdog() {
        // The watchdog must not fire for healthy seeds, and dropping the sweep
        // must join its thread rather than leaking it.
        let mut observed = Vec::new();
        sweep(
            "kr-runtime",
            SeedPlan::Sweep(3),
            Some(Duration::from_secs(600)),
            &mut |seed| observed.push(seed),
        );
        assert_eq!(observed, vec![0, 1, 2]);
    }

    #[test]
    fn the_hang_report_names_the_seed_and_the_same_repro_command() {
        let hang = report("kr-runtime-io", "some::test", 42, "hung at");
        assert!(hang.contains("seed sweep hung at seed 42"));
        assert!(hang.contains("KR_RUNTIME_SEED=42 cargo test -p kr-runtime-io some::test"));
    }

    #[test]
    fn the_failure_report_names_the_seed_and_a_repro_command() {
        let report = failure_report("kr-runtime-io", 42);
        assert!(report.contains("seed 42"));
        assert!(report.contains("KR_RUNTIME_SEED=42 cargo test -p kr-runtime-io"));
        assert!(report.contains("-- --exact --nocapture"));
        assert!(
            report.contains(thread::current().name().expect("test threads are named")),
            "the repro command filters on the current test name",
        );
    }
}
