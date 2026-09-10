//! The seed-sweep watchdog turns a hung seed into a named, reproducible
//! failure.
//!
//! A liveness bug surfaces as a sweep that never returns. Without a watchdog
//! the test harness reports only that the binary was killed, losing the one
//! piece of information needed to investigate: which seed hung. These tests
//! drive the real hang path in a subprocess, because reporting it necessarily
//! ends the process.

use std::process::Command;
use std::time::Duration;

/// Set in the helper subprocess so the hanging test only runs there.
const HANG_HELPER_ENV: &str = "KR_RUNTIME_SEED_WATCHDOG_HELPER";

#[test]
fn a_hung_seed_is_reported_with_its_repro_command_instead_of_hanging_forever() {
    let output = Command::new(std::env::current_exe().expect("locate the watchdog test binary"))
        .args([
            "--ignored",
            "--exact",
            "seed_watchdog_hang_helper",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(HANG_HELPER_ENV, "1")
        .env("KR_RUNTIME_SEED_TIMEOUT_SECS", "1")
        // Start at a seed that is not zero so the report cannot pass by
        // printing a default.
        .env("KR_RUNTIME_SEED", "37")
        .output()
        .expect("run the hang helper process");

    assert!(
        !output.status.success(),
        "a hung sweep must not report success",
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("seed sweep hung at seed 37"),
        "the watchdog must name the hung seed, got:\n{stderr}",
    );
    assert!(
        stderr.contains("KR_RUNTIME_SEED=37 cargo test -p kr-runtime"),
        "the watchdog must print a repro command, got:\n{stderr}",
    );
    assert!(
        stderr.contains("KR_RUNTIME_SEED_TIMEOUT_SECS=0"),
        "the watchdog must say how to disable itself, got:\n{stderr}",
    );
}

#[test]
fn a_healthy_sweep_is_untouched_by_the_watchdog() {
    let mut observed = Vec::new();
    kr_runtime::seed_sweep!(4, |seed| observed.push(seed));
    assert_eq!(observed, vec![0, 1, 2, 3]);
}

#[test]
#[ignore = "helper process for the watchdog test; hangs by design"]
fn seed_watchdog_hang_helper() {
    if std::env::var_os(HANG_HELPER_ENV).is_none() {
        return;
    }
    kr_runtime::seed_sweep!(1, |_seed| {
        loop {
            // A real liveness bug spins or blocks forever; sleeping models that
            // without burning a core while the watchdog observes it.
            std::thread::sleep(Duration::from_millis(50));
        }
    });
}
