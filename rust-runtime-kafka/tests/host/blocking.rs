//! The host runtime's blocking capability: lazy provisioning, capability-
//! following lifetime, drain-before-stop teardown, and per-job panic
//! containment on the shared fleet.

use kr_runtime::{HostConfig, HostConfigError, HostRuntime};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

fn config(blocking_workers: usize) -> HostConfig {
    HostConfig {
        blocking_workers,
        ..HostConfig::default()
    }
}

#[test]
fn zero_blocking_workers_is_rejected_at_construction() {
    assert!(matches!(
        HostRuntime::new(config(0)),
        Err(HostConfigError::ZeroBlockingWorkers)
    ));
}

#[test]
fn queued_jobs_drain_before_workers_stop_and_the_fleet_outlives_the_runtime() {
    let runtime = HostRuntime::new(config(1)).expect("create runtime");
    let blocking = runtime.blocking().expect("provision blocking workers");
    // The workers follow the capability's clones, not the runtime: with the
    // runtime gone, submission stays infallible and teardown still drains.
    drop(runtime);
    let completed = Arc::new(AtomicUsize::new(0));
    for _ in 0..16 {
        let counter = Arc::clone(&completed);
        blocking.submit(move || {
            counter.fetch_add(1, Ordering::AcqRel);
        });
    }
    // Dropping the last clone queues the stop sentinels behind every
    // admitted job and joins the fleet.
    drop(blocking);
    assert_eq!(completed.load(Ordering::Acquire), 16);
}

#[test]
fn a_panicking_job_is_contained_and_the_fleet_keeps_serving() {
    let runtime = HostRuntime::new(config(1)).expect("create runtime");
    let blocking = runtime.blocking().expect("provision blocking workers");
    // With one worker, every later job completing proves the panicking job
    // did not take a worker down with it.
    let (entered, observe) = mpsc::channel();
    blocking.submit(move || {
        entered.send(()).expect("report the panicking job started");
        panic!("injected blocking job panic");
    });
    observe.recv().expect("panicking job ran");
    let completed = Arc::new(AtomicUsize::new(0));
    for _ in 0..4 {
        let counter = Arc::clone(&completed);
        blocking.submit(move || {
            counter.fetch_add(1, Ordering::AcqRel);
        });
    }
    drop(runtime);
    drop(blocking);
    assert_eq!(completed.load(Ordering::Acquire), 4);
}

#[test]
fn the_capability_submits_from_foreign_threads() {
    let runtime = HostRuntime::new(config(2)).expect("create runtime");
    let blocking = runtime.blocking().expect("provision blocking workers");
    let completed = Arc::new(AtomicUsize::new(0));
    let submitters: Vec<_> = (0..4)
        .map(|_| {
            let capability = blocking.clone();
            let counter = Arc::clone(&completed);
            thread::spawn(move || {
                for _ in 0..8 {
                    let counter = Arc::clone(&counter);
                    capability.submit(move || {
                        counter.fetch_add(1, Ordering::AcqRel);
                    });
                }
            })
        })
        .collect();
    for submitter in submitters {
        submitter.join().expect("submitter thread completes");
    }
    drop(runtime);
    drop(blocking);
    assert_eq!(completed.load(Ordering::Acquire), 32);
}
