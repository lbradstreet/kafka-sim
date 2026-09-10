use kr_kafka_host::{SecurityError, control::ControlJobs};
use kr_runtime::{HostConfig, HostRuntime};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
};

#[test]
fn pending_abandonment_retains_control_admission_until_worker_terminal() {
    let runtime = HostRuntime::new(HostConfig {
        blocking_workers: 1,
        ..HostConfig::default()
    })
    .unwrap();
    let blocking = runtime.blocking().unwrap();
    let control = ControlJobs::new(blocking.clone(), 1, 64).unwrap();
    let (release, waiting) = mpsc::channel();
    let owner_budget = Arc::new(());
    let mut response = control
        .submit_guarded(64, owner_budget.clone(), move || {
            waiting.recv().unwrap();
            Ok(17)
        })
        .unwrap();
    assert_eq!(
        Pin::new(&mut response).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    );
    drop(response);
    assert_eq!(Arc::strong_count(&owner_budget), 2);
    assert_eq!(control.usage(), (1, 64));
    assert!(matches!(
        control.submit(1, || Ok(())),
        Err(SecurityError::ResourceExhausted {
            resource: "control jobs",
            limit: 1
        })
    ));
    release.send(()).unwrap();
    let (finished, observed) = mpsc::channel();
    blocking.submit(move || finished.send(()).unwrap());
    observed.recv().unwrap();
    assert_eq!(Arc::strong_count(&owner_budget), 1);
    assert_eq!(control.usage(), (0, 0));
    drop(control);
    drop(blocking);
    runtime.finish().unwrap();
}

#[test]
fn unconsumed_terminal_result_retains_control_bytes() {
    let mut runtime = HostRuntime::new(HostConfig {
        blocking_workers: 1,
        ..HostConfig::default()
    })
    .unwrap();
    let blocking = runtime.blocking().unwrap();
    let control = ControlJobs::new(blocking.clone(), 2, 64).unwrap();
    let owner_budget = Arc::new(());
    let response = control
        .submit_guarded(64, owner_budget.clone(), || Ok(vec![0x5a; 64]))
        .unwrap();
    let (finished, observed) = mpsc::channel();
    blocking.submit(move || finished.send(()).unwrap());
    observed.recv().unwrap();
    assert_eq!(control.usage(), (1, 64));
    let ran = Arc::new(AtomicBool::new(false));
    let job_ran = ran.clone();
    assert!(matches!(
        control.submit(1, move || {
            job_ran.store(true, Ordering::SeqCst);
            Ok(())
        }),
        Err(SecurityError::ResourceExhausted {
            resource: "control bytes",
            limit: 64
        })
    ));
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(Arc::strong_count(&owner_budget), 2);
    assert_eq!(runtime.block_on(response).unwrap().unwrap(), vec![0x5a; 64]);
    assert_eq!(Arc::strong_count(&owner_budget), 1);
    assert_eq!(control.usage(), (0, 0));
    drop(control);
    drop(blocking);
    runtime.finish().unwrap();
}

#[test]
fn control_worker_panic_is_terminal_and_restores_both_credits() {
    let mut runtime = HostRuntime::new(HostConfig::default()).unwrap();
    let control = ControlJobs::new(runtime.blocking().unwrap(), 1, 64).unwrap();
    let response = control
        .submit::<()>(32, || panic!("injected proof panic"))
        .unwrap();
    assert_eq!(
        runtime.block_on(response).unwrap(),
        Err(SecurityError::WorkerPanicked)
    );
    assert_eq!(control.usage(), (0, 0));
    let response = control.submit(64, || Ok(42)).unwrap();
    assert_eq!(runtime.block_on(response).unwrap(), Ok(42));
    drop(control);
    runtime.finish().unwrap();
}

#[test]
fn control_panic_payload_destructor_is_contained_before_terminal_publication() {
    struct Payload(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Payload {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
            panic!("control panic payload destructor");
        }
    }
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let payload = Payload(dropped.clone());
    let mut runtime = HostRuntime::new(HostConfig::default()).unwrap();
    let control = ControlJobs::new(runtime.blocking().unwrap(), 1, 64).unwrap();
    let response = control
        .submit::<()>(64, move || std::panic::panic_any(payload))
        .unwrap();
    assert_eq!(
        runtime.block_on(response).unwrap(),
        Err(SecurityError::WorkerPanicked)
    );
    assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(control.usage(), (0, 0));
    let next = control.submit(64, || Ok(42)).unwrap();
    assert_eq!(runtime.block_on(next).unwrap(), Ok(42));
    drop(control);
    runtime.finish().unwrap();
}
