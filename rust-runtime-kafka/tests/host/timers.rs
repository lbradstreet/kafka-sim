use super::*;

#[test]
fn host_timer_never_completes_before_its_deadline() {
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let duration = RuntimeDuration::from_millis(2).expect("duration fits");
    let started = Instant::now();

    let result = runtime
        .block_on(handle.sleep(duration))
        .expect("runtime drives timer");

    assert_eq!(result, Ok(()));
    assert!(started.elapsed() >= Duration::from_millis(2));
}

#[test]
fn zero_duration_sleep_completes() {
    let mut runtime = HostRuntime::default();
    let sleep = runtime.handle().sleep(RuntimeDuration::ZERO);
    assert_eq!(
        runtime.block_on(sleep).expect("runtime drives sleep"),
        Ok(())
    );
}

#[test]
fn sleep_created_after_shutdown_is_runtime_stopped() {
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    runtime.shutdown().expect("runtime shuts down");
    let mut sleep = handle.sleep(RuntimeDuration::ZERO);

    assert_eq!(
        poll_once(&mut sleep),
        Poll::Ready(Err(TimeError::RuntimeStopped))
    );
}

#[test]
fn eligible_sleep_created_before_shutdown_remains_successful() {
    let mut runtime = HostRuntime::default();
    let mut sleep = runtime.handle().sleep(RuntimeDuration::ZERO);
    runtime.shutdown().expect("runtime shuts down");

    assert_eq!(poll_once(&mut sleep), Poll::Ready(Ok(())));
}

#[test]
fn equal_deadline_timers_fire_in_registration_order() {
    let mut runtime = HostRuntime::default();
    let handle = runtime.handle();
    let log = Rc::new(RefCell::new(Vec::new()));
    let root_handle = handle.clone();
    let root_log = Rc::clone(&log);

    runtime
        .block_on(async move {
            let deadline: RuntimeInstant = root_handle
                .now()
                .checked_add(RuntimeDuration::from_millis(20).expect("duration fits"))
                .expect("deadline fits");
            let mut joins = Vec::new();
            for label in ["first", "second", "third"] {
                let task_handle = root_handle.clone();
                let task_log = Rc::clone(&root_log);
                joins.push(
                    root_handle
                        .spawn(async move {
                            task_handle
                                .sleep_until(deadline)
                                .await
                                .expect("timer succeeds");
                            task_log.borrow_mut().push(label);
                        })
                        .expect("timer task is admitted"),
                );
            }
            for join in joins {
                join.await.expect("timer task completes");
            }
        })
        .expect("runtime drives equal-deadline timers");

    assert_eq!(&*log.borrow(), &["first", "second", "third"]);
}

#[test]
fn aborting_a_sleeping_task_cancels_its_timer_registration() {
    let mut runtime = HostRuntime::new(HostConfig {
        max_timers: 1,
        ..HostConfig::default()
    })
    .expect("config is valid");
    let handle = runtime.handle();
    let root_handle = handle.clone();

    runtime
        .block_on(async move {
            let task_handle = handle.clone();
            let mut join = handle
                .spawn(async move {
                    task_handle
                        .sleep(RuntimeDuration::from_secs(60).expect("duration fits"))
                        .await
                })
                .expect("sleeping task is admitted");

            yield_now().await;
            join.abort();
            assert_eq!((&mut join).await, Err(JoinError::Cancelled));
            assert_eq!(
                root_handle.sleep(RuntimeDuration::ZERO).await,
                Ok(()),
                "the cancelled timer released the only timer slot"
            );
        })
        .expect("runtime applies sleeping-task abort");
}

#[test]
fn sleep_rejects_both_cross_runtime_directions() {
    let mut host_origin = HostRuntime::default();
    let host_sleep = host_origin.handle().sleep(RuntimeDuration::ZERO);
    let mut sim_driver = SimRuntime::default();

    assert_eq!(
        sim_driver
            .block_on(host_sleep)
            .expect("simulation drives its root"),
        Err(TimeError::WrongRuntime)
    );

    let mut sim_origin = SimRuntime::default();
    let sim_sleep = sim_origin.handle().sleep(SimDuration::ZERO);
    let mut host_driver = HostRuntime::default();

    assert_eq!(
        host_driver
            .block_on(sim_sleep)
            .expect("host runtime drives its root"),
        Err(TimeError::WrongRuntime)
    );

    host_origin.shutdown().expect("host origin shuts down");
    sim_origin.shutdown().expect("simulation origin shuts down");
}
