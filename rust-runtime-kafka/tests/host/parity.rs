use kr_runtime::RuntimeHandle;

use super::*;

async fn record_fifo_rounds(log: Rc<RefCell<Vec<(usize, usize)>>>, task: usize) {
    for round in 0..3 {
        log.borrow_mut().push((round, task));
        yield_now().await;
    }
}

async fn fifo_log_scenario(handle: RuntimeHandle) -> Vec<(usize, usize)> {
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut joins = Vec::new();
    for task in 0..3 {
        joins.push(
            handle
                .spawn(record_fifo_rounds(Rc::clone(&log), task))
                .expect("yielding task is admitted"),
        );
    }
    for join in joins {
        join.await.expect("yielding task completes");
    }
    log.take()
}

#[test]
fn fifo_tasks_and_yielding_tasks_have_executor_parity() {
    let mut host = HostRuntime::default();
    let host_log = host
        .block_on(fifo_log_scenario(host.handle().into()))
        .expect("runtime drains FIFO tasks");

    let mut sim = SimRuntime::default();
    let sim_log = sim
        .block_on(fifo_log_scenario(sim.handle().into()))
        .expect("runtime drains FIFO tasks");

    assert_eq!(
        host_log,
        &[
            (0, 0),
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 1),
            (1, 2),
            (2, 0),
            (2, 1),
            (2, 2),
        ]
    );
    assert_eq!(sim_log, host_log);
}

#[test]
fn nested_runtime_drives_are_rejected_in_all_executor_directions() {
    let mut outer_host = HostRuntime::default();
    let inner_host_kind = outer_host
        .block_on(async {
            let mut inner_host = HostRuntime::default();
            inner_host
                .block_on(async {})
                .expect_err("host drive is nested in a host task")
                .kind
        })
        .expect("outer host root itself succeeds");
    assert_eq!(inner_host_kind, HostRunErrorKind::ReentrantDrive);

    let mut host = HostRuntime::default();
    let sim_kind = host
        .block_on(async {
            let mut sim = SimRuntime::default();
            sim.block_on(async {})
                .expect_err("simulation drive is nested in host task")
                .kind
        })
        .expect("host root itself succeeds");
    assert_eq!(sim_kind, RunErrorKind::ReentrantDrive);

    let mut sim = SimRuntime::default();
    let host_kind = sim
        .block_on(async {
            let mut host = HostRuntime::default();
            host.block_on(async {})
                .expect_err("host drive is nested in simulation task")
                .kind
        })
        .expect("simulation root itself succeeds");
    assert_eq!(host_kind, HostRunErrorKind::ReentrantDrive);
}

async fn plain_runtime_scenario(
    value: JoinHandle<usize>,
    cancelled: JoinHandle<()>,
    mut sleep: Sleep,
) -> (
    Result<usize, JoinError>,
    Result<(), JoinError>,
    Result<(), TimeError>,
) {
    std::future::poll_fn(|context| match Pin::new(&mut sleep).poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => panic!("delayed sleep completed on its first poll: {result:?}"),
    })
    .await;
    yield_now().await;
    (value.await, cancelled.await, sleep.await)
}

#[test]
fn plain_async_join_abort_delayed_sleep_and_yield_scenario_has_executor_parity() {
    let mut sim = SimRuntime::default();
    let sim_handle = sim.handle();
    let sim_value = sim_handle
        .spawn(async { 42 })
        .expect("simulation value task is admitted");
    let sim_cancelled = sim_handle
        .spawn(pending::<()>())
        .expect("simulation pending task is admitted");
    sim_cancelled.abort();
    let sim_result = sim
        .block_on(async move {
            let sleep = sim_handle.sleep(SimDuration::from_millis(10).expect("duration fits"));
            plain_runtime_scenario(sim_value, sim_cancelled, sleep).await
        })
        .expect("simulation runs plain scenario");

    let mut host = HostRuntime::default();
    let host_handle = host.handle();
    let host_value = host_handle
        .spawn(async { 42 })
        .expect("host value task is admitted");
    let host_cancelled = host_handle
        .spawn(pending::<()>())
        .expect("host pending task is admitted");
    host_cancelled.abort();
    let host_result = host
        .block_on(async move {
            let sleep = host_handle.sleep(RuntimeDuration::from_millis(10).expect("duration fits"));
            plain_runtime_scenario(host_value, host_cancelled, sleep).await
        })
        .expect("host runtime runs plain scenario");

    assert_eq!(sim_result, host_result);
    assert_eq!(host_result, (Ok(42), Err(JoinError::Cancelled), Ok(())));
}

#[test]
fn root_panic_policies_are_explicit_across_executors() {
    let mut sim = SimRuntime::default();
    let sim_error = sim
        .block_on(async { panic!("parity root panic") })
        .expect_err("simulation root panic is fatal");
    assert!(matches!(
        &sim_error.kind,
        RunErrorKind::TaskPanicked { panic, .. } if panic.message == "parity root panic"
    ));
    assert_eq!(sim_error.disposition(), RunErrorDisposition::Fatal);

    let mut host = HostRuntime::default();
    let host_error = host
        .block_on(async { panic!("parity root panic") })
        .expect_err("host root panic is resumable");
    assert!(matches!(
        &host_error.kind,
        HostRunErrorKind::RootPanicked { panic } if panic.message == "parity root panic"
    ));
    assert_eq!(host_error.disposition(), RunErrorDisposition::Resumable);
}
