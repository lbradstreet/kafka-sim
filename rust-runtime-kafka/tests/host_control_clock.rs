use kr_runtime::HostRuntime;

#[test]
fn foreign_clock_reads_use_the_runtime_epoch_without_admitting_work() {
    let runtime = HostRuntime::default();
    let control = runtime.control();
    let status = control.status();
    let before = runtime.handle().now();
    let observed = std::thread::spawn(move || control.now()).join().unwrap();
    let after = runtime.handle().now();
    assert!(before <= observed && observed <= after);
    assert_eq!(runtime.control().status(), status);
}
