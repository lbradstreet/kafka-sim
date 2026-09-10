use kr_runtime::rng::{RandomStream, RngCheckpoint};
use kr_runtime::{CompletionCertainty, RuntimeConfig, SimDuration, SimRuntime};
use kr_runtime_io::{
    FileIoSubmit, SimCrashError, SimCrashModel, SimDisk, SimFault, SimFsyncFailure, SimOutcome,
    SimStorageConfig, StorageOperation, WriteAtRequest,
};

fn crash_image(seed: u64) -> (Vec<u8>, RngCheckpoint) {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed,
        ..RuntimeConfig::default()
    });
    let fault_random = runtime.random_source(RandomStream::Fault);
    let disk = SimDisk::from_durable_bytes(vec![0x11; 1_024]);
    let storage = disk
        .open_with_crash_random(
            runtime.handle(),
            SimStorageConfig::default(),
            fault_random.clone(),
        )
        .expect("open crash-mode storage");
    let write = runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, vec![0x22; 1_024])))
        .expect("runtime drives dirty write")
        .expect("dirty write succeeds");
    assert_eq!(write.bytes_written, 1_024);

    storage
        .crash_with_model(SimCrashModel::FoundationDbLikeV1)
        .expect("fault randomness resolves crash image");
    let checkpoint = fault_random.random_position();
    let image = disk.durable_bytes();
    drop(storage);
    drop(fault_random);
    runtime.shutdown().expect("crash-model runtime shuts down");
    (image, checkpoint)
}

#[test]
fn foundationdb_like_crash_is_replayable_and_reaches_nonprefix_tears() {
    let mut saw_survived_byte = false;
    let mut saw_garbage = false;
    let mut saw_later_sector_without_earlier_sector = false;

    for seed in 0..128 {
        let first = crash_image(seed);
        let repeated = crash_image(seed);
        assert_eq!(first, repeated, "crash resolution seed {seed} diverged");

        let image = first.0;
        assert_eq!(image.len(), 1_024);
        saw_survived_byte |= image.contains(&0x22);
        saw_garbage |= image.iter().any(|byte| !matches!(*byte, 0x11 | 0x22));
        saw_later_sector_without_earlier_sector |= image[..512].iter().all(|byte| *byte == 0x11)
            && image[512..].iter().any(|byte| *byte != 0x11);
    }

    assert!(saw_survived_byte, "no unsynced sector bytes survived");
    assert!(saw_garbage, "no torn sector was garbage-filled");
    assert!(
        saw_later_sector_without_earlier_sector,
        "crash model never produced a non-prefix surviving sector"
    );
}

#[test]
fn unsynced_length_changes_have_both_legal_crash_outcomes() {
    let mut saw_truncate = false;
    let mut saw_original_length = false;

    for seed in 0..32 {
        let mut runtime = SimRuntime::new(RuntimeConfig {
            seed,
            ..RuntimeConfig::default()
        });
        let fault_random = runtime.random_source(RandomStream::Fault);
        let disk = SimDisk::from_durable_bytes(vec![0x33; 1_024]);
        let storage = disk
            .open_with_crash_random(
                runtime.handle(),
                SimStorageConfig::default(),
                fault_random.clone(),
            )
            .expect("open truncate crash session");
        runtime
            .block_on(storage.submit_set_len(512))
            .expect("runtime drives unsynced truncate")
            .expect("unsynced truncate is accepted");
        storage
            .crash_with_model(SimCrashModel::FoundationDbLikeV1)
            .expect("fault randomness resolves truncate");
        match disk.durable_len() {
            512 => saw_truncate = true,
            1_024 => saw_original_length = true,
            length => panic!("crash produced impossible file length {length}"),
        }
        drop(storage);
        drop(fault_random);
        runtime
            .shutdown()
            .expect("truncate crash runtime shuts down");
    }

    assert!(saw_truncate, "unsynced truncate never survived");
    assert!(saw_original_length, "unsynced truncate was never discarded");
}

#[test]
fn fsync_gated_pages_are_not_resurrected_by_the_crash_model() {
    let mut runtime = SimRuntime::default();
    let fault_random = runtime.random_source(RandomStream::Fault);
    let original = vec![0x11; 1_024];
    let disk = SimDisk::from_durable_bytes(original.clone());
    let storage = disk
        .open_with_crash_random(
            runtime.handle(),
            SimStorageConfig::default(),
            fault_random.clone(),
        )
        .expect("open crash-mode storage");
    runtime
        .block_on(storage.submit_write_at(WriteAtRequest::new(0, vec![0x22; 1_024])))
        .expect("runtime drives dirty write")
        .expect("dirty write succeeds");
    storage
        .inject(
            SimFault::new(
                StorageOperation::Sync,
                SimDuration::ZERO,
                SimOutcome::MayHaveAppliedBefore,
            )
            .with_fsync_failure(SimFsyncFailure::ExcludeDirtyPagesV1),
        )
        .expect("script failed-fsync eligibility result");
    let failure = runtime
        .block_on(storage.submit_sync())
        .expect("runtime drives failed sync")
        .expect_err("scripted sync fails");
    assert_eq!(failure.certainty(), CompletionCertainty::MayHaveApplied);
    let before_crash = fault_random.random_position();

    storage
        .crash_with_model(SimCrashModel::FoundationDbLikeV1)
        .expect("resolve crash after failed fsync");

    assert_eq!(disk.durable_bytes(), original);
    assert_eq!(
        fault_random.random_position(),
        before_crash,
        "no ineligible page should consume a crash-survival choice"
    );
}

#[test]
fn advanced_crash_policy_requires_an_explicit_fault_stream() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let storage = disk
        .open(runtime.handle(), SimStorageConfig::default())
        .expect("open ordinary storage");

    assert_eq!(
        storage.crash_with_model(SimCrashModel::FoundationDbLikeV1),
        Err(SimCrashError::MissingFaultRandom)
    );
    assert!(!storage.status().closed);
    storage.crash();
    runtime
        .shutdown()
        .expect("ordinary crash runtime shuts down");
}

#[test]
fn crash_random_source_must_use_the_fault_domain() {
    let runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let scenario = runtime.random_source(RandomStream::Scenario);

    let error = match disk.open_with_crash_random(
        runtime.handle(),
        SimStorageConfig::default(),
        scenario,
    ) {
        Ok(_) => panic!("scenario randomness must not drive crash outcomes"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        kr_runtime_io::SimOpenError::InvalidConfig("crash_random must use the Fault stream")
    ));
}
