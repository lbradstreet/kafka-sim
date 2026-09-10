use kr_kafka_sim::{
    CampaignLimits, Fault, FaultRule, ReplayManifest, Workload, run_and_retain_failure,
    run_replayed,
};
#[test]
fn default_wire_policy_is_explicit_in_replays_and_missing_legacy_policy_means_raw() {
    use kr_kafka_producer::config::{BatchTargetMode, ProducerConfig};
    assert_eq!(
        ProducerConfig::default().batch_target_mode,
        BatchTargetMode::EstimatedWire
    );
    let manifest = ReplayManifest::from_seed(0, CampaignLimits::default()).unwrap();
    assert_eq!(
        manifest.producer.batch_target_mode,
        BatchTargetMode::EstimatedWire
    );
    let mut value = serde_json::to_value(&manifest).unwrap();
    assert_eq!(value["producer"]["batch_target_mode"], "EstimatedWire");
    value["producer"]
        .as_object_mut()
        .unwrap()
        .remove("batch_target_mode");
    let parsed = ReplayManifest::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(parsed.producer.batch_target_mode, BatchTargetMode::Raw);
    value["producer"]["batch_target_mode"] = "Guess".into();
    assert!(ReplayManifest::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
}
#[test]
fn manifests_round_trip_every_setting_and_reject_invalid_or_stale_inputs() {
    let original = ReplayManifest::from_seed(36, CampaignLimits::default()).unwrap();
    let json = original.to_json().unwrap();
    let parsed = ReplayManifest::from_json(&json).unwrap();
    assert_eq!(json, parsed.to_json().unwrap());
    let mut invalid = original.clone();
    invalid.versions.model += 1;
    assert!(invalid.validate().is_err());
    let mut invalid = original.clone();
    invalid.versions.source_sha256 = "0".repeat(64);
    assert!(invalid.validate().is_err());
    let mut invalid = original.clone();
    invalid.network.operations = usize::MAX;
    assert!(invalid.validate().is_err());
    let mut invalid = original.clone();
    invalid.limits.history_events = usize::MAX;
    assert!(invalid.validate().is_err());
    let mut invalid = original.clone();
    invalid.runtime.tasks = 0;
    assert!(invalid.validate().is_err());
    let mut invalid = original.clone();
    invalid.rng_inputs[0].state ^= 1;
    assert!(invalid.validate().is_err());
    let mut invalid = original.clone();
    invalid.workload.pop();
    assert!(invalid.validate().is_err());
    let mut invalid = original.clone();
    if let Workload::Submit { records } = &mut invalid.workload[0] {
        records[0].headers[0].value.as_mut().unwrap()[0] ^= 1;
    }
    assert!(invalid.validate().is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    value["producer"]["unknown_budget"] = 1.into();
    assert!(ReplayManifest::from_json(&serde_json::to_vec(&value).unwrap()).is_err());
    let report = run_replayed(&parsed).unwrap();
    let mut replay = report.manifest;
    replay.realized_faults.as_mut().unwrap()[0].correlation += 1;
    let failure = run_replayed(&replay).unwrap_err();
    assert!(failure.reason.contains("realized fault plan diverged"));
}
#[test]
fn a_failed_untraced_run_is_repeated_with_bounded_valid_sbe_artifacts() {
    let mut manifest = ReplayManifest::from_seed(0, CampaignLimits::default()).unwrap();
    manifest.fault_plan = vec![FaultRule {
        produce_index: 4096,
        fault: Fault::DropAfterCommit,
    }];
    let directory =
        std::env::temp_dir().join(format!("kr-kafka-sim-artifact-{}", std::process::id()));
    let failure = run_and_retain_failure(&manifest, &directory).unwrap_err();
    assert!(failure.reason.contains("no fault realized"));
    let replay =
        ReplayManifest::from_json(&std::fs::read(directory.join("replay.json")).unwrap()).unwrap();
    assert_eq!(
        failure.manifest.to_json().unwrap(),
        replay.to_json().unwrap()
    );
    let history: kr_kafka_sim::RunFailure =
        serde_json::from_slice(&std::fs::read(directory.join("producer-history.json")).unwrap())
            .unwrap();
    assert_eq!(failure.history, history.history);
    assert_eq!(failure.checkpoint, history.checkpoint);
    let trace = std::fs::File::open(directory.join("runtime.sbe")).unwrap();
    kr_runtime_trace_tool::validate_sbe_trace_artifact(trace).unwrap();
    assert!(
        std::fs::metadata(directory.join("runtime.sbe"))
            .unwrap()
            .len()
            < manifest.limits.trace_bytes as u64 + 1024 * 1024
    );
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn standalone_watchdog_cli_executes_the_same_passive_campaign_driver() {
    let directory =
        std::env::temp_dir().join(format!("kr-kafka-sim-watchdog-{}", std::process::id()));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_kafka-sim"))
        .args(["--seed", "0", "--watchdog-ms", "10000", "--failure-dir"])
        .arg(&directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("acked="));
    let failure = std::process::Command::new(env!("CARGO_BIN_EXE_kafka-sim"))
        .args(["--campaign", "--watchdog-ms", "1", "--failure-dir"])
        .arg(&directory)
        .output()
        .unwrap();
    assert_eq!(failure.status.code(), Some(124));
    if directory.exists() {
        std::fs::remove_dir_all(directory).unwrap();
    }
}
