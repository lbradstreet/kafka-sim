#[path = "support/legacy.rs"]
mod legacy;
use kr_kafka_sim::{CampaignVariant, PINNED_CASES, campaign_manifest, run};
use serde_json::{Value, json};

#[test]
fn pinned_legacy_histories_and_runtime_checkpoints_remain_unchanged() {
    let expected: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/legacy-pinned.json")).unwrap();
    assert_eq!(expected.len(), PINNED_CASES.len());
    for ((seed, variant), golden) in PINNED_CASES.iter().copied().zip(expected) {
        assert_eq!(golden["seed"], seed);
        assert_eq!(golden["variant"], serde_json::to_value(variant).unwrap());
        let mut manifest = campaign_manifest(seed, variant).unwrap();
        manifest.producer.batch_target_mode = kr_kafka_producer::config::BatchTargetMode::Raw;
        let report = run(&manifest).unwrap();
        assert_eq!(
            golden["checkpoint"],
            serde_json::to_value(&report.checkpoint).unwrap()
        );
        assert_eq!(
            golden["legacy_sha256"],
            legacy::digest(&legacy::project(&report)),
            "seed={seed} variant={variant:?}"
        );
    }
}

#[test]
fn compatibility_projection_ignores_only_explicit_additive_diagnostics() {
    let report = run(&campaign_manifest(0, CampaignVariant::Clean).unwrap()).unwrap();
    let raw = serde_json::to_value(&report).unwrap();
    let expected = legacy::project_json(raw.clone());
    let mut additive = raw.clone();
    additive["history"]["version"] = json!(4);
    additive["history"]["entries"].as_array_mut().unwrap().insert(0,
        json!({"ordinal":1,"now_ns":0,"event":{"ConnectionOpened":{"connection":1,"broker":1}}}));
    assert_eq!(expected, legacy::project_json(additive));
    for pointer in [
        "/history/entries/0/now_ns",
        "/coverage/acked",
        "/checkpoint/total_steps",
        "/fault_stats/decisions",
        "/history/entries/0/event/FaultDecision/hook_id",
        "/manifest/fault_decisions/0/budget_remaining",
    ] {
        let mut changed = raw.clone();
        let field = changed.pointer_mut(pointer).unwrap();
        *field = json!(field.as_u64().unwrap() + 1);
        assert_ne!(expected, legacy::project_json(changed), "{pointer}");
    }
    let mut changed = raw;
    changed["history"]["entries"][0]["event"] = json!({"UnknownDiagnostic":{}});
    assert_ne!(expected, legacy::project_json(changed));
}
