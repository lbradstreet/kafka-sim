use kr_kafka_experiments::{ExperimentReport, cli::Index};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};
const BIN: &str = env!("CARGO_BIN_EXE_kafka-experiments");
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "kr-experiment-cli-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn cli(args: &[&str], out: &Path) -> Output {
    Command::new(BIN)
        .args(args)
        .arg("--out")
        .arg(out)
        .output()
        .unwrap()
}
fn success(out: Output) {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
fn fixture(out: &Path, extra: &[&str]) {
    let mut args = vec![
        "--scenario",
        "baseline.closed-loop-inflight",
        "--variant",
        "k1-i1",
    ];
    args.extend_from_slice(extra);
    success(cli(&args, out));
}
#[test]
fn cli_list_and_single_scenario_write_index_and_report_files() {
    let listed = Command::new(BIN).arg("--list").output().unwrap();
    assert!(listed.status.success());
    let text = String::from_utf8(listed.stdout).unwrap();
    assert_eq!(text.lines().filter(|s| !s.starts_with(' ')).count(), 37);
    assert!(text.contains("resources.stop-polling-backpressure"));
    let t = Temp::new();
    fixture(&t.0, &[]);
    let index = Index::load(&t.0).unwrap();
    assert_eq!(index.runs.len(), 1);
    let row = &index.runs[0];
    assert!(row.replay_verified);
    assert_eq!(row.status, "passed");
    let report =
        ExperimentReport::from_json(&fs::read(t.0.join(row.report.as_ref().unwrap())).unwrap())
            .unwrap();
    assert_eq!(report.summary["records"]["acked"], 128);
    assert!(t.0.join(row.checkpoint.as_ref().unwrap()).exists());
    let manifest: Value =
        serde_json::from_slice(&fs::read(t.0.join(row.replay_manifest.as_ref().unwrap())).unwrap())
            .unwrap();
    assert!(manifest["fault_decisions"].is_array());
    assert_eq!(index.bundles.len(), 1);
    let replay = t.0.join("second");
    success(cli(&["--replay-dir", t.0.to_str().unwrap()], &replay));
    assert!(Index::load(&replay).unwrap().runs[0].replay_verified);
}
#[test]
fn saved_checkpoint_and_missing_tape_fail_with_indexed_evidence() {
    let t = Temp::new();
    fixture(&t.0, &[]);
    let index = Index::load(&t.0).unwrap();
    let row = &index.runs[0];
    let checkpoint = t.0.join(row.checkpoint.as_ref().unwrap());
    let bytes = fs::read(&checkpoint).unwrap();
    let mut changed: Value = serde_json::from_slice(&bytes).unwrap();
    changed["total_steps"] = json!(changed["total_steps"].as_u64().unwrap() + 1);
    fs::write(&checkpoint, serde_json::to_vec(&changed).unwrap()).unwrap();
    let out = t.0.join("bad-checkpoint");
    assert!(
        !cli(&["--replay-dir", t.0.to_str().unwrap()], &out)
            .status
            .success()
    );
    let bad = Index::load(&out).unwrap();
    assert!(
        bad.runs[0]
            .error
            .as_ref()
            .unwrap()
            .contains("checkpoint diverged")
    );
    assert!(bad.runs[0].failure.is_some());
    assert!(bad.runs[0].replay_manifest.is_some());
    fs::write(&checkpoint, bytes).unwrap();
    let manifest = t.0.join(row.replay_manifest.as_ref().unwrap());
    let mut m: Value = serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    m["fault_decisions"] = Value::Null;
    fs::write(manifest, serde_json::to_vec(&m).unwrap()).unwrap();
    let out = t.0.join("missing-tape");
    assert!(
        !cli(&["--replay-dir", t.0.to_str().unwrap()], &out)
            .status
            .success()
    );
    assert!(
        Index::load(&out).unwrap().runs[0]
            .error
            .as_ref()
            .unwrap()
            .contains("decision tape")
    );
}
#[test]
fn full_no_replay_is_explicit_and_a_saved_replay_can_verify_it() {
    let t = Temp::new();
    fixture(&t.0, &["--size", "full", "--no-replay"]);
    assert!(!Index::load(&t.0).unwrap().runs[0].replay_verified);
    let out = t.0.join("verified");
    success(cli(&["--replay-dir", t.0.to_str().unwrap()], &out));
    assert!(Index::load(&out).unwrap().runs[0].replay_verified);
}
#[test]
fn failed_run_preserves_realized_manifest_checkpoint_and_complete_history() {
    let t = Temp::new();
    let output = cli(
        &[
            "--scenario",
            "baseline.closed-loop-inflight",
            "--variant",
            "k1-i1",
            "--request-timeout-ms",
            "1",
        ],
        &t.0,
    );
    assert!(!output.status.success());
    let index = Index::load(&t.0).unwrap();
    let row = &index.runs[0];
    assert_eq!(row.status, "failed");
    assert!(row.replay_manifest.is_some());
    assert!(row.checkpoint.is_some());
    let failure: Value =
        serde_json::from_slice(&fs::read(t.0.join(row.failure.as_ref().unwrap())).unwrap())
            .unwrap();
    assert!(!failure["history"]["entries"].as_array().unwrap().is_empty());
    assert!(failure["manifest"]["fault_decisions"].is_array());
}
#[test]
fn index_paths_cannot_escape_the_input_directory() {
    let t = Temp::new();
    fixture(&t.0, &[]);
    let p = t.0.join("index.json");
    let mut index: Value = serde_json::from_slice(&fs::read(&p).unwrap()).unwrap();
    index["runs"][0]["report"] = json!("../outside.json");
    fs::write(p, serde_json::to_vec(&index).unwrap()).unwrap();
    assert!(Index::load(&t.0).unwrap_err().contains("relative normal"));
}

#[test]
fn cli_exports_saved_and_new_runs_as_self_contained_pages() {
    let t = Temp::new();
    let html = t.0.join("html");
    fixture(&t.0, &["--export-html", html.to_str().unwrap()]);
    assert!(html.join("index.html").exists());
    let page = fs::read_to_string(html.join("baseline.closed-loop-inflight.html")).unwrap();
    assert_eq!(page.matches("<script>").count(), 7);
    assert!(!page.contains("<script src="));
    let second = t.0.join("second-export");
    success(cli(&["--export-html", second.to_str().unwrap()], &t.0));
    assert_eq!(
        page,
        fs::read_to_string(second.join("baseline.closed-loop-inflight.html")).unwrap()
    );
    let accidental = t.0.join("must-not-export-old-results");
    assert!(
        !cli(
            &[
                "--scenario",
                "absent",
                "--export-html",
                accidental.to_str().unwrap()
            ],
            &t.0
        )
        .status
        .success()
    );
    assert!(
        !accidental.exists(),
        "a rejected run exported an older index"
    );
    let index = Index::load(&t.0).unwrap();
    let path = t.0.join(index.runs[0].report.as_ref().unwrap());
    let mut r: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    r["summary"]["records"]["acked"] = json!(0);
    fs::write(path, serde_json::to_vec(&r).unwrap()).unwrap();
    assert!(
        !cli(&["--export-html", second.to_str().unwrap()], &t.0)
            .status
            .success()
    );
}
