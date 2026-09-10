//! Sequential, bounded experiment execution and independent replay sidecars.
pub mod args;
pub mod io;
use crate::{
    ExpectationResult, ExperimentBundle, ExperimentReport, Scenario, Size, Variant, catalogue,
    derive_checked,
};
use args::{Mode, Options};
use io::{MAX_INDEX_BYTES, imported, read, write_json};
use kr_kafka_sim::{ReplayManifest, TerminalCheckpoint};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path, time::Instant};
pub const INDEX_SCHEMA: &str = "kr-kafka-experiment-index/v1";
const FAILURE_BYTES: usize = 3 * 1024 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEntry {
    pub scenario: String,
    pub variant: Variant,
    pub seed: String,
    pub size: Size,
    pub status: String,
    pub replay_verified: bool,
    pub report: Option<String>,
    pub replay_manifest: Option<String>,
    pub checkpoint: Option<String>,
    pub failure: Option<String>,
    pub checks: Vec<ExpectationResult>,
    pub versions: Value,
    pub summary: Value,
    pub measurements: Value,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleEntry {
    pub scenario: String,
    pub path: String,
    pub page: usize,
    pub pages: usize,
    pub comparisons: Vec<ExpectationResult>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Index {
    pub schema: String,
    pub runs: Vec<RunEntry>,
    pub bundles: Vec<BundleEntry>,
}
impl Index {
    pub fn load(root: &Path) -> Result<Self, String> {
        let index: Self = serde_json::from_slice(&read(&root.join("index.json"), MAX_INDEX_BYTES)?)
            .map_err(|e| e.to_string())?;
        if index.schema != INDEX_SCHEMA
            || index.runs.is_empty()
            || index.runs.len() > 4096
            || index.bundles.len() > 4096
        {
            return Err("index schema/dimension bounds".into());
        }
        let mut keys = BTreeSet::new();
        let mut per_scenario = std::collections::BTreeMap::<&str, usize>::new();
        for row in &index.runs {
            let count = per_scenario.entry(&row.scenario).or_default();
            *count += 1;
            if *count > 32 {
                return Err("index per-scenario run bound is 32".into());
            }
            let seed = row.seed.parse::<u64>().map_err(|_| "index seed")?;
            if row.seed != seed.to_string()
                || !component(&row.scenario)
                || !component(&row.variant.name)
                || !keys.insert((&row.scenario, &row.variant.name, &row.seed))
                || !matches!(row.status.as_str(), "passed" | "failed")
            {
                return Err("index run identity/status".into());
            }
            if row.status == "passed"
                && (row.report.is_none()
                    || row.replay_manifest.is_none()
                    || row.checkpoint.is_none())
            {
                return Err("successful index entry lacks sidecars".into());
            }
            for path in [
                &row.report,
                &row.replay_manifest,
                &row.checkpoint,
                &row.failure,
            ]
            .into_iter()
            .flatten()
            {
                imported(root, path)?;
            }
        }
        for page in &index.bundles {
            if page.page >= page.pages {
                return Err("index pagination".into());
            }
            imported(root, &page.path)?;
        }
        Ok(index)
    }
}
fn component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        && value != "."
        && value != ".."
}
fn entry(s: &Scenario, v: &Variant, seed: u64, size: Size) -> RunEntry {
    RunEntry {
        scenario: s.id.into(),
        variant: v.clone(),
        seed: seed.to_string(),
        size,
        status: "failed".into(),
        replay_verified: false,
        report: None,
        replay_manifest: None,
        checkpoint: None,
        failure: None,
        checks: vec![],
        versions: Value::Null,
        summary: Value::Null,
        measurements: Value::Null,
        error: None,
    }
}
fn base(row: &RunEntry) -> String {
    format!("{}/{}-seed{}", row.scenario, row.variant.name, row.seed)
}
#[derive(Serialize)]
struct FailureEvidence<'a> {
    reason: &'a str,
    manifest: &'a ReplayManifest,
    history: &'a kr_kafka_sim::DomainHistory,
    checkpoint: Option<&'a TerminalCheckpoint>,
}
fn save_evidence(
    root: &Path,
    row: &mut RunEntry,
    m: &ReplayManifest,
    checkpoint: Option<&TerminalCheckpoint>,
) -> Result<(), String> {
    let file = format!("{}.replay.json", base(row));
    write_json(
        &root.join(&file),
        m,
        kr_kafka_sim::MAX_EXPERIMENT_MANIFEST_BYTES,
    )?;
    row.replay_manifest = Some(file);
    row.versions = serde_json::to_value(&m.versions).map_err(|e| e.to_string())?;
    if let Some(checkpoint) = checkpoint {
        let file = format!("{}.checkpoint.json", base(row));
        write_json(&root.join(&file), checkpoint, 1024 * 1024)?;
        row.checkpoint = Some(file);
    }
    Ok(())
}
fn save_failure(
    root: &Path,
    row: &mut RunEntry,
    failure: &impl Serialize,
    reason: &str,
) -> Result<(), String> {
    let file = format!("{}.failure.json", base(row));
    write_json(&root.join(&file), failure, FAILURE_BYTES)?;
    row.failure = Some(file);
    row.error = Some(reason.chars().take(4096).collect());
    row.status = "failed".into();
    row.checks.push(ExpectationResult {
        name: "execution and required checks".into(),
        status: "failed".into(),
        detail: reason.chars().take(1024).collect(),
    });
    Ok(())
}
fn execute(
    root: &Path,
    s: &Scenario,
    row: &mut RunEntry,
    m: &ReplayManifest,
    replay: bool,
    expected: Option<(&TerminalCheckpoint, Option<&ExperimentReport>)>,
) -> Result<(), String> {
    let started = Instant::now();
    let run = if replay {
        kr_kafka_sim::run_replayed(m)
    } else {
        kr_kafka_sim::run(m)
    };
    let run = match run {
        Ok(r) => r,
        Err(f) => {
            save_evidence(root, row, &f.manifest, f.checkpoint.as_ref())?;
            save_failure(root, row, f.as_ref(), &f.to_string())?;
            return Ok(());
        }
    };
    save_evidence(root, row, &run.manifest, Some(&run.checkpoint))?;
    let execution_ms = started.elapsed().as_millis() as u64;
    let checked: Result<ExperimentReport, String> = (|| {
        if let Some((checkpoint, _)) = expected
            && checkpoint != &run.checkpoint
        {
            return Err("saved terminal checkpoint diverged".into());
        }
        let report = derive_checked(s, &row.variant, row.size, replay, &run)?;
        if let Some((_, Some(old))) = expected {
            let mut old = old.clone();
            old.meta["replay_verified"] = json!(replay);
            if old != report {
                return Err("saved report aggregates/phase evidence diverged".into());
            }
        }
        Ok(report)
    })();
    match checked {
        Ok(report) => {
            let file = format!("{}.json", base(row));
            write_json(&root.join(&file), &report, crate::report::MAX_REPORT_BYTES)?;
            row.report = Some(file);
            row.status = "passed".into();
            row.replay_verified = replay;
            row.summary = report.summary.clone();
            row.checks = report
                .phase_evidence
                .iter()
                .flat_map(|p| p.check_results.clone())
                .collect();
            row.checks.insert(0,ExpectationResult{name:"complete history invariants".into(),status:"passed".into(),detail:"Oracle, accounting, resource limits and all required exact phase checks passed before report sampling".into()});
            row.measurements = json!({"execution_and_sidecars_ms":execution_ms,"total_ms":started.elapsed().as_millis() as u64,"history_events":run.history.entries.len(),"decisions":run.fault_stats.decisions,"runtime_steps":run.checkpoint.total_steps});
        }
        Err(reason) => {
            let f = FailureEvidence {
                reason: &reason,
                manifest: &run.manifest,
                history: &run.history,
                checkpoint: Some(&run.checkpoint),
            };
            save_failure(root, row, &f, &reason)?;
        }
    }
    Ok(())
}
fn save_index(root: &Path, index: &Index) -> Result<(), String> {
    write_json(&root.join("index.json"), index, MAX_INDEX_BYTES)
}
fn bundles(root: &Path, index: &mut Index, s: &Scenario) -> Result<(), String> {
    let rows: Vec<_> = index
        .runs
        .iter()
        .filter(|r| r.scenario == s.id && r.status == "passed")
        .collect();
    if rows.is_empty() {
        return Ok(());
    }
    let mut runs = vec![];
    let mut variants = vec![];
    let mut names = BTreeSet::new();
    let mut seeds = BTreeSet::new();
    for row in rows {
        let report = ExperimentReport::from_json(&read(
            &imported(root, row.report.as_ref().unwrap())?,
            crate::report::MAX_REPORT_BYTES,
        )?)?;
        if names.insert(row.variant.name.clone()) {
            variants.push(
                json!({"name":row.variant.name,"deltas":row.variant.params,"order":variants.len()}),
            );
        }
        seeds.insert(row.seed.clone());
        runs.push(report);
    }
    let mut bundle = ExperimentBundle {
        schema: crate::report::BUNDLE_SCHEMA.into(),
        scenario: runs[0].meta["scenario"].clone(),
        variants,
        seeds: seeds.into_iter().collect(),
        runs,
        comparisons: vec![],
        page: json!({"index":0,"count":1,"total_runs":0}),
    };
    bundle.comparisons = s.comparisons(&bundle);
    let failed = bundle.comparisons.iter().any(|c| c.status == "failed");
    let pages = bundle.paginate()?;
    for page in pages {
        let number = page.page["index"].as_u64().unwrap() as usize;
        let file = format!("{}/bundle-{}.json", s.id, number + 1);
        write_json(&root.join(&file), &page, crate::report::MAX_BUNDLE_BYTES)?;
        index.bundles.push(BundleEntry {
            scenario: s.id.into(),
            path: file,
            page: number,
            pages: page.page["count"].as_u64().unwrap() as usize,
            comparisons: page.comparisons,
        });
    }
    if failed {
        for row in index
            .runs
            .iter_mut()
            .filter(|r| r.scenario == s.id && r.status == "passed")
        {
            row.status = "failed".into();
            row.error = Some("characterized bundle comparison failed".into());
            row.checks.extend(bundle.comparisons.clone());
        }
    }
    Ok(())
}
fn list() -> String {
    let mut out = String::new();
    for s in catalogue() {
        out.push_str(&format!("{} — {}\n", s.id, s.title));
        for v in s.variants {
            out.push_str(&format!("  {}\n", v.name));
        }
    }
    out
}
pub fn run_cli(args: impl IntoIterator<Item = String>) -> Result<(), String> {
    let Some(o) = args::parse(args)? else {
        println!("{}", args::HELP);
        return Ok(());
    };
    if o.mode == Mode::List {
        print!("{}", list());
        return Ok(());
    }
    std::fs::create_dir_all(&o.out).map_err(|e| e.to_string())?;
    match &o.mode {
        Mode::Replay(source) => replay_saved(&o, source),
        Mode::Export => Ok(()),
        _ => generate(&o),
    }?;
    if let Some(destination) = &o.export_html {
        crate::export::export_index(&o.out, destination)?;
    }
    Ok(())
}
fn generate(o: &Options) -> Result<(), String> {
    let selected: Vec<_> = catalogue()
        .into_iter()
        .filter(|s| {
            matches!(&o.mode, Mode::All) || matches!(&o.mode,Mode::Scenario(id) if id==s.id)
        })
        .collect();
    if selected.is_empty() {
        return Err("unknown scenario; use --list".into());
    }
    if let Some(name) = &o.variant
        && !selected[0].variants.iter().any(|v| &v.name == name)
    {
        return Err("unknown variant; use --list".into());
    }
    let mut index = Index {
        schema: INDEX_SCHEMA.into(),
        runs: vec![],
        bundles: vec![],
    };
    for s in selected {
        for original in s
            .variants
            .iter()
            .filter(|v| o.variant.as_ref().is_none_or(|name| name == &v.name))
        {
            let mut v = original.clone();
            args::apply(&mut v.params, &o.overrides);
            let mut row = entry(&s, &v, o.seed, o.size);
            let outcome = match s.build(&v, o.seed, o.size) {
                Ok(m) => execute(&o.out, &s, &mut row, &m, o.replay, None),
                Err(reason) => save_failure(
                    &o.out,
                    &mut row,
                    &json!({"stage":"build","reason":reason}),
                    &reason,
                ),
            };
            if let Err(reason) = outcome {
                row.error = Some(reason);
                row.status = "failed".into();
            }
            eprintln!("{} / {} / seed {}: {}", s.id, v.name, o.seed, row.status);
            index.runs.push(row);
            save_index(&o.out, &index)?;
        }
        if let Err(reason) = bundles(&o.out, &mut index, &s) {
            for row in index.runs.iter_mut().filter(|r| r.scenario == s.id) {
                row.status = "failed".into();
                row.error = Some(format!("bundle: {reason}"));
            }
        }
        save_index(&o.out, &index)?;
    }
    finish(&o.out, &index)
}
fn replay_saved(o: &Options, source: &Path) -> Result<(), String> {
    let old = Index::load(source)?;
    let scenarios = catalogue();
    let mut index = Index {
        schema: INDEX_SCHEMA.into(),
        runs: vec![],
        bundles: vec![],
    };
    for saved in old.runs {
        let s = scenarios
            .iter()
            .find(|s| s.id == saved.scenario)
            .ok_or("saved scenario is absent from this catalogue")?;
        let mut row = entry(
            s,
            &saved.variant,
            saved.seed.parse().map_err(|_| "seed")?,
            saved.size,
        );
        let result = (|| {
            let file = saved
                .replay_manifest
                .as_ref()
                .ok_or("entry has no replay manifest")?;
            let m = ReplayManifest::from_json(&read(
                &imported(source, file)?,
                kr_kafka_sim::MAX_EXPERIMENT_MANIFEST_BYTES,
            )?)?;
            if m.fault_decisions.is_none() {
                return Err("saved manifest lacks a realized decision tape".into());
            }
            if m.seed.to_string() != saved.seed
                || serde_json::to_value(&m.versions).map_err(|e| e.to_string())? != saved.versions
            {
                return Err("index/replay identity or versions disagree".into());
            }
            let checkpoint: TerminalCheckpoint = serde_json::from_slice(&read(
                &imported(
                    source,
                    saved
                        .checkpoint
                        .as_ref()
                        .ok_or("entry has no terminal checkpoint")?,
                )?,
                1024 * 1024,
            )?)
            .map_err(|e| e.to_string())?;
            let report = saved
                .report
                .as_ref()
                .map(|file| {
                    ExperimentReport::from_json(&read(
                        &imported(source, file)?,
                        crate::report::MAX_REPORT_BYTES,
                    )?)
                })
                .transpose()?;
            execute(
                &o.out,
                s,
                &mut row,
                &m,
                true,
                Some((&checkpoint, report.as_ref())),
            )
        })();
        if let Err(reason) = result {
            save_failure(
                &o.out,
                &mut row,
                &json!({"stage":"saved replay validation","reason":reason}),
                &reason,
            )?;
        }
        eprintln!(
            "{} / {} / seed {}: {}",
            s.id, row.variant.name, row.seed, row.status
        );
        index.runs.push(row);
        save_index(&o.out, &index)?;
    }
    for s in scenarios {
        if index.runs.iter().any(|r| r.scenario == s.id) {
            bundles(&o.out, &mut index, &s)?;
        }
    }
    save_index(&o.out, &index)?;
    finish(&o.out, &index)
}
fn finish(root: &Path, index: &Index) -> Result<(), String> {
    let failed = index.runs.iter().filter(|r| r.status == "failed").count();
    if failed > 0 {
        Err(format!(
            "{failed}/{} runs failed; evidence: {}",
            index.runs.len(),
            root.join("index.json").display()
        ))
    } else {
        eprintln!(
            "{} runs passed; {}",
            index.runs.len(),
            root.join("index.json").display()
        );
        Ok(())
    }
}
