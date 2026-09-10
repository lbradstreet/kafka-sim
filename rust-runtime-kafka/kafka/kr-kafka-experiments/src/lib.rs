//! Experiments are separate from the finite correctness gate. Every chart is
//! derived from a complete audited run; sampled rows are only an inspection aid.
pub mod build;
pub mod cli;
pub mod export;
pub mod js_wrapper;
pub mod report;
mod scenarios;
use kr_kafka_sim::{ReplayManifest, RunReport};
pub use report::{ExpectationResult, ExperimentBundle, ExperimentReport, PhaseEvidence};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Size {
    Test,
    Full,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    Baseline,
    Hard,
    Soft,
    Topology,
    Resources,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Params {
    pub in_flight: Option<u8>,
    pub lanes: Option<u8>,
    pub linger_ns: Option<u64>,
    pub backoff_ns: Option<(u64, u64)>,
    pub request_timeout_ns: Option<u64>,
    pub delivery_timeout_ns: Option<u64>,
    pub compression: Option<u8>,
    pub rate_per_s: Option<u64>,
    pub outstanding: Option<u32>,
    pub batch_target_bytes: Option<u32>,
    pub wire_window_bytes: Option<u32>,
    pub value_bytes: Option<u32>,
    pub metadata_max_age_ns: Option<u64>,
    /// None preserves the scenario default; false/true selects Shared/Pressure.
    pub partition_pressure: Option<bool>,
    pub extra: BTreeMap<String, u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Variant {
    pub name: String,
    pub summary: String,
    pub params: Params,
}
#[derive(Clone, Debug)]
pub struct Scenario {
    pub id: &'static str,
    pub title: &'static str,
    pub category: Category,
    pub description: &'static str,
    pub what_to_look_for: &'static str,
    pub variants: Vec<Variant>,
}
impl Scenario {
    pub fn build(&self, v: &Variant, seed: u64, size: Size) -> Result<ReplayManifest, String> {
        scenarios::build(self, v, seed, size)
    }
    pub fn invariants(&self, run: &RunReport) -> Result<(), String> {
        scenarios::invariants(self, run)
    }
    pub fn phase_coverage(&self, run: &RunReport) -> Result<Vec<PhaseEvidence>, String> {
        scenarios::phase_coverage(self, run)
    }
    pub fn comparisons(&self, bundle: &ExperimentBundle) -> Vec<ExpectationResult> {
        scenarios::comparisons(self, bundle)
    }
}
pub fn catalogue() -> Vec<Scenario> {
    scenarios::catalogue()
}
pub fn run_scenario(
    s: &Scenario,
    v: &Variant,
    seed: u64,
    size: Size,
) -> Result<ExperimentReport, String> {
    let m = s.build(v, seed, size)?;
    let run = kr_kafka_sim::run_replayed(&m).map_err(|e| e.to_string())?;
    derive_checked(s, v, size, true, &run)
}
pub fn derive_checked(
    s: &Scenario,
    v: &Variant,
    size: Size,
    replay_verified: bool,
    run: &RunReport,
) -> Result<ExperimentReport, String> {
    s.invariants(run)?;
    let phases = s.phase_coverage(run)?;
    let report = report::derive(s, v, size, replay_verified, run, &phases)?;
    report.validate()?;
    Ok(report)
}
