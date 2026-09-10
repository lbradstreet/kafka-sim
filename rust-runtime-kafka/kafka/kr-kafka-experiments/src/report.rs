//! Versioned presentation schema. Named blocks use JSON values to share the exact
//! column contract with the DOM-free browser validator; construction and imports
//! both pass the same semantic validation before they can be exported.
mod adjustments;
mod derive;
mod validate;
pub use derive::derive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub const SCHEMA: &str = "kr-kafka-experiment/v1";
pub const BUNDLE_SCHEMA: &str = "kr-kafka-experiment-bundle/v1";
pub const MAX_REPORT_BYTES: usize = 5 * 1024 * 1024;
pub const MAX_BUNDLE_BYTES: usize = 48 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExperimentReport {
    pub schema: String,
    pub meta: Value,
    pub topology: Value,
    pub config: Value,
    pub environment: Value,
    pub buckets: Value,
    pub partitions: Value,
    pub records: Value,
    pub summary: Value,
    pub phase_evidence: Vec<PhaseEvidence>,
    pub distributions: Value,
    pub hdr: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PhaseEvidence {
    pub phase: String,
    pub start: u64,
    pub end: u64,
    pub exact_counts: std::collections::BTreeMap<String, u64>,
    /// Bounded selected record/request/connection IDs, as canonical decimals.
    pub witnesses: std::collections::BTreeMap<String, Vec<String>>,
    pub check_results: Vec<ExpectationResult>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExpectationResult {
    pub name: String,
    pub status: String,
    pub detail: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExperimentBundle {
    pub schema: String,
    pub scenario: Value,
    pub variants: Vec<Value>,
    pub seeds: Vec<String>,
    pub runs: Vec<ExperimentReport>,
    pub comparisons: Vec<ExpectationResult>,
    pub page: Value,
}
impl ExperimentReport {
    pub fn validate(&self) -> Result<(), String> {
        validate::report(self)
    }
    pub fn to_json(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        if bytes.len() > MAX_REPORT_BYTES {
            return Err("report exceeds 5 MiB".into());
        }
        Ok(bytes)
    }
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_REPORT_BYTES {
            return Err("report exceeds 5 MiB".into());
        }
        let report: Self = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        report.validate()?;
        Ok(report)
    }
}
impl ExperimentBundle {
    pub fn validate(&self) -> Result<(), String> {
        validate::bundle(self)
    }
    /// Split on both serialized bytes and run count. Comparisons retain their
    /// whole-bundle scope; page metadata makes omitted runs explicit.
    pub fn paginate(&self) -> Result<Vec<Self>, String> {
        let total = self.runs.len();
        if total == 0 {
            return Err("empty bundle".into());
        }
        let mut pages = Vec::new();
        let mut current = self.clone();
        current.runs.clear();
        for run in &self.runs {
            run.validate()?;
            current.runs.push(run.clone());
            if current.runs.len() > 32
                || serde_json::to_vec(&current)
                    .map_err(|e| e.to_string())?
                    .len()
                    > MAX_BUNDLE_BYTES - 1024
            {
                let last = current.runs.pop().unwrap();
                if current.runs.is_empty() {
                    return Err("single report exceeds bundle cap".into());
                }
                pages.push(current.clone());
                current.runs = vec![last];
            }
        }
        pages.push(current);
        let count = pages.len();
        for (index, page) in pages.iter_mut().enumerate() {
            page.page = serde_json::json!({"index":index,"count":count,"total_runs":total});
            page.validate()?;
        }
        Ok(pages)
    }
}
#[cfg(test)]
mod tests;
