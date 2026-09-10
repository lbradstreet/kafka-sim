use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

#[cfg(test)]
use kr_runtime::RUNTIME_REPRODUCTION_SCHEMA_VERSION;
use kr_runtime::SimDuration;
use kr_runtime_ring::file::test_support::{
    WRAP_RECOVERY_SCENARIO, WrapRecoveryDetail, WrapRecoveryTrace, run_wrap_recovery_trace,
};
use kr_runtime_ring::{RingError, RingPhysicalStatus, RingStatus};
use serde::Serialize;
use serde_json::Value;

#[path = "support/json_artifact.rs"]
mod json_artifact;

use json_artifact::{DecimalU64, certainty_name, write_javascript};

const ARTIFACT_SCHEMA: u32 = 4;
const SOURCE_TEST: &str = "storage/kr-runtime-ring/src/file/tests.rs::trim_checkpoint_releases_space_and_recovery_crosses_implicit_wrap";

fn main() -> Result<(), Box<dyn Error>> {
    let destination = std::env::args_os().nth(1).map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("ring-trace-data.js"),
        PathBuf::from,
    );
    let trace = run_wrap_recovery_trace(SimDuration::from_nanos(25));
    let mut output = BufWriter::new(File::create(destination)?);
    write_javascript(
        &mut output,
        "generate_ring_trace",
        "RING_TRACE_DATA",
        &Artifact::from_trace(&trace),
    )?;
    Ok(())
}

#[derive(Serialize)]
struct Artifact<'a> {
    schema: u32,
    scenario: &'static str,
    source_test: &'static str,
    provider: &'static str,
    runtime: RuntimeRecord,
    config: ConfigRecord,
    steps: Vec<StepRecord<'a>>,
}

impl<'a> Artifact<'a> {
    fn from_trace(trace: &'a WrapRecoveryTrace) -> Self {
        let checkpoint = trace.runtime.determinism_checkpoint();
        Self {
            schema: ARTIFACT_SCHEMA,
            scenario: WRAP_RECOVERY_SCENARIO,
            source_test: SOURCE_TEST,
            provider: "FileRing<SimStorage>",
            runtime: RuntimeRecord {
                reproduction_schema: trace.runtime.reproduction.schema_version,
                checkpoint_schema: checkpoint.schema_version,
                rng_version: trace.runtime.reproduction.rng_version,
                seed: DecimalU64(trace.runtime.reproduction.config.seed),
                max_tasks: DecimalUsize(trace.runtime.reproduction.config.max_tasks),
                max_timers: DecimalUsize(trace.runtime.reproduction.config.max_timers),
                max_steps_per_run: DecimalU64(trace.runtime.reproduction.config.max_steps_per_run),
                max_time_ns: trace
                    .runtime
                    .reproduction
                    .config
                    .max_time
                    .map(|time| DecimalU64(time.as_nanos())),
                now_ns: DecimalU64(trace.runtime.now.as_nanos()),
                total_steps: DecimalU64(trace.runtime.total_steps),
                next_enqueue_sequence: DecimalU64(checkpoint.next_enqueue_sequence),
                next_timer_sequence: DecimalU64(checkpoint.next_timer_sequence),
                next_timer_id: DecimalU64(checkpoint.next_timer_id),
            },
            config: ConfigRecord {
                data_capacity_bytes: DecimalU64(trace.config.data_capacity_bytes),
                storage_latency_ns: DecimalU64(trace.storage_latency.as_nanos()),
                max_live_records: trace.config.limits.max_live_records,
                max_live_payload_bytes: trace.config.limits.max_live_payload_bytes,
            },
            steps: trace
                .steps
                .iter()
                .map(|step| {
                    let detail = DetailRecord::from_detail(&step.detail);
                    StepRecord {
                        sequence: step.sequence,
                        phase: step.phase,
                        operation: step.operation,
                        description: step.description,
                        started_at_ns: DecimalU64(step.started_at.as_nanos()),
                        completed_at_ns: DecimalU64(step.completed_at.as_nanos()),
                        duration_ns: DecimalU64(
                            step.completed_at
                                .checked_duration_since(step.started_at)
                                .expect("scenario time is monotonic")
                                .as_nanos(),
                        ),
                        outcome: detail.outcome,
                        certainty: detail.certainty,
                        summary: detail.summary,
                        fields: detail.fields,
                        status: step.status.map(StatusRecord::from_status),
                    }
                })
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct RuntimeRecord {
    reproduction_schema: u32,
    checkpoint_schema: u32,
    rng_version: u32,
    seed: DecimalU64,
    max_tasks: DecimalUsize,
    max_timers: DecimalUsize,
    max_steps_per_run: DecimalU64,
    max_time_ns: Option<DecimalU64>,
    now_ns: DecimalU64,
    total_steps: DecimalU64,
    next_enqueue_sequence: DecimalU64,
    next_timer_sequence: DecimalU64,
    next_timer_id: DecimalU64,
}

#[derive(Serialize)]
struct ConfigRecord {
    data_capacity_bytes: DecimalU64,
    storage_latency_ns: DecimalU64,
    max_live_records: usize,
    max_live_payload_bytes: usize,
}

#[derive(Serialize)]
struct StepRecord<'a> {
    sequence: u32,
    phase: &'a str,
    operation: &'a str,
    description: &'a str,
    started_at_ns: DecimalU64,
    completed_at_ns: DecimalU64,
    duration_ns: DecimalU64,
    outcome: &'static str,
    certainty: Option<&'static str>,
    summary: String,
    fields: BTreeMap<&'static str, Value>,
    status: Option<StatusRecord>,
}

struct DetailRecord {
    outcome: &'static str,
    certainty: Option<&'static str>,
    summary: String,
    fields: BTreeMap<&'static str, Value>,
}

impl DetailRecord {
    fn from_detail(detail: &WrapRecoveryDetail) -> Self {
        let mut fields = BTreeMap::new();
        match detail {
            WrapRecoveryDetail::Created => Self {
                outcome: "success",
                certainty: None,
                summary: "empty file ring initialized".into(),
                fields,
            },
            WrapRecoveryDetail::Appended {
                record_count,
                payload_bytes,
                first_position,
                next_cursor,
                crossed_physical_wrap,
            } => {
                fields.insert("record_count", Value::from(*record_count));
                fields.insert("payload_bytes", Value::from(*payload_bytes));
                insert_u64(&mut fields, "first_position", first_position.get());
                insert_u64(&mut fields, "next_cursor", next_cursor.get());
                fields.insert("crossed_physical_wrap", Value::Bool(*crossed_physical_wrap));
                Self {
                    outcome: "success",
                    certainty: None,
                    summary: format!(
                        "accepted {record_count} record(s) at [{}..{}){}",
                        first_position.get(),
                        next_cursor.get(),
                        if *crossed_physical_wrap {
                            " across physical wrap"
                        } else {
                            ""
                        }
                    ),
                    fields,
                }
            }
            WrapRecoveryDetail::AppendRejected { certainty, error } => {
                fields.insert("error_type", Value::String(error_name(error).into()));
                if let RingError::PhysicalCapacityReached {
                    protected,
                    requested,
                    limit,
                } = error
                {
                    insert_u64(&mut fields, "protected_bytes", *protected);
                    insert_u64(&mut fields, "requested_bytes", *requested);
                    insert_u64(&mut fields, "capacity_bytes", *limit);
                }
                Self {
                    outcome: "rejected",
                    certainty: Some(certainty_name(*certainty)),
                    summary: error.to_string(),
                    fields,
                }
            }
            WrapRecoveryDetail::Trimmed {
                requested,
                accepted_head,
            } => {
                insert_u64(&mut fields, "requested_cursor", requested.get());
                insert_u64(&mut fields, "accepted_head", accepted_head.get());
                Self {
                    outcome: "success",
                    certainty: None,
                    summary: format!("accepted head advanced to {}", accepted_head.get()),
                    fields,
                }
            }
            WrapRecoveryDetail::Synced(sync) => {
                insert_u64(&mut fields, "durable_head", sync.durable_head.get());
                insert_u64(&mut fields, "durable_tail", sync.durable_tail.get());
                fields.insert("reclaimed_records", Value::from(sync.reclaimed_records));
                fields.insert(
                    "reclaimed_payload_bytes",
                    Value::from(sync.reclaimed_payload_bytes),
                );
                Self {
                    outcome: "success",
                    certainty: None,
                    summary: format!(
                        "durable interval is [{}..{}); reclaimed {} record(s)",
                        sync.durable_head.get(),
                        sync.durable_tail.get(),
                        sync.reclaimed_records
                    ),
                    fields,
                }
            }
            WrapRecoveryDetail::Crashed => Self {
                outcome: "crash",
                certainty: None,
                summary: "simulated storage session lost".into(),
                fields,
            },
            WrapRecoveryDetail::Reopened => Self {
                outcome: "recovered",
                certainty: None,
                summary: "generation 4 recovered across physical wrap".into(),
                fields,
            },
            WrapRecoveryDetail::Read {
                requested,
                positions,
                payload_markers,
            } => {
                insert_u64(&mut fields, "requested_cursor", requested.get());
                fields.insert(
                    "positions",
                    Value::Array(
                        positions
                            .iter()
                            .map(|position| Value::String(position.get().to_string()))
                            .collect(),
                    ),
                );
                fields.insert(
                    "payload_markers",
                    Value::Array(payload_markers.iter().copied().map(Value::from).collect()),
                );
                Self {
                    outcome: "success",
                    certainty: None,
                    summary: format!("read {} durable record(s)", positions.len()),
                    fields,
                }
            }
            _ => Self {
                outcome: "unknown",
                certainty: None,
                summary: format!("unrecognized scenario detail: {detail:?}"),
                fields,
            },
        }
    }
}

#[derive(Serialize)]
struct StatusRecord {
    accepted_head: DecimalU64,
    accepted_tail: DecimalU64,
    durable_head: DecimalU64,
    durable_tail: DecimalU64,
    accepted_live_records: usize,
    accepted_live_payload_bytes: usize,
    retained_records: usize,
    retained_payload_bytes: usize,
    pending_reclaim_records: usize,
    pending_reclaim_payload_bytes: usize,
    max_live_records: usize,
    max_live_payload_bytes: usize,
    physical: Option<PhysicalRecord>,
}

impl StatusRecord {
    fn from_status(status: RingStatus) -> Self {
        Self {
            accepted_head: DecimalU64(status.accepted_head.get()),
            accepted_tail: DecimalU64(status.accepted_tail.get()),
            durable_head: DecimalU64(status.durable_head.get()),
            durable_tail: DecimalU64(status.durable_tail.get()),
            accepted_live_records: status.accepted_live_records,
            accepted_live_payload_bytes: status.accepted_live_payload_bytes,
            retained_records: status.retained_records,
            retained_payload_bytes: status.retained_payload_bytes,
            pending_reclaim_records: status.pending_reclaim_records,
            pending_reclaim_payload_bytes: status.pending_reclaim_payload_bytes,
            max_live_records: status.max_live_records,
            max_live_payload_bytes: status.max_live_payload_bytes,
            physical: status.physical.map(PhysicalRecord::from_physical),
        }
    }
}

#[derive(Serialize)]
struct PhysicalRecord {
    data_capacity_bytes: DecimalU64,
    protected_bytes: DecimalU64,
    free_bytes: DecimalU64,
    durable_head_offset: DecimalU64,
    durable_tail_offset: DecimalU64,
    accepted_tail_offset: DecimalU64,
    metadata_generation: DecimalU64,
    recovery_required: bool,
}

impl PhysicalRecord {
    fn from_physical(physical: RingPhysicalStatus) -> Self {
        Self {
            data_capacity_bytes: DecimalU64(physical.data_capacity_bytes),
            protected_bytes: DecimalU64(physical.protected_bytes),
            free_bytes: DecimalU64(physical.free_bytes),
            durable_head_offset: DecimalU64(physical.durable_head_offset),
            durable_tail_offset: DecimalU64(physical.durable_tail_offset),
            accepted_tail_offset: DecimalU64(physical.accepted_tail_offset),
            metadata_generation: DecimalU64(physical.metadata_generation),
            recovery_required: physical.recovery_required,
        }
    }
}

#[derive(Clone, Copy)]
struct DecimalUsize(usize);

impl Serialize for DecimalUsize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

fn insert_u64(fields: &mut BTreeMap<&'static str, Value>, key: &'static str, value: u64) {
    fields.insert(key, Value::String(value.to_string()));
}

fn error_name(error: &RingError) -> &'static str {
    match error {
        RingError::PhysicalCapacityReached { .. } => "physical_capacity_reached",
        _ => "ring_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ring_trace_is_deterministic_and_contains_the_wrap() {
        let render = || {
            let trace = run_wrap_recovery_trace(SimDuration::from_nanos(25));
            let mut bytes = Vec::new();
            write_javascript(
                &mut bytes,
                "generate_ring_trace",
                "RING_TRACE_DATA",
                &Artifact::from_trace(&trace),
            )
            .unwrap();
            bytes
        };

        let first = render();
        assert_eq!(first, render());
        let text = std::str::from_utf8(&first).unwrap();
        assert!(text.contains("\"schema\": 4"));
        let reproduction_schema =
            format!("\"reproduction_schema\": {RUNTIME_REPRODUCTION_SCHEMA_VERSION}");
        assert!(text.contains(reproduction_schema.as_str()));
        assert!(text.contains("\"checkpoint_schema\": 3"));
        assert!(text.contains("\"next_enqueue_sequence\":"));
        assert!(text.contains("\"next_timer_sequence\":"));
        assert!(text.contains("\"next_timer_id\":"));
        assert!(text.contains("\"max_steps_per_run\": \"1000000\""));
        assert!(text.contains("\"max_time_ns\": null"));
        assert!(text.contains("crossed_physical_wrap"));
        assert!(text.contains("physical_capacity_reached"));
        assert!(text.contains("\"protected_bytes\": \"128\""));
    }
}
