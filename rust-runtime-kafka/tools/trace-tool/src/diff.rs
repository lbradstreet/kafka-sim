//! First-divergence comparison between two binary SBE trace artifacts.
//!
//! A terminal `DeterminismCheckpoint` mismatch says two reruns diverged but
//! not where. This module compares two artifacts from the same seed and
//! reports the earliest observable difference: the first retained event that
//! differs, a strict-prefix length difference, or — when the event streams
//! agree — the first differing terminal field.
//!
//! Two artifacts are compared only when they describe reruns of the same
//! experiment. Every reproduction-affecting header field (seed, RNG and
//! schema versions, runtime configuration, driver, retention and sampling
//! policy) must match exactly; anything else is a typed
//! [`ExportError::IncomparableArtifacts`] error, never a divergence report.

use std::fmt;

use kr_runtime::trace::TraceEvent;
use kr_runtime_trace_wire::ReadBuf;
use kr_runtime_trace_wire::artifact_header_codec::ArtifactHeaderDecoder;
use kr_runtime_trace_wire::message_header_codec::MessageHeaderDecoder;
use kr_runtime_trace_wire::random_stream_state_codec::RandomStreamStateDecoder;
use kr_runtime_trace_wire::task_snapshot_codec::TaskSnapshotDecoder;

use super::sbe_artifact::{FRAME_LENGTH_PREFIX, PREAMBLE_LENGTH};
use super::{ExportError, validate_sbe_trace_artifact};

/// The earliest observable difference between two comparable trace artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraceArtifactDivergence {
    /// The retained event streams first differ at this shared index.
    Event {
        /// Zero-based index into both retained event streams.
        index: u64,
        /// The left artifact's event at that index.
        left: Box<TraceEvent>,
        /// The right artifact's event at that index.
        right: Box<TraceEvent>,
    },
    /// One retained event stream is a strict prefix of the other.
    EventCount {
        /// Retained events in the left artifact.
        left: u64,
        /// Retained events in the right artifact.
        right: u64,
    },
    /// The retained event streams agree; terminal runtime state differs.
    Terminal {
        /// The terminal header or snapshot field that differs.
        field: &'static str,
        /// The left artifact's value, rendered for diagnostics.
        left: String,
        /// The right artifact's value, rendered for diagnostics.
        right: String,
    },
}

impl fmt::Display for TraceArtifactDivergence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event { index, left, right } => write!(
                formatter,
                "first divergence at retained event {index}: \
                 left sequence {} {} at {}, right sequence {} {} at {}",
                left.sequence,
                left.kind.tag().name(),
                left.at,
                right.sequence,
                right.kind.tag().name(),
                right.at,
            ),
            Self::EventCount { left, right } => write!(
                formatter,
                "retained event streams agree for {} events, then one ends early \
                 (left retains {left}, right retains {right})",
                left.min(right),
            ),
            Self::Terminal { field, left, right } => write!(
                formatter,
                "retained event streams agree; terminal {field} differs \
                 (left {left}, right {right})"
            ),
        }
    }
}

/// Compares two binary SBE trace artifacts and reports their first
/// divergence.
///
/// Both artifacts are strictly validated first, then required to describe
/// reruns of the same experiment. Divergences are reported in observation
/// order: the first differing retained event, then a strict-prefix length
/// difference, then terminal state in a fixed order (terminal instant,
/// scheduler counters, random-stream positions, task counts and snapshots,
/// harness outcome, retention metadata, and finally the trace fingerprint).
/// `Ok(None)` means the artifacts are equivalent.
///
/// # Errors
///
/// Returns [`ExportError::InvalidBinaryArtifact`] (or another validation
/// error) if either input is not a valid artifact, and
/// [`ExportError::IncomparableArtifacts`] if the two artifacts do not share
/// every reproduction-affecting header field.
pub fn diff_sbe_trace_artifacts(
    left: &[u8],
    right: &[u8],
) -> Result<Option<TraceArtifactDivergence>, ExportError> {
    validate_sbe_trace_artifact(left)?;
    validate_sbe_trace_artifact(right)?;
    let left = parse_artifact(left)?;
    let right = parse_artifact(right)?;
    ensure_comparable(&left.header, &right.header)?;

    if let Some(divergence) = diff_events(&left, &right)? {
        return Ok(Some(divergence));
    }
    Ok(diff_terminal_state(&left, &right))
}

fn diff_events(
    left: &Artifact<'_>,
    right: &Artifact<'_>,
) -> Result<Option<TraceArtifactDivergence>, ExportError> {
    for (index, (left_frame, right_frame)) in
        left.events.iter().zip(right.events.iter()).enumerate()
    {
        if left_frame == right_frame {
            continue;
        }
        let index =
            u64::try_from(index).map_err(|_| invalid("diverging event index does not fit u64"))?;
        return Ok(Some(TraceArtifactDivergence::Event {
            index,
            left: Box::new(decode_event(left_frame)?),
            right: Box::new(decode_event(right_frame)?),
        }));
    }
    if left.events.len() != right.events.len() {
        return Ok(Some(TraceArtifactDivergence::EventCount {
            left: usize_to_u64(left.events.len())?,
            right: usize_to_u64(right.events.len())?,
        }));
    }
    Ok(None)
}

type RenderedField = (&'static str, fn(&Header) -> String);
type CountField = (&'static str, fn(&Header) -> u64);

fn diff_terminal_state(
    left: &Artifact<'_>,
    right: &Artifact<'_>,
) -> Option<TraceArtifactDivergence> {
    let scalar_fields: [RenderedField; 4] = [
        ("instant", |header| format!("{}ns", header.now_ns)),
        ("scheduler step count", |header| {
            header.total_steps.to_string()
        }),
        ("enqueue sequence", |header| {
            header.next_enqueue_sequence.to_string()
        }),
        ("timer sequence", |header| {
            format!("{}/{}", header.next_timer_sequence, header.next_timer_id)
        }),
    ];
    for (field, render) in scalar_fields {
        let left_value = render(&left.header);
        let right_value = render(&right.header);
        if left_value != right_value {
            return Some(TraceArtifactDivergence::Terminal {
                field,
                left: left_value,
                right: right_value,
            });
        }
    }

    for (left_stream, right_stream) in left.random.iter().zip(right.random.iter()) {
        if left_stream != right_stream {
            return Some(TraceArtifactDivergence::Terminal {
                field: "random-stream position",
                left: render_stream(left_stream),
                right: render_stream(right_stream),
            });
        }
    }

    let count_fields: [CountField; 4] = [
        ("ready task count", |header| header.ready_tasks),
        ("live timer count", |header| header.live_timers),
        ("live task count", |header| header.live_tasks),
        ("stopped flag", |header| u64::from(header.stopped)),
    ];
    for (field, read) in count_fields {
        let left_value = read(&left.header);
        let right_value = read(&right.header);
        if left_value != right_value {
            return Some(TraceArtifactDivergence::Terminal {
                field,
                left: left_value.to_string(),
                right: right_value.to_string(),
            });
        }
    }

    if left.tasks != right.tasks {
        let position = left
            .tasks
            .iter()
            .zip(right.tasks.iter())
            .position(|(left_task, right_task)| left_task != right_task);
        let render = |tasks: &[TaskSnapshotFields]| match position {
            Some(index) => render_task(&tasks[index]),
            None => format!("{} snapshots", tasks.len()),
        };
        return Some(TraceArtifactDivergence::Terminal {
            field: "task snapshot",
            left: render(&left.tasks),
            right: render(&right.tasks),
        });
    }

    if left.header.outcome != right.header.outcome {
        return Some(TraceArtifactDivergence::Terminal {
            field: "harness outcome",
            left: left.header.outcome.clone(),
            right: right.header.outcome.clone(),
        });
    }

    let retention_fields: [RenderedField; 3] = [
        ("dropped event count", |header| {
            header.dropped_events.to_string()
        }),
        ("last observed sequence", |header| {
            format!("{:?}", header.last_sequence)
        }),
        ("ordering violation", |header| {
            format!("{:?}", header.ordering_violation)
        }),
    ];
    for (field, render) in retention_fields {
        let left_value = render(&left.header);
        let right_value = render(&right.header);
        if left_value != right_value {
            return Some(TraceArtifactDivergence::Terminal {
                field,
                left: left_value,
                right: right_value,
            });
        }
    }

    if left.header.trace_fingerprint != right.header.trace_fingerprint {
        return Some(TraceArtifactDivergence::Terminal {
            field: "trace fingerprint",
            left: format!("{:#018x}", left.header.trace_fingerprint),
            right: format!("{:#018x}", right.header.trace_fingerprint),
        });
    }

    None
}

fn ensure_comparable(left: &Header, right: &Header) -> Result<(), ExportError> {
    let fields: [RenderedField; 12] = [
        ("seed", |header| format!("{:#018x}", header.seed)),
        ("RNG version", |header| header.rng_version.to_string()),
        ("runtime reproduction schema", |header| {
            header.runtime_reproduction_schema.to_string()
        }),
        ("determinism checkpoint schema", |header| {
            header.determinism_checkpoint_schema.to_string()
        }),
        ("start time", |header| format!("{}ns", header.start_time_ns)),
        ("task capacity", |header| header.max_tasks.to_string()),
        ("timer capacity", |header| header.max_timers.to_string()),
        ("step budget", |header| header.max_steps_per_run.to_string()),
        ("time limit", |header| format!("{:?}", header.max_time_ns)),
        ("driver", |header| header.driver.clone()),
        ("retention policy", |header| {
            format!(
                "mode {} unit {} prefix {} tail {}",
                header.retention_mode,
                header.capacity_unit,
                header.prefix_capacity,
                header.tail_capacity,
            )
        }),
        ("sampling policy", |header| {
            format!(
                "mode {} algorithm {} period {} phase {} fingerprint scope {}",
                header.sampling_mode,
                header.sampling_algorithm_version,
                header.sampling_period,
                header.sampling_phase,
                header.fingerprint_scope,
            )
        }),
    ];
    for (field, render) in fields {
        let left_value = render(left);
        let right_value = render(right);
        if left_value != right_value {
            return Err(ExportError::IncomparableArtifacts {
                field,
                left: left_value,
                right: right_value,
            });
        }
    }
    Ok(())
}

fn render_stream(stream: &RandomStreamFields) -> String {
    format!(
        "tag {:#018x} state {:#018x} draws {}",
        stream.tag, stream.state, stream.draws
    )
}

fn render_task(task: &TaskSnapshotFields) -> String {
    format!(
        "slot {} generation {} state {}",
        task.slot, task.generation, task.state
    )
}

/// Every artifact-header field the comparison reads, in decoded form.
struct Header {
    runtime_reproduction_schema: u32,
    determinism_checkpoint_schema: u32,
    rng_version: u32,
    seed: u64,
    max_tasks: u64,
    max_timers: u64,
    max_steps_per_run: u64,
    max_time_ns: Option<u64>,
    start_time_ns: u64,
    retention_mode: u8,
    capacity_unit: u8,
    prefix_capacity: u64,
    tail_capacity: u64,
    sampling_mode: u8,
    sampling_algorithm_version: u32,
    sampling_period: u64,
    sampling_phase: u64,
    fingerprint_scope: u8,
    dropped_events: u64,
    last_sequence: Option<u64>,
    ordering_violation: Option<(u64, u64)>,
    trace_fingerprint: u64,
    now_ns: u64,
    total_steps: u64,
    next_enqueue_sequence: u64,
    next_timer_sequence: u64,
    next_timer_id: u64,
    ready_tasks: u64,
    live_timers: u64,
    live_tasks: u64,
    stopped: bool,
    random_stream_count: usize,
    task_count: usize,
    event_count: usize,
    driver: String,
    outcome: String,
}

#[derive(Eq, PartialEq)]
struct RandomStreamFields {
    tag: u64,
    state: u64,
    draws: u64,
}

#[derive(Eq, PartialEq)]
struct TaskSnapshotFields {
    slot: u32,
    generation: u32,
    state: u8,
}

struct Artifact<'a> {
    header: Header,
    random: Vec<RandomStreamFields>,
    tasks: Vec<TaskSnapshotFields>,
    /// Full event frames including their length prefixes; canonical encoding
    /// makes byte equality equivalent to event equality.
    events: Vec<&'a [u8]>,
}

/// Splits one pre-validated artifact into decoded metadata and event frames.
///
/// Callers must strictly validate the bytes first; this parser re-checks only
/// enough structure to stay in bounds and fails closed on any inconsistency.
fn parse_artifact(bytes: &[u8]) -> Result<Artifact<'_>, ExportError> {
    let mut cursor = Cursor {
        bytes,
        offset: PREAMBLE_LENGTH,
    };
    let header = decode_header(cursor.frame_payload("artifact header")?)?;
    let random = (0..header.random_stream_count)
        .map(|_| decode_random_stream(cursor.frame_payload("random-stream state")?))
        .collect::<Result<Vec<_>, _>>()?;
    let tasks = (0..header.task_count)
        .map(|_| decode_task(cursor.frame_payload("task snapshot")?))
        .collect::<Result<Vec<_>, _>>()?;
    let events = (0..header.event_count)
        .map(|_| cursor.full_frame("event"))
        .collect::<Result<Vec<_>, _>>()?;
    if cursor.offset != bytes.len() {
        return Err(invalid("trailing data after declared event frames"));
    }
    Ok(Artifact {
        header,
        random,
        tasks,
        events,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn frame_length(&self, description: &str) -> Result<usize, ExportError> {
        let prefix = self
            .bytes
            .get(self.offset..self.offset + FRAME_LENGTH_PREFIX)
            .ok_or_else(|| invalid(format!("truncated {description} frame length")))?;
        let length = u32::from_le_bytes(
            prefix
                .try_into()
                .map_err(|_| invalid(format!("unreadable {description} frame length")))?,
        );
        let length = usize::try_from(length)
            .map_err(|_| invalid(format!("{description} frame length does not fit usize")))?;
        if length < FRAME_LENGTH_PREFIX {
            return Err(invalid(format!("{description} frame length underflows")));
        }
        Ok(length)
    }

    /// Returns the next frame's payload, excluding its length prefix.
    fn frame_payload(&mut self, description: &str) -> Result<&'a [u8], ExportError> {
        let length = self.frame_length(description)?;
        let payload = self
            .bytes
            .get(self.offset + FRAME_LENGTH_PREFIX..self.offset + length)
            .ok_or_else(|| invalid(format!("truncated {description} frame")))?;
        self.offset += length;
        Ok(payload)
    }

    /// Returns the next frame including its length prefix.
    fn full_frame(&mut self, description: &str) -> Result<&'a [u8], ExportError> {
        let length = self.frame_length(description)?;
        let frame = self
            .bytes
            .get(self.offset..self.offset + length)
            .ok_or_else(|| invalid(format!("truncated {description} frame")))?;
        self.offset += length;
        Ok(frame)
    }
}

fn decode_header(payload: &[u8]) -> Result<Header, ExportError> {
    let message_header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let mut decoder = ArtifactHeaderDecoder::default().header(message_header, 0);
    let driver_coordinates = decoder.driver_decoder();
    let driver = std::str::from_utf8(decoder.driver_slice(driver_coordinates))
        .map_err(|_| invalid("artifact driver is not UTF-8"))?
        .to_owned();
    let outcome_coordinates = decoder.outcome_decoder();
    let outcome = std::str::from_utf8(decoder.outcome_slice(outcome_coordinates))
        .map_err(|_| invalid("artifact outcome is not UTF-8"))?
        .to_owned();
    Ok(Header {
        runtime_reproduction_schema: decoder.runtime_reproduction_schema(),
        determinism_checkpoint_schema: decoder.determinism_checkpoint_schema(),
        rng_version: decoder.rng_version(),
        seed: decoder.seed(),
        max_tasks: decoder.max_tasks(),
        max_timers: decoder.max_timers(),
        max_steps_per_run: decoder.max_steps_per_run(),
        max_time_ns: (decoder.max_time_present() != 0).then(|| decoder.max_time_ns()),
        start_time_ns: decoder.start_time_ns(),
        retention_mode: decoder.retention_mode(),
        capacity_unit: decoder.capacity_unit(),
        prefix_capacity: decoder.prefix_capacity(),
        tail_capacity: decoder.tail_capacity(),
        sampling_mode: decoder.sampling_mode(),
        sampling_algorithm_version: decoder.sampling_algorithm_version(),
        sampling_period: decoder.sampling_period(),
        sampling_phase: decoder.sampling_phase(),
        fingerprint_scope: decoder.fingerprint_scope(),
        dropped_events: decoder.dropped_events(),
        last_sequence: (decoder.last_sequence_present() != 0).then(|| decoder.last_sequence()),
        ordering_violation: (decoder.ordering_violation_present() != 0)
            .then(|| (decoder.previous_sequence(), decoder.rejected_sequence())),
        trace_fingerprint: decoder.trace_fingerprint(),
        now_ns: decoder.now_ns(),
        total_steps: decoder.total_steps(),
        next_enqueue_sequence: decoder.next_enqueue_sequence(),
        next_timer_sequence: decoder.next_timer_sequence(),
        next_timer_id: decoder.next_timer_id(),
        ready_tasks: decoder.ready_tasks(),
        live_timers: decoder.live_timers(),
        live_tasks: decoder.live_tasks(),
        stopped: decoder.stopped() != 0,
        random_stream_count: usize::try_from(decoder.random_stream_count())
            .map_err(|_| invalid("random-stream count does not fit usize"))?,
        task_count: usize::try_from(decoder.task_count())
            .map_err(|_| invalid("task count does not fit usize"))?,
        event_count: usize::try_from(decoder.retained_event_count())
            .map_err(|_| invalid("retained event count does not fit usize"))?,
        driver,
        outcome,
    })
}

fn decode_random_stream(payload: &[u8]) -> Result<RandomStreamFields, ExportError> {
    let message_header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let decoder = RandomStreamStateDecoder::default().header(message_header, 0);
    Ok(RandomStreamFields {
        tag: decoder.stream_tag(),
        state: decoder.state(),
        draws: decoder.draws(),
    })
}

fn decode_task(payload: &[u8]) -> Result<TaskSnapshotFields, ExportError> {
    let message_header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let decoder = TaskSnapshotDecoder::default().header(message_header, 0);
    let mut task_id = decoder.task_decoder();
    let (slot, generation) = (task_id.slot(), task_id.generation());
    let decoder = task_id
        .parent()
        .map_err(|_| invalid("task decoder lost its parent"))?;
    Ok(TaskSnapshotFields {
        slot,
        generation,
        state: decoder.state(),
    })
}

fn decode_event(frame: &[u8]) -> Result<TraceEvent, ExportError> {
    kr_runtime::trace::sbe::decode_event(frame)
        .map_err(|error| invalid(format!("invalid event frame: {error}")))
}

fn usize_to_u64(value: usize) -> Result<u64, ExportError> {
    u64::try_from(value).map_err(|_| invalid("count does not fit u64"))
}

fn invalid(message: impl Into<String>) -> ExportError {
    ExportError::InvalidBinaryArtifact(message.into())
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use kr_runtime::trace::RecordingTrace;
    use kr_runtime::{RuntimeConfig, SimDuration, SimRuntime};

    use super::super::{TraceArtifactMetadata, write_sbe_trace_artifact};
    use super::*;

    fn run_artifact(seed: u64, sleep_ns: u64, capacity: usize, second_root: bool) -> Vec<u8> {
        let trace = Rc::new(RecordingTrace::new(capacity));
        let mut runtime = SimRuntime::with_trace(
            RuntimeConfig {
                seed,
                ..RuntimeConfig::default()
            },
            trace.clone(),
        );
        let handle = runtime.handle();
        runtime
            .block_on(async move {
                handle
                    .sleep(SimDuration::from_nanos(sleep_ns))
                    .await
                    .expect("sleep completes");
            })
            .expect("root completes");
        if second_root {
            runtime.block_on(async {}).expect("second root completes");
        }
        let snapshot = runtime.snapshot();
        let mut binary = Vec::new();
        write_sbe_trace_artifact(
            &mut binary,
            &trace,
            &snapshot,
            TraceArtifactMetadata::new("diff-test/1", "completed"),
        )
        .expect("artifact encodes");
        binary
    }

    #[test]
    fn identical_reruns_diff_as_equivalent() {
        let first = run_artifact(7, 5, 128, false);
        let second = run_artifact(7, 5, 128, false);
        assert_eq!(first, second, "same-seed reruns must be byte-identical");
        assert_eq!(
            diff_sbe_trace_artifacts(&first, &second).expect("artifacts are comparable"),
            None,
        );
    }

    #[test]
    fn different_seeds_are_incomparable_not_divergent() {
        let first = run_artifact(7, 5, 128, false);
        let second = run_artifact(8, 5, 128, false);
        let error = diff_sbe_trace_artifacts(&first, &second)
            .expect_err("different seeds must be incomparable");
        let ExportError::IncomparableArtifacts { field, left, right } = error else {
            panic!("expected an incomparability error");
        };
        assert_eq!(field, "seed");
        assert_ne!(left, right);
    }

    #[test]
    fn behavioral_divergence_reports_the_first_differing_event() {
        let first = run_artifact(7, 5, 128, false);
        let second = run_artifact(7, 6, 128, false);
        let divergence = diff_sbe_trace_artifacts(&first, &second)
            .expect("artifacts are comparable")
            .expect("different sleeps must diverge");
        let TraceArtifactDivergence::Event { index, left, right } = &divergence else {
            panic!("expected a first-event divergence, got {divergence:?}");
        };
        assert_eq!(left.sequence, right.sequence, "divergence is in content");
        assert_ne!(left, right);
        let rendered = divergence.to_string();
        assert!(rendered.contains(&format!("retained event {index}")));
        assert!(rendered.contains(left.kind.tag().name()));
    }

    #[test]
    fn a_strict_prefix_reports_an_event_count_divergence() {
        let first = run_artifact(7, 5, 128, false);
        let second = run_artifact(7, 5, 128, true);
        let divergence = diff_sbe_trace_artifacts(&first, &second)
            .expect("artifacts are comparable")
            .expect("extra activity must diverge");
        match divergence {
            TraceArtifactDivergence::EventCount { left, right } => {
                assert!(left < right, "second run retains extra events");
            }
            TraceArtifactDivergence::Event { .. } | TraceArtifactDivergence::Terminal { .. } => {
                panic!("expected an event-count divergence, got {divergence:?}")
            }
        }
    }

    #[test]
    fn terminal_divergence_is_reported_when_no_events_are_retained() {
        let first = run_artifact(7, 5, 0, false);
        let second = run_artifact(7, 6, 0, false);
        let divergence = diff_sbe_trace_artifacts(&first, &second)
            .expect("artifacts are comparable")
            .expect("different terminal instants must diverge");
        let TraceArtifactDivergence::Terminal { field, left, right } = &divergence else {
            panic!("expected a terminal divergence, got {divergence:?}");
        };
        assert_eq!(*field, "instant");
        assert_ne!(left, right);
        assert!(divergence.to_string().contains("terminal instant differs"));
    }
}
