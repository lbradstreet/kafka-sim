use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::rc::Rc;

use kr_runtime::rng::RandomStream;
use kr_runtime::trace::{RecordingTrace, SamplingTrace, TraceEvent, sbe::SbeRecordingTrace};
use kr_runtime::{RuntimeSnapshot, TaskSnapshot, TaskState};
use kr_runtime_trace_wire::artifact_header_codec::{
    ArtifactHeaderDecoder, ArtifactHeaderEncoder, SBE_BLOCK_LENGTH as ARTIFACT_HEADER_BLOCK_LENGTH,
    SBE_TEMPLATE_ID as ARTIFACT_HEADER_TEMPLATE_ID,
};
use kr_runtime_trace_wire::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use kr_runtime_trace_wire::random_stream_state_codec::{
    RandomStreamStateDecoder, RandomStreamStateEncoder,
    SBE_BLOCK_LENGTH as RANDOM_STREAM_STATE_BLOCK_LENGTH,
    SBE_TEMPLATE_ID as RANDOM_STREAM_STATE_TEMPLATE_ID,
};
use kr_runtime_trace_wire::task_snapshot_codec::{
    SBE_BLOCK_LENGTH as TASK_SNAPSHOT_BLOCK_LENGTH, SBE_TEMPLATE_ID as TASK_SNAPSHOT_TEMPLATE_ID,
    TaskSnapshotDecoder, TaskSnapshotEncoder,
};
use kr_runtime_trace_wire::{ReadBuf, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, WriteBuf};

use super::{
    ExportError, SUPPORTED_TRACE_SCHEMA_VERSION, TRACE_ARTIFACT_SCHEMA_VERSION,
    TraceArtifactMetadata,
};

/// Magic bytes at the beginning of every binary trace artifact.
pub const SBE_ARTIFACT_MAGIC: [u8; 8] = *b"DSTRSBE\0";

/// Version of the binary container and its record ordering contract.
pub const SBE_ARTIFACT_CONTAINER_VERSION: u16 = 1;

pub(crate) const PREAMBLE_LENGTH: usize = 16;
pub(crate) const FRAME_LENGTH_PREFIX: usize = size_of::<u32>();
const CONTAINER_FLAGS: u16 = 0;
const CAPACITY_UNIT_EVENTS: u8 = 0;
const CAPACITY_UNIT_BYTES: u8 = 1;
const RETENTION_PREFIX: u8 = 0;
const RETENTION_TAIL: u8 = 1;
const RETENTION_PREFIX_AND_TAIL: u8 = 2;
const SAMPLING_NONE: u8 = 0;
const SAMPLING_PERIODIC: u8 = 1;
const FINGERPRINT_ALL_EVENTS: u8 = 0;
const FINGERPRINT_SAMPLED_EVENTS: u8 = 1;
const TASK_WAITING: u8 = 0;
const TASK_READY: u8 = 1;
const TASK_RUNNING: u8 = 2;
const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;
const MAX_RANDOM_STREAMS: usize = 5;
const MAX_TASK_SNAPSHOTS: usize = 1_000_000;

/// Writes a versioned binary SBE artifact for a retained trace.
///
/// The artifact is deterministic for equal inputs. It contains a fixed
/// container preamble followed by length-framed SBE messages: one artifact
/// header, the declared random-stream states, the declared live-task
/// snapshots, and the declared retained events.
///
/// # Errors
///
/// Returns [`ExportError`] if metadata cannot be represented by the schema or
/// writing the destination fails.
pub fn write_sbe_trace_artifact<W: Write>(
    writer: W,
    trace: &RecordingTrace,
    snapshot: &RuntimeSnapshot,
    metadata: TraceArtifactMetadata<'_>,
) -> Result<(), ExportError> {
    write_sbe_trace_artifact_with_sampling(writer, trace, None, snapshot, metadata)
}

/// Writes a versioned binary SBE artifact whose retained events came through
/// `sampling`.
///
/// # Errors
///
/// Returns [`ExportError::SamplingSinkMismatch`] unless `sampling` directly
/// wraps `trace`, or another [`ExportError`] if encoding or writing fails.
pub fn write_sampled_sbe_trace_artifact<W: Write>(
    writer: W,
    trace: &Rc<RecordingTrace>,
    sampling: &SamplingTrace,
    snapshot: &RuntimeSnapshot,
    metadata: TraceArtifactMetadata<'_>,
) -> Result<(), ExportError> {
    if !sampling.wraps(trace) {
        return Err(ExportError::SamplingSinkMismatch);
    }
    write_sbe_trace_artifact_with_sampling(
        writer,
        trace.as_ref(),
        Some(sampling),
        snapshot,
        metadata,
    )
}

fn write_sbe_trace_artifact_with_sampling<W: Write>(
    mut writer: W,
    trace: &RecordingTrace,
    sampling: Option<&SamplingTrace>,
    snapshot: &RuntimeSnapshot,
    metadata: TraceArtifactMetadata<'_>,
) -> Result<(), ExportError> {
    let events = trace.events();
    validate_snapshot_metadata(snapshot)?;
    validate_sampled_sequences(events.iter().map(|event| event.sequence), sampling)?;
    write_preamble(&mut writer)?;
    let trace_metadata = ArtifactTraceMetadata::from_typed(trace, events.len());
    let header = encode_artifact_header(&trace_metadata, sampling, snapshot, metadata)?;
    write_frame(&mut writer, &header)?;

    write_snapshot_metadata(&mut writer, snapshot)?;

    for event in &events {
        write_event_frame(&mut writer, event)?;
    }

    Ok(())
}

fn write_snapshot_metadata(
    writer: &mut impl Write,
    snapshot: &RuntimeSnapshot,
) -> Result<(), ExportError> {
    for random in &snapshot.random {
        let mut payload =
            vec![0; MESSAGE_HEADER_LENGTH + usize::from(RANDOM_STREAM_STATE_BLOCK_LENGTH)];
        let encoder = RandomStreamStateEncoder::default()
            .wrap(WriteBuf::new(&mut payload), MESSAGE_HEADER_LENGTH);
        let mut header = encoder.header(0);
        let mut encoder = header
            .parent()
            .map_err(|_| ExportError::BinaryEncoding("random-state encoder lost its parent"))?;
        encoder
            .stream_tag(random.stream as u64)
            .state(random.checkpoint.state())
            .draws(random.checkpoint.draws());
        write_frame(writer, &payload)?;
    }

    for task in &snapshot.tasks {
        let payload = encode_task_snapshot(task)?;
        write_frame(writer, &payload)?;
    }

    Ok(())
}

/// Writes a binary artifact directly from a byte-backed SBE recorder.
///
/// Retained event frames are passed from the recorder to the destination
/// without decoding, allocating, or re-encoding them.
///
/// # Errors
///
/// Returns [`ExportError::BufferedTraceEncodingFailures`] if any observed
/// event could not be encoded by the recorder, or another [`ExportError`] if
/// metadata encoding or destination writing fails.
pub fn write_buffered_sbe_trace_artifact<W: Write>(
    writer: W,
    trace: &SbeRecordingTrace,
    snapshot: &RuntimeSnapshot,
    metadata: TraceArtifactMetadata<'_>,
) -> Result<(), ExportError> {
    write_buffered_sbe_trace_artifact_with_sampling(writer, trace, None, snapshot, metadata)
}

/// Writes a sampled binary artifact directly from a byte-backed SBE recorder.
///
/// # Errors
///
/// Returns [`ExportError::SamplingSinkMismatch`] unless `sampling` directly
/// wraps `trace`, [`ExportError::BufferedTraceEncodingFailures`] if the
/// recorder observed an encoding failure, or another [`ExportError`] if
/// metadata encoding or destination writing fails.
pub fn write_sampled_buffered_sbe_trace_artifact<W: Write>(
    writer: W,
    trace: &Rc<SbeRecordingTrace>,
    sampling: &SamplingTrace,
    snapshot: &RuntimeSnapshot,
    metadata: TraceArtifactMetadata<'_>,
) -> Result<(), ExportError> {
    if !sampling.wraps(trace) {
        return Err(ExportError::SamplingSinkMismatch);
    }
    write_buffered_sbe_trace_artifact_with_sampling(
        writer,
        trace.as_ref(),
        Some(sampling),
        snapshot,
        metadata,
    )
}

fn write_buffered_sbe_trace_artifact_with_sampling<W: Write>(
    mut writer: W,
    trace: &SbeRecordingTrace,
    sampling: Option<&SamplingTrace>,
    snapshot: &RuntimeSnapshot,
    metadata: TraceArtifactMetadata<'_>,
) -> Result<(), ExportError> {
    let encoding_failures = trace.encoding_failures();
    if encoding_failures != 0 {
        return Err(ExportError::BufferedTraceEncodingFailures(
            encoding_failures,
        ));
    }
    validate_snapshot_metadata(snapshot)?;
    validate_buffered_sampled_sequences(trace, sampling)?;
    let trace_metadata = ArtifactTraceMetadata::from_buffered(trace);
    write_preamble(&mut writer)?;
    let header = encode_artifact_header(&trace_metadata, sampling, snapshot, metadata)?;
    write_frame(&mut writer, &header)?;

    write_snapshot_metadata(&mut writer, snapshot)?;
    trace.try_visit_encoded_records(|frame| writer.write_all(frame).map_err(ExportError::Io))
}

fn validate_snapshot_metadata(snapshot: &RuntimeSnapshot) -> Result<(), ExportError> {
    if snapshot.random.len() != MAX_RANDOM_STREAMS {
        return Err(ExportError::BinaryEncoding(
            "snapshot must contain every runtime random stream exactly once",
        ));
    }
    let mut random_streams = BTreeSet::new();
    for random in &snapshot.random {
        let tag = random.stream as u64;
        if !is_known_random_stream_tag(tag) {
            return Err(ExportError::BinaryEncoding(
                "random-stream snapshot has an unknown stream tag",
            ));
        }
        if !random_streams.insert(tag) {
            return Err(ExportError::BinaryEncoding(
                "duplicate random-stream snapshot",
            ));
        }
    }

    if snapshot.tasks.len() > MAX_TASK_SNAPSHOTS {
        return Err(ExportError::BinaryEncoding(
            "task snapshots exceed the one-million-task artifact bound",
        ));
    }
    if snapshot.tasks.len() > snapshot.reproduction.config.max_tasks {
        return Err(ExportError::BinaryEncoding(
            "task snapshots exceed the configured task bound",
        ));
    }
    let ready_tasks = snapshot
        .tasks
        .iter()
        .filter(|task| task.state == TaskState::Ready)
        .count();
    if snapshot.ready_tasks != ready_tasks {
        return Err(ExportError::BinaryEncoding(
            "ready task count disagrees with task snapshot states",
        ));
    }
    if snapshot.live_timers > snapshot.reproduction.config.max_timers {
        return Err(ExportError::BinaryEncoding(
            "live timer count exceeds the configured timer bound",
        ));
    }
    let mut task_ids = BTreeSet::new();
    for task in &snapshot.tasks {
        if !task_ids.insert(task.id) {
            return Err(ExportError::BinaryEncoding("duplicate task snapshot"));
        }
    }
    Ok(())
}

fn validate_sampled_sequences(
    sequences: impl IntoIterator<Item = u64>,
    sampling: Option<&SamplingTrace>,
) -> Result<(), ExportError> {
    let Some(sampling) = sampling else {
        return Ok(());
    };
    let period = sampling.period().get();
    let phase = sampling.phase();
    if sequences
        .into_iter()
        .any(|sequence| sequence % period != phase)
    {
        return Err(ExportError::BinaryEncoding(
            "retained event sequence violates periodic sampling metadata",
        ));
    }
    Ok(())
}

fn validate_buffered_sampled_sequences(
    trace: &SbeRecordingTrace,
    sampling: Option<&SamplingTrace>,
) -> Result<(), ExportError> {
    let Some(sampling) = sampling else {
        return Ok(());
    };
    let period = sampling.period().get();
    let phase = sampling.phase();
    trace.try_visit_encoded_records(|frame| {
        let sequence = frame
            .get(FRAME_LENGTH_PREFIX + MESSAGE_HEADER_LENGTH..)
            .and_then(|bytes| bytes.get(..size_of::<u64>()))
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or(ExportError::BinaryEncoding(
                "buffered event frame is too short to contain a sequence",
            ))?;
        if sequence % period != phase {
            return Err(ExportError::BinaryEncoding(
                "retained event sequence violates periodic sampling metadata",
            ));
        }
        Ok(())
    })
}

struct ArtifactTraceMetadata {
    event_count: usize,
    retention_mode: &'static str,
    capacity_unit: u8,
    prefix_capacity: usize,
    tail_capacity: usize,
    retained_bytes: usize,
    dropped_events: u64,
    last_sequence: Option<u64>,
    ordering_violation: Option<kr_runtime::trace::TraceOrderingViolation>,
    fingerprint: u64,
}

impl ArtifactTraceMetadata {
    fn from_typed(trace: &RecordingTrace, event_count: usize) -> Self {
        Self {
            event_count,
            retention_mode: trace.retention().mode_name(),
            capacity_unit: CAPACITY_UNIT_EVENTS,
            prefix_capacity: trace.retention().prefix_capacity(),
            tail_capacity: trace.retention().tail_capacity(),
            retained_bytes: 0,
            dropped_events: trace.dropped(),
            last_sequence: trace.last_sequence(),
            ordering_violation: trace.ordering_violation(),
            fingerprint: trace.fingerprint(),
        }
    }

    fn from_buffered(trace: &SbeRecordingTrace) -> Self {
        let retention = trace.retention();
        Self {
            event_count: trace.len(),
            retention_mode: retention.mode_name(),
            capacity_unit: CAPACITY_UNIT_BYTES,
            prefix_capacity: retention.prefix_capacity_bytes(),
            tail_capacity: retention.tail_capacity_bytes(),
            retained_bytes: trace.retained_bytes(),
            dropped_events: trace.dropped(),
            last_sequence: trace.last_sequence(),
            ordering_violation: trace.ordering_violation(),
            fingerprint: trace.fingerprint(),
        }
    }
}

fn write_preamble(writer: &mut impl Write) -> Result<(), ExportError> {
    let mut preamble = [0_u8; PREAMBLE_LENGTH];
    preamble[..8].copy_from_slice(&SBE_ARTIFACT_MAGIC);
    preamble[8..10].copy_from_slice(&SBE_ARTIFACT_CONTAINER_VERSION.to_le_bytes());
    preamble[10..12].copy_from_slice(&CONTAINER_FLAGS.to_le_bytes());
    preamble[12..16].copy_from_slice(&(PREAMBLE_LENGTH as u32).to_le_bytes());
    writer.write_all(&preamble).map_err(ExportError::Io)
}

fn encode_artifact_header(
    trace: &ArtifactTraceMetadata,
    sampling: Option<&SamplingTrace>,
    snapshot: &RuntimeSnapshot,
    metadata: TraceArtifactMetadata<'_>,
) -> Result<Vec<u8>, ExportError> {
    let variable_length = size_of::<u32>()
        .checked_add(metadata.driver.len())
        .and_then(|length| length.checked_add(size_of::<u32>()))
        .and_then(|length| length.checked_add(metadata.outcome.len()))
        .ok_or(ExportError::BinaryEncoding(
            "artifact header variable data length overflowed",
        ))?;
    let payload_length = MESSAGE_HEADER_LENGTH
        .checked_add(usize::from(ARTIFACT_HEADER_BLOCK_LENGTH))
        .and_then(|length| length.checked_add(variable_length))
        .ok_or(ExportError::BinaryEncoding(
            "artifact header frame length overflowed",
        ))?;
    validate_output_frame_length(payload_length)?;
    let random_count = u32::try_from(snapshot.random.len())
        .map_err(|_| ExportError::BinaryEncoding("too many random-stream snapshots"))?;
    if snapshot.tasks.len() > MAX_TASK_SNAPSHOTS {
        return Err(ExportError::BinaryEncoding(
            "task snapshots exceed the one-million-task artifact bound",
        ));
    }
    let task_count = u32::try_from(snapshot.tasks.len())
        .map_err(|_| ExportError::BinaryEncoding("too many task snapshots"))?;
    let checkpoint = snapshot.determinism_checkpoint();
    let retention_mode = match trace.retention_mode {
        "prefix" => RETENTION_PREFIX,
        "tail" => RETENTION_TAIL,
        "prefix_and_tail" => RETENTION_PREFIX_AND_TAIL,
        _ => return Err(ExportError::BinaryEncoding("unsupported retention mode")),
    };
    let (sampling_mode, sampling_algorithm, sampling_period, sampling_phase, fingerprint_scope) =
        sampling.map_or(
            (SAMPLING_NONE, 0, 0, 0, FINGERPRINT_ALL_EVENTS),
            |sampling| {
                (
                    SAMPLING_PERIODIC,
                    kr_runtime::trace::PERIODIC_SAMPLING_ALGORITHM_VERSION,
                    sampling.period().get(),
                    sampling.phase(),
                    FINGERPRINT_SAMPLED_EVENTS,
                )
            },
        );
    let max_time = snapshot.reproduction.config.max_time;
    let ordering = trace.ordering_violation;

    let mut payload = vec![0; payload_length];
    let encoder =
        ArtifactHeaderEncoder::default().wrap(WriteBuf::new(&mut payload), MESSAGE_HEADER_LENGTH);
    let mut message_header = encoder.header(0);
    let mut encoder = message_header
        .parent()
        .map_err(|_| ExportError::BinaryEncoding("artifact-header encoder lost its parent"))?;
    encoder
        .artifact_schema(TRACE_ARTIFACT_SCHEMA_VERSION)
        .runtime_reproduction_schema(snapshot.reproduction.schema_version)
        .determinism_checkpoint_schema(checkpoint.schema_version)
        .trace_schema(SUPPORTED_TRACE_SCHEMA_VERSION)
        .rng_version(snapshot.reproduction.rng_version)
        .seed(snapshot.reproduction.config.seed)
        .max_tasks(usize_to_u64(snapshot.reproduction.config.max_tasks)?)
        .max_timers(usize_to_u64(snapshot.reproduction.config.max_timers)?)
        .max_steps_per_run(snapshot.reproduction.config.max_steps_per_run)
        .max_time_present(u8::from(max_time.is_some()))
        .max_time_ns(max_time.map_or(0, kr_runtime::SimInstant::as_nanos))
        .retained_event_count(usize_to_u64(trace.event_count)?)
        .retention_mode(retention_mode)
        .capacity_unit(trace.capacity_unit)
        .prefix_capacity(usize_to_u64(trace.prefix_capacity)?)
        .tail_capacity(usize_to_u64(trace.tail_capacity)?)
        .retained_bytes(usize_to_u64(trace.retained_bytes)?)
        .sampling_mode(sampling_mode)
        .sampling_algorithm_version(sampling_algorithm)
        .sampling_period(sampling_period)
        .sampling_phase(sampling_phase)
        .fingerprint_scope(fingerprint_scope)
        .dropped_events(trace.dropped_events)
        .last_sequence_present(u8::from(trace.last_sequence.is_some()))
        .last_sequence(trace.last_sequence.unwrap_or(0))
        .ordering_violation_present(u8::from(trace.ordering_violation.is_some()))
        .previous_sequence(ordering.map_or(0, |value| value.previous_sequence))
        .rejected_sequence(ordering.map_or(0, |value| value.rejected_sequence))
        .trace_fingerprint(trace.fingerprint)
        .now_ns(snapshot.now.as_nanos())
        .total_steps(snapshot.total_steps)
        .next_enqueue_sequence(snapshot.next_enqueue_sequence)
        .next_timer_sequence(snapshot.next_timer_sequence)
        .next_timer_id(snapshot.next_timer_id)
        .ready_tasks(usize_to_u64(snapshot.ready_tasks)?)
        .live_timers(usize_to_u64(snapshot.live_timers)?)
        .live_tasks(usize_to_u64(snapshot.tasks.len())?)
        .stopped(u8::from(snapshot.stopped))
        .random_stream_count(random_count)
        .task_count(task_count)
        .start_time_ns(snapshot.reproduction.config.start_time.as_nanos())
        .driver(metadata.driver)
        .outcome(metadata.outcome);

    debug_assert_eq!(
        encoder.encoded_length() + MESSAGE_HEADER_LENGTH,
        payload.len()
    );
    Ok(payload)
}

fn encode_task_snapshot(task: &TaskSnapshot) -> Result<Vec<u8>, ExportError> {
    let payload_length = MESSAGE_HEADER_LENGTH
        .checked_add(usize::from(TASK_SNAPSHOT_BLOCK_LENGTH))
        .ok_or(ExportError::BinaryEncoding(
            "task snapshot frame length overflowed",
        ))?;
    validate_output_frame_length(payload_length)?;
    let state = match task.state {
        TaskState::Waiting => TASK_WAITING,
        TaskState::Ready => TASK_READY,
        TaskState::Running => TASK_RUNNING,
    };
    let mut payload = vec![0; payload_length];
    let encoder =
        TaskSnapshotEncoder::default().wrap(WriteBuf::new(&mut payload), MESSAGE_HEADER_LENGTH);
    let mut message_header = encoder.header(0);
    let mut encoder = message_header
        .parent()
        .map_err(|_| ExportError::BinaryEncoding("task encoder lost its parent"))?;
    encoder.state(state);
    let mut task_id = encoder.task_encoder();
    task_id
        .slot(task.id.slot())
        .generation(task.id.generation());
    let encoder = task_id
        .parent()
        .map_err(|_| ExportError::BinaryEncoding("task ID encoder lost its parent"))?;

    debug_assert_eq!(
        encoder.encoded_length() + MESSAGE_HEADER_LENGTH,
        payload.len()
    );
    Ok(payload)
}

fn usize_to_u64(value: usize) -> Result<u64, ExportError> {
    u64::try_from(value)
        .map_err(|_| ExportError::BinaryEncoding("platform usize does not fit the SBE uint64"))
}

fn validate_output_frame_length(length: usize) -> Result<(), ExportError> {
    let framed_length = length
        .checked_add(FRAME_LENGTH_PREFIX)
        .ok_or(ExportError::BinaryEncoding("SBE frame length overflowed"))?;
    if framed_length > MAX_FRAME_LENGTH {
        return Err(ExportError::BinaryEncoding(
            "an SBE frame exceeds the 16 MiB container limit",
        ));
    }
    u32::try_from(framed_length)
        .map(|_| ())
        .map_err(|_| ExportError::BinaryEncoding("an SBE frame exceeds uint32 framing"))
}

fn write_frame(writer: &mut impl Write, payload: &[u8]) -> Result<(), ExportError> {
    validate_output_frame_length(payload.len())?;
    let framed_length = payload
        .len()
        .checked_add(FRAME_LENGTH_PREFIX)
        .ok_or(ExportError::BinaryEncoding("SBE frame length overflowed"))?;
    let length = u32::try_from(framed_length)
        .map_err(|_| ExportError::BinaryEncoding("an SBE frame exceeds uint32 framing"))?;
    writer
        .write_all(&length.to_le_bytes())
        .map_err(ExportError::Io)?;
    writer.write_all(payload).map_err(ExportError::Io)
}

fn write_event_frame(writer: &mut impl Write, event: &TraceEvent) -> Result<(), ExportError> {
    let length = kr_runtime::trace::sbe::encoded_event_length(event)
        .map_err(|error| ExportError::BinaryEventEncoding(error.to_string()))?;
    if length > MAX_FRAME_LENGTH {
        return Err(ExportError::BinaryEncoding(
            "an event frame exceeds the 16 MiB container limit",
        ));
    }
    let mut frame = vec![0; length];
    let written = kr_runtime::trace::sbe::encode_event(event, &mut frame)
        .map_err(|error| ExportError::BinaryEventEncoding(error.to_string()))?;
    if written != frame.len() {
        return Err(ExportError::BinaryEncoding(
            "event encoder returned an inconsistent frame length",
        ));
    }
    writer.write_all(&frame).map_err(ExportError::Io)
}

/// Strictly validates one binary SBE trace artifact without retaining event
/// frames in memory.
///
/// # Errors
///
/// Returns [`ExportError`] for malformed, unsupported, truncated, or trailing
/// binary data or source I/O failure.
pub fn validate_sbe_trace_artifact<R: Read>(mut reader: R) -> Result<(), ExportError> {
    read_preamble(&mut reader)?;
    let header_frame = read_frame(&mut reader, "artifact header")?;
    let decoded = decode_artifact_header(&header_frame)?;
    let mut random_tags = BTreeSet::new();
    for _ in 0..decoded.random_stream_count {
        let frame = read_frame(&mut reader, "random-stream state")?;
        let stream_tag = decode_random_stream_state(&frame)?;
        if !random_tags.insert(stream_tag) {
            return Err(invalid("duplicate random-stream state"));
        }
    }
    let mut task_ids = BTreeSet::new();
    let mut ready_tasks = 0_usize;
    for _ in 0..decoded.task_count {
        let frame = read_frame(&mut reader, "task snapshot")?;
        let task = decode_task_snapshot(&frame)?;
        if !task_ids.insert(task.id) {
            return Err(invalid("duplicate task snapshot"));
        }
        ready_tasks += usize::from(task.ready);
    }
    if ready_tasks != decoded.ready_tasks {
        return Err(invalid(
            "ready task count disagrees with task snapshot states",
        ));
    }

    let mut retained_event_bytes = 0_usize;
    let mut previous_sequence = None;
    for _ in 0..decoded.event_count {
        let frame = read_event_frame(&mut reader)?;
        retained_event_bytes = retained_event_bytes
            .checked_add(frame.len())
            .ok_or_else(|| invalid("retained event byte count overflowed"))?;
        let event = decode_event_frame(&frame)?;
        if let Some(previous) = previous_sequence
            && event.sequence <= previous
        {
            return Err(invalid(format!(
                "event sequences must increase strictly; {} follows {previous}",
                event.sequence
            )));
        }
        if decoded
            .sampling
            .is_some_and(|sampling| event.sequence % sampling.period != sampling.phase)
        {
            return Err(invalid(format!(
                "event sequence {} violates periodic sampling metadata",
                event.sequence
            )));
        }
        previous_sequence = Some(event.sequence);
    }
    if decoded
        .retained_bytes
        .is_some_and(|declared| declared != retained_event_bytes)
    {
        return Err(invalid(format!(
            "declared retained bytes {} disagree with {} encoded event bytes",
            decoded.retained_bytes.unwrap_or(0),
            retained_event_bytes
        )));
    }
    reject_trailing_data(&mut reader)?;
    Ok(())
}

fn decode_event_frame(frame: &[u8]) -> Result<TraceEvent, ExportError> {
    kr_runtime::trace::sbe::decode_event(frame)
        .map_err(|error| invalid(format!("invalid event frame: {error}")))
}

fn read_preamble(reader: &mut impl Read) -> Result<(), ExportError> {
    let mut preamble = [0_u8; PREAMBLE_LENGTH];
    read_exact_record(reader, &mut preamble, "container preamble")?;
    if preamble[..8] != SBE_ARTIFACT_MAGIC {
        return Err(invalid("bad container magic"));
    }
    if u16::from_le_bytes([preamble[8], preamble[9]]) != SBE_ARTIFACT_CONTAINER_VERSION {
        return Err(invalid("unsupported container version"));
    }
    if u16::from_le_bytes([preamble[10], preamble[11]]) != CONTAINER_FLAGS {
        return Err(invalid("unsupported container flags"));
    }
    if u32::from_le_bytes([preamble[12], preamble[13], preamble[14], preamble[15]])
        != PREAMBLE_LENGTH as u32
    {
        return Err(invalid("unsupported container header length"));
    }
    Ok(())
}

fn read_frame(reader: &mut impl Read, description: &str) -> Result<Vec<u8>, ExportError> {
    let mut length = [0_u8; FRAME_LENGTH_PREFIX];
    read_exact_record(reader, &mut length, description)?;
    let length = usize::try_from(u32::from_le_bytes(length))
        .map_err(|_| invalid(format!("{description} length does not fit usize")))?;
    let minimum = FRAME_LENGTH_PREFIX + MESSAGE_HEADER_LENGTH;
    if !(minimum..=MAX_FRAME_LENGTH).contains(&length) {
        return Err(invalid(format!(
            "{description} length {length} is outside {}..={MAX_FRAME_LENGTH}",
            minimum
        )));
    }
    let mut payload = vec![0; length - FRAME_LENGTH_PREFIX];
    read_exact_record(reader, &mut payload, description)?;
    Ok(payload)
}

fn read_event_frame(reader: &mut impl Read) -> Result<Vec<u8>, ExportError> {
    let mut prefix = [0_u8; FRAME_LENGTH_PREFIX];
    read_exact_record(reader, &mut prefix, "event")?;
    let length = usize::try_from(u32::from_le_bytes(prefix))
        .map_err(|_| invalid("event frame length does not fit usize"))?;
    let maximum = kr_runtime::trace::sbe::MAX_EVENT_FRAME_LENGTH.min(MAX_FRAME_LENGTH);
    if !(kr_runtime::trace::sbe::MIN_EVENT_FRAME_LENGTH..=maximum).contains(&length) {
        return Err(invalid(format!(
            "event frame length {length} is outside {}..={}",
            kr_runtime::trace::sbe::MIN_EVENT_FRAME_LENGTH,
            maximum
        )));
    }
    let mut frame = vec![0; length];
    frame[..FRAME_LENGTH_PREFIX].copy_from_slice(&prefix);
    read_exact_record(reader, &mut frame[FRAME_LENGTH_PREFIX..], "event")?;
    Ok(frame)
}

fn read_exact_record(
    reader: &mut impl Read,
    buffer: &mut [u8],
    description: &str,
) -> Result<(), ExportError> {
    reader.read_exact(buffer).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            invalid(format!("truncated {description}"))
        } else {
            ExportError::Io(error)
        }
    })
}

fn reject_trailing_data(reader: &mut impl Read) -> Result<(), ExportError> {
    let mut byte = [0_u8; 1];
    match reader.read(&mut byte) {
        Ok(0) => Ok(()),
        Ok(_) => Err(invalid("trailing data after declared event frames")),
        Err(error) => Err(ExportError::Io(error)),
    }
}

fn validate_message_header(
    payload: &[u8],
    expected_template: u16,
    expected_block_length: u16,
    description: &str,
) -> Result<(), ExportError> {
    if payload.len() < MESSAGE_HEADER_LENGTH {
        return Err(invalid(format!("truncated {description} SBE header")));
    }
    let block_length = read_u16(payload, 0);
    let template_id = read_u16(payload, 2);
    let schema_id = read_u16(payload, 4);
    let version = read_u16(payload, 6);
    if template_id != expected_template {
        return Err(invalid(format!(
            "unexpected {description} template ID {template_id}"
        )));
    }
    if schema_id != SBE_SCHEMA_ID {
        return Err(invalid(format!(
            "unsupported {description} SBE schema ID {schema_id}"
        )));
    }
    if version != SBE_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported {description} SBE schema version {version}"
        )));
    }
    if block_length != expected_block_length {
        return Err(invalid(format!(
            "unexpected {description} block length {block_length}"
        )));
    }
    Ok(())
}

fn validate_variable_tail(
    payload: &[u8],
    fixed_end: usize,
    fields: usize,
    description: &str,
) -> Result<(), ExportError> {
    let mut cursor = fixed_end;
    for _ in 0..fields {
        let length_end = cursor
            .checked_add(size_of::<u32>())
            .ok_or_else(|| invalid(format!("{description} variable length overflowed")))?;
        if length_end > payload.len() {
            return Err(invalid(format!(
                "truncated {description} variable-data length"
            )));
        }
        let length = usize::try_from(read_u32(payload, cursor))
            .map_err(|_| invalid(format!("{description} variable data does not fit usize")))?;
        cursor = length_end
            .checked_add(length)
            .ok_or_else(|| invalid(format!("{description} variable data overflowed")))?;
        if cursor > payload.len() {
            return Err(invalid(format!("truncated {description} variable data")));
        }
    }
    if cursor != payload.len() {
        return Err(invalid(format!(
            "trailing bytes inside {description} frame"
        )));
    }
    Ok(())
}

struct DecodedArtifactHeader {
    event_count: usize,
    retained_bytes: Option<usize>,
    sampling: Option<DecodedSampling>,
    random_stream_count: usize,
    task_count: usize,
    ready_tasks: usize,
}

#[derive(Clone, Copy)]
struct DecodedSampling {
    period: u64,
    phase: u64,
}

fn decode_artifact_header(payload: &[u8]) -> Result<DecodedArtifactHeader, ExportError> {
    validate_message_header(
        payload,
        ARTIFACT_HEADER_TEMPLATE_ID,
        ARTIFACT_HEADER_BLOCK_LENGTH,
        "artifact header",
    )?;
    validate_variable_tail(
        payload,
        MESSAGE_HEADER_LENGTH + usize::from(ARTIFACT_HEADER_BLOCK_LENGTH),
        2,
        "artifact header",
    )?;
    let message_header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let mut decoder = ArtifactHeaderDecoder::default().header(message_header, 0);

    if decoder.artifact_schema() != TRACE_ARTIFACT_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported artifact schema {}",
            decoder.artifact_schema()
        )));
    }
    if decoder.trace_schema() != SUPPORTED_TRACE_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported trace schema {}",
            decoder.trace_schema()
        )));
    }
    let max_tasks = decode_usize(decoder.max_tasks(), "max_tasks")?;
    let max_timers = decode_usize(decoder.max_timers(), "max_timers")?;
    let event_count = decode_usize(decoder.retained_event_count(), "retained event count")?;
    let (event_count_capacity, retained_bytes) = match decoder.capacity_unit() {
        CAPACITY_UNIT_EVENTS => {
            if decoder.retained_bytes() != 0 {
                return Err(invalid(
                    "event-count artifact has a nonzero retained byte count",
                ));
            }
            (true, None)
        }
        CAPACITY_UNIT_BYTES => (
            false,
            Some(decode_usize(decoder.retained_bytes(), "retained bytes")?),
        ),
        value => return Err(invalid(format!("unknown trace capacity unit {value}"))),
    };
    let prefix_capacity = decode_usize(decoder.prefix_capacity(), "prefix capacity")?;
    let tail_capacity = decode_usize(decoder.tail_capacity(), "tail capacity")?;
    let capacity = prefix_capacity
        .checked_add(tail_capacity)
        .ok_or_else(|| invalid("combined retention capacity overflowed"))?;
    match decoder.retention_mode() {
        RETENTION_PREFIX if tail_capacity == 0 => {}
        RETENTION_TAIL if prefix_capacity == 0 => {}
        RETENTION_PREFIX_AND_TAIL => {}
        RETENTION_PREFIX | RETENTION_TAIL => {
            return Err(invalid("retention mode disagrees with its capacities"));
        }
        value => return Err(invalid(format!("unknown retention mode {value}"))),
    }
    if event_count_capacity && event_count > capacity {
        return Err(invalid("retained event count exceeds retention capacity"));
    }
    if retained_bytes.is_some_and(|retained| retained > capacity) {
        return Err(invalid("retained bytes exceed byte retention capacity"));
    }

    let sampling = match decoder.sampling_mode() {
        SAMPLING_NONE => {
            if decoder.sampling_algorithm_version() != 0
                || decoder.sampling_period() != 0
                || decoder.sampling_phase() != 0
                || decoder.fingerprint_scope() != FINGERPRINT_ALL_EVENTS
            {
                return Err(invalid("noncanonical disabled sampling metadata"));
            }
            None
        }
        SAMPLING_PERIODIC => {
            let period = decoder.sampling_period();
            if decoder.sampling_algorithm_version()
                != kr_runtime::trace::PERIODIC_SAMPLING_ALGORITHM_VERSION
                || period == 0
                || decoder.sampling_phase() >= period
                || decoder.fingerprint_scope() != FINGERPRINT_SAMPLED_EVENTS
            {
                return Err(invalid("invalid periodic sampling metadata"));
            }
            Some(DecodedSampling {
                period,
                phase: decoder.sampling_phase(),
            })
        }
        value => return Err(invalid(format!("unknown sampling mode {value}"))),
    };
    decode_optional_u64(
        decoder.max_time_present(),
        decoder.max_time_ns(),
        "max time",
    )?;
    decode_optional_u64(
        decoder.last_sequence_present(),
        decoder.last_sequence(),
        "last sequence",
    )?;
    let ordering_present = decode_bool(
        decoder.ordering_violation_present(),
        "ordering-violation presence",
    )?;
    if !ordering_present && (decoder.previous_sequence() != 0 || decoder.rejected_sequence() != 0) {
        return Err(invalid(
            "absent ordering violation has nonzero sequence values",
        ));
    }
    decode_bool(decoder.stopped(), "stopped")?;
    let random_stream_count = usize::try_from(decoder.random_stream_count())
        .map_err(|_| invalid("random-stream count does not fit usize"))?;
    if random_stream_count != MAX_RANDOM_STREAMS {
        return Err(invalid(
            "random-stream count must equal the complete runtime stream set",
        ));
    }
    let task_count = usize::try_from(decoder.task_count())
        .map_err(|_| invalid("task count does not fit usize"))?;
    if task_count > MAX_TASK_SNAPSHOTS {
        return Err(invalid(
            "task count exceeds the one-million-task artifact bound",
        ));
    }
    let live_tasks = decode_usize(decoder.live_tasks(), "live task count")?;
    if task_count != live_tasks || task_count > max_tasks {
        return Err(invalid(
            "declared task frames disagree with the bounded live-task count",
        ));
    }
    let ready_tasks = decode_usize(decoder.ready_tasks(), "ready task count")?;
    if ready_tasks > live_tasks {
        return Err(invalid("ready task count exceeds live task count"));
    }
    let live_timers = decode_usize(decoder.live_timers(), "live timer count")?;
    if live_timers > max_timers {
        return Err(invalid("live timer count exceeds configured maximum"));
    }
    if decoder.start_time_ns() > decoder.now_ns() {
        return Err(invalid("start time exceeds the terminal instant"));
    }

    let driver_coordinates = decoder.driver_decoder();
    std::str::from_utf8(decoder.driver_slice(driver_coordinates))
        .map_err(|_| invalid("artifact driver is not UTF-8"))?;
    let outcome_coordinates = decoder.outcome_decoder();
    std::str::from_utf8(decoder.outcome_slice(outcome_coordinates))
        .map_err(|_| invalid("artifact outcome is not UTF-8"))?;

    Ok(DecodedArtifactHeader {
        event_count,
        retained_bytes,
        sampling,
        random_stream_count,
        task_count,
        ready_tasks,
    })
}

fn decode_random_stream_state(payload: &[u8]) -> Result<u64, ExportError> {
    validate_message_header(
        payload,
        RANDOM_STREAM_STATE_TEMPLATE_ID,
        RANDOM_STREAM_STATE_BLOCK_LENGTH,
        "random-stream state",
    )?;
    let expected_length = MESSAGE_HEADER_LENGTH + usize::from(RANDOM_STREAM_STATE_BLOCK_LENGTH);
    if payload.len() != expected_length {
        return Err(invalid("random-stream state frame has trailing bytes"));
    }
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let decoder = RandomStreamStateDecoder::default().header(header, 0);
    let stream_tag = decoder.stream_tag();
    if !is_known_random_stream_tag(stream_tag) {
        return Err(invalid(format!(
            "unknown random-stream tag {stream_tag:#018x}"
        )));
    }
    Ok(stream_tag)
}

struct DecodedTaskSnapshot {
    id: (u32, u32),
    ready: bool,
}

fn decode_task_snapshot(payload: &[u8]) -> Result<DecodedTaskSnapshot, ExportError> {
    validate_message_header(
        payload,
        TASK_SNAPSHOT_TEMPLATE_ID,
        TASK_SNAPSHOT_BLOCK_LENGTH,
        "task snapshot",
    )?;
    let expected_length = MESSAGE_HEADER_LENGTH + usize::from(TASK_SNAPSHOT_BLOCK_LENGTH);
    if payload.len() != expected_length {
        return Err(invalid("task snapshot frame has trailing bytes"));
    }
    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let decoder = TaskSnapshotDecoder::default().header(header, 0);
    let mut task_id = decoder.task_decoder();
    let id = (task_id.slot(), task_id.generation());
    let decoder = task_id
        .parent()
        .map_err(|_| invalid("task decoder lost its parent"))?;
    let state = decoder.state();
    match state {
        TASK_WAITING | TASK_READY | TASK_RUNNING => {}
        value => return Err(invalid(format!("unknown task state {value}"))),
    }
    Ok(DecodedTaskSnapshot {
        id,
        ready: state == TASK_READY,
    })
}

fn decode_optional_u64(
    present: u8,
    value: u64,
    description: &str,
) -> Result<Option<u64>, ExportError> {
    if decode_bool(present, description)? {
        Ok(Some(value))
    } else if value == 0 {
        Ok(None)
    } else {
        Err(invalid(format!("absent {description} has a nonzero value")))
    }
}

fn decode_bool(value: u8, description: &str) -> Result<bool, ExportError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(invalid(format!("invalid {description} flag {value}"))),
    }
}

fn decode_usize(value: u64, description: &str) -> Result<usize, ExportError> {
    usize::try_from(value).map_err(|_| invalid(format!("{description} does not fit usize")))
}

fn is_known_random_stream_tag(tag: u64) -> bool {
    [
        RandomStream::Schedule,
        RandomStream::Scenario,
        RandomStream::Workload,
        RandomStream::Fault,
        RandomStream::Debug,
    ]
    .into_iter()
    .any(|stream| stream as u64 == tag)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn invalid(message: impl Into<String>) -> ExportError {
    ExportError::InvalidBinaryArtifact(message.into())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use kr_runtime::trace::{
        EventKind, SamplingTrace, TraceEvent, TraceSink, sbe::SbeRecordingTrace,
    };
    use kr_runtime::{PanicRecord, SimInstant};

    use super::*;
    use crate::browser_sbe_fixture::{DRIVER, OUTCOME, browser_sbe_fixture};

    const BROWSER_SBE_GOLDEN: &[u8] = include_bytes!("../testdata/browser-sbe-c1-s1v1-a8-t5.sbe");

    fn current_browser_golden_stem() -> String {
        format!(
            "browser-sbe-c{}-s{}v{}-a{}-t{}",
            SBE_ARTIFACT_CONTAINER_VERSION,
            SBE_SCHEMA_ID,
            SBE_SCHEMA_VERSION,
            TRACE_ARTIFACT_SCHEMA_VERSION,
            SUPPORTED_TRACE_SCHEMA_VERSION,
        )
    }

    fn build_browser_golden() -> Vec<u8> {
        let fixture = browser_sbe_fixture();
        let (trace, sampling) = fixture.buffered_trace();
        let mut binary = Vec::new();
        write_sampled_buffered_sbe_trace_artifact(
            &mut binary,
            &trace,
            &sampling,
            &fixture.snapshot,
            TraceArtifactMetadata::new(DRIVER, OUTCOME),
        )
        .expect("browser SBE fixture encodes");
        binary
    }

    #[test]
    fn browser_sbe_golden_matches_current_writer_and_validates() {
        assert_eq!(
            current_browser_golden_stem(),
            "browser-sbe-c1-s1v1-a8-t5",
            "schema coordinates changed; rename the version-labelled golden",
        );
        let binary = build_browser_golden();
        assert_eq!(
            binary, BROWSER_SBE_GOLDEN,
            "SBE golden changed; regenerate it after reviewing the format change",
        );
        validate_sbe_trace_artifact(binary.as_slice())
            .expect("current browser SBE golden validates");
    }

    #[test]
    fn typed_and_buffered_sbe_writers_are_deterministic_and_strictly_valid() {
        let fixture = browser_sbe_fixture();
        let metadata = TraceArtifactMetadata::new("deterministic/雪", "completed");

        let (typed, typed_sampling) = fixture.typed_trace();
        let mut typed_first = Vec::new();
        write_sampled_sbe_trace_artifact(
            &mut typed_first,
            &typed,
            &typed_sampling,
            &fixture.snapshot,
            metadata,
        )
        .expect("typed SBE artifact encodes");
        let mut typed_second = Vec::new();
        write_sampled_sbe_trace_artifact(
            &mut typed_second,
            &typed,
            &typed_sampling,
            &fixture.snapshot,
            metadata,
        )
        .expect("typed SBE artifact re-encodes");
        assert_eq!(typed_first, typed_second);
        validate_sbe_trace_artifact(typed_first.as_slice()).expect("typed SBE artifact validates");

        let (buffered, buffered_sampling) = fixture.buffered_trace();
        let mut buffered_binary = Vec::new();
        write_sampled_buffered_sbe_trace_artifact(
            &mut buffered_binary,
            &buffered,
            &buffered_sampling,
            &fixture.snapshot,
            metadata,
        )
        .expect("buffered SBE artifact encodes");
        validate_sbe_trace_artifact(buffered_binary.as_slice())
            .expect("buffered SBE artifact validates");
    }

    #[test]
    fn sampling_writer_rejects_an_unrelated_sink() {
        let fixture = browser_sbe_fixture();
        let (trace, _) = fixture.typed_trace();
        let unrelated = SamplingTrace::new(
            std::rc::Rc::new(kr_runtime::trace::RecordingTrace::new(1)),
            NonZeroU64::new(1).expect("nonzero period"),
        );
        let error = write_sampled_sbe_trace_artifact(
            Vec::new(),
            &trace,
            &unrelated,
            &fixture.snapshot,
            TraceArtifactMetadata::new("test/1", "failed"),
        )
        .expect_err("unrelated sampling sink must be rejected");
        assert!(matches!(error, ExportError::SamplingSinkMismatch));
    }

    #[test]
    fn buffered_writer_rejects_encoding_failures_before_writing() {
        let fixture = browser_sbe_fixture();
        let task = fixture
            .events
            .iter()
            .find_map(TraceEvent::task_id)
            .expect("fixture task ID");
        let trace = SbeRecordingTrace::new(64 * 1_024);
        trace.record(TraceEvent::new(
            0,
            SimInstant::ZERO,
            EventKind::TaskPanicked {
                task,
                panic: PanicRecord {
                    message: "x".repeat(kr_runtime::MAX_PANIC_MESSAGE_BYTES + 1),
                    message_truncated: false,
                },
            },
        ));
        assert_eq!(trace.encoding_failures(), 1);

        let mut destination = Vec::new();
        let error = write_buffered_sbe_trace_artifact(
            &mut destination,
            &trace,
            &fixture.snapshot,
            TraceArtifactMetadata::new("test/1", "failed"),
        )
        .expect_err("encoding failures must prevent artifact publication");
        assert!(matches!(
            error,
            ExportError::BufferedTraceEncodingFailures(1)
        ));
        assert!(destination.is_empty());
    }

    #[test]
    fn strict_validator_rejects_every_truncation_and_trailing_data() {
        let binary = build_browser_golden();
        for end in 0..binary.len() {
            assert!(
                validate_sbe_trace_artifact(&binary[..end]).is_err(),
                "truncation at byte {end} unexpectedly validated",
            );
        }

        let mut trailing = binary;
        trailing.push(0);
        assert!(validate_sbe_trace_artifact(trailing.as_slice()).is_err());
    }

    #[test]
    fn strict_validator_rejects_corrupt_container_coordinates() {
        let binary = build_browser_golden();
        for offset in [0, 8, 10, 12] {
            let mut corrupt = binary.clone();
            corrupt[offset] ^= 0xff;
            assert!(
                validate_sbe_trace_artifact(corrupt.as_slice()).is_err(),
                "corruption at container byte {offset} unexpectedly validated",
            );
        }
    }

    #[test]
    fn writers_reject_noncanonical_snapshot_metadata_before_output() {
        let fixture = browser_sbe_fixture();
        let (trace, _) = fixture.typed_trace();
        let metadata = TraceArtifactMetadata::new("test/1", "failed");

        let mut duplicate_random = fixture.snapshot.clone();
        duplicate_random.random[1] = duplicate_random.random[0];
        let mut output = Vec::new();
        let error = write_sbe_trace_artifact(&mut output, &trace, &duplicate_random, metadata)
            .expect_err("duplicate random snapshot must be rejected");
        assert!(matches!(error, ExportError::BinaryEncoding(_)));
        assert!(output.is_empty());

        let mut missing_random = fixture.snapshot.clone();
        missing_random.random.pop();
        let mut output = Vec::new();
        let error = write_sbe_trace_artifact(&mut output, &trace, &missing_random, metadata)
            .expect_err("incomplete random snapshot set must be rejected");
        assert!(matches!(error, ExportError::BinaryEncoding(_)));
        assert!(output.is_empty());

        let mut inconsistent_ready_count = fixture.snapshot.clone();
        let actual_ready = inconsistent_ready_count
            .tasks
            .iter()
            .filter(|task| task.state == TaskState::Ready)
            .count();
        inconsistent_ready_count.ready_tasks = usize::from(actual_ready == 0);
        let mut output = Vec::new();
        let error =
            write_sbe_trace_artifact(&mut output, &trace, &inconsistent_ready_count, metadata)
                .expect_err("ready count inconsistent with task states must be rejected");
        assert!(matches!(error, ExportError::BinaryEncoding(_)));
        assert!(output.is_empty());

        let mut duplicate_task = fixture.snapshot.clone();
        duplicate_task.tasks.push(duplicate_task.tasks[0].clone());
        let mut output = Vec::new();
        let error = write_sbe_trace_artifact(&mut output, &trace, &duplicate_task, metadata)
            .expect_err("duplicate task snapshot must be rejected");
        assert!(matches!(error, ExportError::BinaryEncoding(_)));
        assert!(output.is_empty());
    }

    #[test]
    fn strict_validator_cross_checks_complete_snapshot_metadata() {
        const READY_TASKS_BODY_OFFSET: usize = 199;
        const RANDOM_STREAM_COUNT_BODY_OFFSET: usize = 224;
        const HEADER_BODY_OFFSET: usize =
            PREAMBLE_LENGTH + FRAME_LENGTH_PREFIX + MESSAGE_HEADER_LENGTH;

        let mut missing_random = build_browser_golden();
        assert_eq!(
            read_u32(
                &missing_random,
                HEADER_BODY_OFFSET + RANDOM_STREAM_COUNT_BODY_OFFSET,
            ),
            MAX_RANDOM_STREAMS as u32,
        );
        missing_random[HEADER_BODY_OFFSET + RANDOM_STREAM_COUNT_BODY_OFFSET
            ..HEADER_BODY_OFFSET + RANDOM_STREAM_COUNT_BODY_OFFSET + size_of::<u32>()]
            .copy_from_slice(&4_u32.to_le_bytes());
        let error = validate_sbe_trace_artifact(missing_random.as_slice())
            .expect_err("incomplete random stream set must be rejected");
        assert!(error.to_string().contains("complete runtime stream set"));

        let mut duplicate_random = build_browser_golden();
        let header_frame_length = usize::try_from(read_u32(&duplicate_random, PREAMBLE_LENGTH))
            .expect("header frame length fits usize");
        let first_random_frame = PREAMBLE_LENGTH + header_frame_length;
        let first_random_frame_length =
            usize::try_from(read_u32(&duplicate_random, first_random_frame))
                .expect("random frame length fits usize");
        let second_random_frame = first_random_frame + first_random_frame_length;
        let first_tag = first_random_frame + FRAME_LENGTH_PREFIX + MESSAGE_HEADER_LENGTH;
        let second_tag = second_random_frame + FRAME_LENGTH_PREFIX + MESSAGE_HEADER_LENGTH;
        assert_ne!(
            &duplicate_random[first_tag..first_tag + size_of::<u64>()],
            &duplicate_random[second_tag..second_tag + size_of::<u64>()],
        );
        duplicate_random.copy_within(first_tag..first_tag + size_of::<u64>(), second_tag);
        let error = validate_sbe_trace_artifact(duplicate_random.as_slice())
            .expect_err("duplicate random stream must be rejected");
        assert!(error.to_string().contains("duplicate random-stream"));

        let mut inconsistent_ready_count = build_browser_golden();
        assert_eq!(
            u64::from_le_bytes(
                inconsistent_ready_count[HEADER_BODY_OFFSET + READY_TASKS_BODY_OFFSET
                    ..HEADER_BODY_OFFSET + READY_TASKS_BODY_OFFSET + size_of::<u64>()]
                    .try_into()
                    .expect("ready task count bytes"),
            ),
            1,
        );
        inconsistent_ready_count[HEADER_BODY_OFFSET + READY_TASKS_BODY_OFFSET
            ..HEADER_BODY_OFFSET + READY_TASKS_BODY_OFFSET + size_of::<u64>()]
            .copy_from_slice(&2_u64.to_le_bytes());
        let error = validate_sbe_trace_artifact(inconsistent_ready_count.as_slice())
            .expect_err("ready count inconsistent with task states must be rejected");
        assert!(
            error
                .to_string()
                .contains("ready task count disagrees with task snapshot states")
        );
    }

    #[test]
    fn strict_validator_rejects_a_start_time_beyond_the_terminal_instant() {
        const NOW_BODY_OFFSET: usize = 159;
        const START_TIME_BODY_OFFSET: usize = 232;
        const HEADER_BODY_OFFSET: usize =
            PREAMBLE_LENGTH + FRAME_LENGTH_PREFIX + MESSAGE_HEADER_LENGTH;

        let mut late_start = build_browser_golden();
        let now_bytes = HEADER_BODY_OFFSET + NOW_BODY_OFFSET
            ..HEADER_BODY_OFFSET + NOW_BODY_OFFSET + size_of::<u64>();
        let start_time_bytes = HEADER_BODY_OFFSET + START_TIME_BODY_OFFSET
            ..HEADER_BODY_OFFSET + START_TIME_BODY_OFFSET + size_of::<u64>();
        assert_eq!(
            u64::from_le_bytes(late_start[now_bytes.clone()].try_into().expect("now bytes")),
            u64::MAX,
        );
        assert_eq!(
            u64::from_le_bytes(
                late_start[start_time_bytes.clone()]
                    .try_into()
                    .expect("start time bytes"),
            ),
            0,
        );
        late_start[now_bytes].copy_from_slice(&0_u64.to_le_bytes());
        late_start[start_time_bytes].copy_from_slice(&1_u64.to_le_bytes());
        let error = validate_sbe_trace_artifact(late_start.as_slice())
            .expect_err("a start time beyond the terminal instant must be rejected");
        assert!(
            error
                .to_string()
                .contains("start time exceeds the terminal instant")
        );
    }

    #[test]
    fn sampled_writers_reject_retained_events_outside_the_declared_sequence_class() {
        let fixture = browser_sbe_fixture();
        let event = TraceEvent::new(1, SimInstant::ZERO, EventKind::RuntimeStarted { seed: 7 });
        let typed = std::rc::Rc::new(kr_runtime::trace::RecordingTrace::new(1));
        let typed_sampling = SamplingTrace::new(
            typed.clone(),
            NonZeroU64::new(2).expect("nonzero sampling period"),
        );
        typed.record(event.clone());
        let mut output = Vec::new();
        let error = write_sampled_sbe_trace_artifact(
            &mut output,
            &typed,
            &typed_sampling,
            &fixture.snapshot,
            TraceArtifactMetadata::new("test/1", "failed"),
        )
        .expect_err("typed writer must verify sampled sequence membership");
        assert!(matches!(error, ExportError::BinaryEncoding(_)));
        assert!(output.is_empty());

        let buffered = std::rc::Rc::new(SbeRecordingTrace::new(1_024));
        let buffered_sampling = SamplingTrace::new(
            buffered.clone(),
            NonZeroU64::new(2).expect("nonzero sampling period"),
        );
        buffered.record(event);
        let mut output = Vec::new();
        let error = write_sampled_buffered_sbe_trace_artifact(
            &mut output,
            &buffered,
            &buffered_sampling,
            &fixture.snapshot,
            TraceArtifactMetadata::new("test/1", "failed"),
        )
        .expect_err("buffered writer must verify sampled sequence membership");
        assert!(matches!(error, ExportError::BinaryEncoding(_)));
        assert!(output.is_empty());
    }

    #[test]
    fn strict_validator_rejects_events_outside_the_sampled_sequence_class() {
        let mut binary = build_browser_golden();
        const SAMPLING_PERIOD_BODY_OFFSET: usize = 100;
        let offset = PREAMBLE_LENGTH
            + FRAME_LENGTH_PREFIX
            + MESSAGE_HEADER_LENGTH
            + SAMPLING_PERIOD_BODY_OFFSET;
        assert_eq!(
            u64::from_le_bytes(binary[offset..offset + 8].try_into().expect("period bytes")),
            1,
        );
        binary[offset..offset + 8].copy_from_slice(&2_u64.to_le_bytes());
        let error = validate_sbe_trace_artifact(binary.as_slice())
            .expect_err("sampled sequence mismatch must be rejected");
        assert!(error.to_string().contains("sampling metadata"));
    }

    #[test]
    fn runtime_viewer_accepts_only_binary_sbe_artifacts() {
        let viewer = include_str!("../index.html");
        assert!(viewer.contains("accept=\".sbe,application/octet-stream\""));
        assert!(viewer.contains("src=\"trace-viewer-core.js\""));
        assert!(viewer.contains("src=\"runtime-trace-time.js\""));
        assert!(viewer.contains("src=\"dst-trace-sample.js\""));
        assert!(viewer.contains("loadEmbeddedSample"));
        for retired in [
            ".ndjson",
            ".jsonl",
            "application/json",
            "application/x-ndjson",
            "file.text()",
            "parseArtifact",
            "SAMPLE_RECORDS",
            "const ARTIFACT_SCHEMA_VERSION",
            "function formatDuration",
            "embedded schema-7 sample",
        ] {
            assert!(
                !viewer.contains(retired),
                "retired runtime trace input {retired:?} remains wired",
            );
        }
        for bound in [
            "const MAX_PRESENTATION_EVENTS = 100_000;",
            "const MAX_TASK_CATALOG = 4_096;",
            "const MAX_EVENT_FAMILIES = 16;",
            "const MAX_HIT_REGIONS = MAX_PRESENTATION_EVENTS;",
            "const MAX_CANVAS_CSS_WIDTH = 8_192;",
            "const MAX_CANVAS_CSS_HEIGHT = 8_192;",
            "const MAX_CANVAS_PIXELS = 16 * 1024 * 1024;",
            "state.hitRegions.length < MAX_HIT_REGIONS",
            "Math.sqrt(MAX_CANVAS_PIXELS / Math.max(1, width * height))",
            "maxTaskSnapshots: MAX_TASK_CATALOG",
            "maxEvents: MAX_PRESENTATION_EVENTS",
            "id=\"time-basis-select\"",
            "minimum = state.timeRange.start;",
            "maximum = state.timeRange.end;",
            "globalThis.DstTraceSbe.schema.artifactSchema",
        ] {
            assert!(viewer.contains(bound), "viewer is missing bound {bound:?}");
        }
    }
}
