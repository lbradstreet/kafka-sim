//! Framed SBE encoding for individual runtime trace events.
//!
//! Each frame has this layout:
//!
//! ```text
//! u32 little-endian frame length (including this prefix)
//! 8-byte SBE message header
//! SBE message body
//! ```
//!
//! This module deliberately stops at the event-frame boundary. Artifact
//! metadata, retention, buffering, and I/O are owned by higher layers.

use std::fmt;

use kr_runtime_trace_wire as codec;
use kr_runtime_trace_wire::WriteBuf;

use super::{EventKind, RandomChoiceKind, TaskCancellationReason, TraceEvent};
use crate::rng::RandomStream;
use crate::task::{MAX_PANIC_MESSAGE_BYTES, PanicRecord, TaskId};
use crate::time::SimInstant;
use crate::timer::TimerId;

mod recording;

pub use recording::{
    SbeEncodingFailure, SbeEncodingFailureReason, SbeRecordingTrace, SbeTraceRetention,
};

/// Size of the little-endian total-frame-length prefix.
pub const FRAME_LENGTH_PREFIX_LENGTH: usize = size_of::<u32>();

/// Size of the standard SBE message header used by this schema.
pub const SBE_MESSAGE_HEADER_LENGTH: usize = codec::message_header_codec::ENCODED_LENGTH;

/// SBE schema identifier accepted by this event decoder.
pub const SBE_SCHEMA_ID: u16 = codec::SBE_SCHEMA_ID;

/// SBE schema version accepted by this event decoder.
pub const SBE_SCHEMA_VERSION: u16 = codec::SBE_SCHEMA_VERSION;

/// Smallest possible encoded event frame.
pub const MIN_EVENT_FRAME_LENGTH: usize = FRAME_LENGTH_PREFIX_LENGTH
    + SBE_MESSAGE_HEADER_LENGTH
    + codec::runtime_stopped_codec::SBE_BLOCK_LENGTH as usize;

/// Largest frame length representable by the framing prefix.
pub const MAX_EVENT_FRAME_LENGTH: usize = u32::MAX as usize;

const SBE_HEADER_OFFSET: usize = FRAME_LENGTH_PREFIX_LENGTH;
const SBE_BODY_OFFSET: usize = SBE_HEADER_OFFSET + SBE_MESSAGE_HEADER_LENGTH;
const VARIABLE_DATA_LENGTH: usize = size_of::<u32>();

/// Failure while sizing or encoding an event frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EncodeError {
    /// A panic payload violates the runtime trace schema's bounded-message contract.
    PanicMessageTooLong { length: usize, maximum: usize },
    /// The complete frame cannot be represented by its 32-bit length prefix.
    FrameTooLong { length: usize, maximum: usize },
    /// The caller-owned destination cannot hold the complete frame.
    BufferTooSmall { required: usize, available: usize },
    /// A generated encoder unexpectedly lost its parent encoder.
    CodecState,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PanicMessageTooLong { length, maximum } => write!(
                formatter,
                "panic message has {length} bytes, exceeding the trace limit of {maximum}"
            ),
            Self::FrameTooLong { length, maximum } => write!(
                formatter,
                "event frame requires {length} bytes, exceeding the framing limit of {maximum}"
            ),
            Self::BufferTooSmall {
                required,
                available,
            } => write!(
                formatter,
                "event frame requires {required} bytes, but the destination has {available}"
            ),
            Self::CodecState => formatter.write_str("generated SBE encoder lost its parent"),
        }
    }
}

impl std::error::Error for EncodeError {}

/// Failure while decoding one untrusted event frame.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DecodeError {
    /// The input does not contain the complete four-byte frame-length prefix.
    LengthPrefixTruncated { available: usize },
    /// The declared total frame length is too small to contain any event.
    InvalidFrameLength { declared: usize, minimum: usize },
    /// The supplied bytes end before the declared frame boundary.
    FrameTruncated { declared: usize, available: usize },
    /// Exact-frame decoding was given bytes after the declared frame boundary.
    TrailingBytes { declared: usize, available: usize },
    /// The SBE header belongs to a different schema.
    UnexpectedSchemaId { actual: u16, expected: u16 },
    /// The frame uses an SBE schema version this decoder does not implement.
    UnsupportedSchemaVersion { actual: u16, supported: u16 },
    /// The SBE template is not an event template known by this schema.
    UnknownTemplateId { template_id: u16 },
    /// The header block length disagrees with the selected template.
    UnexpectedBlockLength {
        template_id: u16,
        actual: u16,
        expected: u16,
    },
    /// The declared frame boundary disagrees with the template's encoded body.
    UnexpectedMessageLength {
        template_id: u16,
        actual: usize,
        expected: usize,
    },
    /// A schema boolean contained a value other than zero or one.
    InvalidBoolean { field: &'static str, value: u8 },
    /// An optional task identifier carried nonzero bytes while absent.
    NonCanonicalAbsentTaskId { field: &'static str },
    /// A task-cancellation reason tag is unknown.
    UnknownCancellationReason { value: u8 },
    /// A deterministic random-stream tag is unknown.
    UnknownRandomStream { tag: u64 },
    /// A panic payload violates the runtime trace schema's bounded-message contract.
    PanicMessageTooLong { length: usize, maximum: usize },
    /// A panic payload is not valid UTF-8.
    InvalidUtf8,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthPrefixTruncated { available } => write!(
                formatter,
                "event frame length prefix needs 4 bytes, but only {available} are available"
            ),
            Self::InvalidFrameLength { declared, minimum } => write!(
                formatter,
                "event frame declares {declared} bytes, below the minimum of {minimum}"
            ),
            Self::FrameTruncated {
                declared,
                available,
            } => write!(
                formatter,
                "event frame declares {declared} bytes, but only {available} are available"
            ),
            Self::TrailingBytes {
                declared,
                available,
            } => write!(
                formatter,
                "event frame declares {declared} bytes, but {available} were supplied"
            ),
            Self::UnexpectedSchemaId { actual, expected } => write!(
                formatter,
                "SBE schema id {actual} does not match event schema id {expected}"
            ),
            Self::UnsupportedSchemaVersion { actual, supported } => write!(
                formatter,
                "SBE schema version {actual} is not supported; this decoder supports {supported}"
            ),
            Self::UnknownTemplateId { template_id } => {
                write!(
                    formatter,
                    "SBE template id {template_id} is not an event template"
                )
            }
            Self::UnexpectedBlockLength {
                template_id,
                actual,
                expected,
            } => write!(
                formatter,
                "SBE template {template_id} declares block length {actual}, expected {expected}"
            ),
            Self::UnexpectedMessageLength {
                template_id,
                actual,
                expected,
            } => write!(
                formatter,
                "SBE template {template_id} occupies {actual} frame bytes, expected {expected}"
            ),
            Self::InvalidBoolean { field, value } => {
                write!(formatter, "SBE boolean {field} has invalid value {value}")
            }
            Self::NonCanonicalAbsentTaskId { field } => {
                write!(
                    formatter,
                    "absent task identifier {field} has nonzero bytes"
                )
            }
            Self::UnknownCancellationReason { value } => {
                write!(formatter, "task cancellation reason tag {value} is unknown")
            }
            Self::UnknownRandomStream { tag } => {
                write!(formatter, "random stream tag {tag:#018x} is unknown")
            }
            Self::PanicMessageTooLong { length, maximum } => write!(
                formatter,
                "panic message has {length} bytes, exceeding the trace limit of {maximum}"
            ),
            Self::InvalidUtf8 => formatter.write_str("panic message is not valid UTF-8"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Returns the exact number of bytes [`encode_event`] will write.
///
/// This calculation is deterministic and performs no allocation. It rejects
/// manually constructed panic records that exceed the runtime's trace bound.
pub fn encoded_event_length(event: &TraceEvent) -> Result<usize, EncodeError> {
    let body_length = match &event.kind {
        EventKind::RuntimeStarted { .. } => codec::runtime_started_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TaskSpawned { .. } => codec::task_spawned_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TaskEnqueued { .. } => codec::task_enqueued_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TaskPollStarted { .. } => {
            codec::task_poll_started_codec::SBE_BLOCK_LENGTH.into()
        }
        EventKind::TaskPending { .. } => codec::task_pending_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TaskCompleted { .. } => codec::task_completed_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TaskCancelled { .. } => codec::task_cancelled_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TaskPanicked { panic, .. } => panic_body_length(
            codec::task_panicked_codec::SBE_BLOCK_LENGTH,
            panic.message.len(),
        )?,
        EventKind::TaskDropPanicked { panic, .. } => panic_body_length(
            codec::task_drop_panicked_codec::SBE_BLOCK_LENGTH,
            panic.message.len(),
        )?,
        EventKind::WakerPanicked { panic, .. } => panic_body_length(
            codec::waker_panicked_codec::SBE_BLOCK_LENGTH,
            panic.message.len(),
        )?,
        EventKind::TimerScheduled { .. } => codec::timer_scheduled_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TimerFired { .. } => codec::timer_fired_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TimerCancelled { .. } => codec::timer_cancelled_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::TimeAdvanced { .. } => codec::time_advanced_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::RuntimeStalled { .. } => codec::runtime_stalled_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::BudgetExhausted { .. } => codec::budget_exhausted_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::RuntimeStopped => codec::runtime_stopped_codec::SBE_BLOCK_LENGTH.into(),
        EventKind::RandomChoice { choice, .. } => match choice {
            RandomChoiceKind::U64 => codec::random_choice_u64_codec::SBE_BLOCK_LENGTH.into(),
            RandomChoiceKind::Below { .. } => {
                codec::random_choice_below_codec::SBE_BLOCK_LENGTH.into()
            }
            RandomChoiceKind::BoolRatio { .. } => {
                codec::random_choice_bool_ratio_codec::SBE_BLOCK_LENGTH.into()
            }
        },
    };

    let length = SBE_BODY_OFFSET
        .checked_add(body_length)
        .ok_or(EncodeError::FrameTooLong {
            length: usize::MAX,
            maximum: MAX_EVENT_FRAME_LENGTH,
        })?;
    if length > MAX_EVENT_FRAME_LENGTH {
        return Err(EncodeError::FrameTooLong {
            length,
            maximum: MAX_EVENT_FRAME_LENGTH,
        });
    }
    Ok(length)
}

fn panic_body_length(block_length: u16, message_length: usize) -> Result<usize, EncodeError> {
    if message_length > MAX_PANIC_MESSAGE_BYTES {
        return Err(EncodeError::PanicMessageTooLong {
            length: message_length,
            maximum: MAX_PANIC_MESSAGE_BYTES,
        });
    }
    usize::from(block_length)
        .checked_add(VARIABLE_DATA_LENGTH)
        .and_then(|length| length.checked_add(message_length))
        .ok_or(EncodeError::FrameTooLong {
            length: usize::MAX,
            maximum: MAX_EVENT_FRAME_LENGTH,
        })
}

macro_rules! with_encoder {
    ($module:ident, $encoder_type:ident, $encoder:ident, $destination:expr, $body:block) => {{
        let $encoder = codec::$module::$encoder_type::default()
            .wrap(WriteBuf::new($destination), SBE_BODY_OFFSET);
        let mut header = $encoder.header(SBE_HEADER_OFFSET);
        let mut $encoder = header.parent().map_err(|_| EncodeError::CodecState)?;
        $body
    }};
}

macro_rules! encode_task {
    ($encoder:ident, $task:expr) => {{
        let mut task_encoder = $encoder.task_encoder();
        task_encoder
            .slot($task.slot())
            .generation($task.generation());
        $encoder = task_encoder.parent().map_err(|_| EncodeError::CodecState)?;
    }};
}

macro_rules! encode_final_task {
    ($encoder:ident, $task:expr) => {{
        let mut task_encoder = $encoder.task_encoder();
        task_encoder
            .slot($task.slot())
            .generation($task.generation());
        let _encoder = task_encoder.parent().map_err(|_| EncodeError::CodecState)?;
    }};
}

/// Encodes one complete length-prefixed SBE event into a caller-owned slice.
///
/// On success the returned byte count is exactly [`encoded_event_length`]. If
/// sizing fails or `destination` is too small, the destination is not modified.
pub fn encode_event(event: &TraceEvent, destination: &mut [u8]) -> Result<usize, EncodeError> {
    let frame_length = encoded_event_length(event)?;
    if destination.len() < frame_length {
        return Err(EncodeError::BufferTooSmall {
            required: frame_length,
            available: destination.len(),
        });
    }
    let destination = &mut destination[..frame_length];
    destination[..FRAME_LENGTH_PREFIX_LENGTH].copy_from_slice(&(frame_length as u32).to_le_bytes());

    let sequence = event.sequence;
    let at_ns = event.at.as_nanos();
    match &event.kind {
        EventKind::RuntimeStarted { seed } => with_encoder!(
            runtime_started_codec,
            RuntimeStartedEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns).seed(*seed);
            }
        ),
        EventKind::TaskSpawned { task, parent } => with_encoder!(
            task_spawned_codec,
            TaskSpawnedEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .parent_present(u8::from(parent.is_some()));
                encode_task!(encoder, task);
                let mut parent_encoder = encoder.parent_encoder();
                let parent = parent.unwrap_or(TaskId::from_parts(0, 0));
                parent_encoder
                    .slot(parent.slot())
                    .generation(parent.generation());
                let _encoder = parent_encoder
                    .parent()
                    .map_err(|_| EncodeError::CodecState)?;
            }
        ),
        EventKind::TaskEnqueued {
            task,
            sequence: ready_sequence,
        } => with_encoder!(
            task_enqueued_codec,
            TaskEnqueuedEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .ready_sequence(*ready_sequence);
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TaskPollStarted { task } => with_encoder!(
            task_poll_started_codec,
            TaskPollStartedEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns);
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TaskPending { task } => with_encoder!(
            task_pending_codec,
            TaskPendingEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns);
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TaskCompleted { task } => with_encoder!(
            task_completed_codec,
            TaskCompletedEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns);
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TaskCancelled { task, reason } => with_encoder!(
            task_cancelled_codec,
            TaskCancelledEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .reason(encode_cancellation_reason(*reason));
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TaskPanicked { task, panic } => with_encoder!(
            task_panicked_codec,
            TaskPanickedEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .message_truncated(u8::from(panic.message_truncated));
                encode_task!(encoder, task);
                encoder.message(&panic.message);
            }
        ),
        EventKind::TaskDropPanicked { task, panic } => with_encoder!(
            task_drop_panicked_codec,
            TaskDropPanickedEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .message_truncated(u8::from(panic.message_truncated));
                encode_task!(encoder, task);
                encoder.message(&panic.message);
            }
        ),
        EventKind::WakerPanicked { task, panic } => with_encoder!(
            waker_panicked_codec,
            WakerPanickedEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .message_truncated(u8::from(panic.message_truncated));
                encode_task!(encoder, task);
                encoder.message(&panic.message);
            }
        ),
        EventKind::TimerScheduled { id, task, deadline } => with_encoder!(
            timer_scheduled_codec,
            TimerScheduledEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .timer(id.get())
                    .deadline_ns(deadline.as_nanos());
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TimerFired { id, task } => with_encoder!(
            timer_fired_codec,
            TimerFiredEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns).timer(id.get());
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TimerCancelled { id, task } => with_encoder!(
            timer_cancelled_codec,
            TimerCancelledEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns).timer(id.get());
                encode_final_task!(encoder, task);
            }
        ),
        EventKind::TimeAdvanced { from, to } => with_encoder!(
            time_advanced_codec,
            TimeAdvancedEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .from_ns(from.as_nanos())
                    .to_ns(to.as_nanos());
            }
        ),
        EventKind::RuntimeStalled { live_tasks } => with_encoder!(
            runtime_stalled_codec,
            RuntimeStalledEncoder,
            encoder,
            destination,
            {
                encoder
                    .sequence(sequence)
                    .at_ns(at_ns)
                    .live_tasks(*live_tasks);
            }
        ),
        EventKind::BudgetExhausted { steps } => with_encoder!(
            budget_exhausted_codec,
            BudgetExhaustedEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns).steps(*steps);
            }
        ),
        EventKind::RuntimeStopped => with_encoder!(
            runtime_stopped_codec,
            RuntimeStoppedEncoder,
            encoder,
            destination,
            {
                encoder.sequence(sequence).at_ns(at_ns);
            }
        ),
        EventKind::RandomChoice {
            stream,
            choice,
            draws_before,
            draws_after,
            value,
        } => match choice {
            RandomChoiceKind::U64 => with_encoder!(
                random_choice_u64_codec,
                RandomChoiceU64Encoder,
                encoder,
                destination,
                {
                    encoder
                        .sequence(sequence)
                        .at_ns(at_ns)
                        .stream_tag(*stream as u64)
                        .draws_before(*draws_before)
                        .draws_after(*draws_after)
                        .value(*value);
                }
            ),
            RandomChoiceKind::Below { upper_exclusive } => with_encoder!(
                random_choice_below_codec,
                RandomChoiceBelowEncoder,
                encoder,
                destination,
                {
                    encoder
                        .sequence(sequence)
                        .at_ns(at_ns)
                        .stream_tag(*stream as u64)
                        .draws_before(*draws_before)
                        .draws_after(*draws_after)
                        .value(*value)
                        .upper_exclusive(*upper_exclusive);
                }
            ),
            RandomChoiceKind::BoolRatio {
                numerator,
                denominator,
            } => with_encoder!(
                random_choice_bool_ratio_codec,
                RandomChoiceBoolRatioEncoder,
                encoder,
                destination,
                {
                    encoder
                        .sequence(sequence)
                        .at_ns(at_ns)
                        .stream_tag(*stream as u64)
                        .draws_before(*draws_before)
                        .draws_after(*draws_after)
                        .value(*value)
                        .numerator(*numerator)
                        .denominator(*denominator);
                }
            ),
        },
    }

    Ok(frame_length)
}

const fn encode_cancellation_reason(reason: TaskCancellationReason) -> u8 {
    match reason {
        TaskCancellationReason::ExplicitAbort => 0,
        TaskCancellationReason::BlockOnFailure => 1,
        TaskCancellationReason::RuntimeStopped => 2,
    }
}

#[derive(Clone, Copy)]
struct ValidatedFrame<'a> {
    bytes: &'a [u8],
    template_id: u16,
}

/// Decodes one exact length-prefixed SBE event frame.
///
/// The input is treated as untrusted. Framing and all offsets used by the
/// selected template are validated before fields are read. Supplying multiple
/// concatenated frames is an error; pass exactly the first declared frame.
pub fn decode_event(frame: &[u8]) -> Result<TraceEvent, DecodeError> {
    let frame = validate_frame(frame)?;
    let bytes = frame.bytes;
    let sequence = read_u64(bytes, SBE_BODY_OFFSET);
    let at = SimInstant::from_nanos(read_u64(bytes, SBE_BODY_OFFSET + 8));
    let offset = SBE_BODY_OFFSET + 16;

    let kind = match frame.template_id {
        codec::runtime_started_codec::SBE_TEMPLATE_ID => EventKind::RuntimeStarted {
            seed: read_u64(bytes, offset),
        },
        codec::task_spawned_codec::SBE_TEMPLATE_ID => {
            let task = read_task(bytes, offset);
            let parent_present = decode_boolean("parentPresent", bytes[offset + 8])?;
            let encoded_parent = read_task(bytes, offset + 9);
            if !parent_present && (encoded_parent.slot() != 0 || encoded_parent.generation() != 0) {
                return Err(DecodeError::NonCanonicalAbsentTaskId { field: "parent" });
            }
            let parent = parent_present.then_some(encoded_parent);
            EventKind::TaskSpawned { task, parent }
        }
        codec::task_enqueued_codec::SBE_TEMPLATE_ID => EventKind::TaskEnqueued {
            task: read_task(bytes, offset),
            sequence: read_u64(bytes, offset + 8),
        },
        codec::task_poll_started_codec::SBE_TEMPLATE_ID => EventKind::TaskPollStarted {
            task: read_task(bytes, offset),
        },
        codec::task_pending_codec::SBE_TEMPLATE_ID => EventKind::TaskPending {
            task: read_task(bytes, offset),
        },
        codec::task_completed_codec::SBE_TEMPLATE_ID => EventKind::TaskCompleted {
            task: read_task(bytes, offset),
        },
        codec::task_cancelled_codec::SBE_TEMPLATE_ID => EventKind::TaskCancelled {
            task: read_task(bytes, offset),
            reason: decode_cancellation_reason(bytes[offset + 8])?,
        },
        codec::task_panicked_codec::SBE_TEMPLATE_ID => EventKind::TaskPanicked {
            task: read_task(bytes, offset),
            panic: decode_panic(
                bytes,
                offset + 8,
                codec::task_panicked_codec::SBE_BLOCK_LENGTH,
            )?,
        },
        codec::task_drop_panicked_codec::SBE_TEMPLATE_ID => EventKind::TaskDropPanicked {
            task: read_task(bytes, offset),
            panic: decode_panic(
                bytes,
                offset + 8,
                codec::task_drop_panicked_codec::SBE_BLOCK_LENGTH,
            )?,
        },
        codec::waker_panicked_codec::SBE_TEMPLATE_ID => EventKind::WakerPanicked {
            task: read_task(bytes, offset),
            panic: decode_panic(
                bytes,
                offset + 8,
                codec::waker_panicked_codec::SBE_BLOCK_LENGTH,
            )?,
        },
        codec::timer_scheduled_codec::SBE_TEMPLATE_ID => EventKind::TimerScheduled {
            id: TimerId::from_u64(read_u64(bytes, offset)),
            task: read_task(bytes, offset + 8),
            deadline: SimInstant::from_nanos(read_u64(bytes, offset + 16)),
        },
        codec::timer_fired_codec::SBE_TEMPLATE_ID => EventKind::TimerFired {
            id: TimerId::from_u64(read_u64(bytes, offset)),
            task: read_task(bytes, offset + 8),
        },
        codec::timer_cancelled_codec::SBE_TEMPLATE_ID => EventKind::TimerCancelled {
            id: TimerId::from_u64(read_u64(bytes, offset)),
            task: read_task(bytes, offset + 8),
        },
        codec::time_advanced_codec::SBE_TEMPLATE_ID => EventKind::TimeAdvanced {
            from: SimInstant::from_nanos(read_u64(bytes, offset)),
            to: SimInstant::from_nanos(read_u64(bytes, offset + 8)),
        },
        codec::runtime_stalled_codec::SBE_TEMPLATE_ID => EventKind::RuntimeStalled {
            live_tasks: read_u64(bytes, offset),
        },
        codec::budget_exhausted_codec::SBE_TEMPLATE_ID => EventKind::BudgetExhausted {
            steps: read_u64(bytes, offset),
        },
        codec::runtime_stopped_codec::SBE_TEMPLATE_ID => EventKind::RuntimeStopped,
        codec::random_choice_u64_codec::SBE_TEMPLATE_ID => EventKind::RandomChoice {
            stream: decode_random_stream(read_u64(bytes, offset))?,
            choice: RandomChoiceKind::U64,
            draws_before: read_u64(bytes, offset + 8),
            draws_after: read_u64(bytes, offset + 16),
            value: read_u64(bytes, offset + 24),
        },
        codec::random_choice_below_codec::SBE_TEMPLATE_ID => EventKind::RandomChoice {
            stream: decode_random_stream(read_u64(bytes, offset))?,
            choice: RandomChoiceKind::Below {
                upper_exclusive: read_u64(bytes, offset + 32),
            },
            draws_before: read_u64(bytes, offset + 8),
            draws_after: read_u64(bytes, offset + 16),
            value: read_u64(bytes, offset + 24),
        },
        codec::random_choice_bool_ratio_codec::SBE_TEMPLATE_ID => EventKind::RandomChoice {
            stream: decode_random_stream(read_u64(bytes, offset))?,
            choice: RandomChoiceKind::BoolRatio {
                numerator: read_u64(bytes, offset + 32),
                denominator: read_u64(bytes, offset + 40),
            },
            draws_before: read_u64(bytes, offset + 8),
            draws_after: read_u64(bytes, offset + 16),
            value: read_u64(bytes, offset + 24),
        },
        _ => unreachable!("validated event template"),
    };

    Ok(TraceEvent::new(sequence, at, kind))
}

fn validate_frame(frame: &[u8]) -> Result<ValidatedFrame<'_>, DecodeError> {
    if frame.len() < FRAME_LENGTH_PREFIX_LENGTH {
        return Err(DecodeError::LengthPrefixTruncated {
            available: frame.len(),
        });
    }
    let declared = read_u32(frame, 0) as usize;
    if declared < MIN_EVENT_FRAME_LENGTH {
        return Err(DecodeError::InvalidFrameLength {
            declared,
            minimum: MIN_EVENT_FRAME_LENGTH,
        });
    }
    if frame.len() < declared {
        return Err(DecodeError::FrameTruncated {
            declared,
            available: frame.len(),
        });
    }
    if frame.len() > declared {
        return Err(DecodeError::TrailingBytes {
            declared,
            available: frame.len(),
        });
    }

    let block_length = read_u16(frame, SBE_HEADER_OFFSET);
    let template_id = read_u16(frame, SBE_HEADER_OFFSET + 2);
    let schema_id = read_u16(frame, SBE_HEADER_OFFSET + 4);
    let version = read_u16(frame, SBE_HEADER_OFFSET + 6);
    if schema_id != SBE_SCHEMA_ID {
        return Err(DecodeError::UnexpectedSchemaId {
            actual: schema_id,
            expected: SBE_SCHEMA_ID,
        });
    }
    if version != SBE_SCHEMA_VERSION {
        return Err(DecodeError::UnsupportedSchemaVersion {
            actual: version,
            supported: SBE_SCHEMA_VERSION,
        });
    }

    let (expected_block_length, has_panic_message) = template_shape(template_id)?;
    if block_length != expected_block_length {
        return Err(DecodeError::UnexpectedBlockLength {
            template_id,
            actual: block_length,
            expected: expected_block_length,
        });
    }

    let fixed_length = SBE_BODY_OFFSET + usize::from(expected_block_length);
    let expected_length = if has_panic_message {
        if declared < fixed_length + VARIABLE_DATA_LENGTH {
            return Err(DecodeError::UnexpectedMessageLength {
                template_id,
                actual: declared,
                expected: fixed_length + VARIABLE_DATA_LENGTH,
            });
        }
        let message_length = read_u32(frame, fixed_length) as usize;
        if message_length > MAX_PANIC_MESSAGE_BYTES {
            return Err(DecodeError::PanicMessageTooLong {
                length: message_length,
                maximum: MAX_PANIC_MESSAGE_BYTES,
            });
        }
        fixed_length + VARIABLE_DATA_LENGTH + message_length
    } else {
        fixed_length
    };
    if declared != expected_length {
        return Err(DecodeError::UnexpectedMessageLength {
            template_id,
            actual: declared,
            expected: expected_length,
        });
    }

    Ok(ValidatedFrame {
        bytes: frame,
        template_id,
    })
}

fn template_shape(template_id: u16) -> Result<(u16, bool), DecodeError> {
    let shape = match template_id {
        codec::runtime_started_codec::SBE_TEMPLATE_ID => {
            (codec::runtime_started_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::task_spawned_codec::SBE_TEMPLATE_ID => {
            (codec::task_spawned_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::task_enqueued_codec::SBE_TEMPLATE_ID => {
            (codec::task_enqueued_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::task_poll_started_codec::SBE_TEMPLATE_ID => {
            (codec::task_poll_started_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::task_pending_codec::SBE_TEMPLATE_ID => {
            (codec::task_pending_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::task_completed_codec::SBE_TEMPLATE_ID => {
            (codec::task_completed_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::task_cancelled_codec::SBE_TEMPLATE_ID => {
            (codec::task_cancelled_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::task_panicked_codec::SBE_TEMPLATE_ID => {
            (codec::task_panicked_codec::SBE_BLOCK_LENGTH, true)
        }
        codec::task_drop_panicked_codec::SBE_TEMPLATE_ID => {
            (codec::task_drop_panicked_codec::SBE_BLOCK_LENGTH, true)
        }
        codec::waker_panicked_codec::SBE_TEMPLATE_ID => {
            (codec::waker_panicked_codec::SBE_BLOCK_LENGTH, true)
        }
        codec::timer_scheduled_codec::SBE_TEMPLATE_ID => {
            (codec::timer_scheduled_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::timer_fired_codec::SBE_TEMPLATE_ID => {
            (codec::timer_fired_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::timer_cancelled_codec::SBE_TEMPLATE_ID => {
            (codec::timer_cancelled_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::time_advanced_codec::SBE_TEMPLATE_ID => {
            (codec::time_advanced_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::runtime_stalled_codec::SBE_TEMPLATE_ID => {
            (codec::runtime_stalled_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::budget_exhausted_codec::SBE_TEMPLATE_ID => {
            (codec::budget_exhausted_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::runtime_stopped_codec::SBE_TEMPLATE_ID => {
            (codec::runtime_stopped_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::random_choice_u64_codec::SBE_TEMPLATE_ID => {
            (codec::random_choice_u64_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::random_choice_below_codec::SBE_TEMPLATE_ID => {
            (codec::random_choice_below_codec::SBE_BLOCK_LENGTH, false)
        }
        codec::random_choice_bool_ratio_codec::SBE_TEMPLATE_ID => (
            codec::random_choice_bool_ratio_codec::SBE_BLOCK_LENGTH,
            false,
        ),
        _ => return Err(DecodeError::UnknownTemplateId { template_id }),
    };
    Ok(shape)
}

fn decode_panic(
    frame: &[u8],
    truncated_offset: usize,
    block_length: u16,
) -> Result<PanicRecord, DecodeError> {
    let message_truncated = decode_boolean("messageTruncated", frame[truncated_offset])?;
    let length_offset = SBE_BODY_OFFSET + usize::from(block_length);
    let message_length = read_u32(frame, length_offset) as usize;
    let message_start = length_offset + VARIABLE_DATA_LENGTH;
    let message = std::str::from_utf8(&frame[message_start..message_start + message_length])
        .map_err(|_| DecodeError::InvalidUtf8)?
        .to_owned();
    Ok(PanicRecord {
        message,
        message_truncated,
    })
}

fn decode_boolean(field: &'static str, value: u8) -> Result<bool, DecodeError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::InvalidBoolean { field, value }),
    }
}

fn decode_cancellation_reason(value: u8) -> Result<TaskCancellationReason, DecodeError> {
    match value {
        0 => Ok(TaskCancellationReason::ExplicitAbort),
        1 => Ok(TaskCancellationReason::BlockOnFailure),
        2 => Ok(TaskCancellationReason::RuntimeStopped),
        _ => Err(DecodeError::UnknownCancellationReason { value }),
    }
}

fn decode_random_stream(tag: u64) -> Result<RandomStream, DecodeError> {
    match tag {
        tag if tag == RandomStream::Schedule as u64 => Ok(RandomStream::Schedule),
        tag if tag == RandomStream::Scenario as u64 => Ok(RandomStream::Scenario),
        tag if tag == RandomStream::Workload as u64 => Ok(RandomStream::Workload),
        tag if tag == RandomStream::Fault as u64 => Ok(RandomStream::Fault),
        tag if tag == RandomStream::Debug as u64 => Ok(RandomStream::Debug),
        _ => Err(DecodeError::UnknownRandomStream { tag }),
    }
}

fn read_task(frame: &[u8], offset: usize) -> TaskId {
    TaskId::from_parts(read_u32(frame, offset), read_u32(frame, offset + 4))
}

fn read_u16(frame: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([frame[offset], frame[offset + 1]])
}

fn read_u32(frame: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        frame[offset],
        frame[offset + 1],
        frame[offset + 2],
        frame[offset + 3],
    ])
}

fn read_u64(frame: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        frame[offset],
        frame[offset + 1],
        frame[offset + 2],
        frame[offset + 3],
        frame[offset + 4],
        frame[offset + 5],
        frame[offset + 6],
        frame[offset + 7],
    ])
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::*;

    /// One event of every schema shape, with boundary-valued fields.
    pub(crate) fn all_events() -> Vec<TraceEvent> {
        let task = TaskId::from_parts(3, 4);
        let parent = TaskId::from_parts(u32::MAX, u32::MAX);
        let timer = TimerId::from_u64(u64::MAX);
        let panic = PanicRecord {
            message: "broken quote: \"; snow: 雪".to_owned(),
            message_truncated: true,
        };
        let kinds = vec![
            EventKind::RuntimeStarted { seed: u64::MAX },
            EventKind::TaskSpawned {
                task,
                parent: Some(parent),
            },
            EventKind::TaskEnqueued {
                task,
                sequence: u64::MAX,
            },
            EventKind::TaskPollStarted { task },
            EventKind::TaskPending { task },
            EventKind::TaskCompleted { task },
            EventKind::TaskCancelled {
                task,
                reason: TaskCancellationReason::ExplicitAbort,
            },
            EventKind::TaskCancelled {
                task,
                reason: TaskCancellationReason::BlockOnFailure,
            },
            EventKind::TaskCancelled {
                task,
                reason: TaskCancellationReason::RuntimeStopped,
            },
            EventKind::TaskPanicked {
                task,
                panic: panic.clone(),
            },
            EventKind::TaskDropPanicked {
                task,
                panic: panic.clone(),
            },
            EventKind::WakerPanicked { task, panic },
            EventKind::TimerScheduled {
                id: timer,
                task,
                deadline: SimInstant::MAX,
            },
            EventKind::TimerFired { id: timer, task },
            EventKind::TimerCancelled { id: timer, task },
            EventKind::TimeAdvanced {
                from: SimInstant::ZERO,
                to: SimInstant::MAX,
            },
            EventKind::RuntimeStalled {
                live_tasks: u64::MAX,
            },
            EventKind::BudgetExhausted { steps: u64::MAX },
            EventKind::RuntimeStopped,
            EventKind::RandomChoice {
                stream: RandomStream::Schedule,
                choice: RandomChoiceKind::U64,
                draws_before: 1,
                draws_after: 2,
                value: u64::MAX,
            },
            EventKind::RandomChoice {
                stream: RandomStream::Workload,
                choice: RandomChoiceKind::Below {
                    upper_exclusive: u64::MAX,
                },
                draws_before: 3,
                draws_after: 5,
                value: 8,
            },
            EventKind::RandomChoice {
                stream: RandomStream::Fault,
                choice: RandomChoiceKind::BoolRatio {
                    numerator: 13,
                    denominator: 21,
                },
                draws_before: 34,
                draws_after: 55,
                value: 1,
            },
        ];

        kinds
            .into_iter()
            .enumerate()
            .map(|(index, kind)| TraceEvent::new(index as u64, SimInstant::MAX, kind))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::test_fixtures::all_events;
    use super::*;
    use crate::trace::{TRACE_FINGERPRINT_OFFSET, fold_trace_fingerprint};

    fn encoded(event: &TraceEvent) -> Vec<u8> {
        let expected_length = encoded_event_length(event).expect("event is encodable");
        let mut frame = vec![0xa5; expected_length];
        assert_eq!(
            encode_event(event, &mut frame).expect("event encodes"),
            expected_length
        );
        assert_eq!(read_u32(&frame, 0) as usize, expected_length);
        frame
    }

    #[test]
    fn every_event_shape_round_trips_and_preserves_fingerprint() {
        let mut original_fingerprint = TRACE_FINGERPRINT_OFFSET;
        let mut decoded_fingerprint = TRACE_FINGERPRINT_OFFSET;

        for event in all_events() {
            let frame = encoded(&event);
            let decoded = decode_event(&frame).expect("event decodes");
            assert_eq!(decoded, event, "failed for {:?}", event.kind.tag());
            original_fingerprint = fold_trace_fingerprint(original_fingerprint, &event);
            decoded_fingerprint = fold_trace_fingerprint(decoded_fingerprint, &decoded);
        }

        assert_eq!(decoded_fingerprint, original_fingerprint);
    }

    #[test]
    fn task_spawned_without_parent_round_trips() {
        let event = TraceEvent::new(
            1,
            SimInstant::from_nanos(2),
            EventKind::TaskSpawned {
                task: TaskId::from_parts(3, 4),
                parent: None,
            },
        );
        assert_eq!(decode_event(&encoded(&event)), Ok(event));
    }

    #[test]
    fn task_spawned_without_parent_rejects_noncanonical_parent_bytes() {
        let event = TraceEvent::new(
            1,
            SimInstant::from_nanos(2),
            EventKind::TaskSpawned {
                task: TaskId::from_parts(3, 4),
                parent: None,
            },
        );
        let mut frame = encoded(&event);
        frame[SBE_BODY_OFFSET + 16 + 9] = 1;
        assert_eq!(
            decode_event(&frame),
            Err(DecodeError::NonCanonicalAbsentTaskId { field: "parent" })
        );
    }

    #[test]
    fn sizing_and_small_destination_fail_before_writing() {
        let event = &all_events()[0];
        let required = encoded_event_length(event).expect("event is encodable");
        let mut destination = vec![0xa5; required - 1];
        assert_eq!(
            encode_event(event, &mut destination),
            Err(EncodeError::BufferTooSmall {
                required,
                available: required - 1,
            })
        );
        assert!(destination.iter().all(|byte| *byte == 0xa5));

        let oversized = TraceEvent::new(
            0,
            SimInstant::ZERO,
            EventKind::TaskPanicked {
                task: TaskId::from_parts(0, 0),
                panic: PanicRecord {
                    message: "x".repeat(MAX_PANIC_MESSAGE_BYTES + 1),
                    message_truncated: true,
                },
            },
        );
        assert_eq!(
            encoded_event_length(&oversized),
            Err(EncodeError::PanicMessageTooLong {
                length: MAX_PANIC_MESSAGE_BYTES + 1,
                maximum: MAX_PANIC_MESSAGE_BYTES,
            })
        );
    }

    #[test]
    fn truncated_and_mismatched_frames_are_rejected() {
        assert_eq!(
            decode_event(&[0, 0, 0]),
            Err(DecodeError::LengthPrefixTruncated { available: 3 })
        );

        let event = &all_events()[0];
        let frame = encoded(event);
        assert_eq!(
            decode_event(&frame[..frame.len() - 1]),
            Err(DecodeError::FrameTruncated {
                declared: frame.len(),
                available: frame.len() - 1,
            })
        );

        let mut trailing = frame.clone();
        trailing.push(0);
        assert_eq!(
            decode_event(&trailing),
            Err(DecodeError::TrailingBytes {
                declared: frame.len(),
                available: frame.len() + 1,
            })
        );

        let mut short_declared = frame;
        short_declared[..4].copy_from_slice(&1_u32.to_le_bytes());
        assert_eq!(
            decode_event(&short_declared),
            Err(DecodeError::InvalidFrameLength {
                declared: 1,
                minimum: MIN_EVENT_FRAME_LENGTH,
            })
        );
    }

    #[test]
    fn unknown_or_incompatible_headers_are_rejected() {
        let frame = encoded(&all_events()[0]);

        let mut wrong_schema = frame.clone();
        wrong_schema[SBE_HEADER_OFFSET + 4..SBE_HEADER_OFFSET + 6]
            .copy_from_slice(&(SBE_SCHEMA_ID + 1).to_le_bytes());
        assert_eq!(
            decode_event(&wrong_schema),
            Err(DecodeError::UnexpectedSchemaId {
                actual: SBE_SCHEMA_ID + 1,
                expected: SBE_SCHEMA_ID,
            })
        );

        let mut wrong_version = frame.clone();
        wrong_version[SBE_HEADER_OFFSET + 6..SBE_HEADER_OFFSET + 8]
            .copy_from_slice(&(SBE_SCHEMA_VERSION + 1).to_le_bytes());
        assert_eq!(
            decode_event(&wrong_version),
            Err(DecodeError::UnsupportedSchemaVersion {
                actual: SBE_SCHEMA_VERSION + 1,
                supported: SBE_SCHEMA_VERSION,
            })
        );

        let mut unknown_template = frame.clone();
        unknown_template[SBE_HEADER_OFFSET + 2..SBE_HEADER_OFFSET + 4]
            .copy_from_slice(&999_u16.to_le_bytes());
        assert_eq!(
            decode_event(&unknown_template),
            Err(DecodeError::UnknownTemplateId { template_id: 999 })
        );

        let mut retired_template = frame.clone();
        retired_template[SBE_HEADER_OFFSET + 2..SBE_HEADER_OFFSET + 4]
            .copy_from_slice(&101_u16.to_le_bytes());
        assert_eq!(
            decode_event(&retired_template),
            Err(DecodeError::UnknownTemplateId { template_id: 101 })
        );

        let mut wrong_block = frame;
        wrong_block[SBE_HEADER_OFFSET..SBE_HEADER_OFFSET + 2].copy_from_slice(&0_u16.to_le_bytes());
        assert_eq!(
            decode_event(&wrong_block),
            Err(DecodeError::UnexpectedBlockLength {
                template_id: codec::runtime_started_codec::SBE_TEMPLATE_ID,
                actual: 0,
                expected: codec::runtime_started_codec::SBE_BLOCK_LENGTH,
            })
        );
    }

    #[test]
    fn invalid_enum_boolean_utf8_and_variable_lengths_are_rejected() {
        let event = TraceEvent::new(
            0,
            SimInstant::ZERO,
            EventKind::TaskCancelled {
                task: TaskId::from_parts(0, 0),
                reason: TaskCancellationReason::ExplicitAbort,
            },
        );
        let mut invalid_reason = encoded(&event);
        invalid_reason[SBE_BODY_OFFSET + 24] = 9;
        assert_eq!(
            decode_event(&invalid_reason),
            Err(DecodeError::UnknownCancellationReason { value: 9 })
        );

        let event = TraceEvent::new(
            0,
            SimInstant::ZERO,
            EventKind::TaskSpawned {
                task: TaskId::from_parts(0, 0),
                parent: None,
            },
        );
        let mut invalid_boolean = encoded(&event);
        invalid_boolean[SBE_BODY_OFFSET + 24] = 2;
        assert_eq!(
            decode_event(&invalid_boolean),
            Err(DecodeError::InvalidBoolean {
                field: "parentPresent",
                value: 2,
            })
        );

        let event = TraceEvent::new(
            0,
            SimInstant::ZERO,
            EventKind::TaskPanicked {
                task: TaskId::from_parts(0, 0),
                panic: PanicRecord {
                    message: "ok".to_owned(),
                    message_truncated: false,
                },
            },
        );
        let frame = encoded(&event);
        let length_offset =
            SBE_BODY_OFFSET + usize::from(codec::task_panicked_codec::SBE_BLOCK_LENGTH);

        let mut invalid_utf8 = frame.clone();
        invalid_utf8[length_offset + 4] = 0xff;
        assert_eq!(decode_event(&invalid_utf8), Err(DecodeError::InvalidUtf8));

        let mut invalid_length = frame;
        invalid_length[length_offset..length_offset + 4].copy_from_slice(&3_u32.to_le_bytes());
        assert_eq!(
            decode_event(&invalid_length),
            Err(DecodeError::UnexpectedMessageLength {
                template_id: codec::task_panicked_codec::SBE_TEMPLATE_ID,
                actual: invalid_length.len(),
                expected: invalid_length.len() + 1,
            })
        );
    }
}
