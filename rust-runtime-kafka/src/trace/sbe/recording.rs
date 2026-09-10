//! Bounded byte-backed retention for framed SBE trace events.

use std::cell::RefCell;

use super::{DecodeError, EncodeError, decode_event, encode_event, encoded_event_length};
use crate::trace::{
    TRACE_FINGERPRINT_OFFSET, TraceEvent, TraceOrderingViolation, TraceSink, fold_trace_fingerprint,
};

/// Which accepted events a bounded [`SbeRecordingTrace`] retains.
///
/// Capacities count complete encoded frames, including each frame's length
/// prefix and SBE message header. A frame is either retained whole or omitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SbeTraceRetention {
    /// Retain the longest initial sequence of complete frames that fits.
    Prefix {
        /// Maximum bytes occupied by retained prefix frames.
        capacity_bytes: usize,
    },
    /// Retain the longest most-recent sequence of complete frames that fits.
    Tail {
        /// Maximum bytes occupied by retained tail frames.
        capacity_bytes: usize,
    },
    /// Retain an initial byte-bounded prefix and a recent byte-bounded tail.
    PrefixAndTail {
        /// Maximum bytes occupied by retained prefix frames.
        prefix_capacity_bytes: usize,
        /// Maximum bytes occupied by retained tail frames.
        tail_capacity_bytes: usize,
    },
}

impl SbeTraceRetention {
    /// Maximum total bytes retained by this policy.
    ///
    /// # Panics
    ///
    /// Panics if the combined prefix-plus-tail capacity exceeds `usize`.
    #[must_use]
    pub const fn capacity_bytes(self) -> usize {
        match self {
            Self::Prefix { capacity_bytes } | Self::Tail { capacity_bytes } => capacity_bytes,
            Self::PrefixAndTail {
                prefix_capacity_bytes,
                tail_capacity_bytes,
            } => match prefix_capacity_bytes.checked_add(tail_capacity_bytes) {
                Some(capacity_bytes) => capacity_bytes,
                None => panic!("combined SBE trace retention capacity overflowed"),
            },
        }
    }

    /// Maximum bytes available to initial prefix frames.
    #[must_use]
    pub const fn prefix_capacity_bytes(self) -> usize {
        match self {
            Self::Prefix { capacity_bytes } => capacity_bytes,
            Self::Tail { .. } => 0,
            Self::PrefixAndTail {
                prefix_capacity_bytes,
                ..
            } => prefix_capacity_bytes,
        }
    }

    /// Maximum bytes available to recent tail frames.
    #[must_use]
    pub const fn tail_capacity_bytes(self) -> usize {
        match self {
            Self::Prefix { .. } => 0,
            Self::Tail { capacity_bytes } => capacity_bytes,
            Self::PrefixAndTail {
                tail_capacity_bytes,
                ..
            } => tail_capacity_bytes,
        }
    }

    /// Stable artifact name for this retention policy.
    #[must_use]
    pub const fn mode_name(self) -> &'static str {
        match self {
            Self::Prefix { .. } => "prefix",
            Self::Tail { .. } => "tail",
            Self::PrefixAndTail { .. } => "prefix_and_tail",
        }
    }
}

/// Stable category for an event that could not be SBE encoded.
///
/// Encoding failures are reported independently from retention drops. The
/// category deliberately excludes size values so artifact metadata remains
/// stable across platforms with different `usize` widths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SbeEncodingFailureReason {
    /// A directly constructed panic record exceeded the trace schema bound.
    PanicMessageTooLong,
    /// The complete event frame exceeded the framing format's bound.
    FrameTooLong,
    /// A generated encoder unexpectedly lost its parent state.
    CodecState,
    /// The encoder rejected storage that had already passed exact sizing.
    InternalBufferTooSmall,
}

impl SbeEncodingFailureReason {
    fn from_encode_error(error: &EncodeError) -> Self {
        match error {
            EncodeError::PanicMessageTooLong { .. } => Self::PanicMessageTooLong,
            EncodeError::FrameTooLong { .. } => Self::FrameTooLong,
            EncodeError::CodecState => Self::CodecState,
            EncodeError::BufferTooSmall { .. } => Self::InternalBufferTooSmall,
        }
    }
}

/// First accepted event that could not be SBE encoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SbeEncodingFailure {
    /// Global trace sequence carried by the unencodable event.
    pub sequence: u64,
    /// Stable reason category.
    pub reason: SbeEncodingFailureReason,
}

#[derive(Debug)]
struct LinearFrames {
    bytes: Box<[u8]>,
    used: usize,
    frames: usize,
}

impl LinearFrames {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: vec![0; capacity].into_boxed_slice(),
            used: 0,
            frames: 0,
        }
    }

    const fn capacity(&self) -> usize {
        self.bytes.len()
    }

    const fn remaining(&self) -> usize {
        self.capacity() - self.used
    }

    fn append(&mut self, event: &TraceEvent, frame_length: usize) -> Result<(), EncodeError> {
        debug_assert!(frame_length <= self.remaining());
        let end = self.used + frame_length;
        encode_event(event, &mut self.bytes[self.used..end])?;
        self.used = end;
        self.frames += 1;
        Ok(())
    }

    fn frame_length_at(&self, offset: usize) -> usize {
        internal_frame_length(&self.bytes, offset, self.used)
    }

    fn try_visit<E>(&self, visitor: &mut dyn FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        let mut offset = 0;
        while offset < self.used {
            let frame_length = self.frame_length_at(offset);
            visitor(&self.bytes[offset..offset + frame_length])?;
            offset += frame_length;
        }
        Ok(())
    }
}

/// A byte-bounded ring whose individual frames are always physically contiguous.
///
/// `wrap_at` is the logical end of the older physical segment. Bytes from
/// `wrap_at` to the allocation end are padding and do not contribute to
/// `used`. A zero frame-length marker is written there when at least four bytes
/// remain, while the explicit offset also represents shorter padding.
#[derive(Debug)]
struct TailFrameRing {
    bytes: Box<[u8]>,
    head: usize,
    write: usize,
    wrap_at: Option<usize>,
    used: usize,
    frames: usize,
    #[cfg(test)]
    compactions: usize,
}

impl TailFrameRing {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: vec![0; capacity].into_boxed_slice(),
            head: 0,
            write: 0,
            wrap_at: None,
            used: 0,
            frames: 0,
            #[cfg(test)]
            compactions: 0,
        }
    }

    const fn capacity(&self) -> usize {
        self.bytes.len()
    }

    fn clear(&mut self) -> usize {
        let removed = self.frames;
        self.head = 0;
        self.write = 0;
        self.wrap_at = None;
        self.used = 0;
        self.frames = 0;
        removed
    }

    /// Evicts only the minimum oldest frames required by the byte capacity,
    /// then places the new frame. Physical fragmentation triggers compaction,
    /// never an additional eviction.
    fn make_room_and_append(
        &mut self,
        event: &TraceEvent,
        required: usize,
    ) -> Result<usize, (usize, EncodeError)> {
        debug_assert!(required <= self.capacity());
        let mut evicted = 0;
        while self.used > self.capacity() - required {
            self.evict_oldest();
            evicted += 1;
        }

        if self.frames == 0 {
            self.reset_empty();
        }

        let start = if self.frames == 0 {
            0
        } else if let Some(_wrap_at) = self.wrap_at {
            let free = self.head - self.write;
            if required <= free {
                self.write
            } else {
                self.compact();
                self.write
            }
        } else {
            let free_at_end = self.capacity() - self.write;
            if required <= free_at_end {
                self.write
            } else if required <= self.head {
                self.mark_wrap();
                0
            } else {
                self.compact();
                self.write
            }
        };

        let end = start + required;
        if let Err(error) = encode_event(event, &mut self.bytes[start..end]) {
            return Err((evicted, error));
        }
        if self.frames == 0 {
            self.head = start;
        }
        self.write = end;
        self.used += required;
        self.frames += 1;
        Ok(evicted)
    }

    fn evict_oldest(&mut self) {
        assert!(self.frames != 0, "cannot evict from an empty SBE tail ring");
        let segment_end = self.wrap_at.unwrap_or(self.write);
        let frame_length = internal_frame_length(&self.bytes, self.head, segment_end);
        self.head += frame_length;
        self.used -= frame_length;
        self.frames -= 1;

        if self.frames == 0 {
            self.reset_empty();
        } else if self.wrap_at == Some(self.head) {
            self.head = 0;
            self.wrap_at = None;
        }
    }

    fn reset_empty(&mut self) {
        self.head = 0;
        self.write = 0;
        self.wrap_at = None;
        self.used = 0;
        self.frames = 0;
    }

    fn mark_wrap(&mut self) {
        debug_assert!(self.wrap_at.is_none());
        debug_assert!(self.frames != 0);
        if self.capacity() - self.write >= size_of::<u32>() {
            self.bytes[self.write..self.write + size_of::<u32>()].fill(0);
        }
        self.wrap_at = Some(self.write);
        self.write = 0;
    }

    fn compact(&mut self) {
        if self.frames == 0 {
            self.reset_empty();
            return;
        }

        match self.wrap_at {
            Some(wrap_at) => {
                // Before rotation: [newer frames, free, older frames, padding].
                // Rotating the active prefix produces [older, newer, free].
                self.bytes[..wrap_at].rotate_left(self.head);
            }
            None => {
                self.bytes.copy_within(self.head..self.write, 0);
            }
        }
        self.head = 0;
        self.write = self.used;
        self.wrap_at = None;
        #[cfg(test)]
        {
            self.compactions += 1;
        }
    }

    fn try_visit<E>(&self, visitor: &mut dyn FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        let mut offset = self.head;
        for _ in 0..self.frames {
            let segment_end = match self.wrap_at {
                Some(wrap_at) if offset >= self.head => wrap_at,
                _ => self.write,
            };
            let frame_length = internal_frame_length(&self.bytes, offset, segment_end);
            visitor(&self.bytes[offset..offset + frame_length])?;
            offset += frame_length;
            if self.wrap_at == Some(offset) {
                offset = 0;
            }
        }
        Ok(())
    }
}

/// Decodes every internally retained frame produced by a `try_visit`-style
/// traversal into `events`.
///
/// Retained frames were written by this module's own encoder, so a decode
/// failure is an internal invariant violation, not an input error.
fn decode_retained_frames(
    events: &mut Vec<TraceEvent>,
    try_visit: impl FnOnce(&mut dyn FnMut(&[u8]) -> Result<(), DecodeError>) -> Result<(), DecodeError>,
) {
    try_visit(&mut |frame| {
        events.push(
            decode_event(frame)
                .expect("internally retained SBE frame must remain valid after encoding"),
        );
        Ok(())
    })
    .expect("infallible internal SBE frame visitor");
}

fn internal_frame_length(bytes: &[u8], offset: usize, segment_end: usize) -> usize {
    assert!(
        offset + size_of::<u32>() <= segment_end,
        "internally retained SBE frame has a truncated length prefix"
    );
    let prefix = &bytes[offset..offset + size_of::<u32>()];
    let length = u32::from_le_bytes(prefix.try_into().expect("four-byte frame prefix")) as usize;
    assert!(
        length >= size_of::<u32>() && offset + length <= segment_end,
        "internally retained SBE frame has an invalid length"
    );
    length
}

#[derive(Debug)]
struct SbeRecordingState {
    prefix: LinearFrames,
    tail: TailFrameRing,
    prefix_closed: bool,
    dropped: u64,
    last_sequence: Option<u64>,
    ordering_violation: Option<TraceOrderingViolation>,
    fingerprint: u64,
    encoding_failures: u64,
    first_encoding_failure: Option<SbeEncodingFailure>,
}

impl SbeRecordingState {
    fn add_dropped(&mut self, count: usize) {
        self.dropped = self.dropped.saturating_add(count as u64);
    }

    fn note_encoding_failure(&mut self, sequence: u64, error: &EncodeError) {
        self.encoding_failures = self.encoding_failures.saturating_add(1);
        if self.first_encoding_failure.is_none() {
            self.first_encoding_failure = Some(SbeEncodingFailure {
                sequence,
                reason: SbeEncodingFailureReason::from_encode_error(error),
            });
        }
    }

    fn suffix_barrier(&mut self) {
        let removed = self.tail.clear();
        self.add_dropped(removed.saturating_add(1));
    }

    /// Decodes a snapshot of all retained events in sequence order.
    fn decode_all(&self) -> Vec<TraceEvent> {
        let mut events = Vec::with_capacity(self.prefix.frames + self.tail.frames);
        decode_retained_frames(&mut events, |visitor| self.prefix.try_visit(visitor));
        decode_retained_frames(&mut events, |visitor| self.tail.try_visit(visitor));
        events
    }
}

/// A fixed-capacity trace sink that retains framed SBE events in byte buffers.
///
/// Construction allocates both configured buffers at their final sizes.
/// Recording a schema-valid runtime event then performs no allocation: frames
/// are encoded directly into fixed storage. A full tail evicts whole leading
/// frames by advancing its ring head; compaction occurs only when padding or
/// split free space would otherwise force an unnecessary extra eviction.
///
/// Prefix retention is monotonic. Once an accepted event does not fit (or
/// cannot be encoded), later smaller frames never backfill the prefix. An
/// oversized or unencodable tail event is a suffix barrier: it clears the
/// prior tail so all subsequently retained tail events remain a true suffix.
/// The canonical fingerprint covers every ordering-valid event before sizing,
/// encoding, or retention is attempted.
///
/// This recorder is intended for the simulator's single event-loop thread and
/// is not `Sync`.
#[derive(Debug)]
pub struct SbeRecordingTrace {
    retention: SbeTraceRetention,
    state: RefCell<SbeRecordingState>,
}

impl SbeRecordingTrace {
    /// Creates a prefix recorder with at most `capacity_bytes` encoded bytes.
    #[must_use]
    pub fn new(capacity_bytes: usize) -> Self {
        Self::with_retention(SbeTraceRetention::Prefix { capacity_bytes })
    }

    /// Creates a recorder and allocates its fixed buffers.
    ///
    /// # Panics
    ///
    /// Panics if combined capacity overflows `usize` or the requested buffers
    /// cannot be allocated.
    #[must_use]
    pub fn with_retention(retention: SbeTraceRetention) -> Self {
        let _ = retention.capacity_bytes();
        let prefix_capacity = retention.prefix_capacity_bytes();
        let tail_capacity = retention.tail_capacity_bytes();
        Self {
            retention,
            state: RefCell::new(SbeRecordingState {
                prefix: LinearFrames::new(prefix_capacity),
                tail: TailFrameRing::new(tail_capacity),
                prefix_closed: prefix_capacity == 0,
                dropped: 0,
                last_sequence: None,
                ordering_violation: None,
                fingerprint: TRACE_FINGERPRINT_OFFSET,
                encoding_failures: 0,
                first_encoding_failure: None,
            }),
        }
    }

    /// Configured retention policy.
    #[must_use]
    pub const fn retention(&self) -> SbeTraceRetention {
        self.retention
    }

    /// Maximum total bytes retained by this recorder.
    #[must_use]
    pub const fn capacity_bytes(&self) -> usize {
        self.retention.capacity_bytes()
    }

    /// Number of bytes occupied by complete retained frames.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        let state = self.state.borrow();
        state.prefix.used + state.tail.used
    }

    /// Number of complete retained event frames.
    #[must_use]
    pub fn len(&self) -> usize {
        let state = self.state.borrow();
        state.prefix.frames + state.tail.frames
    }

    /// Returns whether no events are currently retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of ordering-valid events absent from the current snapshot.
    ///
    /// Encoding failures are included because the corresponding event is
    /// absent, and are also reported independently by
    /// [`Self::encoding_failures`].
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.state.borrow().dropped
    }

    /// Sequence of the most recently accepted ordering-valid event.
    #[must_use]
    pub fn last_sequence(&self) -> Option<u64> {
        self.state.borrow().last_sequence
    }

    /// First rejected non-increasing sequence pair, if any.
    #[must_use]
    pub fn ordering_violation(&self) -> Option<TraceOrderingViolation> {
        self.state.borrow().ordering_violation
    }

    /// Canonical typed digest of every ordering-valid accepted event.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        self.state.borrow().fingerprint
    }

    /// Number of ordering-valid events that could not be SBE encoded.
    #[must_use]
    pub fn encoding_failures(&self) -> u64 {
        self.state.borrow().encoding_failures
    }

    /// First event that could not be SBE encoded, if any.
    #[must_use]
    pub fn first_encoding_failure(&self) -> Option<SbeEncodingFailure> {
        self.state.borrow().first_encoding_failure
    }

    /// Decodes a snapshot of retained events in sequence order.
    #[must_use]
    pub fn events(&self) -> Vec<TraceEvent> {
        self.state.borrow().decode_all()
    }

    /// Consumes this recorder and decodes retained events in sequence order.
    #[must_use]
    pub fn into_events(self) -> Vec<TraceEvent> {
        self.state.into_inner().decode_all()
    }

    /// Visits each complete encoded frame in sequence order without copying.
    ///
    /// The visitor's error is returned immediately and later frames are not
    /// visited. The recorder is immutably borrowed for the full traversal, so a
    /// visitor must not re-enter this recorder through a captured reference.
    pub fn try_visit_encoded_records<E>(
        &self,
        mut visitor: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        let state = self.state.borrow();
        state.prefix.try_visit(&mut visitor)?;
        state.tail.try_visit(&mut visitor)
    }

    fn retain_encoded(
        retention: SbeTraceRetention,
        state: &mut SbeRecordingState,
        event: &TraceEvent,
        frame_length: usize,
    ) {
        if !state.prefix_closed {
            if frame_length <= state.prefix.remaining() {
                if let Err(error) = state.prefix.append(event, frame_length) {
                    state.prefix_closed = true;
                    state.note_encoding_failure(event.sequence, &error);
                    state.suffix_barrier();
                }
                return;
            }
            state.prefix_closed = true;
        }

        let tail_capacity = retention.tail_capacity_bytes();
        if tail_capacity == 0 {
            state.add_dropped(1);
            return;
        }
        if frame_length > tail_capacity {
            state.suffix_barrier();
            return;
        }

        match state.tail.make_room_and_append(event, frame_length) {
            Ok(evicted) => state.add_dropped(evicted),
            Err((evicted, error)) => {
                state.add_dropped(evicted);
                state.note_encoding_failure(event.sequence, &error);
                state.suffix_barrier();
            }
        }
    }
}

impl TraceSink for SbeRecordingTrace {
    fn record(&self, event: TraceEvent) {
        let mut state = self.state.borrow_mut();

        if let Some(last) = state.last_sequence
            && event.sequence <= last
        {
            if state.ordering_violation.is_none() {
                state.ordering_violation = Some(TraceOrderingViolation {
                    previous_sequence: last,
                    rejected_sequence: event.sequence,
                });
            }
            return;
        }

        state.last_sequence = Some(event.sequence);
        state.fingerprint = fold_trace_fingerprint(state.fingerprint, &event);

        match encoded_event_length(&event) {
            Ok(frame_length) => {
                Self::retain_encoded(self.retention, &mut state, &event, frame_length);
            }
            Err(error) => {
                state.prefix_closed = true;
                state.note_encoding_failure(event.sequence, &error);
                state.suffix_barrier();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::rc::Rc;

    use super::*;
    use crate::task::{MAX_PANIC_MESSAGE_BYTES, PanicRecord, TaskId};
    use crate::time::SimInstant;
    use crate::trace::sbe::test_fixtures::all_events;
    use crate::trace::{EventKind, EventKindTag, RecordingTrace, SamplingTrace};

    fn event(sequence: u64) -> TraceEvent {
        TraceEvent::new(
            sequence,
            SimInstant::from_nanos(sequence),
            EventKind::RuntimeStarted { seed: sequence },
        )
    }

    fn panic_event(sequence: u64, message_bytes: usize) -> TraceEvent {
        TraceEvent::new(
            sequence,
            SimInstant::from_nanos(sequence),
            EventKind::TaskPanicked {
                task: TaskId::from_parts(0, 0),
                panic: PanicRecord {
                    message: "x".repeat(message_bytes),
                    message_truncated: false,
                },
            },
        )
    }

    #[test]
    fn prefix_honors_exact_byte_boundary_and_one_short() {
        let first = event(0);
        let frame_length = encoded_event_length(&first).expect("event is encodable");

        let exact = SbeRecordingTrace::new(frame_length);
        exact.record(first.clone());
        assert_eq!(exact.capacity_bytes(), frame_length);
        assert_eq!(exact.retained_bytes(), frame_length);
        assert_eq!(exact.len(), 1);
        assert!(!exact.is_empty());
        assert_eq!(exact.dropped(), 0);
        assert_eq!(exact.events().as_slice(), std::slice::from_ref(&first));

        let short = SbeRecordingTrace::new(frame_length - 1);
        short.record(first);
        assert_eq!(short.retained_bytes(), 0);
        assert!(short.is_empty());
        assert_eq!(short.dropped(), 1);
    }

    #[test]
    fn prefix_does_not_backfill_after_first_frame_misses() {
        let small = event(1);
        let small_length = encoded_event_length(&small).expect("event is encodable");
        let large = TraceEvent::new(
            0,
            SimInstant::ZERO,
            EventKind::TaskPanicked {
                task: TaskId::from_parts(0, 0),
                panic: PanicRecord {
                    message: "larger".repeat(16),
                    message_truncated: false,
                },
            },
        );
        assert!(encoded_event_length(&large).expect("event is encodable") > small_length);

        let trace = SbeRecordingTrace::new(small_length);
        trace.record(large);
        trace.record(small);

        assert!(trace.events().is_empty());
        assert_eq!(trace.dropped(), 2);
        assert_eq!(trace.last_sequence(), Some(1));
    }

    #[test]
    fn tail_wraps_repeatedly_without_per_append_compaction() {
        let frame_length = encoded_event_length(&event(0)).expect("event is encodable");
        let trace = SbeRecordingTrace::with_retention(SbeTraceRetention::Tail {
            capacity_bytes: frame_length * 2,
        });

        for sequence in 0..128 {
            trace.record(event(sequence));
        }

        assert_eq!(trace.retained_bytes(), frame_length * 2);
        assert_eq!(trace.events(), [event(126), event(127)]);
        assert_eq!(trace.dropped(), 126);
        assert_eq!(trace.state.borrow().tail.compactions, 0);
    }

    #[test]
    fn tail_compacts_fragmentation_without_losing_a_suffix_frame() {
        let older = panic_event(0, 160);
        let middle = event(1);
        let newer = panic_event(2, 140);
        let last = event(3);
        let older_length = encoded_event_length(&older).expect("event is encodable");
        let middle_length = encoded_event_length(&middle).expect("event is encodable");
        let newer_length = encoded_event_length(&newer).expect("event is encodable");
        let last_length = encoded_event_length(&last).expect("event is encodable");
        assert_eq!(older_length - newer_length, 20);
        assert_eq!(middle_length, last_length);
        assert!(last_length > older_length - newer_length);
        let capacity = older_length + middle_length + last_length;
        let trace = SbeRecordingTrace::with_retention(SbeTraceRetention::Tail {
            capacity_bytes: capacity,
        });

        trace.record(older);
        trace.record(middle.clone());
        trace.record(newer.clone());

        {
            let state = trace.state.borrow();
            let wrap_at = state.tail.wrap_at.expect("third frame wraps");
            assert_eq!(state.tail.compactions, 0);
            assert_eq!(capacity - wrap_at, last_length);
            assert_eq!(state.tail.used, middle_length + newer_length);
        }
        assert_eq!(trace.retained_bytes(), middle_length + newer_length);

        // Total frame bytes fit, but the free bytes are split between the
        // head gap and wrap padding. Compaction must retain the full suffix.
        trace.record(last.clone());

        assert_eq!(trace.events(), [middle, newer, last]);
        assert_eq!(
            trace.retained_bytes(),
            middle_length + newer_length + last_length
        );
        assert_eq!(trace.dropped(), 1);
        assert_eq!(trace.state.borrow().tail.compactions, 1);
    }

    #[test]
    fn variable_size_tail_matches_a_maximal_suffix_oracle_across_wraps() {
        let capacity = 512;
        let trace = SbeRecordingTrace::with_retention(SbeTraceRetention::Tail {
            capacity_bytes: capacity,
        });
        let mut oracle = Vec::<TraceEvent>::new();
        let mut oracle_bytes = 0;

        for sequence in 0..256 {
            let next = if sequence % 5 == 0 {
                event(sequence)
            } else {
                panic_event(sequence, ((sequence * 37) % 173) as usize)
            };
            let next_length = encoded_event_length(&next).expect("event is encodable");
            oracle_bytes += next_length;
            oracle.push(next.clone());
            while oracle_bytes > capacity {
                oracle_bytes -=
                    encoded_event_length(&oracle.remove(0)).expect("event remains encodable");
            }

            trace.record(next);

            assert_eq!(trace.retained_bytes(), oracle_bytes);
            assert_eq!(trace.events(), oracle, "suffix differs at {sequence}");
        }
    }

    #[test]
    fn tail_oversized_frame_is_a_suffix_barrier_and_later_frames_recover() {
        let small = event(0);
        let capacity = encoded_event_length(&small).expect("event is encodable");
        let oversized = TraceEvent::new(
            1,
            SimInstant::ZERO,
            EventKind::TaskPanicked {
                task: TaskId::from_parts(0, 0),
                panic: PanicRecord {
                    message: "oversized".repeat(16),
                    message_truncated: false,
                },
            },
        );
        assert!(encoded_event_length(&oversized).expect("event is encodable") > capacity);
        let trace = SbeRecordingTrace::with_retention(SbeTraceRetention::Tail {
            capacity_bytes: capacity,
        });

        trace.record(small);
        trace.record(oversized);
        assert!(trace.is_empty());
        assert_eq!(trace.dropped(), 2);

        trace.record(event(2));
        assert_eq!(trace.events(), [event(2)]);
        assert_eq!(trace.dropped(), 2);
        assert_eq!(trace.encoding_failures(), 0);
    }

    #[test]
    fn unencodable_event_is_reported_and_is_a_tail_suffix_barrier() {
        let small_length = encoded_event_length(&event(0)).expect("event is encodable");
        let trace = SbeRecordingTrace::with_retention(SbeTraceRetention::Tail {
            capacity_bytes: small_length,
        });
        let unencodable = |sequence| {
            TraceEvent::new(
                sequence,
                SimInstant::ZERO,
                EventKind::TaskPanicked {
                    task: TaskId::from_parts(0, 0),
                    panic: PanicRecord {
                        message: "x".repeat(MAX_PANIC_MESSAGE_BYTES + 1),
                        message_truncated: false,
                    },
                },
            )
        };
        let inputs = [event(0), unencodable(1), event(2), unencodable(3), event(4)];
        let reference = RecordingTrace::new(inputs.len());

        for input in &inputs {
            trace.record(input.clone());
            reference.record(input.clone());
        }

        assert_eq!(trace.events(), [event(4)]);
        assert_eq!(trace.dropped(), 4);
        assert_eq!(trace.encoding_failures(), 2);
        assert_eq!(
            trace.first_encoding_failure(),
            Some(SbeEncodingFailure {
                sequence: 1,
                reason: SbeEncodingFailureReason::PanicMessageTooLong,
            })
        );
        assert_eq!(trace.fingerprint(), reference.fingerprint());
    }

    #[test]
    fn prefix_and_tail_keeps_prefix_across_an_unencodable_suffix_barrier() {
        let small_length = encoded_event_length(&event(0)).expect("event is encodable");
        let trace = SbeRecordingTrace::with_retention(SbeTraceRetention::PrefixAndTail {
            prefix_capacity_bytes: small_length,
            tail_capacity_bytes: small_length,
        });
        let unencodable = TraceEvent::new(
            1,
            SimInstant::ZERO,
            EventKind::TaskPanicked {
                task: TaskId::from_parts(0, 0),
                panic: PanicRecord {
                    message: "x".repeat(MAX_PANIC_MESSAGE_BYTES + 1),
                    message_truncated: false,
                },
            },
        );

        trace.record(event(0));
        trace.record(unencodable);
        trace.record(event(2));

        assert_eq!(trace.events(), [event(0), event(2)]);
        assert_eq!(trace.dropped(), 1);
        assert_eq!(trace.encoding_failures(), 1);
    }

    #[test]
    fn prefix_and_tail_retains_ordered_ends_with_byte_bounds() {
        let frame_length = encoded_event_length(&event(0)).expect("event is encodable");
        let retention = SbeTraceRetention::PrefixAndTail {
            prefix_capacity_bytes: frame_length,
            tail_capacity_bytes: frame_length * 2,
        };
        assert_eq!(retention.capacity_bytes(), frame_length * 3);
        assert_eq!(retention.prefix_capacity_bytes(), frame_length);
        assert_eq!(retention.tail_capacity_bytes(), frame_length * 2);
        assert_eq!(retention.mode_name(), "prefix_and_tail");
        let trace = SbeRecordingTrace::with_retention(retention);

        for sequence in 0..4 {
            trace.record(event(sequence));
        }

        assert_eq!(trace.events(), [event(0), event(2), event(3)]);
        assert_eq!(trace.retained_bytes(), frame_length * 3);
        assert_eq!(trace.dropped(), 1);
    }

    #[test]
    fn every_event_variant_round_trips_through_recorder() {
        let events = all_events();
        let tags = events
            .iter()
            .map(|event| event.kind.tag())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(tags.len(), EventKindTag::ALL.len());
        assert!(EventKindTag::ALL.iter().all(|tag| tags.contains(tag)));
        let capacity = events
            .iter()
            .map(|event| encoded_event_length(event).expect("event is encodable"))
            .sum();
        let trace = SbeRecordingTrace::new(capacity);

        for event in events.iter().cloned() {
            trace.record(event);
        }

        assert_eq!(trace.len(), events.len());
        assert_eq!(trace.retained_bytes(), capacity);
        assert_eq!(trace.events(), events);
        assert_eq!(trace.encoding_failures(), 0);
        assert_eq!(trace.first_encoding_failure(), None);
    }

    #[test]
    fn ordering_validation_excludes_rejected_event_from_all_metadata() {
        let frame_length = encoded_event_length(&event(0)).expect("event is encodable");
        let trace = SbeRecordingTrace::new(frame_length * 3);
        let reference = RecordingTrace::new(3);

        trace.record(event(1));
        reference.record(event(1));
        trace.record(TraceEvent::new(
            1,
            SimInstant::MAX,
            EventKind::RuntimeStarted { seed: 999 },
        ));
        trace.record(event(3));
        reference.record(event(3));

        assert_eq!(trace.events(), [event(1), event(3)]);
        assert_eq!(
            trace.ordering_violation(),
            Some(TraceOrderingViolation {
                previous_sequence: 1,
                rejected_sequence: 1,
            })
        );
        assert_eq!(trace.last_sequence(), Some(3));
        assert_eq!(trace.fingerprint(), reference.fingerprint());
        assert_eq!(trace.dropped(), 0);
    }

    #[test]
    fn fingerprint_matches_typed_reference_before_retention_and_encoding() {
        let events = all_events();
        let trace = SbeRecordingTrace::new(0);
        let reference = RecordingTrace::new(0);

        for event in events {
            trace.record(event.clone());
            reference.record(event);
        }

        assert_eq!(trace.fingerprint(), reference.fingerprint());
        assert_eq!(trace.last_sequence(), reference.last_sequence());
        assert_eq!(trace.dropped(), reference.dropped());
        assert!(trace.is_empty());
    }

    #[test]
    fn sampling_preserves_global_sequence_gaps() {
        let frame_length = encoded_event_length(&event(0)).expect("event is encodable");
        let inner = Rc::new(SbeRecordingTrace::new(frame_length * 3));
        let sampling = SamplingTrace::new(
            inner.clone(),
            NonZeroU64::new(2).expect("period is nonzero"),
        );

        for sequence in 0..6 {
            let event = event(sequence);
            if sampling.should_record(sequence, event.kind.tag()) {
                sampling.record(event);
            }
        }

        assert_eq!(inner.events(), [event(0), event(2), event(4)]);
        assert_eq!(inner.last_sequence(), Some(4));
        assert_eq!(inner.dropped(), 0);
    }

    #[test]
    fn encoded_record_visitor_is_zero_copy_ordered_and_short_circuits() {
        let frame_length = encoded_event_length(&event(0)).expect("event is encodable");
        let trace = SbeRecordingTrace::new(frame_length * 3);
        for sequence in 0..3 {
            trace.record(event(sequence));
        }

        let mut decoded = Vec::new();
        trace
            .try_visit_encoded_records::<DecodeError>(|frame| {
                decoded.push(decode_event(frame)?);
                Ok(())
            })
            .expect("internal frames decode");
        assert_eq!(decoded, [event(0), event(1), event(2)]);

        let mut visited = 0;
        let result = trace.try_visit_encoded_records(|_| {
            visited += 1;
            if visited == 2 { Err("stop") } else { Ok(()) }
        });
        assert_eq!(result, Err("stop"));
        assert_eq!(visited, 2);
    }

    #[test]
    fn into_events_decodes_without_cloning_retained_trace_events() {
        let frame_length = encoded_event_length(&event(0)).expect("event is encodable");
        let trace = SbeRecordingTrace::new(frame_length * 2);
        trace.record(event(0));
        trace.record(event(1));

        assert_eq!(trace.into_events(), [event(0), event(1)]);
    }
}
