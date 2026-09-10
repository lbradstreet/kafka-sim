use std::collections::BTreeSet;

use kr_runtime_trace_wire::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use kr_runtime_trace_wire::runtime_started_codec::{RuntimeStartedDecoder, RuntimeStartedEncoder};
use kr_runtime_trace_wire::task_panicked_codec::{TaskPanickedDecoder, TaskPanickedEncoder};
use kr_runtime_trace_wire::{ReadBuf, WriteBuf};

#[test]
fn generated_schema_coordinates_and_changed_shapes_are_pinned() {
    assert_eq!(kr_runtime_trace_wire::SBE_SCHEMA_ID, 1);
    assert_eq!(kr_runtime_trace_wire::SBE_SCHEMA_VERSION, 1);
    assert_eq!(kr_runtime_trace_wire::SBE_SEMANTIC_VERSION, "2.0.0");
    assert_eq!(
        (
            kr_runtime_trace_wire::artifact_header_codec::SBE_TEMPLATE_ID,
            kr_runtime_trace_wire::artifact_header_codec::SBE_BLOCK_LENGTH,
        ),
        (4, 240),
    );
    assert_eq!(
        (
            kr_runtime_trace_wire::task_snapshot_codec::SBE_TEMPLATE_ID,
            kr_runtime_trace_wire::task_snapshot_codec::SBE_BLOCK_LENGTH,
        ),
        (5, 9),
    );
    assert_eq!(
        (
            kr_runtime_trace_wire::task_spawned_codec::SBE_TEMPLATE_ID,
            kr_runtime_trace_wire::task_spawned_codec::SBE_BLOCK_LENGTH,
        ),
        (120, 33),
    );
}

#[test]
fn generated_template_ids_are_unique_and_stable() {
    let template_ids = [
        kr_runtime_trace_wire::artifact_header_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::random_stream_state_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_snapshot_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::runtime_started_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_spawned_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_enqueued_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_poll_started_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_pending_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_completed_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_cancelled_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_panicked_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::task_drop_panicked_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::waker_panicked_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::timer_scheduled_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::timer_fired_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::timer_cancelled_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::time_advanced_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::runtime_stalled_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::budget_exhausted_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::runtime_stopped_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::random_choice_u64_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::random_choice_below_codec::SBE_TEMPLATE_ID,
        kr_runtime_trace_wire::random_choice_bool_ratio_codec::SBE_TEMPLATE_ID,
    ];

    assert_eq!(
        template_ids,
        [
            4, 2, 5, 100, 120, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
            115, 116, 117, 118, 119,
        ]
    );
    assert_eq!(
        template_ids.iter().copied().collect::<BTreeSet<_>>().len(),
        template_ids.len()
    );
    for retired in [1, 3, 101] {
        assert!(
            !template_ids.contains(&retired),
            "retired template ID {retired} was reused"
        );
    }
}

#[test]
fn fixed_message_round_trips_through_generated_codec() {
    let mut bytes = [0_u8; 64];
    let encoder =
        RuntimeStartedEncoder::default().wrap(WriteBuf::new(&mut bytes), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut encoder = header.parent().expect("runtime-started encoder is present");
    encoder.sequence(7).at_ns(11).seed(u64::MAX - 1);
    let encoded_length = MESSAGE_HEADER_LENGTH + encoder.encoded_length();

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes[..encoded_length]), 0);
    let decoder = RuntimeStartedDecoder::default().header(header, 0);

    assert_eq!(decoder.sequence(), 7);
    assert_eq!(decoder.at_ns(), 11);
    assert_eq!(decoder.seed(), u64::MAX - 1);
    assert_eq!(
        decoder.encoded_length(),
        usize::from(kr_runtime_trace_wire::runtime_started_codec::SBE_BLOCK_LENGTH)
    );
}

#[test]
fn variable_utf8_message_round_trips_through_generated_codec() {
    let message = "broken quote: \"; snow: 雪";
    let expected_length = MESSAGE_HEADER_LENGTH
        + usize::from(kr_runtime_trace_wire::task_panicked_codec::SBE_BLOCK_LENGTH)
        + size_of::<u32>()
        + message.len();
    let mut bytes = vec![0_u8; expected_length];
    let encoder =
        TaskPanickedEncoder::default().wrap(WriteBuf::new(&mut bytes), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut encoder = header.parent().expect("task-panicked encoder is present");
    encoder.sequence(13).at_ns(17).message_truncated(1);
    let mut task = encoder.task_encoder();
    task.slot(19).generation(23);
    let mut encoder = task.parent().expect("task encoder parent is present");
    encoder.message(message);
    assert_eq!(
        MESSAGE_HEADER_LENGTH + encoder.encoded_length(),
        expected_length
    );

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
    let decoder = TaskPanickedDecoder::default().header(header, 0);
    assert_eq!(decoder.sequence(), 13);
    assert_eq!(decoder.at_ns(), 17);
    assert_eq!(decoder.message_truncated(), 1);
    let mut task = decoder.task_decoder();
    assert_eq!(task.slot(), 19);
    assert_eq!(task.generation(), 23);
    let mut decoder = task.parent().expect("task decoder parent is present");
    let coordinates = decoder.message_decoder();
    assert_eq!(
        std::str::from_utf8(decoder.message_slice(coordinates)).expect("message is UTF-8"),
        message
    );
    assert_eq!(
        MESSAGE_HEADER_LENGTH + decoder.encoded_length(),
        expected_length
    );
}
