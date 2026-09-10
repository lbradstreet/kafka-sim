use kr_runtime_ring_trace_wire::completion_certainty::CompletionCertainty;
use kr_runtime_ring_trace_wire::message_header_codec::{
    ENCODED_LENGTH as MESSAGE_HEADER_LENGTH, MessageHeaderDecoder,
};
use kr_runtime_ring_trace_wire::session_state::SessionState;
use kr_runtime_ring_trace_wire::storage_operation::StorageOperation;
use kr_runtime_ring_trace_wire::storage_outcome::StorageOutcome;
use kr_runtime_ring_trace_wire::storage_trace_artifact_codec::encoder::StepsEncoder;
use kr_runtime_ring_trace_wire::storage_trace_artifact_codec::{
    SBE_BLOCK_LENGTH, SBE_TEMPLATE_ID, StorageTraceArtifactDecoder, StorageTraceArtifactEncoder,
};
use kr_runtime_ring_trace_wire::{
    ReadBuf, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, SBE_SEMANTIC_VERSION, WriteBuf,
};

#[test]
fn generated_coordinates_are_stable() {
    assert_eq!(SBE_SCHEMA_ID, 2);
    assert_eq!(SBE_SCHEMA_VERSION, 0);
    assert_eq!(SBE_SEMANTIC_VERSION, "1.0.0");
    assert_eq!(SBE_TEMPLATE_ID, 1);
    assert_eq!(SBE_BLOCK_LENGTH, 169);
    assert_eq!(
        StepsEncoder::<StorageTraceArtifactEncoder<'_>>::block_length(),
        157
    );
    assert_eq!(MESSAGE_HEADER_LENGTH, 8);
    assert_eq!(
        kr_runtime_ring_trace_wire::storage_config_snapshot_codec::ENCODED_LENGTH,
        64
    );
    assert_eq!(
        kr_runtime_ring_trace_wire::runtime_metadata_codec::ENCODED_LENGTH,
        85
    );
    assert_eq!(
        kr_runtime_ring_trace_wire::storage_status_snapshot_codec::ENCODED_LENGTH,
        55
    );

    assert_eq!(u8::from(StorageOperation::WriteAt), 2);
    assert_eq!(u8::from(StorageOutcome::Failed), 2);
    assert_eq!(u8::from(CompletionCertainty::MayHaveApplied), 3);
    assert_eq!(u8::from(SessionState::Closed), 2);
}

#[test]
fn artifact_group_optional_fields_and_variable_data_round_trip() {
    let mut bytes = vec![0_u8; 4 * 1024];
    let encoder = StorageTraceArtifactEncoder::default()
        .wrap(WriteBuf::new(&mut bytes), MESSAGE_HEADER_LENGTH);
    let mut header = encoder.header(0);
    let mut encoder = header.parent().expect("artifact encoder is present");
    encoder
        .artifact_schema_version(1)
        .started_at_ns(u64::MAX - 100)
        .completed_at_ns(u64::MAX - 1);

    let mut config = encoder.config_encoder();
    config
        .max_file_bytes(4096)
        .max_read_bytes(128)
        .max_write_bytes(256)
        .max_read_chunk(64)
        .max_write_chunk(32)
        .max_in_flight(4)
        .max_scripted_faults(8)
        .default_latency_ns(17);
    let encoder = config.parent().expect("config parent is present");

    let mut runtime = encoder.runtime_encoder();
    runtime
        .reproduction_schema(1)
        .checkpoint_schema(2)
        .rng_version(3)
        .stopped(0)
        .seed(u64::MAX - 2)
        .now_ns(u64::MAX - 3)
        .total_steps(u64::MAX - 4)
        .next_enqueue_sequence(u64::MAX - 5)
        .next_timer_sequence(u64::MAX - 6)
        .next_timer_id(u64::MAX - 7)
        .ready_tasks(0)
        .live_timers(0)
        .live_tasks(1);
    let encoder = runtime.parent().expect("runtime parent is present");

    let mut steps = encoder.steps_encoder(1, StepsEncoder::default());
    assert_eq!(steps.advance().expect("step group is valid"), Some(0));
    steps
        .sequence(0)
        .started_at_ns(u64::MAX - 20)
        .completed_at_ns(u64::MAX - 10)
        .operation(StorageOperation::WriteAt)
        .outcome(StorageOutcome::Failed)
        .certainty(CompletionCertainty::MayHaveApplied)
        .offset_opt(Some(u64::MAX - 30))
        .transfer_length_opt(Some(u64::MAX - 40))
        .result_length_opt(None);

    let mut before = steps.before_encoder();
    before
        .session(SessionState::Open)
        .accepted_len(3)
        .durable_len(2)
        .fsync_gate_version(1)
        .has_fsync_gated_data(0)
        .in_flight(1)
        .in_flight_limit(4)
        .pending_faults(1)
        .fault_hits(7)
        .closed(0);
    let steps = before.parent().expect("before-status parent is present");

    let mut after = steps.after_encoder();
    after
        .session(SessionState::Closed)
        .accepted_len(4)
        .durable_len(2)
        .fsync_gate_version(1)
        .has_fsync_gated_data(1)
        .in_flight(0)
        .in_flight_limit(4)
        .pending_faults(0)
        .fault_hits(8)
        .closed(1);
    let mut steps = after.parent().expect("after-status parent is present");
    steps
        .phase("ambiguous-sync")
        .description("A bounded description with UTF-8 snow: 雪")
        .summary("completion was lost")
        .request_bytes(&[1, 2, 3])
        .result_bytes(&[4, 5])
        .accepted_bytes_before(&[10, 11, 12])
        .durable_bytes_before(&[10, 11])
        .accepted_bytes_after(&[10, 11, 12, 13])
        .durable_bytes_after(&[10, 11]);
    assert_eq!(steps.advance().expect("step group is valid"), None);
    let mut encoder = steps.parent().expect("steps parent is present");
    encoder
        .scenario("ambiguous_sync_then_recovery")
        .source_test("io/kr-runtime-io/src/storage/test_support.rs::tests::storage_trace")
        .provider("SimStorage")
        .generator("kr-runtime-trace-tool/generate_storage_trace");
    let encoded_length = MESSAGE_HEADER_LENGTH + encoder.encoded_length();

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes[..encoded_length]), 0);
    assert_eq!(header.block_length(), SBE_BLOCK_LENGTH);
    assert_eq!(header.template_id(), SBE_TEMPLATE_ID);
    assert_eq!(header.schema_id(), SBE_SCHEMA_ID);
    assert_eq!(header.version(), SBE_SCHEMA_VERSION);
    let decoder = StorageTraceArtifactDecoder::default().header(header, 0);
    assert_eq!(decoder.artifact_schema_version(), 1);
    assert_eq!(decoder.started_at_ns(), u64::MAX - 100);
    assert_eq!(decoder.completed_at_ns(), u64::MAX - 1);

    let mut config = decoder.config_decoder();
    assert_eq!(config.max_file_bytes(), 4096);
    assert_eq!(config.max_write_chunk(), 32);
    let decoder = config.parent().expect("config parent is present");
    let mut runtime = decoder.runtime_decoder();
    assert_eq!(runtime.seed(), u64::MAX - 2);
    assert_eq!(runtime.total_steps(), u64::MAX - 4);
    let decoder = runtime.parent().expect("runtime parent is present");

    let mut steps = decoder.steps_decoder();
    assert_eq!(steps.count(), 1);
    assert_eq!(steps.advance().expect("step group is valid"), Some(0));
    assert_eq!(steps.operation(), StorageOperation::WriteAt);
    assert_eq!(steps.outcome(), StorageOutcome::Failed);
    assert_eq!(steps.certainty(), CompletionCertainty::MayHaveApplied);
    assert_eq!(steps.offset(), Some(u64::MAX - 30));
    assert_eq!(steps.transfer_length(), Some(u64::MAX - 40));
    assert_eq!(steps.result_length(), None);

    let mut before = steps.before_decoder();
    assert_eq!(before.session(), SessionState::Open);
    assert_eq!(before.accepted_len(), 3);
    let steps = before.parent().expect("before-status parent is present");
    let mut after = steps.after_decoder();
    assert_eq!(after.session(), SessionState::Closed);
    assert_eq!(after.closed(), 1);
    let mut steps = after.parent().expect("after-status parent is present");

    let phase = steps.phase_decoder();
    assert_eq!(steps.phase_slice(phase), b"ambiguous-sync");
    let description = steps.description_decoder();
    assert!(
        std::str::from_utf8(steps.description_slice(description))
            .expect("description is UTF-8")
            .ends_with('雪')
    );
    let summary = steps.summary_decoder();
    assert_eq!(steps.summary_slice(summary), b"completion was lost");
    let request = steps.request_bytes_decoder();
    assert_eq!(steps.request_bytes_slice(request), [1, 2, 3]);
    let result = steps.result_bytes_decoder();
    assert_eq!(steps.result_bytes_slice(result), [4, 5]);
    let accepted_before = steps.accepted_bytes_before_decoder();
    assert_eq!(
        steps.accepted_bytes_before_slice(accepted_before),
        [10, 11, 12]
    );
    let durable_before = steps.durable_bytes_before_decoder();
    assert_eq!(steps.durable_bytes_before_slice(durable_before), [10, 11]);
    let accepted_after = steps.accepted_bytes_after_decoder();
    assert_eq!(
        steps.accepted_bytes_after_slice(accepted_after),
        [10, 11, 12, 13]
    );
    let durable_after = steps.durable_bytes_after_decoder();
    assert_eq!(steps.durable_bytes_after_slice(durable_after), [10, 11]);
    assert_eq!(steps.advance().expect("step group is valid"), None);

    let mut decoder = steps.parent().expect("steps parent is present");
    let scenario = decoder.scenario_decoder();
    assert_eq!(
        decoder.scenario_slice(scenario),
        b"ambiguous_sync_then_recovery"
    );
    let source_test = decoder.source_test_decoder();
    assert!(
        decoder
            .source_test_slice(source_test)
            .ends_with(b"storage_trace")
    );
    let provider = decoder.provider_decoder();
    assert_eq!(decoder.provider_slice(provider), b"SimStorage");
    let generator = decoder.generator_decoder();
    assert_eq!(
        decoder.generator_slice(generator),
        b"kr-runtime-trace-tool/generate_storage_trace"
    );
    assert_eq!(
        MESSAGE_HEADER_LENGTH + decoder.encoded_length(),
        encoded_length
    );
}
