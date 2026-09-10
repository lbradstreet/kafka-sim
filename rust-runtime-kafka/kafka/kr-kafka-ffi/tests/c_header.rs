//! Compile every C layout against Rust sizes, alignments and field offsets.
use kr_kafka_ffi::*;
use std::{
    fmt::Write,
    mem::{align_of, offset_of, size_of},
    process::Command,
};

#[test]
fn c_header_matches_every_exported_field() {
    let mut source = String::from("#include \"kr_kafka.h\"\n");
    macro_rules! layout {
        ($rust:ty, $c:literal, $($field:ident),+ $(,)?) => {
            writeln!(source, "_Static_assert(sizeof({}) == {}, \"size\");", $c, size_of::<$rust>()).unwrap();
            writeln!(source, "_Static_assert(_Alignof({}) == {}, \"alignment\");", $c, align_of::<$rust>()).unwrap();
            $(writeln!(source, "_Static_assert(offsetof({}, {}) == {}, \"offset\");", $c, stringify!($field), offset_of!($rust, $field)).unwrap();)+
        };
    }
    layout!(
        KrTopicStatus,
        "kr_topic_status",
        struct_size,
        status,
        generation,
        topic_id,
        partition_count,
        reason
    );
    layout!(
        KrMetadataSnapshot,
        "kr_metadata_snapshot",
        struct_size,
        status,
        generation,
        topic_id,
        partition_count,
        reason,
        snapshot,
        broker_count
    );
    layout!(
        KrMetadataBroker,
        "kr_metadata_broker",
        struct_size,
        id,
        port,
        host_len,
        rack_len,
        rack_present
    );
    layout!(
        KrMetadataPartition,
        "kr_metadata_partition",
        struct_size,
        partition,
        leader,
        leader_epoch,
        error_code,
        replica_count,
        isr_count,
        offline_count
    );
    layout!(KrSpan, "kr_span", ptr, len);
    layout!(KrBroker, "kr_broker", struct_size, host, port);
    layout!(
        KrHeader,
        "kr_header",
        struct_size,
        key,
        value,
        value_is_null
    );
    layout!(
        KrRecord,
        "kr_record",
        struct_size,
        topic,
        partition_hint,
        lane_hint,
        key,
        key_is_null,
        value,
        value_is_null,
        headers,
        header_count,
        timestamp_ms,
        user_token,
        delivery_timeout_ns
    );
    layout!(
        KrEvent,
        "kr_event",
        struct_size,
        kind,
        token,
        user_token,
        topic,
        topic_id,
        partition,
        outcome,
        reason,
        base_offset,
        base_offset_present,
        timestamp_ms,
        timestamp_present,
        attempts,
        count
    );
    layout!(
        KrProducerConfig,
        "kr_producer_config",
        struct_size,
        delivery_timeout_ns,
        request_timeout_ns,
        linger_max_ns,
        metadata_max_age_ns,
        topic_resolve_timeout_ns,
        retry_backoff_min_ns,
        retry_backoff_max_ns,
        input_bytes,
        compressed_bytes,
        control_reserve_bytes,
        codec_workspace_bytes,
        max_in_flight_per_connection,
        lanes,
        codec_contexts,
        max_attempts,
        request_max_partitions,
        brokers_max,
        worker_jobs,
        connection_wire_window_bytes,
        batch_target_bytes,
        batch_target_mode,
        request_batching_policy,
        reserved_request_policy,
        batch_hard_bytes,
        request_target_bytes,
        request_hard_bytes,
        record_descriptors,
        staging_bytes_per_connection,
        rx_bytes_per_connection,
        delivery_event_capacity,
        release_event_capacity,
        mailbox_capacity,
        max_open_topics,
        pending_records_per_topic,
        max_live_leases,
        max_batches,
        codec_window_log,
        output_chunk_bytes,
        progressive_threshold,
        tls_plaintext_bytes,
        tls_ciphertext_bytes,
        max_header_count,
        max_submissions_per_poll,
        max_submission_records,
        max_completions_per_poll,
        sim_encode_bytes_per_poll,
        target_poll_ms,
        coalesce_below_bytes,
        linger_skip_below_rate,
        unkeyed_policy,
        unkeyed_run_bytes,
        partitioner,
        compression,
        compression_level,
        transport,
        security,
        sasl_mechanism,
        tls_system_roots,
        client_id,
        bootstrap,
        bootstrap_count,
        tls_roots,
        tls_root_count,
        tls_server_name,
        username,
        password
    );
    let directory =
        std::env::temp_dir().join(format!("kr-kafka-ffi-layout-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let source_path = directory.join("layout.c");
    std::fs::write(&source_path, source).unwrap();
    let compiler = std::env::var_os("CC").unwrap_or_else(|| "cc".into());
    let output = Command::new(compiler)
        .args(["-std=c11", "-Werror", "-fsyntax-only", "-I"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/include"))
        .arg(&source_path)
        .output()
        .expect("C compiler is required to verify the public header");
    let _ = std::fs::remove_dir_all(directory);
    assert!(
        output.status.success(),
        "C header mismatch: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
