use crate::{KR_ERR_EXHAUSTED, KR_ERR_INVALID, KrProducerConfig, KrSpan, memory};
use kr_kafka_producer::{
    config::{
        BrokerEndpoint, Compression, ProducerConfig, SaslMechanism, Secret, SecurityConfig,
        TlsConfig, TransportPolicy,
    },
    routing::{PartitionerConfig, UnkeyedPolicy},
};

pub(crate) unsafe fn decode(raw: KrProducerConfig) -> Result<ProducerConfig, i32> {
    if raw.reserved_request_policy != 0 {
        return Err(KR_ERR_INVALID);
    }
    let mut config = ProducerConfig {
        delivery_timeout: kr_runtime::RuntimeDuration::from_nanos(raw.delivery_timeout_ns),
        request_timeout: kr_runtime::RuntimeDuration::from_nanos(raw.request_timeout_ns),
        linger_max: kr_runtime::RuntimeDuration::from_nanos(raw.linger_max_ns),
        metadata_max_age: kr_runtime::RuntimeDuration::from_nanos(raw.metadata_max_age_ns),
        topic_resolve_timeout: kr_runtime::RuntimeDuration::from_nanos(
            raw.topic_resolve_timeout_ns,
        ),
        retry_backoff_min: kr_runtime::RuntimeDuration::from_nanos(raw.retry_backoff_min_ns),
        retry_backoff_max: kr_runtime::RuntimeDuration::from_nanos(raw.retry_backoff_max_ns),
        input_bytes: raw.input_bytes.try_into().map_err(|_| KR_ERR_INVALID)?,
        compressed_bytes: raw
            .compressed_bytes
            .try_into()
            .map_err(|_| KR_ERR_INVALID)?,
        control_reserve_bytes: raw
            .control_reserve_bytes
            .try_into()
            .map_err(|_| KR_ERR_INVALID)?,
        codec_workspace_bytes: raw
            .codec_workspace_bytes
            .try_into()
            .map_err(|_| KR_ERR_INVALID)?,
        max_in_flight_per_connection: raw
            .max_in_flight_per_connection
            .try_into()
            .map_err(|_| KR_ERR_INVALID)?,
        lanes: raw.lanes.try_into().map_err(|_| KR_ERR_INVALID)?,
        codec_contexts: raw.codec_contexts.try_into().map_err(|_| KR_ERR_INVALID)?,
        max_attempts: raw.max_attempts.try_into().map_err(|_| KR_ERR_INVALID)?,
        request_max_partitions: raw
            .request_max_partitions
            .try_into()
            .map_err(|_| KR_ERR_INVALID)?,
        brokers_max: raw.brokers_max.try_into().map_err(|_| KR_ERR_INVALID)?,
        worker_jobs: raw.worker_jobs.try_into().map_err(|_| KR_ERR_INVALID)?,
        connection_wire_window_bytes: raw.connection_wire_window_bytes,
        batch_target_bytes: raw.batch_target_bytes,
        batch_target_mode: match raw.batch_target_mode {
            0 => kr_kafka_producer::config::BatchTargetMode::EstimatedWire,
            1 => kr_kafka_producer::config::BatchTargetMode::Raw,
            _ => return Err(KR_ERR_INVALID),
        },
        batch_hard_bytes: raw.batch_hard_bytes,
        request_batching_policy: match raw.request_batching_policy {
            0 => kr_kafka_producer::config::RequestBatchingPolicy::Sealed,
            1 => kr_kafka_producer::config::RequestBatchingPolicy::SinglePartition,
            2 => kr_kafka_producer::config::RequestBatchingPolicy::BrokerReady,
            _ => return Err(KR_ERR_INVALID),
        },
        request_target_bytes: raw.request_target_bytes,
        request_hard_bytes: raw.request_hard_bytes,
        record_descriptors: raw.record_descriptors,
        staging_bytes_per_connection: raw.staging_bytes_per_connection,
        rx_bytes_per_connection: raw.rx_bytes_per_connection,
        delivery_event_capacity: raw.delivery_event_capacity,
        release_event_capacity: raw.release_event_capacity,
        mailbox_capacity: raw.mailbox_capacity,
        max_open_topics: raw.max_open_topics,
        pending_records_per_topic: raw.pending_records_per_topic,
        max_live_leases: raw.max_live_leases,
        max_batches: raw.max_batches,
        codec_window_log: raw.codec_window_log,
        output_chunk_bytes: raw.output_chunk_bytes,
        progressive_threshold: raw.progressive_threshold,
        tls_plaintext_bytes: raw.tls_plaintext_bytes,
        tls_ciphertext_bytes: raw.tls_ciphertext_bytes,
        max_header_count: raw.max_header_count,
        max_submissions_per_poll: raw.max_submissions_per_poll,
        max_submission_records: raw.max_submission_records,
        max_completions_per_poll: raw.max_completions_per_poll,
        sim_encode_bytes_per_poll: raw.sim_encode_bytes_per_poll,
        target_poll_ms: raw.target_poll_ms,
        coalesce_below_bytes: raw.coalesce_below_bytes,
        linger_skip_below_rate: if raw.linger_skip_below_rate == 0 {
            None
        } else {
            Some(raw.linger_skip_below_rate)
        },
        unkeyed_policy: match raw.unkeyed_policy {
            0 => UnkeyedPolicy::UniformBytes {
                run_bytes: raw.unkeyed_run_bytes,
            },
            1 => UnkeyedPolicy::Adaptive {
                run_bytes: raw.unkeyed_run_bytes,
            },
            _ => return Err(KR_ERR_INVALID),
        },
        partitioner: match raw.partitioner {
            0 => PartitionerConfig::Builtin,
            1 => PartitionerConfig::External,
            _ => return Err(KR_ERR_INVALID),
        },
        compression: match raw.compression {
            0 => Compression::None,
            1 => Compression::Zstd {
                level: raw
                    .compression_level
                    .try_into()
                    .map_err(|_| KR_ERR_INVALID)?,
            },
            _ => return Err(KR_ERR_INVALID),
        },
        transport: match raw.transport {
            0 => TransportPolicy::Uring,
            1 => TransportPolicy::Readiness,
            2 => TransportPolicy::Auto,
            _ => return Err(KR_ERR_INVALID),
        },
        ..ProducerConfig::default()
    };
    config.validate().map_err(|_| KR_ERR_INVALID)?;
    if !raw.client_id.ptr.is_null() || raw.client_id.len != 0 {
        // SAFETY: the create caller supplies immutable config spans for this call.
        config.client_id = unsafe { string(raw.client_id, i16::MAX as usize) }?;
    }
    if raw.bootstrap_count > 0 {
        if raw.bootstrap_count > u32::from(config.brokers_max) {
            return Err(KR_ERR_INVALID);
        }
        memory::check(raw.bootstrap, raw.bootstrap_count as usize)?;
        let mut bootstrap = Vec::new();
        bootstrap
            .try_reserve_exact(raw.bootstrap_count as usize)
            .map_err(|_| KR_ERR_EXHAUSTED)?;
        for index in 0..raw.bootstrap_count as usize {
            // SAFETY: bounds checked above; caller provides initialized versioned entries.
            let broker = unsafe { memory::versioned(raw.bootstrap.wrapping_add(index)) }?;
            // SAFETY: the broker name follows the input span validity contract.
            let host = unsafe { string(broker.host, 253) }?;
            bootstrap.push(BrokerEndpoint {
                host,
                port: broker.port.try_into().map_err(|_| KR_ERR_INVALID)?,
            });
        }
        config.bootstrap = bootstrap;
    }
    if raw.tls_system_roots > 1 || raw.security > 2 {
        return Err(KR_ERR_INVALID);
    }
    config.security = if raw.security == 0 {
        SecurityConfig::Plaintext
    } else {
        if raw.tls_root_count > 1024 {
            return Err(KR_ERR_INVALID);
        }
        memory::check(raw.tls_roots, raw.tls_root_count as usize)?;
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(raw.tls_root_count as usize)
            .map_err(|_| KR_ERR_EXHAUSTED)?;
        let mut remaining = config.control_reserve_bytes.min(4 * 1024 * 1024);
        for index in 0..raw.tls_root_count as usize {
            // SAFETY: caller supplies this initialized array of immutable spans.
            let root = unsafe { raw.tls_roots.wrapping_add(index).read() };
            // SAFETY: the certificate is readable for its configured length.
            let root = unsafe { memory::span(root, remaining.min(64 * 1024)) }?;
            remaining = remaining.checked_sub(root.len()).ok_or(KR_ERR_INVALID)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(root.len())
                .map_err(|_| KR_ERR_EXHAUSTED)?;
            bytes.extend_from_slice(root);
            roots.push(bytes);
        }
        let server_name = if raw.tls_server_name.len == 0 {
            None
        } else {
            // SAFETY: the optional server name is immutable for the create call.
            Some(unsafe { string(raw.tls_server_name, 253) }?)
        };
        let tls = TlsConfig {
            roots_der: roots,
            use_system_roots: raw.tls_system_roots == 1,
            server_name,
        };
        if raw.security == 1 {
            SecurityConfig::Tls { tls }
        } else {
            let mechanism = match raw.sasl_mechanism {
                0 => SaslMechanism::Plain,
                1 => SaslMechanism::ScramSha256,
                2 => SaslMechanism::ScramSha512,
                _ => return Err(KR_ERR_INVALID),
            };
            // SAFETY: credentials are copied while their immutable spans are valid.
            let username = unsafe { string(raw.username, 4096) }?;
            // SAFETY: the password follows the same create-call validity contract.
            let password = Secret::new(unsafe { string(raw.password, 4096) }?);
            SecurityConfig::SaslTls {
                tls,
                mechanism,
                username,
                password,
            }
        }
    };
    config.validate().map_err(|_| KR_ERR_INVALID)?;
    Ok(config)
}
pub(crate) unsafe fn string(span: KrSpan, limit: usize) -> Result<String, i32> {
    // SAFETY: forwarded span contract; the borrow does not outlive this function.
    let bytes = unsafe { memory::span(span, limit) }?;
    let value = std::str::from_utf8(bytes).map_err(|_| KR_ERR_INVALID)?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| KR_ERR_EXHAUSTED)?;
    owned.push_str(value);
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_kafka_producer::config::BatchTargetMode;

    #[test]
    fn request_policy_values_and_reserved_bits_are_checked() {
        use kr_kafka_producer::config::RequestBatchingPolicy::*;
        for (value, expected) in [(0, Sealed), (1, SinglePartition), (2, BrokerReady)] {
            let raw = KrProducerConfig {
                request_batching_policy: value,
                ..Default::default()
            };
            // SAFETY: default spans are empty and all scalar fields initialized.
            let decoded = unsafe { decode(raw) }.unwrap();
            assert_eq!(decoded.request_batching_policy, expected);
        }
        for raw in [
            KrProducerConfig {
                request_batching_policy: 3,
                ..Default::default()
            },
            KrProducerConfig {
                reserved_request_policy: 1,
                ..Default::default()
            },
        ] {
            // SAFETY: invalid scalars are rejected before accessing spans.
            assert_eq!(unsafe { decode(raw) }.unwrap_err(), KR_ERR_INVALID);
        }
    }

    #[test]
    fn batch_target_default_override_and_unknown_value_are_explicit() {
        for (value, expected) in [
            (0, BatchTargetMode::EstimatedWire),
            (1, BatchTargetMode::Raw),
        ] {
            let raw = KrProducerConfig {
                batch_target_mode: value,
                ..Default::default()
            };
            // SAFETY: default spans are empty and all scalar fields are initialized.
            let decoded = unsafe { decode(raw) }.unwrap();
            assert_eq!(decoded.batch_target_mode, expected);
        }
        let raw = KrProducerConfig {
            batch_target_mode: 2,
            ..Default::default()
        };
        // SAFETY: the invalid scalar is rejected before any span is accessed.
        assert_eq!(unsafe { decode(raw) }.unwrap_err(), KR_ERR_INVALID);
    }
}
