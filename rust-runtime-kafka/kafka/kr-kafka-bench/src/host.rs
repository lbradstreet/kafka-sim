use crate::{Histogram, OfferedLoad, config::Profile, corpus};
use kr_kafka_producer::{
    client::ProducerClient,
    credit::Resource,
    types::{DeliveryKind, Event, RecordDescriptor},
};
use kr_kafka_producer_host::producer::{HostProducer, HostStatus};
use kr_runtime::RuntimeDuration;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    thread,
    time::{Duration, Instant},
};

mod metrics;

fn clock_ticks() -> Option<u64> {
    let output = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()?;
    std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse()
        .ok()
}
fn process_cpu_ns(ticks: Option<u64>) -> Option<u64> {
    let ticks = ticks.filter(|n| *n != 0)?;
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let user: u64 = fields.get(11)?.parse().ok()?;
    let system: u64 = fields.get(12)?.parse().ok()?;
    Some(((u128::from(user) + u128::from(system)) * 1_000_000_000 / u128::from(ticks)) as u64)
}
fn peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
#[derive(Default)]
struct Metrics {
    offered: u64,
    accepted: u64,
    acked: u64,
    not_written: u64,
    unknown: u64,
    rejected: BTreeMap<String, u64>,
    pending: BTreeMap<u64, u64>,
    lateness: Histogram,
    admission: Histogram,
    offered_admission: Histogram,
    offer_delivery: Histogram,
    submit_delivery: Histogram,
    ack_delivery: Histogram,
    attempts: u64,
    input_copy_bytes: u64,
    credit_peaks: [usize; Resource::COUNT],
    credit_limits: [usize; Resource::COUNT],
    mailbox_peak: usize,
    live_inputs_peak: usize,
    samples: u64,
    provider_queue_peak: usize,
    provider_streams_peak: usize,
    provider_operations_peak: Option<usize>,
    provider_bytes_peak: Option<usize>,
    runtime_ingress_peak: usize,
    runtime_ingress_limit: usize,
    diagnostic_samples: u64,
    error: Option<String>,
    closed: bool,
}
impl Metrics {
    fn drain(&mut self, client: &ProducerClient, start: Instant, load: &OfferedLoad) {
        let mut events = [Event::Closed { unresolved: 0 }; 256];
        let count = client.poll_events(&mut events);
        for event in &events[..count] {
            match event {
                Event::Delivery(delivery) => {
                    let now = nanos(start);
                    let Some(submitted) = self.pending.remove(&delivery.user_token) else {
                        self.error = Some("duplicate or unrecognized delivery".into());
                        continue;
                    };
                    let elapsed = now.saturating_sub(load.due_ns(delivery.user_token));
                    self.offer_delivery.record(elapsed);
                    self.submit_delivery.record(now.saturating_sub(submitted));
                    self.attempts += u64::from(delivery.attempts);
                    match delivery.outcome.kind {
                        DeliveryKind::Acked => {
                            self.acked += 1;
                            self.ack_delivery.record(elapsed);
                        }
                        DeliveryKind::NotWritten => self.not_written += 1,
                        DeliveryKind::Unknown => self.unknown += 1,
                    }
                }
                Event::Fatal { code } => self.error = Some(format!("producer fatal code {code}")),
                Event::Closed { unresolved } => {
                    self.closed = true;
                    if *unresolved != 0 {
                        self.error = Some(format!("close unresolved count {unresolved}"));
                    }
                }
                _ => {}
            }
        }
    }
    fn sample(
        &mut self,
        client: &ProducerClient,
        diagnostics: Option<&kr_kafka_host::diagnostics::HostDiagnostics>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(diagnostics) = diagnostics {
            let snapshot = diagnostics.snapshot();
            if let Some(provider) = snapshot.provider {
                self.provider_queue_peak = self.provider_queue_peak.max(provider.queued_commands);
                self.provider_streams_peak = self.provider_streams_peak.max(provider.streams);
                if let Some(n) = provider.retained_operations {
                    self.provider_operations_peak =
                        Some(self.provider_operations_peak.unwrap_or(0).max(n));
                }
                if let Some(n) = provider.retained_bytes {
                    self.provider_bytes_peak = Some(self.provider_bytes_peak.unwrap_or(0).max(n));
                }
            }
            if let Some(runtime) = snapshot.runtime {
                self.runtime_ingress_peak = self.runtime_ingress_peak.max(runtime.queued_ingress);
                self.runtime_ingress_limit = runtime.ingress_limit;
                if runtime.overflowed {
                    self.error = Some("runtime ingress overflowed".into());
                }
            }
            if snapshot.completions.overflowed {
                self.error = Some("completion diagnostics overflowed".into());
            }
            self.diagnostic_samples += 1;
        }
        let status = client.status()?;
        for (index, pool) in status.credits.iter().enumerate() {
            self.credit_peaks[index] = self.credit_peaks[index].max(pool.held);
            self.credit_limits[index] = pool.limit;
        }
        self.mailbox_peak = self.mailbox_peak.max(status.mailbox.data_len);
        self.live_inputs_peak = self.live_inputs_peak.max(status.inputs.live);
        self.samples += 1;
        if status.owner_aborted {
            self.error = Some("owner aborted".into());
        }
        Ok(())
    }
}

pub fn run(profile: Profile) -> Result<Value, Box<dyn std::error::Error>> {
    let config = profile.producer()?;
    let validated = config.validate()?;
    // Corpus construction, TLS provisioning, codec calibration, and initial
    // metadata resolution are explicitly outside the measured interval.
    let payloads = corpus(
        profile.record_bytes,
        profile.seed,
        profile.pattern == "incompressible",
    );
    let host = if profile.native_diagnostics {
        HostProducer::start_with_diagnostics(config.clone())?
    } else {
        HostProducer::start(config.clone())?
    };
    let diagnostics = host.diagnostics();
    let client = host.client();
    let producer_metrics = client.metrics()?;
    let topic = client.open_topic(&profile.topic)?;
    let ready_deadline = Instant::now() + Duration::from_millis(profile.delivery_timeout_ms);
    loop {
        let mut events = [Event::Closed { unresolved: 0 }; 64];
        let count = client.poll_events(&mut events);
        if let Some(partitions) = events[..count].iter().find_map(|event| match event {
            Event::TopicReady {
                topic: handle,
                partitions,
                ..
            } if *handle == topic => Some(*partitions),
            _ => None,
        }) {
            if partitions != profile.partitions as i32 {
                return Err("topic partition count differs from benchmark profile".into());
            }
            break;
        }
        if events[..count].iter().any(|e| {
            matches!(
                e,
                Event::Fatal { .. } | Event::TopicFailed { .. } | Event::Closed { .. }
            )
        }) || Instant::now() >= ready_deadline
            || !matches!(host.status(), HostStatus::Running)
        {
            return Err("topic setup failed or timed out before measurement".into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    let mut load = OfferedLoad::new(profile.rate, profile.records)?;
    let mut metrics = Metrics::default();
    let initial_status = client.status()?;
    let ticks = clock_ticks();
    let cpu_start = process_cpu_ns(ticks);
    let start = Instant::now();
    let mut sample_at = 0;
    while let Some(due) = load.next_due_ns() {
        let now = nanos(start);
        metrics.drain(&client, start, &load);
        if now >= sample_at {
            metrics.sample(&client, diagnostics.as_ref())?;
            sample_at = now.saturating_add(1_000_000);
        }
        if let Some((index, due)) = load.take_due(now) {
            let payload = &payloads[index as usize % payloads.len()];
            let (partition_hint, key) = profile.route(index);
            let record = RecordDescriptor {
                topic,
                partition_hint,
                lane_hint: None,
                key: key.as_ref().map(|k| k.as_slice()),
                value: Some(payload),
                headers: &[],
                timestamp_ms: 0,
                user_token: index,
                delivery_timeout: None,
            };
            let before = nanos(start);
            let admitted = client.submit_copy(&[record]);
            let after = nanos(start);
            metrics.offered += 1;
            metrics.lateness.record(before.saturating_sub(due));
            metrics.admission.record(after.saturating_sub(before));
            if admitted.accepted == 1 {
                metrics.accepted += 1;
                metrics.offered_admission.record(after.saturating_sub(due));
                metrics.input_copy_bytes += payload.len() as u64 + key.map_or(0, |_| 8);
                metrics.pending.insert(index, before);
            } else {
                *metrics
                    .rejected
                    .entry(format!("{:?}", admitted.error))
                    .or_default() += 1;
            }
        } else {
            thread::sleep(Duration::from_nanos(due.saturating_sub(now).min(100_000)));
        }
    }
    let offered_elapsed = nanos(start);
    let close_error = host
        .close(RuntimeDuration::from_nanos(
            profile.delivery_timeout_ms * 1_000_000,
        ))
        .err()
        .map(|e| e.to_string());
    let drain_deadline =
        Instant::now() + Duration::from_millis(profile.delivery_timeout_ms + 10_000);
    while (matches!(host.status(), HostStatus::Running)
        || !metrics.closed
        || !metrics.pending.is_empty())
        && Instant::now() < drain_deadline
    {
        metrics.drain(&client, start, &load);
        metrics.sample(&client, diagnostics.as_ref())?;
        if !matches!(host.status(), HostStatus::Running) {
            metrics.drain(&client, start, &load);
            break;
        }
        thread::sleep(Duration::from_micros(100));
    }
    let delivery_elapsed = nanos(start);
    let cpu_ns = process_cpu_ns(ticks)
        .zip(cpu_start)
        .map(|(end, begin)| end.saturating_sub(begin));
    metrics.sample(&client, diagnostics.as_ref())?;
    let final_status = client.status()?;
    let before = initial_status.telemetry;
    let after = final_status.telemetry;
    let delta = |end: u64, begin: u64| end.saturating_sub(begin);
    if after.overflowed {
        metrics.error = Some("producer telemetry overflowed".into());
    }
    let telemetry = json!({
        "copied_input_bytes": delta(final_status.copied_input_bytes, initial_status.copied_input_bytes),
        "actor_polls": delta(after.actor_polls, before.actor_polls),
        "codec_input_bytes": delta(after.codec_input_bytes, before.codec_input_bytes),
        "lifetime_max_codec_quantum_bytes": after.max_codec_quantum_bytes,
        "lifetime_max_host_poll_nanos": after.max_host_poll_nanos,
        "produce_wire_bytes_confirmed": delta(after.produce_wire_bytes_confirmed, before.produce_wire_bytes_confirmed),
        "produce_staging_copy_bytes": delta(after.produce_staging_copy_bytes, before.produce_staging_copy_bytes),
        "produce_coalesced_copy_bytes": delta(after.produce_coalesced_copy_bytes, before.produce_coalesced_copy_bytes),
        "tls_instrumented": after.tls_instrumented,
        "all_connection_tls_ciphertext_bytes_confirmed": after.tls_instrumented.then(|| delta(after.tls_ciphertext_bytes_confirmed, before.tls_ciphertext_bytes_confirmed)),
        "tls_ciphertext_copy_bytes": after.tls_instrumented.then(|| delta(after.tls_ciphertext_copy_bytes, before.tls_ciphertext_copy_bytes)),
        "tls_plaintext_copy_bytes": after.tls_instrumented.then(|| delta(after.tls_plaintext_copy_bytes, before.tls_plaintext_copy_bytes)),
        "overflowed": after.overflowed
    });
    let backend = format!("{:?}", host.backend());
    let calibration = host.calibration();
    let status = host.status();
    // Never turn a bounded benchmark timeout into an unbounded join. A stopped
    // run is marked incomplete; provider-owned allocations obey normal teardown.
    let joined = if matches!(status, HostStatus::Running) {
        None
    } else {
        Some(host.join())
    };
    let complete = metrics.closed
        && metrics.pending.is_empty()
        && metrics.error.is_none()
        && close_error.is_none()
        && joined.as_ref().is_some_and(Result::is_ok);
    // No interval requests during measurement: owner destruction publishes the
    // final lifetime bank, and histogram scans stay on this reader after join.
    let producer_distributions = joined
        .as_ref()
        .and_then(|_| producer_metrics.try_take_snapshot())
        .map(|snapshot| metrics::summary(&snapshot));
    let batch_seals = joined.as_ref().and_then(|result| result.as_ref().ok()).map(|status| {
        let seals = status.batch_seals;
        json!({"scope":"producer lifetime", "reason_order":["target","linger","flush_or_close","hard_limit","deadline","context_reclaimed","sparse"],
            "by_reason":seals.by_reason, "raw_bytes":seals.raw_bytes, "target_bytes":seals.target_bytes,
            "raw_target_fill_ratio":(seals.target_bytes != 0).then(|| seals.raw_bytes as f64 / seals.target_bytes as f64), "overflowed":seals.overflowed})
    });
    let native_diagnostics = diagnostics.map(|diagnostics| {
        let s = diagnostics.snapshot().completions;
        json!({"scope":"completion counters cover stream I/O since connection setup, including TLS and control; queue peaks sampled during offered interval and drain", "instrumented":true,
            "sampled_provider_queue_peak":metrics.provider_queue_peak, "sampled_provider_streams_peak":metrics.provider_streams_peak,
            "sampled_provider_retained_operations_peak":metrics.provider_operations_peak, "sampled_provider_retained_bytes_peak":metrics.provider_bytes_peak,
            "sampled_runtime_ingress_peak":metrics.runtime_ingress_peak, "runtime_ingress_limit":metrics.runtime_ingress_limit, "samples":metrics.diagnostic_samples,
            "observed_operations":s.observed_operations,"published":s.published,"consumed":s.consumed,"abandoned":s.abandoned,"unpublished_discarded":s.unpublished_discarded,
            "pending":s.pending,"lifetime_peak_pending":s.peak_pending,"undrained":s.undrained,"lifetime_peak_undrained":s.peak_undrained,
            "drain_delay_ns_histogram":s.drain_delay_ns.to_vec(),"drain_histogram_bounds":"bucket0=0; buckets1..63=2^index; bucket64=u64::MAX inclusive upper bounds",
            "max_drain_delay_ns":s.max_drain_delay_ns,"overflowed":s.overflowed})
    });
    let mut unavailable = vec!["allocations (separate heaptrack profile)"];
    if native_diagnostics.is_none() {
        unavailable.extend([
            "provider_queue_pressure (native_diagnostics disabled)",
            "completion_drain_delay (native_diagnostics disabled)",
        ]);
    }
    if batch_seals.is_none() {
        unavailable.push("batch_fill_and_seal_reasons (owner did not join successfully)");
    }
    if producer_distributions.is_none() {
        unavailable.push(
            "producer_distributions (metrics disabled or owner did not publish a final interval)",
        );
    }
    let pools: Vec<_> = Resource::ALL.iter().enumerate().map(|(index, resource)| json!({
        "resource": format!("{resource:?}"), "limit": metrics.credit_limits[index], "sampled_peak_held": metrics.credit_peaks[index],
        "lifetime_peak_held": final_status.credits[index].peak_held,
        "reserved_during_interval": final_status.credits[index].reserved.saturating_sub(initial_status.credits[index].reserved),
        "released_during_interval": final_status.credits[index].released.saturating_sub(initial_status.credits[index].released)
    })).collect();
    Ok(json!({
        "schema": "kr-kafka-open-loop/v1", "implementation": "kr-kafka", "complete": complete,
        "profile": profile, "effective_producer_config": format!("{config:?}"),
        "backend": backend, "provider_topology": "one owner plus bounded shared native provider and control workers",
        "contiguous_staging": true, "calibration": {"sample_bytes": calibration.sample_bytes, "elapsed_ns": calibration.elapsed.as_nanos().to_string(), "encode_bytes_per_poll": calibration.encode_bytes_per_poll},
        "configured_memory": format!("{:?}", validated.memory),
        "offered": metrics.offered, "accepted": metrics.accepted, "rejected": metrics.offered - metrics.accepted,
        "rejections_by_reason": metrics.rejected, "acked": metrics.acked, "not_written": metrics.not_written,
        "unknown": metrics.unknown, "unresolved": metrics.pending.len(), "attempts_total": metrics.attempts,
        "offered_elapsed_ns": offered_elapsed, "delivery_elapsed_ns": delivery_elapsed,
        "acked_records_per_second": metrics.acked as f64 * 1e9 / delivery_elapsed.max(1) as f64,
        "acked_raw_bytes_per_second": metrics.acked as f64 * profile.record_bytes as f64 * 1e9 / delivery_elapsed.max(1) as f64,
        "process_cpu_ns": cpu_ns, "cpu_ns_per_ack": cpu_ns.filter(|_| metrics.acked != 0).map(|n| n as f64 / metrics.acked as f64),
        "process_peak_rss_kib": peak_rss_kib(),
        "scheduler_lateness": metrics.lateness.distribution(), "admission_call": metrics.admission.distribution(),
        "offered_to_admission": metrics.offered_admission.distribution(),
        "offered_to_delivery": metrics.offer_delivery.distribution(), "submit_to_delivery": metrics.submit_delivery.distribution(),
        "offered_to_ack": metrics.ack_delivery.distribution(),
        "histogram": "1025 fixed counters; quantiles upper bounds with <=6.25% bucket width above 16ns",
        "sample_interval_ns": 1_000_000, "samples": metrics.samples, "credit_pools": pools,
        "sampled_mailbox_peak": metrics.mailbox_peak, "sampled_live_inputs_peak": metrics.live_inputs_peak,
        "api_input_copied_bytes": metrics.input_copy_bytes,
        "telemetry": telemetry,
        "producer_distributions": producer_distributions,
        "corpus_bytes": payloads.iter().map(Vec::len).sum::<usize>(),
        "native_diagnostics": native_diagnostics, "batch_seals": batch_seals, "unavailable_metrics": unavailable,
        "host_status": format!("{status:?}"), "join": joined.map(|r| format!("{r:?}")), "error": metrics.error, "close_error": close_error
    }))
}
