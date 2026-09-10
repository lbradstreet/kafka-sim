mod experiment;

use crate::{
    Coverage, DomainEvent, DomainHistory, RecordSpec, ReplayManifest, Workload, history::Audit,
    stream::ModelConnector,
};
use kr_kafka_broker_model::BrokerModel;
use kr_kafka_producer::{
    actor::{ActorConfig, ProducerActor},
    admission::AdmissionError,
    client::{ClientClock, ProducerClient},
    engine::ProducerEngine,
    input::{LeasedHeader, LeasedRecordDescriptor},
    transport::WriteMode,
    types::*,
};
use kr_runtime::{
    DeterminismCheckpoint, RuntimeConfig, RuntimeDuration, RuntimeHandle, RuntimeInstant,
    RuntimeSnapshot, SimRuntime, rng::RandomStream, trace::sbe::SbeRecordingTrace,
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    future::{Future, poll_fn},
    path::Path,
    rc::Rc,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TerminalCheckpoint {
    pub schema_version: u32,
    pub now_ns: u64,
    pub total_steps: u64,
    pub next_enqueue_sequence: u64,
    pub next_timer_sequence: u64,
    pub next_timer_id: u64,
    pub ready_tasks: usize,
    pub live_timers: usize,
    pub live_tasks: usize,
    pub stopped: bool,
    pub random: Vec<crate::RngInput>,
}
impl TerminalCheckpoint {
    fn from_runtime(value: &DeterminismCheckpoint) -> Self {
        Self {
            schema_version: value.schema_version,
            now_ns: value.now.as_nanos(),
            total_steps: value.total_steps,
            next_enqueue_sequence: value.next_enqueue_sequence,
            next_timer_sequence: value.next_timer_sequence,
            next_timer_id: value.next_timer_id,
            ready_tasks: value.ready_tasks,
            live_timers: value.live_timers,
            live_tasks: value.live_tasks,
            stopped: value.stopped,
            random: value
                .random
                .iter()
                .map(|stream| crate::RngInput {
                    stream: format!("{:?}", stream.stream),
                    seed: kr_runtime::rng::derive_stream_seed(
                        value.reproduction.config.seed,
                        stream.stream,
                    ),
                    state: stream.checkpoint.state(),
                    draws: stream.checkpoint.draws(),
                })
                .collect(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunReport {
    pub manifest: ReplayManifest,
    pub history: DomainHistory,
    pub coverage: Coverage,
    pub fault_stats: crate::faults::FaultStats,
    pub fetched_records: u64,
    pub checkpoint: TerminalCheckpoint,
    pub pool_peaks: Vec<u64>,
    pub batch_raw_bytes: u64,
    pub batch_target_bytes: u64,
    /// Successful global HDR sample counts in Metric::ALL order. Recording
    /// diagnostics are outside the domain history and runtime checkpoint.
    pub metrics_counts: [u64; kr_kafka_producer::telemetry::metrics::Metric::COUNT],
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics_samples: Vec<crate::MetricsSample>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missed_metrics_requests: Vec<crate::MissedMetricsRequest>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunFailure {
    pub reason: String,
    pub manifest: ReplayManifest,
    pub history: DomainHistory,
    pub checkpoint: Option<TerminalCheckpoint>,
}
impl std::fmt::Display for RunFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seed {}: {}", self.manifest.seed, self.reason)
    }
}
impl std::error::Error for RunFailure {}

pub fn run(manifest: &ReplayManifest) -> Result<RunReport, Box<RunFailure>> {
    run_inner(manifest, None).0
}
/// A second untraced execution must match the complete producer-domain history
/// and the runtime's behavioral checkpoint; a seed-only comparison is rejected.
pub fn run_replayed(manifest: &ReplayManifest) -> Result<RunReport, Box<RunFailure>> {
    let first = run(manifest)?;
    let second = run(&first.manifest)?;
    if first.history != second.history
        || first.checkpoint != second.checkpoint
        || first.coverage != second.coverage
        || first.fault_stats != second.fault_stats
        || first.fetched_records != second.fetched_records
        || first.metrics_counts != second.metrics_counts
        || first.metrics_samples != second.metrics_samples
        || first.missed_metrics_requests != second.missed_metrics_requests
    {
        return Err(Box::new(RunFailure {
            reason: "deterministic rerun diverged".into(),
            manifest: first.manifest,
            history: first.history,
            checkpoint: Some(first.checkpoint),
        }));
    }
    Ok(first)
}
/// Verifies the bounded recorder cannot change producer events or behavioral
/// RNG/scheduler state. Debug trace detail is deliberately excluded.
pub fn verify_trace_transparency(manifest: &ReplayManifest) -> Result<RunReport, Box<RunFailure>> {
    let ordinary = run(manifest)?;
    let (recorded, _) = run_inner(
        &ordinary.manifest,
        Some(Rc::new(SbeRecordingTrace::new(manifest.limits.trace_bytes))),
    );
    let recorded = recorded?;
    if ordinary.history != recorded.history
        || ordinary.checkpoint != recorded.checkpoint
        || ordinary.coverage != recorded.coverage
        || ordinary.fault_stats != recorded.fault_stats
        || ordinary.fetched_records != recorded.fetched_records
        || ordinary.metrics_counts != recorded.metrics_counts
        || ordinary.metrics_samples != recorded.metrics_samples
        || ordinary.missed_metrics_requests != recorded.missed_metrics_requests
    {
        return Err(Box::new(RunFailure {
            reason: "trace changed behavioral execution".into(),
            manifest: ordinary.manifest,
            history: ordinary.history,
            checkpoint: Some(ordinary.checkpoint),
        }));
    }
    Ok(ordinary)
}
/// Ordinary runs remain untraced. A failure is repeated once with a bounded SBE
/// recorder, and all export work occurs after the runtime has stopped.
pub fn run_and_retain_failure(
    manifest: &ReplayManifest,
    directory: &Path,
) -> Result<RunReport, Box<RunFailure>> {
    match run_replayed(manifest) {
        Ok(report) => Ok(report),
        Err(original) => {
            let trace = Rc::new(SbeRecordingTrace::new(manifest.limits.trace_bytes));
            let (traced, snapshot) = run_inner(&original.manifest, Some(trace.clone()));
            let failure = match traced {
                Err(failure) => failure,
                Ok(report) => Box::new(RunFailure {
                    reason: format!("untraced failure did not repeat: {}", original.reason),
                    manifest: report.manifest,
                    history: report.history,
                    checkpoint: Some(report.checkpoint),
                }),
            };
            let write = (|| -> Result<(), String> {
                std::fs::create_dir_all(directory).map_err(|e| e.to_string())?;
                std::fs::write(directory.join("replay.json"), failure.manifest.to_json()?)
                    .map_err(|e| e.to_string())?;
                std::fs::write(
                    directory.join("producer-history.json"),
                    serde_json::to_vec_pretty(&failure).map_err(|e| e.to_string())?,
                )
                .map_err(|e| e.to_string())?;
                if let Some(snapshot) = snapshot {
                    let file = std::fs::File::create(directory.join("runtime.sbe"))
                        .map_err(|e| e.to_string())?;
                    kr_runtime_trace_tool::write_buffered_sbe_trace_artifact(
                        file,
                        &trace,
                        &snapshot,
                        kr_runtime_trace_tool::TraceArtifactMetadata::new(
                            "kr-kafka-sim/1",
                            "failed",
                        ),
                    )
                    .map_err(|e| e.to_string())?;
                }
                Ok(())
            })();
            if let Err(error) = write {
                return Err(Box::new(RunFailure {
                    reason: format!("{}; artifact export: {error}", failure.reason),
                    ..*failure
                }));
            }
            Err(failure)
        }
    }
}
fn run_inner(
    manifest: &ReplayManifest,
    trace: Option<Rc<SbeRecordingTrace>>,
) -> (Result<RunReport, Box<RunFailure>>, Option<RuntimeSnapshot>) {
    let empty = || DomainHistory {
        version: crate::HISTORY_VERSION,
        entries: Vec::new(),
    };
    if let Err(reason) = manifest.validate() {
        return (
            Err(Box::new(RunFailure {
                reason,
                manifest: manifest.clone(),
                history: empty(),
                checkpoint: None,
            })),
            None,
        );
    }
    let cfg = RuntimeConfig {
        seed: manifest.seed,
        max_tasks: manifest.runtime.tasks,
        max_timers: manifest.runtime.timers,
        max_steps_per_run: manifest.limits.steps,
        start_time: RuntimeInstant::from_nanos(manifest.start_ns),
        max_time: Some(RuntimeInstant::from_nanos(
            manifest.start_ns + manifest.limits.elapsed_ns,
        )),
    };
    let mut runtime = match trace {
        Some(trace) => SimRuntime::with_trace(cfg, trace),
        None => SimRuntime::new(cfg),
    };
    let diagnostics = runtime.handle();
    let handle = RuntimeHandle::Sim(runtime.handle());
    let audit = Rc::new(RefCell::new(Audit::new(
        manifest.limits.history_events,
        manifest.limits.records as usize,
    )));
    audit.borrow_mut().configure_links(manifest);
    if manifest.observe_requests {
        audit.borrow_mut().request_capture =
            Some(crate::request_observation::Capture::new(manifest));
    }
    let faults = match crate::faults::FaultEngine::new(
        manifest.faults.clone(),
        manifest.fault_decisions.clone(),
    ) {
        Ok(engine) => Rc::new(RefCell::new(engine)),
        Err(reason) => {
            return (
                Err(Box::new(RunFailure {
                    reason,
                    manifest: manifest.clone(),
                    history: empty(),
                    checkpoint: None,
                })),
                None,
            );
        }
    };
    let setup = (|| -> Result<_, String> {
        let (network, model) = crate::setup::network_and_model(&runtime, manifest)?;
        let engine =
            ProducerEngine::new(manifest.producer.clone(), None).map_err(|e| e.to_string())?;
        let credits = engine.credits();
        let connector = ModelConnector::new(
            handle.clone(),
            network.clone(),
            audit.clone(),
            model.clone(),
            Rc::new(manifest.clone()),
            faults.clone(),
            runtime.random_source(RandomStream::Fault),
        )?;
        let probe_connector = manifest.fetch_probe.then(|| connector.clone());
        let (client, actor) = ProducerActor::new(
            handle.clone(),
            engine,
            connector,
            ClientClock::Simulation,
            ActorConfig {
                write_mode: if manifest.driver.vectored {
                    WriteMode::Vectored
                } else {
                    WriteMode::Staging
                },
                encode_bytes_per_poll: manifest.driver.encode_bytes,
                sim_encode_cost: RuntimeDuration::from_nanos(manifest.driver.encode_cost_ns),
            },
        )
        .map_err(|e| e.to_string())?;
        let actor = handle.spawn(actor).map_err(|e| e.to_string())?;
        Ok((network, model, credits, client, actor, probe_connector))
    })();
    let (network, model, credits, client, actor, probe_connector) = match setup {
        Ok(value) => value,
        Err(reason) => {
            let _ = runtime.finish();
            let snapshot = diagnostics.snapshot();
            return (
                Err(Box::new(RunFailure {
                    reason,
                    manifest: manifest.clone(),
                    history: audit.borrow().history.clone(),
                    checkpoint: Some(TerminalCheckpoint::from_runtime(
                        &snapshot.determinism_checkpoint(),
                    )),
                })),
                Some(snapshot),
            );
        }
    };
    let metrics = client.metrics().expect("simulation owner metrics handle");
    let sampling = manifest.metrics_sampling;
    let sample_output = sampling.map(|_| Rc::new(RefCell::new(crate::metrics::Samples::default())));
    let sample_run = sample_output.clone();
    let sampling_end = manifest.start_ns + manifest.limits.elapsed_ns;
    let workload = manifest.workload.clone();
    let experiment_manifest = manifest.experiment.as_ref().map(|_| manifest.clone());
    let start_ns = manifest.start_ns;
    let topology = manifest.topics.clone();
    let old_broker = manifest.produce_max_version < 13;
    let recovery_round_timeout_ns = manifest.recovery_round_timeout_ns;
    let admission_retry_limit = manifest.driver.admission_retry_limit;
    let admission_retry_delay_ns = manifest.driver.admission_retry_delay_ns;
    let audit_run = audit.clone();
    let credits_run = credits.clone();
    let model_run = model.clone();
    let faults_run = faults.clone();
    let execution = runtime.block_on(async move {
        let sampler = match sampling {
            Some(config) => Some(crate::metrics::Sampler::spawn(
                &handle,
                &client,
                config,
                start_ns,
                sampling_end,
                sample_run.expect("sampling output"),
            )?),
            None => None,
        };
        let mut topics = Vec::new();
        for topic in &topology {
            topics.push(
                client
                    .open_topic_at(&topic.name, handle.now())
                    .map_err(|e| e.to_string())?,
            );
        }
        let mut closed = false;
        let mut last_flush = None;
        let mut round_deadline = None;
        let mut current_ids: Vec<_> = topology.iter().map(|topic| topic.id).collect();
        if let Some(manifest) = experiment_manifest {
            closed = experiment::Driver {
                client: &client,
                audit: &audit_run,
                credits: &credits_run,
                handle: &handle,
                model: &model_run,
                manifest: &manifest,
                topics: &mut topics,
            }
            .drive()
            .await?;
        }
        for (step, operation) in workload.into_iter().enumerate() {
            audit_run.borrow_mut().record(
                handle.now().as_nanos(),
                DomainEvent::WorkloadStep {
                    index: step as u32,
                    action: workload_name(&operation),
                },
            );
            if closed {
                break;
            }
            let round_remaining = round_deadline
                .map(|deadline: u64| deadline.saturating_sub(handle.now().as_nanos()));
            if round_remaining == Some(0) {
                return Err("aggregate recovery round virtual-time budget exceeded".into());
            }
            match operation {
                Workload::SettleAllAccepted {
                    timeout_ns,
                    require_acked,
                } => {
                    let watermark = audit_run
                        .borrow()
                        .accepted
                        .last_key_value()
                        .map_or(0, |(token, _)| *token);
                    wait_barrier(
                        &client,
                        &audit_run,
                        &credits_run,
                        &handle,
                        Barrier {
                            count: 0,
                            flush: None,
                            watermark: Some(watermark),
                            timeout_ns,
                            require_acked,
                        },
                        &mut closed,
                    )
                    .await?;
                }
                Workload::SleepUntil { at_ns } => {
                    handle
                        .sleep_until(RuntimeInstant::from_nanos(
                            start_ns.checked_add(at_ns).ok_or("sleep-until overflow")?,
                        ))
                        .await
                        .map_err(|e| e.to_string())?;
                }
                Workload::Submit { records } => {
                    let mut offset = 0;
                    let mut attempts = 0;
                    while offset < records.len() {
                        attempts += 1;
                        if attempts > admission_retry_limit {
                            return Err("workload admission driving bound".into());
                        }
                        let count = if records[offset].native {
                            1
                        } else {
                            records[offset..]
                                .iter()
                                .take_while(|record| !record.native)
                                .count()
                        };
                        let (result, lease) = submit(
                            &client,
                            &records[offset..offset + count],
                            &topics,
                            handle.now(),
                        )?;
                        for (index, record) in records[offset..offset + count]
                            .iter()
                            .take(result.accepted as usize)
                            .enumerate()
                        {
                            let id = if old_broker {
                                [0; 16]
                            } else {
                                current_ids[record.topic as usize]
                            };
                            audit_run.borrow_mut().accept(
                                handle.now().as_nanos(),
                                record.id,
                                result.token(index as u32).expect("accepted token").0,
                                crate::history::AdmissionRoute {
                                    topic: id,
                                    partition: record.partition,
                                    key_routed: record.key_routed,
                                    handle: topics[record.topic as usize].0,
                                    resolved: !old_broker
                                        && client.topic_id(topics[record.topic as usize]).is_ok(),
                                },
                                lease.map(|lease| lease.0),
                            )?;
                        }
                        offset += result.accepted as usize;
                        if let Some(lease) = lease {
                            client.release(lease).map_err(|e| e.to_string())?;
                        }
                        audit_run
                            .borrow_mut()
                            .credits(handle.now().as_nanos(), &credits_run)?;
                        if result.accepted as usize != count {
                            audit_run.borrow_mut().coverage.backpressure += 1;
                            audit_run.borrow_mut().record(
                                handle.now().as_nanos(),
                                DomainEvent::Rejected {
                                    count: count as u32 - result.accepted,
                                },
                            );
                            if !matches!(
                                result.error,
                                Some(AdmissionError::Credit(_) | AdmissionError::BulkLimit)
                            ) {
                                break;
                            }
                            handle
                                .sleep(RuntimeDuration::from_nanos(admission_retry_delay_ns))
                                .await
                                .map_err(|e| e.to_string())?;
                            drain(
                                &client,
                                &audit_run,
                                &credits_run,
                                handle.now().as_nanos(),
                                &mut closed,
                            )?;
                            if closed {
                                break;
                            }
                        }
                    }
                }
                Workload::WaitDeliveries { count } => {
                    while audit_run.borrow().coverage.acked
                        + audit_run.borrow().coverage.not_written
                        + audit_run.borrow().coverage.unknown
                        < u64::from(count)
                        && !closed
                    {
                        let event = poll_fn(|cx| client.poll_event(cx))
                            .await
                            .map_err(|e| e.to_string())?
                            .ok_or("events ended before baseline delivery")?;
                        closed = matches!(event, Event::Closed { .. });
                        audit_run
                            .borrow_mut()
                            .event(handle.now().as_nanos(), event)?;
                        audit_run
                            .borrow_mut()
                            .credits(handle.now().as_nanos(), &credits_run)?;
                    }
                }
                Workload::Flush => {
                    let token = client
                        .flush_at(handle.now())
                        .map_err(|error| format!("flush admission: {error}"))?;
                    audit_run
                        .borrow_mut()
                        .flush(handle.now().as_nanos(), token)?;
                    last_flush = Some(token);
                }
                Workload::AwaitFlush {
                    timeout_ns,
                    require_acked,
                } => {
                    let token = last_flush.ok_or("await flush without an admitted flush")?;
                    wait_barrier(
                        &client,
                        &audit_run,
                        &credits_run,
                        &handle,
                        Barrier {
                            count: 0,
                            flush: Some(token),
                            watermark: None,
                            timeout_ns: round_remaining
                                .map_or(timeout_ns, |left| left.min(timeout_ns)),
                            require_acked,
                        },
                        &mut closed,
                    )
                    .await?;
                }
                Workload::Settle {
                    count,
                    timeout_ns,
                    require_acked,
                } => {
                    wait_barrier(
                        &client,
                        &audit_run,
                        &credits_run,
                        &handle,
                        Barrier {
                            count,
                            flush: None,
                            watermark: None,
                            timeout_ns: round_remaining
                                .map_or(timeout_ns, |left| left.min(timeout_ns)),
                            require_acked,
                        },
                        &mut closed,
                    )
                    .await?;
                }
                Workload::BeginRound { round } => {
                    faults_run.borrow_mut().begin_round(round)?;
                    round_deadline = recovery_round_timeout_ns.map(|budget| {
                        handle.now().as_nanos()
                            + if round == 0 {
                                budget.min(2_000_000_000)
                            } else {
                                budget
                            }
                    });
                }
                Workload::CreateTopic { topic } => {
                    let spec = &topology[topic as usize];
                    model_run
                        .borrow_mut()
                        .create_topic_with_id(
                            &spec.name,
                            current_ids[topic as usize],
                            &spec.leaders,
                        )
                        .map_err(|error| error.to_string())?;
                }
                Workload::DeleteTopic { topic } => {
                    model_run
                        .borrow_mut()
                        .delete_topic(current_ids[topic as usize])
                        .map_err(|error| error.to_string())?;
                }
                Workload::RecreateTopic { topic, new_id } => {
                    let spec = &topology[topic as usize];
                    model_run
                        .borrow_mut()
                        .delete_topic(current_ids[topic as usize])
                        .map_err(|error| error.to_string())?;
                    audit_run
                        .borrow_mut()
                        .recreated
                        .insert(current_ids[topic as usize]);
                    model_run
                        .borrow_mut()
                        .create_topic_with_id(&spec.name, new_id, &spec.leaders)
                        .map_err(|error| error.to_string())?;
                    // Only explicitly reopened handles bind to this new identity.
                }
                Workload::AddPartitions {
                    topic,
                    additional_leaders,
                } => {
                    let id = current_ids[topic as usize];
                    let mut before = 0;
                    while model_run.borrow().leader(id, before).is_ok() {
                        before += 1;
                    }
                    model_run
                        .borrow_mut()
                        .add_partitions(id, &additional_leaders)
                        .map_err(|error| error.to_string())?;
                    let expected = before + additional_leaders.len() as i32;
                    if model_run.borrow().leader(id, expected - 1).is_err()
                        || model_run.borrow().leader(id, expected).is_ok()
                    {
                        return Err("partition growth exact-count oracle".into());
                    }
                }
                Workload::MoveLeader {
                    topic,
                    partition,
                    broker,
                } => {
                    model_run
                        .borrow_mut()
                        .move_leader(current_ids[topic as usize], partition, broker)
                        .map_err(|error| error.to_string())?;
                    audit_run.borrow_mut().coverage.leader_moves += 1;
                }
                Workload::CloseTopic { topic } => client
                    .close_topic(topics[topic as usize])
                    .map_err(|error| error.to_string())?,
                Workload::OpenTopic { topic } => {
                    topics[topic as usize] = client
                        .open_topic_at(&topology[topic as usize].name, handle.now())
                        .map_err(|error| error.to_string())?;
                    // Wait for public resolution before subsequent admissions use the new UUID.
                    loop {
                        match client.topic_id(topics[topic as usize]) {
                            Ok(id) => {
                                current_ids[topic as usize] = id.0;
                                break;
                            }
                            Err(_) => {
                                let event = poll_fn(|cx| client.poll_event(cx))
                                    .await
                                    .map_err(|error| error.to_string())?
                                    .ok_or("topic reopen ended")?;
                                if matches!(event, Event::Closed { .. } | Event::Fatal { .. }) {
                                    return Err("topic reopen failed".into());
                                }
                                audit_run
                                    .borrow_mut()
                                    .event(handle.now().as_nanos(), event)?;
                            }
                        }
                    }
                }
                Workload::Sleep { nanos } | Workload::StopPolling { nanos } => handle
                    .sleep(RuntimeDuration::from_nanos(nanos))
                    .await
                    .map_err(|e| e.to_string())?,
                Workload::Cancel { record_id } => {
                    if let Some(&token) = audit_run.borrow().ids.get(&record_id) {
                        let _ = client.cancel(RecordToken(token));
                    }
                }
                Workload::Close { deadline_ns } => {
                    let _ = client.close_at(handle.now(), RuntimeDuration::from_nanos(deadline_ns));
                }
            }
            if let Some(error) = audit_run.borrow().error.clone() {
                return Err(error);
            }
        }
        while !closed {
            let event = poll_fn(|cx| client.poll_event(cx))
                .await
                .map_err(|e| e.to_string())?
                .ok_or("events ended without Closed")?;
            closed = matches!(event, Event::Closed { .. });
            audit_run
                .borrow_mut()
                .event(handle.now().as_nanos(), event)?;
            audit_run
                .borrow_mut()
                .credits(handle.now().as_nanos(), &credits_run)?;
        }
        let status = actor
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        if let Some(sampler) = sampler {
            sampler.stop().await?;
        }
        if !status.closed {
            return Err("actor returned before Closed".into());
        }
        if status.batch_seals.overflowed {
            return Err("batch seal counter overflow".into());
        }
        audit_run.borrow_mut().coverage.linger_seals = status.batch_seals.by_reason[1];
        audit_run.borrow_mut().coverage.target_seals = status.batch_seals.by_reason[0];
        audit_run.borrow_mut().batch_fill = (
            status.batch_seals.raw_bytes,
            status.batch_seals.target_bytes,
        );
        drop(client);
        match probe_connector {
            Some(mut connector) => crate::probe::verify(&mut connector).await,
            None => Ok(0),
        }
    });
    let mut fetched_records = 0;
    let mut failure = match execution {
        Ok(Ok(count)) => {
            fetched_records = count;
            None
        }
        Ok(Err(error)) => Some(error),
        Err(error) => Some(error.to_string()),
    };
    if failure.is_none()
        && let Err(error) = runtime.run_until_stalled()
    {
        failure = Some(error.to_string());
    }
    if failure.is_none() {
        let status = network.status();
        if status.connections != 0
            || status.inflight_operations != 0
            || status.outstanding_read_bytes != 0
            || status.outstanding_write_bytes != 0
        {
            failure = Some(format!(
                "provider ownership retained before runtime shutdown: {status:?}"
            ));
        }
    }
    if failure.is_none() {
        let snapshot = diagnostics.snapshot();
        let checkpoint = snapshot.determinism_checkpoint();
        if checkpoint.live_tasks != 0 || checkpoint.live_timers != 0 || checkpoint.ready_tasks != 0
        {
            failure = Some(format!(
                "runtime ownership retained before shutdown: tasks={} timers={} ready={}",
                checkpoint.live_tasks, checkpoint.live_timers, checkpoint.ready_tasks
            ));
        }
    }
    if let Err(error) = runtime.finish() {
        failure.get_or_insert_with(|| error.to_string());
    }
    let snapshot = diagnostics.snapshot();
    let mut samples = sample_output
        .map(|output| std::mem::take(&mut *output.borrow_mut()))
        .unwrap_or_default();
    if sampling.is_some()
        && let Err(error) = samples.collect(&metrics, snapshot.now.as_nanos())
    {
        failure.get_or_insert(error);
    }
    let mut metrics_counts = samples.counts;
    while let Some(interval) = metrics.try_take_snapshot() {
        for metric in kr_kafka_producer::telemetry::metrics::Metric::ALL {
            let count = interval
                .distribution(kr_kafka_producer::telemetry::metrics::Scope::Global, metric)
                .map_or(0, |distribution| distribution.count());
            metrics_counts[metric as usize] = metrics_counts[metric as usize]
                .checked_add(count)
                .expect("bounded simulation sample count");
        }
    }
    let network_status = network.status();
    if network_status.inflight_operations != 0
        || network_status.outstanding_read_bytes != 0
        || network_status.outstanding_write_bytes != 0
    {
        failure.get_or_insert_with(|| {
            format!("C8 provider retains operations after finish: {network_status:?}")
        });
    }
    drop(network);
    let mut audit = audit.borrow_mut();
    audit.drain_requests();
    if let Some(capture) = &audit.request_capture
        && let Err(error) = capture.finish()
    {
        failure.get_or_insert(error);
    }
    if let Some(error) = audit.error.clone() {
        failure = Some(match failure {
            Some(reason) if reason != error => format!("{error}; workload: {reason}"),
            _ => error,
        });
    }
    if let Err(error) = audit.credits(snapshot.now.as_nanos(), &credits) {
        failure.get_or_insert(error);
    }
    if !credits.is_empty() {
        failure.get_or_insert("C7 teardown retains producer credit".into());
    }
    if let Err(error) = audit.oracle.finish(model.borrow().log(), |record| {
        crate::manifest::record_id(
            record
                .headers
                .iter()
                .map(|header| (header.key.as_str(), header.value.as_deref())),
        )
        .ok()
        .and_then(|id| audit.ids.get(&id).copied())
    }) {
        failure.get_or_insert_with(|| error.to_string());
    }
    let workload_records: std::collections::BTreeMap<_, _> = manifest
        .workload
        .iter()
        .filter_map(|op| match op {
            Workload::Submit { records } => Some(records),
            _ => None,
        })
        .flatten()
        .map(|record| (record.id, record))
        .collect();
    for batch in model.borrow().log() {
        for record in &batch.records {
            let expected = crate::manifest::record_id(
                record
                    .headers
                    .iter()
                    .map(|header| (header.key.as_str(), header.value.as_deref())),
            )
            .ok()
            .and_then(|id| {
                workload_records
                    .get(&id)
                    .map(|r| (*r).clone())
                    .or_else(|| manifest.experiment.as_ref()?.record(id, manifest))
            });
            if expected
                .as_ref()
                .is_some_and(|expected| !audit.ids.contains_key(&expected.id))
            {
                failure.get_or_insert("committed record was never accepted".into());
            }
            if expected.as_ref().is_none_or(|expected| {
                expected.key != record.key
                    || expected.value != record.value
                    || expected.timestamp_ms != record.timestamp
                    || expected.headers.len() != record.headers.len()
                    || expected
                        .headers
                        .iter()
                        .zip(&record.headers)
                        .any(|(a, b)| a.key != b.key || a.value != b.value)
            }) {
                failure.get_or_insert(
                    "committed record differs from immutable workload payload".into(),
                );
            }
        }
    }
    if manifest.require_all_acked
        && (audit.coverage.acked
            != manifest
                .experiment
                .as_ref()
                .map_or(workload_records.len() as u64, |_| audit.coverage.accepted)
            || audit.coverage.not_written != 0
            || audit.coverage.unknown != 0)
    {
        failure.get_or_insert(
            "finite-fault campaign failed to acknowledge every valid submission".into(),
        );
    }
    if let Err(error) = faults.borrow().finish() {
        failure.get_or_insert(error);
    }
    if manifest.require_fault_coverage {
        let faults = faults.borrow();
        for (index, rule) in manifest.faults.scripts.iter().enumerate() {
            if faults.stats().script_firings.get(index).copied() != Some(rule.take) {
                failure.get_or_insert_with(|| {
                    format!(
                        "coverage: required fault script {index} did not exhaust its take budget"
                    )
                });
            }
        }
        if faults.stats().committed_response_losses == 0 {
            failure.get_or_insert("coverage: no actual committed response was lost".into());
        }
        if !manifest.faults.isolations.is_empty() && faults.stats().isolation_closed == 0 {
            failure.get_or_insert("coverage: isolation closed no live socket".into());
        }
    }
    if manifest.produce_max_version == 13
        && audit.coverage.acked < u64::from(manifest.minimum_acked)
    {
        failure.get_or_insert("coverage: no Acked record".into());
    }
    if !manifest.fault_plan.is_empty() && audit.realized.is_empty() {
        failure.get_or_insert("coverage: no fault realized".into());
    }
    if manifest
        .realized_faults
        .as_ref()
        .is_some_and(|expected| *expected != audit.realized)
    {
        failure.get_or_insert("realized fault plan diverged during replay".into());
    }
    let mut manifest = manifest.clone();
    manifest.realized_faults = Some(audit.realized.clone());
    manifest.fault_decisions = Some(faults.borrow().tape().to_vec());
    let checkpoint = TerminalCheckpoint::from_runtime(&snapshot.determinism_checkpoint());
    let report = match failure {
        Some(reason) => Err(Box::new(RunFailure {
            reason,
            manifest,
            history: audit.history.clone(),
            checkpoint: Some(checkpoint),
        })),
        None => Ok(RunReport {
            manifest,
            history: audit.history.clone(),
            coverage: audit.coverage.clone(),
            fault_stats: faults.borrow().stats().clone(),
            fetched_records,
            checkpoint,
            pool_peaks: credits
                .snapshot()
                .iter()
                .map(|pool| pool.peak_held as u64)
                .collect(),
            batch_raw_bytes: audit.batch_fill.0,
            batch_target_bytes: audit.batch_fill.1,
            metrics_counts,
            metrics_samples: samples.samples,
            missed_metrics_requests: samples.missed,
        }),
    };
    (report, Some(snapshot))
}
fn drain(
    client: &ProducerClient,
    audit: &Rc<RefCell<Audit>>,
    credits: &kr_kafka_producer::credit::SharedCredits,
    now: u64,
    closed: &mut bool,
) -> Result<(), String> {
    let mut events = [Event::Fatal { code: 0 }; 64];
    let n = client.poll_events(&mut events);
    for event in &events[..n] {
        *closed |= matches!(event, Event::Closed { .. });
        audit.borrow_mut().event(now, *event)?;
    }
    audit.borrow_mut().credits(now, credits)
}
fn native_refusal(
    error: kr_kafka_producer::client::ClientError,
) -> Result<kr_kafka_producer::admission::Submitted, String> {
    use kr_kafka_producer::{client::ClientError, credit::CreditError, input::InputError};
    let error = match error {
        ClientError::Credit(error @ CreditError::ResourceExhausted { .. })
        | ClientError::Input(InputError::Credit(error @ CreditError::ResourceExhausted { .. })) => {
            AdmissionError::Credit(error)
        }
        ClientError::Input(InputError::ResourceExhausted { .. }) => AdmissionError::BulkLimit,
        ClientError::Closed | ClientError::Input(InputError::Closed) => AdmissionError::Closed,
        error => return Err(error.to_string()),
    };
    Ok(kr_kafka_producer::admission::Submitted {
        accepted: 0,
        first_token: None,
        error: Some(error),
    })
}
pub(crate) fn submit(
    client: &ProducerClient,
    records: &[RecordSpec],
    topics: &[TopicHandle],
    now: RuntimeInstant,
) -> Result<(kr_kafka_producer::admission::Submitted, Option<LeaseId>), String> {
    if records[0].native {
        let record = &records[0];
        let value = record.value.as_deref().unwrap_or_default();
        let key = record.key.as_deref().unwrap_or_default();
        let length = key.len()
            + value.len()
            + record
                .headers
                .iter()
                .map(|header| header.key.len() + header.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>();
        let mut buffer = match client.acquire(length.max(1) as u32, record.lane) {
            Ok(buffer) => buffer,
            Err(error) => return native_refusal(error).map(|result| (result, None)),
        };
        buffer.as_mut_slice()[..key.len()].copy_from_slice(key);
        buffer.as_mut_slice()[key.len()..key.len() + value.len()].copy_from_slice(value);
        let mut offset = key.len() + value.len();
        let mut headers = Vec::new();
        for header in &record.headers {
            let start = offset;
            offset += header.key.len();
            buffer.as_mut_slice()[start..offset].copy_from_slice(header.key.as_bytes());
            let range = start as u32..offset as u32;
            let value = header.value.as_ref().map(|value| {
                let start = offset;
                offset += value.len();
                buffer.as_mut_slice()[start..offset].copy_from_slice(value);
                start as u32..offset as u32
            });
            headers.push(LeasedHeader { key: range, value });
        }
        let lease = match buffer.commit(length as u32) {
            Ok(lease) => lease,
            Err(error) => return native_refusal(error.error.into()).map(|result| (result, None)),
        };
        let descriptor = LeasedRecordDescriptor {
            topic: topics[record.topic as usize],
            partition_hint: (!record.key_routed).then_some(record.partition),
            lane_hint: Some(record.lane),
            key: record.key.as_ref().map(|_| 0..key.len() as u32),
            value: record
                .value
                .as_ref()
                .map(|_| key.len() as u32..(key.len() + value.len()) as u32),
            headers: &headers,
            timestamp_ms: record.timestamp_ms,
            user_token: record.id,
            delivery_timeout: None,
        };
        Ok((
            client.submit_leased_at(now, lease, &[descriptor]),
            Some(lease),
        ))
    } else {
        // These mutable caller buffers are distinct from immutable manifest/oracle data.
        // Erase only accepted inputs before the owner can poll its deferred mailbox.
        let mut caller_records = records.to_vec();
        let headers: Vec<Vec<_>> = caller_records
            .iter()
            .map(|record| {
                record
                    .headers
                    .iter()
                    .map(|header| Header {
                        key: &header.key,
                        value: header.value.as_deref(),
                    })
                    .collect()
            })
            .collect();
        let descriptors: Vec<_> = caller_records
            .iter()
            .zip(&headers)
            .map(|(record, headers)| RecordDescriptor {
                topic: topics[record.topic as usize],
                partition_hint: (!record.key_routed).then_some(record.partition),
                lane_hint: Some(record.lane),
                key: record.key.as_deref(),
                value: record.value.as_deref(),
                headers,
                timestamp_ms: record.timestamp_ms,
                user_token: record.id,
                delivery_timeout: None,
            })
            .collect();
        let submitted = client.submit_copy_at(now, &descriptors);
        drop(descriptors);
        drop(headers);
        for record in caller_records.iter_mut().take(submitted.accepted as usize) {
            if let Some(key) = &mut record.key {
                key.fill(0);
            }
            if let Some(value) = &mut record.value {
                value.fill(0);
            }
            for header in &mut record.headers {
                if let Some(value) = &mut header.value {
                    value.fill(0);
                }
            }
        }
        Ok((submitted, None))
    }
}

struct Barrier {
    count: u32,
    flush: Option<FlushToken>,
    watermark: Option<u64>,
    timeout_ns: u64,
    require_acked: bool,
}
async fn wait_barrier(
    client: &ProducerClient,
    audit: &Rc<RefCell<Audit>>,
    credits: &kr_kafka_producer::credit::SharedCredits,
    handle: &RuntimeHandle,
    barrier: Barrier,
    closed: &mut bool,
) -> Result<(), String> {
    let mut timer = Box::pin(handle.sleep(RuntimeDuration::from_nanos(barrier.timeout_ns)));
    loop {
        let done = {
            let state = audit.borrow();
            match (barrier.flush, barrier.watermark) {
                (Some(token), _) => state.flushes_done.contains(&token.0),
                (_, Some(watermark)) => state
                    .pending_tokens
                    .first()
                    .is_none_or(|token| *token > watermark),
                (None, None) => {
                    state.coverage.acked + state.coverage.not_written + state.coverage.unknown
                        >= u64::from(barrier.count)
                }
            }
        };
        if done {
            let state = audit.borrow();
            if barrier.require_acked
                && (state.coverage.not_written != 0 || state.coverage.unknown != 0)
            {
                return Err("recoverable round returned a non-Acked delivery".into());
            }
            return Ok(());
        }
        if *closed {
            return Err("producer closed before settlement barrier".into());
        }
        let event = poll_fn(|cx| {
            if timer.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(Err(
                    "settlement virtual-time budget exceeded".to_owned()
                ));
            }
            client.poll_event(cx).map(|result| {
                result
                    .map_err(|error| error.to_string())
                    .and_then(|event| event.ok_or("events ended before settlement".into()))
            })
        })
        .await?;
        *closed = matches!(event, Event::Closed { .. });
        audit.borrow_mut().event(handle.now().as_nanos(), event)?;
        audit
            .borrow_mut()
            .credits(handle.now().as_nanos(), credits)?;
    }
}

fn workload_name(operation: &Workload) -> String {
    match operation {
        Workload::SettleAllAccepted { .. } => "SettleAllAccepted".into(),
        Workload::SleepUntil { at_ns } => format!("SleepUntil:{at_ns}"),
        Workload::Submit { records } => format!("Submit:{}", records.len()),
        Workload::WaitDeliveries { count } => format!("WaitDeliveries:{count}"),
        Workload::BeginRound { round } => format!("BeginRound:{round}"),
        Workload::Settle { count, .. } => format!("Settle:{count}"),
        Workload::AwaitFlush { .. } => "AwaitFlush".into(),
        Workload::Flush => "Flush".into(),
        Workload::Sleep { .. } => "Sleep".into(),
        Workload::StopPolling { .. } => "StopPolling".into(),
        Workload::Cancel { record_id } => format!("Cancel:{record_id}"),
        Workload::Close { .. } => "Close".into(),
        Workload::CreateTopic { topic } => format!("CreateTopic:{topic}"),
        Workload::DeleteTopic { topic } => format!("DeleteTopic:{topic}"),
        Workload::RecreateTopic { topic, .. } => format!("RecreateTopic:{topic}"),
        Workload::AddPartitions { topic, .. } => format!("AddPartitions:{topic}"),
        Workload::MoveLeader { topic, .. } => format!("MoveLeader:{topic}"),
        Workload::CloseTopic { topic } => format!("CloseTopic:{topic}"),
        Workload::OpenTopic { topic } => format!("OpenTopic:{topic}"),
    }
}
