//! Owner-thread bridge for running other producer implementations in this simulator.
//! This is an experimental test API, not the production producer ABI.
use crate::{
    DomainEvent, DomainHistory, RecordSpec, ReplayManifest, TimedControl,
    history::{AdmissionRoute, Audit},
    stream::{AuditedStream, ModelConnector},
};
use kr_kafka_broker_model::BrokerModel;
use kr_kafka_producer::{
    actor::{ActorConfig, ProducerActor},
    client::{ClientClock, ProducerClient},
    engine::ProducerEngine,
    transport::WriteMode,
    types::*,
};
use kr_runtime::{
    RuntimeConfig, RuntimeDuration, RuntimeHandle, RuntimeInstant, SimRuntime, rng::RandomStream,
};
use kr_runtime_io::network::*;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    future::{Future, poll_fn},
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Default)]
struct Mailbox {
    events: VecDeque<Value>,
    waker: Option<Waker>,
}
impl Mailbox {
    fn push(&mut self, event: Value) {
        self.events.push_back(event);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// One session belongs to its creating thread. Commands never use host clocks,
/// sockets, sleeps, or producer threads. Time advances only through `advance`.
pub struct ExternalSession {
    runtime: SimRuntime,
    handle: RuntimeHandle,
    network: SimNetwork,
    manifest: Rc<ReplayManifest>,
    audit: Rc<RefCell<Audit>>,
    model: Rc<RefCell<BrokerModel>>,
    connector: ModelConnector,
    mailbox: Rc<RefCell<Mailbox>>,
    streams: Rc<RefCell<BTreeMap<u64, Rc<ColdStream<AuditedStream>>>>>,
    generations: Rc<RefCell<BTreeMap<u64, u64>>>,
    writes: Rc<RefCell<BTreeMap<u64, VecDeque<Vec<u8>>>>>,
    client: Option<ProducerClient>,
    topics: Rc<RefCell<Vec<TopicHandle>>>,
    ids: Rc<RefCell<Vec<[u8; 16]>>>,
    records: BTreeMap<u64, (usize, u32, usize)>,
    faults: Rc<RefCell<crate::faults::FaultEngine>>,
}
impl ExternalSession {
    pub fn new(manifest: ReplayManifest, native: bool) -> Result<Self, String> {
        manifest.validate()?;
        let runtime = SimRuntime::new(RuntimeConfig {
            seed: manifest.seed,
            max_tasks: manifest.runtime.tasks,
            max_timers: manifest.runtime.timers,
            max_steps_per_run: manifest.limits.steps,
            start_time: RuntimeInstant::from_nanos(manifest.start_ns),
            max_time: Some(RuntimeInstant::from_nanos(
                manifest.start_ns + manifest.limits.elapsed_ns,
            )),
        });
        let handle = RuntimeHandle::Sim(runtime.handle());
        let audit = Rc::new(RefCell::new(Audit::new(
            manifest.limits.history_events,
            manifest.limits.records as usize,
        )));
        audit.borrow_mut().configure_links(&manifest);
        if native && manifest.observe_requests {
            audit.borrow_mut().request_capture =
                Some(crate::request_observation::Capture::new(&manifest));
        }
        let faults = Rc::new(RefCell::new(crate::faults::FaultEngine::new(
            manifest.faults.clone(),
            manifest.fault_decisions.clone(),
        )?));
        let (network, model) = crate::setup::network_and_model(&runtime, &manifest)?;
        let manifest = Rc::new(manifest);
        let connector = ModelConnector::new(
            handle.clone(),
            network.clone(),
            audit.clone(),
            model.clone(),
            manifest.clone(),
            faults.clone(),
            runtime.random_source(RandomStream::Fault),
        )?;
        let mut topics = Vec::new();
        let client = if native {
            let engine =
                ProducerEngine::new(manifest.producer.clone(), None).map_err(|e| e.to_string())?;
            let (client, actor) = ProducerActor::new(
                handle.clone(),
                engine,
                connector.clone(),
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
            handle.spawn(actor).map_err(|e| e.to_string())?;
            for topic in &manifest.topics {
                topics.push(
                    client
                        .open_topic_at(&topic.name, handle.now())
                        .map_err(|e| e.to_string())?,
                );
            }
            Some(client)
        } else {
            None
        };
        let ids = Rc::new(RefCell::new(manifest.topics.iter().map(|t| t.id).collect()));
        let session = Self {
            runtime,
            handle,
            network,
            manifest,
            audit,
            model,
            connector,
            mailbox: Rc::new(RefCell::new(Mailbox::default())),
            streams: Rc::new(RefCell::new(BTreeMap::new())),
            generations: Rc::new(RefCell::new(BTreeMap::new())),
            writes: Rc::new(RefCell::new(BTreeMap::new())),
            client,
            topics: Rc::new(RefCell::new(topics)),
            ids,
            records: BTreeMap::new(),
            faults,
        };
        session.schedule_controls()?;
        Ok(session)
    }
    pub fn now(&self) -> u64 {
        self.handle.now().as_nanos() - self.manifest.start_ns
    }
    pub fn manifest(&self) -> &ReplayManifest {
        &self.manifest
    }

    fn schedule_controls(&self) -> Result<(), String> {
        for (index, action) in self
            .manifest
            .experiment
            .as_ref()
            .ok_or("missing experiment")?
            .scheduled_actions
            .iter()
            .cloned()
            .enumerate()
        {
            let handle = self.handle.clone();
            let model = self.model.clone();
            let ids = self.ids.clone();
            let manifest = self.manifest.clone();
            let audit = self.audit.clone();
            let mailbox = self.mailbox.clone();
            let client = self.client.clone();
            let topics = self.topics.clone();
            self.handle
                .spawn(async move {
                    let result = async {
                        handle
                            .sleep_until(RuntimeInstant::from_nanos(
                                manifest.start_ns + action.at_ns,
                            ))
                            .await
                            .map_err(|e| e.to_string())?;
                        audit.borrow_mut().record(
                            handle.now().as_nanos(),
                            DomainEvent::ScheduledControl {
                                index: index as u32,
                                at_ns: action.at_ns,
                                action: action.action.clone(),
                            },
                        );
                        let mut ids = ids.borrow_mut();
                        let mut model = model.borrow_mut();
                        match &action.action {
                            TimedControl::CreateTopic { topic } => {
                                let t = &manifest.topics[*topic as usize];
                                model
                                    .create_topic_with_id(&t.name, ids[*topic as usize], &t.leaders)
                                    .map_err(|e| e.to_string())?;
                            }
                            TimedControl::DeleteTopic { topic } => model
                                .delete_topic(ids[*topic as usize])
                                .map_err(|e| e.to_string())?,
                            TimedControl::RecreateTopic { topic, new_id } => {
                                let t = &manifest.topics[*topic as usize];
                                model
                                    .delete_topic(ids[*topic as usize])
                                    .map_err(|e| e.to_string())?;
                                model
                                    .create_topic_with_id(&t.name, *new_id, &t.leaders)
                                    .map_err(|e| e.to_string())?;
                                ids[*topic as usize] = *new_id;
                            }
                            TimedControl::AddPartitions {
                                topic,
                                additional_leaders,
                            } => model
                                .add_partitions(ids[*topic as usize], additional_leaders)
                                .map_err(|e| e.to_string())?,
                            TimedControl::MoveLeader {
                                topic,
                                partition,
                                broker,
                            } => model
                                .move_leader(ids[*topic as usize], *partition, *broker)
                                .map_err(|e| e.to_string())?,
                            TimedControl::CloseTopic { topic } => {
                                if let Some(c) = &client {
                                    c.close_topic(topics.borrow()[*topic as usize])
                                        .map_err(|e| e.to_string())?;
                                }
                            }
                            TimedControl::OpenTopic { topic } => {
                                if let Some(c) = &client {
                                    topics.borrow_mut()[*topic as usize] = c
                                        .open_topic_at(
                                            &manifest.topics[*topic as usize].name,
                                            handle.now(),
                                        )
                                        .map_err(|e| e.to_string())?;
                                }
                            }
                            TimedControl::Flush => {
                                if let Some(c) = &client {
                                    c.flush_at(handle.now()).map_err(|e| e.to_string())?;
                                }
                            }
                            TimedControl::Close { deadline_ns } => {
                                if let Some(c) = &client {
                                    c.close_at(
                                        handle.now(),
                                        RuntimeDuration::from_nanos(*deadline_ns),
                                    )
                                    .map_err(|e| e.to_string())?;
                                }
                            }
                        }
                        mailbox
                            .borrow_mut()
                            .push(json!({"kind":"control", "index":index,
                        "at_ns":action.at_ns, "action":action.action}));
                        Ok::<_, String>(())
                    }
                    .await;
                    if let Err(error) = result {
                        audit.borrow_mut().fail(error);
                    }
                })
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Stop at the next external completion or the supplied relative deadline.
    pub fn advance(&mut self, until: u64) -> Result<Value, String> {
        if until < self.now() || until > self.manifest.limits.elapsed_ns {
            return Err("advance bounds".into());
        }
        let mailbox = self.mailbox.clone();
        let mut timer = Box::pin(
            self.handle
                .sleep_until(RuntimeInstant::from_nanos(self.manifest.start_ns + until)),
        );
        self.runtime
            .block_on(poll_fn(|cx| {
                if !mailbox.borrow().events.is_empty() {
                    return Poll::Ready(());
                }
                if timer.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
                mailbox.borrow_mut().waker = Some(cx.waker().clone());
                Poll::Pending
            }))
            .map_err(|e| e.to_string())?;
        self.audit
            .borrow_mut()
            .advance_links(self.handle.now().as_nanos());
        self.audit.borrow_mut().drain_requests();
        if let Some(error) = &self.audit.borrow().error {
            return Err(error.clone());
        }
        Ok(
            json!({"now_ns":self.now(), "events":self.mailbox.borrow_mut().events.drain(..).collect::<Vec<_>>()}),
        )
    }

    pub fn connect(&mut self, id: u64, broker: i32, timeout: u64) -> Result<(), String> {
        let generation = {
            let mut g = self.generations.borrow_mut();
            let next = g.get(&id).copied().unwrap_or(0) + 1;
            g.insert(id, next);
            next
        };
        let connector = self.connector.clone();
        let streams = self.streams.clone();
        let generations = self.generations.clone();
        let mailbox = self.mailbox.clone();
        let handle = self.handle.clone();
        let deadline = self
            .handle
            .now()
            .checked_add(RuntimeDuration::from_nanos(timeout))
            .ok_or("connect deadline")?;
        self.handle
            .spawn(async move {
                let result = connector.connect_external(broker, deadline).await;
                match result {
                    Ok(stream) => {
                        let stream = Rc::new(ColdStream::new(stream));
                        if generations.borrow().get(&id) != Some(&generation) {
                            let _ = stream.close().await;
                            return;
                        }
                        streams.borrow_mut().insert(id, stream.clone());
                        mailbox
                            .borrow_mut()
                            .push(json!({"kind":"connected", "id":id}));
                        loop {
                            let result = async {
                                let mut bytes = Vec::new();
                                let mut expected = 4;
                                while bytes.len() < expected {
                                    let read = stream
                                        .read(ReadRequest {
                                            max_bytes: expected - bytes.len(),
                                            buffer: bytes,
                                        })
                                        .await
                                        .map_err(|e| e.to_string())?;
                                    bytes = read.buffer;
                                    if read.end_of_stream {
                                        return Err("eof".to_string());
                                    }
                                    if bytes.len() == 4 && expected == 4 {
                                        let n = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                                        if !(4..=4 * 1024 * 1024).contains(&n) {
                                            return Err("response frame bounds".into());
                                        }
                                        expected = n as usize + 4;
                                    }
                                }
                                Ok::<_, String>(bytes)
                            }
                            .await;
                            if generations.borrow().get(&id) != Some(&generation) {
                                break;
                            }
                            match result {
                                Ok(bytes) => mailbox
                                    .borrow_mut()
                                    .push(json!({"kind":"receive", "id":id, "bytes":bytes})),
                                Err(error) => {
                                    streams.borrow_mut().remove(&id);
                                    mailbox.borrow_mut().push(
                                        json!({"kind":"disconnected", "id":id, "error":error}),
                                    );
                                    let _ = stream.close().await;
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        if generations.borrow().get(&id) == Some(&generation) {
                            mailbox.borrow_mut().push(
                                json!({"kind":"disconnected", "id":id, "error":error.to_string()}),
                            );
                        }
                    }
                }
                let _ = handle;
            })
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub fn write(&mut self, id: u64, bytes: Vec<u8>) -> Result<(), String> {
        if bytes.len() > self.manifest.producer.rx_bytes_per_connection as usize {
            return Err("request frame bounds".into());
        }
        let stream = self
            .streams
            .borrow()
            .get(&id)
            .cloned()
            .ok_or("connection not ready")?;
        let queue = self.writes.clone();
        {
            let mut queues = queue.borrow_mut();
            if let Some(pending) = queues.get_mut(&id) {
                if pending.len()
                    >= usize::from(self.manifest.producer.max_in_flight_per_connection) + 4
                {
                    return Err("external write queue capacity".into());
                }
                pending.push_back(bytes);
                return Ok(());
            }
            queues.insert(id, VecDeque::from([bytes]));
        }
        let mailbox = self.mailbox.clone();
        self.handle
            .spawn(async move {
                loop {
                    let bytes = queue
                        .borrow_mut()
                        .get_mut(&id)
                        .and_then(VecDeque::pop_front);
                    let Some(bytes) = bytes else {
                        queue.borrow_mut().remove(&id);
                        return;
                    };
                    let mut offset = 0;
                    while offset < bytes.len() {
                        match stream
                            .write(WriteRequest {
                                buffer: bytes[offset..].to_vec(),
                            })
                            .await
                        {
                            Ok(result) if result.bytes_written > 0 => {
                                offset += result.bytes_written
                            }
                            _ => {
                                queue.borrow_mut().remove(&id);
                                let _ = stream.close().await;
                                return;
                            }
                        }
                    }
                    mailbox.borrow_mut().push(json!({"kind":"sent", "id":id}));
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub fn disconnect(&mut self, id: u64) -> Result<(), String> {
        if let Some(g) = self.generations.borrow_mut().get_mut(&id) {
            *g += 1;
        }
        let stream = self.streams.borrow_mut().remove(&id);
        if let Some(stream) = stream {
            self.handle
                .spawn(async move {
                    let _ = stream.close().await;
                })
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub fn materialize(
        &mut self,
        load: usize,
        index: u32,
        partitions: usize,
    ) -> Result<Value, String> {
        let load_index = load;
        let load = self
            .manifest
            .experiment
            .as_ref()
            .ok_or("missing experiment")?
            .loads
            .get(load)
            .ok_or("load index")?;
        if index >= load.shape.offer_budget()? {
            return Err("record index".into());
        }
        let record = load
            .template
            .materialize(index, partitions, self.manifest.producer.lanes)?;
        let result = serde_json::to_value(&record).map_err(|e| e.to_string())?;
        self.records
            .insert(record.id, (load_index, index, partitions));
        Ok(result)
    }
    fn materialized(&self, id: u64) -> Result<RecordSpec, String> {
        let &(load, index, partitions) = self.records.get(&id).ok_or("unmaterialized record")?;
        self.manifest
            .experiment
            .as_ref()
            .ok_or("missing experiment")?
            .loads[load]
            .template
            .materialize(index, partitions, self.manifest.producer.lanes)
    }
    pub fn accept(&mut self, id: u64) -> Result<Value, String> {
        if self.audit.borrow().ids.contains_key(&id) {
            return Err("duplicate admission ID".into());
        }
        let record = self.materialized(id)?;
        let token = if let Some(client) = &self.client {
            let (result, lease) = crate::runner::submit(
                client,
                std::slice::from_ref(&record),
                &self.topics.borrow(),
                self.handle.now(),
            )?;
            if let Some(lease) = lease {
                client.release(lease).map_err(|e| e.to_string())?;
            }
            let Some(token) = result.token(0) else {
                return Ok(json!({"accepted":false, "error":format!("{:?}",result.error)}));
            };
            token.0
        } else {
            id
        };
        self.audit.borrow_mut().accept(
            self.handle.now().as_nanos(),
            id,
            token,
            AdmissionRoute {
                topic: self.ids.borrow()[record.topic as usize],
                partition: record.partition,
                handle: record.topic + 1,
                resolved: true,
                key_routed: record.key_routed,
            },
            None,
        )?;
        Ok(json!({"accepted":true, "token":token}))
    }
    pub fn poll_native(&self) -> Result<Value, String> {
        let client = self.client.as_ref().ok_or("not a native session")?;
        let mut buffer = [Event::Closed { unresolved: 0 }; 512];
        let n = client.poll_events(&mut buffer);
        let events: Vec<_> = buffer[..n].iter().map(|e| match e {
            Event::Delivery(d) => json!({"kind":"delivery", "id":d.user_token, "partition":d.partition.partition,
                "topic_id":d.partition.topic.0, "outcome":d.outcome.kind as u32,
                "reason":d.outcome.reason as u32, "offset":d.base_offset.get(), "attempts":d.attempts}),
            _ => json!({"kind":"native", "event":format!("{e:?}")}),
        }).collect();
        Ok(json!(events))
    }
    pub fn metadata(&self, topic: usize) -> Result<Value, String> {
        let c = self.client.as_ref().ok_or("not a native session")?;
        Ok(
            json!({"partitions":c.metadata_topic(*self.topics.borrow().get(topic).ok_or("topic index")?)
            .map_err(|e| e.to_string())?.map_or(0, |t| t.partitions)}),
        )
    }
    pub fn close_native(&self, timeout: u64) -> Result<(), String> {
        self.client
            .as_ref()
            .ok_or("not a native session")?
            .close_at(self.handle.now(), RuntimeDuration::from_nanos(timeout))
            .map_err(|e| e.to_string())
    }
    pub fn evidence(&self) -> Result<Value, String> {
        let model = self.model.borrow();
        let mut log = Vec::new();
        for batch in model.log() {
            for record in &batch.records {
                let id = crate::record_id(
                    record
                        .headers
                        .iter()
                        .map(|h| (h.key.as_str(), h.value.as_deref())),
                )?;
                let expected = self.materialized(id)?;
                if record.key != expected.key
                    || record.value != expected.value
                    || record.timestamp != expected.timestamp_ms
                    || record.headers.len() != expected.headers.len()
                    || record
                        .headers
                        .iter()
                        .zip(&expected.headers)
                        .any(|(a, b)| a.key != b.key || a.value != b.value)
                {
                    return Err(format!("broker payload differs for record {id}"));
                }
                log.push(json!({"id":id,"topic_id":batch.topic,"partition":batch.partition,"offset":record.offset}));
            }
        }
        Ok(
            json!({"now_ns":self.now(), "log":log, "history":self.audit.borrow().history,
            "fault_stats":self.faults.borrow().stats(), "network":format!("{:?}",self.network.status())}),
        )
    }
    /// Stream the complete evidence to disk without constructing a second
    /// in-memory JSON tree. Large Full workloads use this path across Panama.
    pub fn export_evidence(&self, path: &std::path::Path) -> Result<(), String> {
        use std::io::Write;
        let mut out =
            std::io::BufWriter::new(std::fs::File::create(path).map_err(|e| e.to_string())?);
        write!(out, "{{\"now_ns\":{},\"log\":[", self.now()).map_err(|e| e.to_string())?;
        let model = self.model.borrow();
        let mut first = true;
        for batch in model.log() {
            for record in &batch.records {
                let id = crate::record_id(
                    record
                        .headers
                        .iter()
                        .map(|h| (h.key.as_str(), h.value.as_deref())),
                )?;
                let expected = self.materialized(id)?;
                if record.key != expected.key
                    || record.value != expected.value
                    || record.timestamp != expected.timestamp_ms
                    || record.headers.len() != expected.headers.len()
                    || record
                        .headers
                        .iter()
                        .zip(&expected.headers)
                        .any(|(a, b)| a.key != b.key || a.value != b.value)
                {
                    return Err(format!("broker payload differs for record {id}"));
                }
                if !first {
                    out.write_all(b",").map_err(|e| e.to_string())?;
                }
                first = false;
                serde_json::to_writer(&mut out, &json!({"id":id,"topic_id":batch.topic,"partition":batch.partition,"offset":record.offset}))
                    .map_err(|e| e.to_string())?;
            }
        }
        out.write_all(b"],\"history\":")
            .map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut out, &self.audit.borrow().history).map_err(|e| e.to_string())?;
        out.write_all(b",\"fault_stats\":")
            .map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut out, &self.faults.borrow().stats())
            .map_err(|e| e.to_string())?;
        out.write_all(b",\"network\":").map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut out, &format!("{:?}", self.network.status()))
            .map_err(|e| e.to_string())?;
        out.write_all(b"}").map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())
    }
    pub fn export_history(&self, path: &std::path::Path) -> Result<(), String> {
        use std::io::Write;
        let mut out =
            std::io::BufWriter::new(std::fs::File::create(path).map_err(|e| e.to_string())?);
        serde_json::to_writer(&mut out, &self.audit.borrow().history).map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())
    }
    /// Cancel both client and environment tasks, then release all bridge-owned
    /// streams. This is a teardown operation, not a producer close observation.
    pub fn shutdown(&mut self) -> Result<Value, String> {
        self.runtime.shutdown().map_err(|e| e.to_string())?;
        self.client = None;
        self.streams.borrow_mut().clear();
        self.writes.borrow_mut().clear();
        let status = self.network.status();
        if status.connections != 0
            || status.inflight_operations != 0
            || status.outstanding_read_bytes != 0
            || status.outstanding_write_bytes != 0
        {
            return Err(format!(
                "external session retained provider resources: {status:?}"
            ));
        }
        Ok(
            json!({"connections":status.connections, "operations":status.inflight_operations,
            "read_bytes":status.outstanding_read_bytes, "write_bytes":status.outstanding_write_bytes}),
        )
    }
    pub fn history(&self) -> DomainHistory {
        self.audit.borrow().history.clone()
    }
}
