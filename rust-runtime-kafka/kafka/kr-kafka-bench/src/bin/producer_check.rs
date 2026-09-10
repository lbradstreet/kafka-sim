//! Bounded real-broker correctness history. This is deliberately separate from
//! the open-loop performance binary: expected wire bytes and every terminal
//! delivery remain available for an independent Kafka consumer to verify.
#![forbid(unsafe_code)]
use kr_kafka_bench::config::Profile;
use kr_kafka_producer::transport::WriteMode;
use kr_kafka_producer::{
    client::{ClientError, ProducerClient},
    config::{ProducerConfig, SecurityConfig},
    credit::Resource,
    input::{LeasedHeader, LeasedRecordDescriptor},
    types::{DeliveryKind, Event, FailureReason, Header, RecordDescriptor, TopicHandle},
};
use kr_kafka_producer_host::producer::{HostProducer, HostProducerOptions, HostStatus};
use kr_runtime::RuntimeDuration;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    io::{Read, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, sync_channel},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
const SCHEMA: &str = "kr-kafka-producer-check/v1";
fn default_phase_timeout() -> u64 {
    30_000
}
fn default_pause() -> u64 {
    250
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Scenario {
    Basic,
    Pending,
    StoppedPolling,
    Restart,
    LeaderLoss,
    Recreate,
    CloseDeadline,
    Unavailable,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum InputMode {
    Copy,
    Leased,
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum RequestedWriteMode {
    #[default]
    Staging,
    Vectored,
}
impl RequestedWriteMode {
    fn native(self) -> WriteMode {
        match self {
            Self::Staging => WriteMode::Staging,
            Self::Vectored => WriteMode::Vectored,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Staging => "staging",
            Self::Vectored => "vectored",
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    run_id: String,
    profile: Profile,
    lanes: u8,
    input_mode: InputMode,
    #[serde(default)]
    write_mode: RequestedWriteMode,
    scenario: Scenario,
    #[serde(default)]
    barriers: bool,
    #[serde(default = "default_phase_timeout")]
    phase_timeout_ms: u64,
    #[serde(default = "default_pause")]
    poll_pause_ms: u64,
    #[serde(default)]
    tls_server_name: Option<String>,
}
impl Config {
    fn producer(&self) -> Result<ProducerConfig> {
        if self.schema != SCHEMA
            || self.run_id.is_empty()
            || self.run_id.len() > 64
            || !self
                .run_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || ![1, 4].contains(&self.lanes)
            || !(1..=2000).contains(&self.profile.records)
            || self.profile.record_bytes > 8192
            || self.profile.partitions > 64
            || !(1..=120_000).contains(&self.phase_timeout_ms)
            || self.poll_pause_ms > 10_000
            || self.profile.delivery_timeout_ms > 120_000
        {
            return Err("invalid correctness harness bounds".into());
        }
        if matches!(
            self.scenario,
            Scenario::Restart | Scenario::LeaderLoss | Scenario::Recreate | Scenario::CloseDeadline
        ) && (self.profile.records < 3 || !self.barriers)
        {
            return Err("fault scenarios require >=3 records and barriers".into());
        }
        if self.scenario == Scenario::LeaderLoss {
            let third = self.profile.records / 3;
            if self.profile.partitions == 0
                || !(third..2 * third).any(|id| id % u64::from(self.profile.partitions) == 0)
            {
                return Err("leader_loss middle cohort must include partition 0".into());
            }
        }
        let mut config = self.profile.producer()?;
        config.lanes = self.lanes;
        if let Some(name) = &self.tls_server_name {
            if name.is_empty() || name.len() > 253 {
                return Err("invalid TLS server-name override".into());
            }
            match &mut config.security {
                SecurityConfig::Tls { tls } | SecurityConfig::SaslTls { tls, .. } => {
                    tls.server_name = Some(name.clone())
                }
                SecurityConfig::Plaintext => {
                    return Err("TLS server-name override requires TLS".into());
                }
            }
        }
        config.brokers_max = 4.max(config.bootstrap.len().try_into()?);
        config.max_open_topics = 4;
        config.max_batches = 256;
        config.record_descriptors = if self.scenario == Scenario::StoppedPolling {
            32
        } else {
            2048
        };
        config.delivery_event_capacity = config.record_descriptors;
        config.pending_records_per_topic = config.record_descriptors;
        config.max_live_leases = 128;
        config.release_event_capacity = 128;
        config.compressed_bytes = 8 * 1024 * 1024;
        config.codec_contexts = self.lanes;
        config.metadata_max_age = RuntimeDuration::from_nanos(100_000_000);
        config.client_id = format!("kr-check-{}", self.run_id);
        config.validate()?;
        Ok(config)
    }
}
#[derive(Clone, Serialize)]
struct ExpectedHeader {
    key: String,
    value_hex: Option<String>,
}
#[derive(Serialize)]
struct Accepted {
    record_id: u64,
    token: u64,
    topic_handle: u32,
    generation: u32,
    expected_partition: i32,
    lane: u8,
    key_hex: Option<String>,
    value_hex: Option<String>,
    headers: Vec<ExpectedHeader>,
    timestamp_ms: i64,
}
#[derive(Serialize)]
struct Delivery {
    record_id: u64,
    token: u64,
    topic_handle: u32,
    topic_id: String,
    partition: i32,
    kind: &'static str,
    reason: String,
    offset: Option<i64>,
    timestamp_ms: Option<i64>,
    attempts: u32,
    event_index: u64,
}
#[derive(Serialize)]
struct LeaseHistory {
    lease: u64,
    record_id: u64,
    accepted: bool,
    release_requested: bool,
    released_event: Option<u64>,
}
#[derive(Serialize)]
struct FlushHistory {
    token: u64,
    accepted_watermark: usize,
    done_event: Option<u64>,
}
#[derive(Serialize)]
struct Report {
    schema: &'static str,
    run_id: String,
    bootstrap: String,
    topic: String,
    partitions: u32,
    config: serde_json::Value,
    complete: bool,
    checkpoint: String,
    error: Option<String>,
    backend: Option<String>,
    write_mode: &'static str,
    limitations: Vec<&'static str>,
    topic_ids: BTreeMap<u32, String>,
    accepted: Vec<Accepted>,
    deliveries: Vec<Delivery>,
    input_releases: Vec<LeaseHistory>,
    flushes: Vec<FlushHistory>,
    rejected_attempts: u64,
    offered_record_ids: BTreeSet<u64>,
    unaccepted_record_ids: Vec<u64>,
    rejected_reasons: BTreeMap<String, u64>,
    stopped_polling_rejections: u64,
    topic_failures: Vec<serde_json::Value>,
    fatal: Vec<u32>,
    closed: bool,
    closed_unresolved: Option<u32>,
    joined: bool,
    final_credits: Vec<serde_json::Value>,
    final_status: Option<String>,
}
impl Report {
    fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            schema: SCHEMA,
            run_id: config.run_id.clone(),
            bootstrap: config.profile.bootstrap.clone(),
            topic: config.profile.topic.clone(),
            partitions: config.profile.partitions,
            config: serde_json::to_value(config)?,
            complete: false,
            checkpoint: "starting".into(),
            error: None,
            backend: None,
            write_mode: config.write_mode.name(),
            limitations: vec![
                "TLS may copy plaintext into bounded encryption records even when the actor uses vectored writes.",
                "Real broker faults do not prescribe a particular socket certainty outcome; committed-log verification determines whether each reported outcome is sound.",
            ],
            topic_ids: BTreeMap::new(),
            accepted: Vec::new(),
            deliveries: Vec::new(),
            input_releases: Vec::new(),
            flushes: Vec::new(),
            rejected_attempts: 0,
            offered_record_ids: BTreeSet::new(),
            unaccepted_record_ids: Vec::new(),
            rejected_reasons: BTreeMap::new(),
            stopped_polling_rejections: 0,
            topic_failures: Vec::new(),
            fatal: Vec::new(),
            closed: false,
            closed_unresolved: None,
            joined: false,
            final_credits: Vec::new(),
            final_status: None,
        })
    }
}
struct WireRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    timestamp: i64,
}
fn wire_record(config: &Config, id: u64, timestamp: i64) -> WireRecord {
    let key = if id.is_multiple_of(11) {
        None
    } else if id.is_multiple_of(13) {
        Some(Vec::new())
    } else {
        Some(format!("key-{}-{id}", config.run_id).into_bytes())
    };
    let value = if id.is_multiple_of(17) {
        None
    } else if id.is_multiple_of(19) {
        Some(Vec::new())
    } else {
        let mut value = vec![b'a'; config.profile.record_bytes];
        if config.profile.pattern == "incompressible" {
            let mut state = id ^ config.profile.seed;
            for byte in &mut value {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *byte = (state >> 56) as u8;
            }
        }
        let prefix = format!("kr-check:{}:{id}:", config.run_id);
        let n = prefix.len().min(value.len());
        value[..n].copy_from_slice(&prefix.as_bytes()[..n]);
        Some(value)
    };
    WireRecord {
        key,
        value,
        timestamp,
        headers: vec![
            (
                "kr-check-run".into(),
                Some(config.run_id.as_bytes().to_vec()),
            ),
            ("kr-check-id".into(), Some(id.to_string().into_bytes())),
            ("duplicate".into(), None),
            ("duplicate".into(), Some(Vec::new())),
            ("binary".into(), Some(vec![0, 255, id as u8])),
        ],
    }
}
fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 15) as usize] as char);
    }
    output
}
fn append(bytes: &mut Vec<u8>, value: &[u8]) -> Range<u32> {
    let start = bytes.len() as u32;
    bytes.extend_from_slice(value);
    start..bytes.len() as u32
}
fn write_report(path: &Path, report: &Report) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(report)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
fn continue_reader() -> Receiver<std::result::Result<(), String>> {
    let (send, receive) = sync_channel(1);
    std::thread::spawn(move || {
        let mut input = std::io::stdin().lock();
        loop {
            let mut line = Vec::with_capacity(64);
            let result = loop {
                let mut byte = [0];
                match input.read(&mut byte) {
                    Ok(0) => break Err("barrier stdin ended".into()),
                    Ok(_) if byte[0] == b'\n' => {
                        break if line == b"continue" {
                            Ok(())
                        } else {
                            Err("barrier requires continue".into())
                        };
                    }
                    Ok(_) if line.len() < 63 => line.push(byte[0]),
                    Ok(_) => break Err("barrier command exceeds 63 bytes".into()),
                    Err(error) => break Err(error.to_string()),
                }
            };
            let done = result.is_err();
            if send.send(result).is_err() || done {
                break;
            }
        }
    });
    receive
}
fn check_fatal_policy(scenario: Scenario, fatal: &[u32]) -> Result<()> {
    // An expired explicit close deadline deliberately calls engine.fail_all
    // (Closed), then drains every terminal ownership obligation. This is an
    // expected notification only for the two deliberately unavailable cases;
    // it never excuses a different/duplicate Fatal or a failed close invariant.
    if fatal.is_empty()
        || (matches!(scenario, Scenario::CloseDeadline | Scenario::Unavailable)
            && fatal == [FailureReason::Closed as u32])
    {
        Ok(())
    } else {
        Err(format!("producer emitted unexpected Fatal: {fatal:?}").into())
    }
}
fn observe_closed(report: &mut Report, unresolved: u32) -> Result<()> {
    if report.closed {
        return Err("duplicate Closed".into());
    }
    // This driver admits sequentially and records each successful admission
    // before its next event poll. Test C10 at this exact FIFO event boundary;
    // later deliveries/releases must not repair a prematurely emitted Closed.
    if report.accepted.len() != report.deliveries.len()
        || report
            .input_releases
            .iter()
            .any(|lease| !lease.release_requested || lease.released_event.is_none())
        || unresolved as usize
            != report
                .deliveries
                .iter()
                .filter(|delivery| delivery.kind == "Unknown")
                .count()
    {
        return Err("premature or inconsistent Closed event".into());
    }
    report.closed = true;
    report.closed_unresolved = Some(unresolved);
    Ok(())
}

fn check_retry_owner(
    fatal: &[u32],
    closed: bool,
    owner_aborted: bool,
    status: Option<&HostStatus>,
) -> Result<()> {
    if !fatal.is_empty() {
        return Err(format!("producer emitted Fatal while retrying admission: {fatal:?}").into());
    }
    if closed || owner_aborted || !matches!(status, Some(HostStatus::Running)) {
        return Err("owner closed or stopped while retrying admission".into());
    }
    Ok(())
}

struct Harness<'a> {
    config: &'a Config,
    report: &'a mut Report,
    path: &'a Path,
    host: Option<HostProducer>,
    client: ProducerClient,
    continue_rx: Option<Receiver<std::result::Result<(), String>>>,
    topic_generations: BTreeMap<u32, u32>,
    delivered: BTreeSet<u64>,
    leases: BTreeMap<u64, usize>,
    event_index: u64,
    attempts: u64,
    timestamp: i64,
}
impl Harness<'_> {
    fn tick(&mut self) -> Result<usize> {
        let mut events = [Event::Closed { unresolved: 0 }; 256];
        let count = self.client.poll_events(&mut events);
        for event in events.into_iter().take(count) {
            if self.report.closed {
                return Err("event observed after Closed".into());
            }
            self.event_index += 1;
            match event {
                Event::TopicReady { topic, id, .. } => {
                    let generation = *self
                        .topic_generations
                        .get(&topic.0)
                        .ok_or("unknown TopicReady handle")?;
                    let id = hex(&id.0);
                    if self
                        .report
                        .topic_ids
                        .insert(generation, id.clone())
                        .is_some_and(|old| old != id)
                    {
                        return Err("topic handle rebound to another UUID".into());
                    }
                }
                Event::Delivery(d) => {
                    if !self.delivered.insert(d.user_token) {
                        return Err("duplicate Delivery event".into());
                    }
                    let accepted = self
                        .report
                        .accepted
                        .iter()
                        .find(|a| a.record_id == d.user_token)
                        .ok_or("Delivery for an unaccepted record")?;
                    if accepted.token != d.token.0 || accepted.topic_handle != d.topic.0 {
                        return Err("Delivery identity differs from accepted record".into());
                    }
                    let kind = match d.outcome.kind {
                        DeliveryKind::Acked => "Acked",
                        DeliveryKind::NotWritten => "NotWritten",
                        DeliveryKind::Unknown => "Unknown",
                    };
                    self.report.deliveries.push(Delivery {
                        record_id: d.user_token,
                        token: d.token.0,
                        topic_handle: d.topic.0,
                        topic_id: hex(&d.partition.topic.0),
                        partition: d.partition.partition,
                        kind,
                        reason: format!("{:?}", d.outcome.reason),
                        offset: d.base_offset.get(),
                        timestamp_ms: d.timestamp.get(),
                        attempts: d.attempts,
                        event_index: self.event_index,
                    });
                }
                Event::InputReleased { lease } => {
                    let index = *self
                        .leases
                        .get(&lease.0)
                        .ok_or("release for unregistered native input")?;
                    let row = &mut self.report.input_releases[index];
                    if row.released_event.replace(self.event_index).is_some() {
                        return Err("duplicate native input release".into());
                    }
                }
                Event::FlushDone { token } => {
                    let row = self
                        .report
                        .flushes
                        .iter_mut()
                        .find(|f| f.token == token.0)
                        .ok_or("unrequested flush event")?;
                    if row.done_event.replace(self.event_index).is_some()
                        || self.report.accepted[..row.accepted_watermark]
                            .iter()
                            .any(|a| !self.delivered.contains(&a.record_id))
                    {
                        return Err("duplicate or premature FlushDone".into());
                    }
                }
                Event::TopicFailed { topic, code } => self.report.topic_failures.push(
                    json!({"topic_handle":topic.0,"code":code,"event_index":self.event_index}),
                ),
                Event::Fatal { code } => self.report.fatal.push(code),
                Event::Closed { unresolved } => observe_closed(self.report, unresolved)?,
            }
        }
        Ok(count)
    }
    /// Joining the owner proves publication is finished, not that the
    /// application's bounded event ring has already been consumed.
    fn drain_stopped_owner(&mut self, deadline: Instant) -> Result<()> {
        let pools = self.client.status()?.credits;
        let capacity = [
            Resource::DeliveryEvents,
            Resource::ReleaseEvents,
            Resource::ControlEvents,
        ]
        .into_iter()
        .try_fold(0usize, |sum, resource| {
            sum.checked_add(pools[resource as usize].limit)
        })
        .ok_or("terminal event capacity overflow")?;
        // A stopped owner cannot refill the ring. Each nonempty poll consumes
        // at least one charged event; one additional poll proves it is empty.
        for _ in 0..=capacity {
            if Instant::now() >= deadline {
                return Err("bounded terminal event drain deadline exceeded".into());
            }
            if self.tick()? == 0 {
                return Ok(());
            }
        }
        Err("stopped owner exceeded its terminal event capacity".into())
    }
    fn wait(
        &mut self,
        label: &str,
        duration: Duration,
        predicate: impl Fn(&Self) -> bool,
    ) -> Result<()> {
        let deadline = Instant::now() + duration;
        loop {
            self.tick()?;
            if predicate(self) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("{label} deadline exceeded").into());
            }
            if self
                .host
                .as_ref()
                .is_some_and(|h| !matches!(h.status(), HostStatus::Running))
            {
                return Err(format!("owner stopped while waiting for {label}").into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn ready(&mut self, generation: u32) -> Result<()> {
        self.wait(
            "topic readiness",
            Duration::from_millis(self.config.phase_timeout_ms),
            |h| h.report.topic_ids.contains_key(&generation),
        )
    }
    fn open(&mut self, generation: u32) -> Result<TopicHandle> {
        let topic = self.client.open_topic(&self.config.profile.topic)?;
        self.topic_generations.insert(topic.0, generation);
        Ok(topic)
    }
    fn reopen_after_close(&mut self, generation: u32) -> Result<TopicHandle> {
        let deadline = Instant::now() + Duration::from_millis(self.config.phase_timeout_ms);
        loop {
            self.tick()?;
            match self.client.open_topic(&self.config.profile.topic) {
                Ok(topic) => {
                    self.topic_generations.insert(topic.0, generation);
                    return Ok(topic);
                }
                // CloseTopic is an owner command. The client name remains
                // reserved until that old handle's obligations actually retire.
                Err(ClientError::TopicAlreadyOpen) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(ClientError::TopicAlreadyOpen) => {
                    return Err("old topic name retirement deadline exceeded".into());
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    fn phase(&mut self, phase: &str, barrier: bool) -> Result<()> {
        self.report.checkpoint = phase.into();
        write_report(self.path, self.report)?;
        println!(
            "{}",
            json!({"kind":"phase","phase":phase,"checkpoint_path":self.path,"accepted":self.report.accepted.len(),"delivered":self.report.deliveries.len(),"topic_ids":self.report.topic_ids})
        );
        std::io::stdout().flush()?;
        if barrier && let Some(receiver) = &self.continue_rx {
            receiver
                .recv_timeout(Duration::from_millis(self.config.phase_timeout_ms))
                .map_err(|_| "barrier deadline exceeded")?
                .map_err(|e| -> Box<dyn Error> { e.into() })?;
        }
        Ok(())
    }
    fn submit_once(&mut self, id: u64, topic: TopicHandle, generation: u32) -> Result<bool> {
        self.report.offered_record_ids.insert(id);
        self.attempts += 1;
        if self.attempts > self.config.profile.records * 64 + 1024 {
            return Err("bounded admission attempt limit exceeded".into());
        }
        let record = wire_record(self.config, id, self.timestamp + id as i64);
        let partition = (id % u64::from(self.config.profile.partitions)) as i32;
        let lane = (id % u64::from(self.config.lanes)) as u8;
        let admitted = match self.config.input_mode {
            InputMode::Copy => {
                let headers: Vec<_> = record
                    .headers
                    .iter()
                    .map(|(key, value)| Header {
                        key,
                        value: value.as_deref(),
                    })
                    .collect();
                self.client.submit_copy(&[RecordDescriptor {
                    topic,
                    partition_hint: Some(partition),
                    lane_hint: Some(lane),
                    key: record.key.as_deref(),
                    value: record.value.as_deref(),
                    headers: &headers,
                    timestamp_ms: record.timestamp,
                    user_token: id,
                    delivery_timeout: None,
                }])
            }
            InputMode::Leased => {
                let mut bytes = Vec::new();
                let key = record.key.as_ref().map(|value| append(&mut bytes, value));
                let value = record.value.as_ref().map(|value| append(&mut bytes, value));
                let headers: Vec<_> = record
                    .headers
                    .iter()
                    .map(|(key, value)| LeasedHeader {
                        key: append(&mut bytes, key.as_bytes()),
                        value: value.as_ref().map(|v| append(&mut bytes, v)),
                    })
                    .collect();
                let mut buffer = match self.client.acquire(bytes.len().try_into()?, lane) {
                    Ok(buffer) => buffer,
                    Err(error) => {
                        self.reject(format!("acquire:{error:?}"));
                        return Ok(false);
                    }
                };
                buffer.as_mut_slice()[..bytes.len()].copy_from_slice(&bytes);
                let lease = buffer
                    .commit(bytes.len().try_into()?)
                    .map_err(|e| format!("native commit failed: {e:?}"))?;
                let admitted = self.client.submit_leased(
                    lease,
                    &[LeasedRecordDescriptor {
                        topic,
                        partition_hint: Some(partition),
                        lane_hint: Some(lane),
                        key,
                        value,
                        headers: &headers,
                        timestamp_ms: record.timestamp,
                        user_token: id,
                        delivery_timeout: None,
                    }],
                );
                let row = self.report.input_releases.len();
                self.leases.insert(lease.0, row);
                self.report.input_releases.push(LeaseHistory {
                    lease: lease.0,
                    record_id: id,
                    accepted: admitted.accepted == 1,
                    release_requested: false,
                    released_event: None,
                });
                self.client.release(lease)?;
                self.report.input_releases[row].release_requested = true;
                admitted
            }
        };
        if admitted.accepted == 0 {
            self.reject(format!("{:?}", admitted.error));
            return Ok(false);
        }
        if admitted.accepted != 1 {
            return Err("singleton accepted count invalid".into());
        }
        self.report.accepted.push(Accepted {
            record_id: id,
            token: admitted
                .first_token
                .ok_or("accepted record has no token")?
                .0,
            topic_handle: topic.0,
            generation,
            expected_partition: partition,
            lane,
            key_hex: record.key.as_deref().map(hex),
            value_hex: record.value.as_deref().map(hex),
            headers: record
                .headers
                .into_iter()
                .map(|(key, value)| ExpectedHeader {
                    key,
                    value_hex: value.as_deref().map(hex),
                })
                .collect(),
            timestamp_ms: record.timestamp,
        });
        Ok(true)
    }
    fn reject(&mut self, reason: String) {
        self.report.rejected_attempts += 1;
        *self.report.rejected_reasons.entry(reason).or_default() += 1;
    }
    fn submit_retry(
        &mut self,
        ids: impl IntoIterator<Item = u64>,
        topic: TopicHandle,
        generation: u32,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(self.config.phase_timeout_ms);
        for id in ids {
            loop {
                self.tick()?;
                let owner_status = self.host.as_ref().map(HostProducer::status);
                check_retry_owner(
                    &self.report.fatal,
                    self.report.closed,
                    self.client.status()?.owner_aborted,
                    owner_status.as_ref(),
                )?;
                if self.submit_once(id, topic, generation)? {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err("admission deadline exceeded".into());
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        Ok(())
    }
    fn once(
        &mut self,
        ids: impl IntoIterator<Item = u64>,
        topic: TopicHandle,
        generation: u32,
    ) -> Result<Vec<u64>> {
        let mut deferred = Vec::new();
        for id in ids {
            if !self.submit_once(id, topic, generation)? {
                deferred.push(id);
            }
        }
        Ok(deferred)
    }
    fn flush(&mut self) -> Result<()> {
        let token = self.client.flush()?;
        self.report.flushes.push(FlushHistory {
            token: token.0,
            accepted_watermark: self.report.accepted.len(),
            done_event: None,
        });
        self.wait(
            "flush",
            Duration::from_millis(self.config.phase_timeout_ms),
            |h| {
                h.report
                    .flushes
                    .last()
                    .is_some_and(|f| f.done_event.is_some())
            },
        )
    }
    fn require_acked(&self, ids: Range<u64>) -> Result<()> {
        for id in ids {
            if !self
                .report
                .deliveries
                .iter()
                .any(|d| d.record_id == id && d.kind == "Acked")
            {
                return Err(format!("healthy cohort record {id} was not acknowledged").into());
            }
        }
        Ok(())
    }
    fn scenario(&mut self) -> Result<()> {
        let count = self.config.profile.records;
        let third = count / 3;
        let topic = self.open(0)?;
        if !matches!(
            self.config.scenario,
            Scenario::Pending | Scenario::Unavailable
        ) {
            self.ready(0)?;
        }
        self.phase("ready", false)?;
        match self.config.scenario {
            Scenario::Basic | Scenario::Pending => self.submit_retry(0..count, topic, 0)?,
            Scenario::StoppedPolling => {
                let before = self.report.rejected_attempts;
                let deferred = self.once(0..count, topic, 0)?;
                std::thread::sleep(Duration::from_millis(self.config.poll_pause_ms));
                self.report.stopped_polling_rejections = self.report.rejected_attempts - before;
                if self.report.stopped_polling_rejections == 0 {
                    return Err("stopped application did not reach backpressure".into());
                }
                self.phase("polling_resumed", false)?;
                self.submit_retry(deferred, topic, 0)?;
            }
            Scenario::Restart | Scenario::LeaderLoss | Scenario::CloseDeadline => {
                self.submit_retry(0..third, topic, 0)?;
                self.flush()?;
                self.require_acked(0..third)?;
                self.phase("before_fault", true)?;
                let accepted_before_fault = self.report.accepted.len();
                let deferred = self.once(third..2 * third, topic, 0)?;
                if self.report.accepted.len() == accepted_before_fault {
                    return Err(
                        "fault cohort admitted no records; fault case is inconclusive".into(),
                    );
                }
                std::thread::sleep(Duration::from_millis(self.config.poll_pause_ms));
                self.phase("after_fault", true)?;
                if self.config.scenario != Scenario::CloseDeadline {
                    self.submit_retry(deferred, topic, 0)?;
                    if self.config.scenario == Scenario::LeaderLoss {
                        // The orchestrator keeps the failed leader down until
                        // this committed-log checkpoint proves live failover.
                        self.flush()?;
                        self.require_acked(third..2 * third)?;
                        self.phase("fault_settled", true)?;
                    }
                    self.submit_retry(2 * third..count, topic, 0)?;
                }
            }
            Scenario::Unavailable => {
                self.phase("before_fault", true)?;
                let _ = self.once(0..count, topic, 0)?;
                if self.report.accepted.is_empty() {
                    return Err(
                        "unavailable cohort admitted no records; fault case is inconclusive".into(),
                    );
                }
                self.phase("after_fault", true)?;
            }
            Scenario::Recreate => {
                self.submit_retry(0..third, topic, 0)?;
                self.flush()?;
                self.require_acked(0..third)?;
                self.phase("before_recreate", true)?;
                let old_id = self
                    .report
                    .topic_ids
                    .get(&0)
                    .cloned()
                    .ok_or("old identity missing")?;
                let first_old = self.report.accepted.len();
                let _ = self.once(third..2 * third, topic, 0)?;
                self.wait(
                    "old handle settlement",
                    Duration::from_millis(self.config.phase_timeout_ms),
                    |h| h.report.deliveries.len() == h.report.accepted.len(),
                )?;
                if self.report.accepted[first_old..].iter().any(|a| {
                    self.report
                        .deliveries
                        .iter()
                        .any(|d| d.record_id == a.record_id && d.kind == "Acked")
                }) {
                    return Err("old UUID record acknowledged after topic recreation".into());
                }
                self.client.close_topic(topic)?;
                let newer = self.reopen_after_close(1)?;
                self.ready(1)?;
                if self.report.topic_ids.get(&1) == Some(&old_id) {
                    return Err("recreated topic kept original identity".into());
                }
                self.phase("recreated", true)?;
                self.submit_retry(2 * third..count, newer, 1)?;
            }
        }
        if !matches!(
            self.config.scenario,
            Scenario::CloseDeadline | Scenario::Unavailable
        ) {
            self.flush()?;
            if self.config.scenario == Scenario::Recreate {
                self.require_acked(2 * third..count)?;
            }
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        let deadline_case = matches!(
            self.config.scenario,
            Scenario::CloseDeadline | Scenario::Unavailable
        );
        let close_ms = if deadline_case || self.report.error.is_some() {
            250
        } else {
            self.config.profile.delivery_timeout_ms
        };
        if let Some(host) = &self.host {
            let _ = host.close(RuntimeDuration::from_nanos(close_ms * 1_000_000));
        }
        let deadline = Instant::now() + Duration::from_millis(self.config.phase_timeout_ms);
        loop {
            self.tick()?;
            if self
                .host
                .as_ref()
                .is_some_and(|h| !matches!(h.status(), HostStatus::Running))
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err("bounded owner close deadline exceeded; join skipped".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        self.tick()?;
        let host = self.host.take().ok_or("owner absent")?;
        let joined = host.join()?;
        self.report.joined = true;
        self.report.final_status = Some(format!("{joined:?}"));
        self.drain_stopped_owner(deadline)?;
        let status = self.client.status()?;
        self.report.final_credits=Resource::ALL.iter().enumerate().map(|(i,r)|json!({"resource":format!("{r:?}"),"held":status.credits[i].held,"limit":status.credits[i].limit,"reserved":status.credits[i].reserved,"released":status.credits[i].released})).collect();
        if self.report.accepted.len() != self.report.deliveries.len()
            || !self.report.closed
            || self.report.closed_unresolved
                != Some(
                    self.report
                        .deliveries
                        .iter()
                        .filter(|d| d.kind == "Unknown")
                        .count() as u32,
                )
            || self
                .report
                .input_releases
                .iter()
                .any(|l| !l.release_requested || l.released_event.is_none())
            || status.credits.iter().any(|p| p.held != 0)
        {
            return Err("terminal ownership/delivery/close invariant failed".into());
        }
        if matches!(
            self.config.scenario,
            Scenario::Basic
                | Scenario::Pending
                | Scenario::StoppedPolling
                | Scenario::Restart
                | Scenario::LeaderLoss
        ) && self.report.deliveries.iter().any(|d| d.kind != "Acked")
        {
            return Err("healthy/recovered scenario has non-Acked delivery".into());
        }
        check_fatal_policy(self.config.scenario, &self.report.fatal)?;
        self.phase("closed", false)?;
        Ok(())
    }
}
fn execute(config: &Config, path: &Path, report: &mut Report) -> Result<()> {
    let producer_config = config.producer()?;
    let host = HostProducer::start_with_options(
        producer_config,
        HostProducerOptions {
            write_mode: config.write_mode.native(),
            diagnostics: true,
        },
    )?;
    report.backend = Some(format!("{:?}", host.backend()));
    let client = host.client();
    let timestamp = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let mut h = Harness {
        config,
        report,
        path,
        client,
        host: Some(host),
        continue_rx: config.barriers.then(continue_reader),
        topic_generations: BTreeMap::new(),
        delivered: BTreeSet::new(),
        leases: BTreeMap::new(),
        event_index: 0,
        attempts: 0,
        timestamp,
    };
    if let Err(error) = h.scenario() {
        h.report.error = Some(error.to_string());
    }
    if let Err(error) = h.finish() {
        h.report.error = Some(match h.report.error.take() {
            Some(old) => format!("{old}; cleanup: {error}"),
            None => error.to_string(),
        });
    }
    let accepted: BTreeSet<_> = h.report.accepted.iter().map(|row| row.record_id).collect();
    h.report.unaccepted_record_ids = h
        .report
        .offered_record_ids
        .difference(&accepted)
        .copied()
        .collect();
    h.report.complete = h.report.error.is_none();
    Ok(())
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: producer_check CONFIG.json RESULT.json".into());
    }
    let config: Config = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let path = PathBuf::from(&args[1]);
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut report = Report::new(&config)?;
    write_report(&path, &report)?;
    if let Err(error) = execute(&config, &path, &mut report) {
        report.error = Some(error.to_string());
    }
    write_report(&path, &report)?;
    if !report.complete {
        return Err(report
            .error
            .unwrap_or_else(|| "correctness run incomplete".into())
            .into());
    }
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("producer correctness check failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        let mut profile: Profile =
            serde_json::from_str(include_str!("../../../benchmarks/profile.json")).unwrap();
        profile.records = 800;
        Config {
            schema: SCHEMA.into(),
            run_id: "native-case-42".into(),
            profile,
            lanes: 4,
            input_mode: InputMode::Leased,
            write_mode: RequestedWriteMode::Staging,
            scenario: Scenario::Basic,
            barriers: false,
            phase_timeout_ms: 30_000,
            poll_pause_ms: 250,
            tls_server_name: None,
        }
    }
    #[test]
    fn leader_loss_requires_fault_cohort_on_the_partition_whose_leader_is_killed() {
        let mut config = config();
        config.scenario = Scenario::LeaderLoss;
        config.barriers = true;
        config.profile.partitions = 4;
        config.profile.records = 6; // IDs2,3 never revisit partition0.
        assert!(
            config
                .producer()
                .unwrap_err()
                .to_string()
                .contains("partition 0")
        );
        config.profile.records = 12; // IDs4..7 cover every partition.
        assert!(config.producer().is_ok());
        config.profile.records = 192;
        assert!(config.producer().is_ok());
        config.profile.records = 1000;
        assert!(config.producer().is_ok());
        config.profile.records = 12;
        config.profile.partitions = 8;
        assert!(config.producer().is_err());
        config.scenario = Scenario::Restart;
        assert!(config.producer().is_ok());
    }
    #[test]
    fn closed_checks_fifo_delivery_and_release_state_before_any_later_event() {
        let mut report = Report::new(&config()).unwrap();
        report.accepted.push(Accepted {
            record_id: 7,
            token: 1,
            topic_handle: 1,
            generation: 0,
            expected_partition: 0,
            lane: 0,
            key_hex: None,
            value_hex: None,
            headers: vec![],
            timestamp_ms: 123,
        });
        report.input_releases.push(LeaseHistory {
            lease: 1,
            record_id: 7,
            accepted: true,
            release_requested: true,
            released_event: None,
        });
        // Closed preceding the delivery cannot be made valid by later events.
        assert!(observe_closed(&mut report, 0).is_err());
        assert!(!report.closed);
        report.deliveries.push(Delivery {
            record_id: 7,
            token: 1,
            topic_handle: 1,
            topic_id: "00".repeat(16),
            partition: 0,
            kind: "Unknown",
            reason: "Closed".into(),
            offset: None,
            timestamp_ms: None,
            attempts: 1,
            event_index: 1,
        });
        // Delivery alone is insufficient while a native input release is due.
        assert!(observe_closed(&mut report, 1).is_err());
        assert!(!report.closed);
        report.input_releases[0].released_event = Some(2);
        assert!(observe_closed(&mut report, 0).is_err());
        assert!(!report.closed);
        // In-order Delivery, InputReleased, Closed preserves the Unknown total.
        observe_closed(&mut report, 1).unwrap();
        assert!(report.closed);
        assert_eq!(report.closed_unresolved, Some(1));
        assert!(observe_closed(&mut report, 1).is_err());
    }
    #[test]
    fn stopped_owner_drains_more_than_three_polls_before_terminal_checks() {
        use kr_kafka_producer::{
            actor::{ActorConfig, ProducerActor},
            client::ClientClock,
            connector::{ConnectError, ConnectTarget, Connected, Connector},
            engine::ProducerEngine,
        };
        use kr_runtime::{RuntimeConfig, RuntimeHandle, RuntimeInstant, SimRuntime};
        use kr_runtime_io::network::MemoryStream;

        struct UnavailableConnector;
        impl Connector for UnavailableConnector {
            type Stream = MemoryStream;
            type ConnectFuture =
                std::future::Ready<std::result::Result<Connected<MemoryStream>, ConnectError>>;
            fn connect(&mut self, _: ConnectTarget) -> Self::ConnectFuture {
                std::future::ready(Err(ConnectError::Timeout))
            }
        }
        let mut config = config();
        config.profile.records = 1000;
        config.input_mode = InputMode::Copy;
        config.scenario = Scenario::Unavailable;
        config.barriers = true;
        let mut report = Report::new(&config).unwrap();
        let mut runtime = SimRuntime::new(RuntimeConfig {
            max_steps_per_run: 50_000,
            ..Default::default()
        });
        let handle = RuntimeHandle::Sim(runtime.handle());
        let engine = ProducerEngine::new(config.producer().unwrap(), None).unwrap();
        let credits = engine.credits();
        let (client, actor) = ProducerActor::new(
            handle.clone(),
            engine,
            UnavailableConnector,
            ClientClock::Simulation,
            ActorConfig::default(),
        )
        .unwrap();
        let now = RuntimeInstant::ZERO;
        let topic = client.open_topic_at("unavailable", now).unwrap();
        let records: Vec<_> = (0..1000)
            .map(|record_id| RecordDescriptor {
                topic,
                partition_hint: Some(0),
                lane_hint: Some(0),
                key: None,
                value: Some(b"queued until application polls"),
                headers: &[],
                timestamp_ms: 123,
                user_token: record_id,
                delivery_timeout: None,
            })
            .collect();
        let accepted = client.submit_copy_at(now, &records);
        assert_eq!(accepted.accepted, 1000);
        let first = accepted.first_token.unwrap().0;
        for record_id in 0..1000 {
            report.accepted.push(Accepted {
                record_id,
                token: first + record_id,
                topic_handle: topic.0,
                generation: 0,
                expected_partition: 0,
                lane: 0,
                key_hex: None,
                value_hex: Some(hex(b"queued until application polls")),
                headers: vec![],
                timestamp_ms: 123,
            });
        }
        client.close_at(now, RuntimeDuration::ZERO).unwrap();
        let join = handle.spawn(actor).unwrap();
        // Deliberately do not poll a single application event until the actual
        // portable owner has completed and its runtime has stopped.
        let status = runtime
            .block_on(async { join.await.unwrap().unwrap() })
            .unwrap();
        runtime.finish().unwrap();
        assert!(status.closed);
        assert_eq!((status.accepted, status.terminal), (1000, 1000));
        assert_eq!(
            credits.snapshot()[Resource::DeliveryEvents as usize].held,
            1000
        );
        let mut harness = Harness {
            config: &config,
            report: &mut report,
            path: Path::new("unused-stopped-owner-report.json"),
            host: None,
            client,
            continue_rx: None,
            topic_generations: BTreeMap::from([(topic.0, 0)]),
            delivered: BTreeSet::new(),
            leases: BTreeMap::new(),
            event_index: 0,
            attempts: 0,
            timestamp: 123,
        };
        for _ in 0..3 {
            assert_eq!(harness.tick().unwrap(), 256);
        }
        assert!(harness.report.deliveries.len() < 1000);
        assert!(!harness.report.closed);
        assert!(credits.snapshot()[Resource::DeliveryEvents as usize].held > 0);
        harness
            .drain_stopped_owner(Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert_eq!(harness.report.deliveries.len(), 1000);
        assert!(harness.report.closed);
        assert_eq!(harness.report.closed_unresolved, Some(0));
        assert_eq!(harness.tick().unwrap(), 0);
        assert!(credits.is_empty(), "{:?}", credits.snapshot());
        assert!(check_fatal_policy(Scenario::Unavailable, &harness.report.fatal).is_ok());
    }
    #[test]
    fn close_deadline_notification_matches_real_pending_engine_settlement() {
        use kr_kafka_producer::{
            admission::Admission,
            engine::ProducerEngine,
            routing::PartitionChoice,
            types::{RecordToken, WorkBudget},
        };
        use kr_runtime::RuntimeInstant;
        let config = config().producer().unwrap();
        let now = RuntimeInstant::ZERO;
        let mut engine = ProducerEngine::new(config.clone(), None).unwrap();
        let topic = engine.open_topic("unavailable", now).unwrap();
        let credits = engine.credits();
        let mut admission = Admission::new(
            &config,
            credits.clone(),
            config.validate().unwrap().effective_batch_payload_bytes,
        );
        let (submitted, batch) = admission.prepare_copy(
            now,
            &[RecordDescriptor {
                topic,
                partition_hint: Some(0),
                lane_hint: Some(0),
                key: None,
                value: Some(b"accepted with no metadata"),
                headers: &[],
                timestamp_ms: 123,
                user_token: 7,
                delivery_timeout: None,
            }],
            &[Ok(0)],
        );
        assert_eq!(submitted.accepted, 1);
        engine
            .admit(now, batch.unwrap(), &[PartitionChoice::Pending])
            .unwrap();
        engine.close(now, now, RecordToken(1)).unwrap();
        let mut events = Vec::new();
        for _ in 0..4096 {
            engine.on_deadline(
                now,
                WorkBudget {
                    bytes: 4096,
                    items: 1,
                },
            );
            if let Some(event) = engine.pop_event() {
                events.push(event.event);
            }
            if engine.status().closed && !engine.has_terminal_work() {
                while let Some(event) = engine.pop_event() {
                    events.push(event.event);
                }
                break;
            }
        }
        assert!(engine.status().closed);
        let fatal: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Fatal { code } => Some(*code),
                _ => None,
            })
            .collect();
        assert_eq!(fatal, [FailureReason::Closed as u32]);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::Closed { unresolved: 0 }))
                .count(),
            1
        );
        let deliveries: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Delivery(delivery) => Some(delivery),
                _ => None,
            })
            .collect();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].user_token, 7);
        assert_eq!(deliveries[0].outcome.kind, DeliveryKind::NotWritten);
        assert_eq!(deliveries[0].outcome.reason, FailureReason::Closed);
        assert!(credits.snapshot().iter().all(|pool| pool.held == 0));
        assert!(check_fatal_policy(Scenario::CloseDeadline, &fatal).is_ok());
        assert!(check_fatal_policy(Scenario::Unavailable, &fatal).is_ok());
        assert!(check_fatal_policy(Scenario::Basic, &fatal).is_err());
    }
    #[test]
    fn expected_forced_close_does_not_hide_other_fatals_or_retry_terminal_owners() {
        for scenario in [
            Scenario::CloseDeadline,
            Scenario::Unavailable,
            Scenario::Basic,
            Scenario::Restart,
        ] {
            assert!(check_fatal_policy(scenario, &[]).is_ok());
            for codes in [
                vec![FailureReason::RuntimeFailed as u32],
                vec![FailureReason::Authentication as u32],
                vec![
                    FailureReason::Closed as u32,
                    FailureReason::RuntimeFailed as u32,
                ],
                vec![FailureReason::Closed as u32, FailureReason::Closed as u32],
            ] {
                assert!(check_fatal_policy(scenario, &codes).is_err());
            }
        }
        assert!(check_retry_owner(&[], false, false, Some(&HostStatus::Running)).is_ok());
        // Once-only old-handle probes do not use this retry guard. A retry loop
        // must fail on its first observation of any global terminal state.
        for code in [
            FailureReason::Closed,
            FailureReason::RuntimeFailed,
            FailureReason::Authentication,
        ] {
            assert!(
                check_retry_owner(&[code as u32], false, false, Some(&HostStatus::Running))
                    .is_err()
            );
        }
        assert!(check_retry_owner(&[], true, false, Some(&HostStatus::Running)).is_err());
        assert!(check_retry_owner(&[], false, true, Some(&HostStatus::Running)).is_err());
        assert!(check_retry_owner(&[], false, false, None).is_err());
        assert!(
            check_retry_owner(
                &[],
                false,
                false,
                Some(&HostStatus::Failed(
                    kr_kafka_producer_host::producer::HostError::Panicked
                )),
            )
            .is_err()
        );
    }
    #[test]
    fn correctness_profile_is_bounded_and_uses_actual_requested_lanes_and_backend() {
        let mut config = config();
        for lanes in [1, 4] {
            for compression in ["none", "zstd1"] {
                for mode in ["copy", "leased"] {
                    config.lanes = lanes;
                    config.profile.compression = compression.into();
                    config.input_mode = if mode == "copy" {
                        InputMode::Copy
                    } else {
                        InputMode::Leased
                    };
                    let producer = config.producer().unwrap();
                    assert_eq!(producer.lanes, lanes);
                    assert_eq!(producer.record_descriptors, 2048);
                    assert_eq!(producer.max_live_leases, 128);
                    assert_eq!(producer.max_open_topics, 4);
                    assert_eq!(
                        producer.transport,
                        kr_kafka_producer::config::TransportPolicy::Readiness
                    );
                }
            }
        }
        config.scenario = Scenario::StoppedPolling;
        assert_eq!(config.producer().unwrap().delivery_event_capacity, 32);
        config.scenario = Scenario::Restart;
        assert!(config.producer().is_err());
        config.barriers = true;
        assert!(config.producer().is_ok());
        config.profile.records = 2001;
        assert!(config.producer().is_err());
        config.profile.records = 1000;
        config.profile.record_bytes = 8193;
        assert!(config.producer().is_err());
    }
    #[test]
    fn write_mode_defaults_and_explicit_vectored_profile_are_preserved() {
        let config = config();
        let mut value = serde_json::to_value(&config).unwrap();
        value.as_object_mut().unwrap().remove("write_mode");
        let defaulted: Config = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(defaulted.write_mode.native(), WriteMode::Staging);
        value["write_mode"] = json!("vectored");
        let vectored: Config = serde_json::from_value(value).unwrap();
        assert_eq!(vectored.write_mode.native(), WriteMode::Vectored);
        assert_eq!(Report::new(&vectored).unwrap().write_mode, "vectored");
    }
    #[test]
    fn independent_identity_headers_cover_null_empty_binary_and_duplicate_fields() {
        let config = config();
        for id in 0..800 {
            let record = wire_record(&config, id, 1_700_000_000_000 + id as i64);
            let run: Vec<_> = record
                .headers
                .iter()
                .filter(|(k, _)| k == "kr-check-run")
                .collect();
            let identity: Vec<_> = record
                .headers
                .iter()
                .filter(|(k, _)| k == "kr-check-id")
                .collect();
            assert_eq!(run.len(), 1);
            assert_eq!(identity.len(), 1);
            assert_eq!(run[0].1.as_deref(), Some(config.run_id.as_bytes()));
            assert_eq!(identity[0].1.as_deref(), Some(id.to_string().as_bytes()));
            assert_eq!(record.headers[2], ("duplicate".into(), None));
            assert_eq!(record.headers[3], ("duplicate".into(), Some(Vec::new())));
            assert_eq!(record.headers[4].1, Some(vec![0, 255, id as u8]));
        }
        assert!(wire_record(&config, 0, 0).value.is_none());
        assert_eq!(wire_record(&config, 19, 0).value, Some(Vec::new()));
        assert_eq!(hex(&[0, 1, 15, 16, 255]), "00010f10ff");
    }
    #[test]
    fn failure_report_keeps_public_profile_without_reading_or_disclosing_credentials() {
        let mut config = config();
        config.profile.security = "plain".into();
        config.profile.username_env = Some("CHECK_USERNAME_ENV".into());
        config.profile.password_env = Some("CHECK_PASSWORD_ENV".into());
        let mut report = Report::new(&config).unwrap();
        report.error = Some("setup unavailable".into());
        report.topic_ids.insert(0, "010203".into());
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["schema"], SCHEMA);
        assert_eq!(value["topic_ids"]["0"], "010203");
        assert_eq!(value["complete"], false);
        assert_eq!(value["accepted"], json!([]));
        assert_eq!(
            value["config"]["profile"]["password_env"],
            "CHECK_PASSWORD_ENV"
        );
    }
}
