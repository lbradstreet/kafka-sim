use crate::{HISTORY_VERSION, RealizedFault};
use kr_kafka_broker_model::{
    AcceptedRecord, CreditObservation, DeliveryOracle, ObservedDelivery, ObservedOutcome,
    ObservedResponse, OracleLimits,
};
use kr_kafka_producer::{
    credit::{Resource, SharedCredits},
    types::*,
};
use kr_shared_bytes::SharedBytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DomainEvent {
    /// Decision-time credit state; recorded only on a partition-pressure denial.
    DescriptorPressure {
        record_id: u64,
        topic: Option<[u8; 16]>,
        partition: Option<i32>,
        capacity: u64,
        shared_limit: u64,
        total_held: u64,
        class_held: u64,
    },
    BrokerFrameAbandoned {
        connection: u64,
        correlation: i32,
        broker: i32,
        window: u32,
    },
    ClientRequestDispatched {
        connection: u64,
        correlation: i32,
        api: i16,
        request_id: u64,
        tokens: Vec<u64>,
        wire_bytes: u64,
        batches: Vec<crate::DispatchBatch>,
    },
    ClientRequestWriteCompleted {
        request_id: u64,
        connection: u64,
        correlation: i32,
    },
    ClientRequestFinished {
        request_id: u64,
        connection: u64,
        correlation: i32,
        confirmed: u64,
        result: String,
        certainty: String,
    },
    LinkStateChanged {
        broker: i32,
        direction: crate::LinkDirection,
        state: Option<crate::OutageMode>,
        window: u32,
    },
    SetupFinished {
        connection: u64,
        broker: i32,
        started_ns: u64,
        deadline_ns: u64,
        resolved_ns: u64,
        elapsed_ns: u64,
        result: String,
    },
    Offered {
        load: u32,
        record_id: u64,
        due_ns: u64,
    },
    AdmissionAttempt {
        record_id: u64,
        attempt: u32,
        error: Option<String>,
    },
    Refused {
        record_id: u64,
        due_ns: u64,
        offered_ns: u64,
        error: String,
    },
    ScheduledControl {
        index: u32,
        at_ns: u64,
        action: crate::TimedControl,
    },
    PollingChanged {
        window: u32,
        paused: bool,
    },
    OffersStopped {
        planned: u32,
        offered: u32,
        cancelled: u32,
    },
    /// A modeled transport pair exists, including capability setup traffic.
    ConnectionOpened {
        connection: u64,
        broker: i32,
        lane: u8,
    },
    /// The broker-side service relinquished its connection registration.
    ConnectionClosed {
        connection: u64,
        reason: String,
    },
    FetchVerified {
        topic: [u8; 16],
        partition: i32,
        from_offset: i64,
        next_offset: i64,
        high_watermark: i64,
        ids: Vec<u64>,
    },
    WorkloadStep {
        index: u32,
        action: String,
    },
    Accepted {
        record_id: u64,
        token: u64,
        topic: [u8; 16],
        partition: i32,
        lease: Option<u64>,
    },
    Rejected {
        count: u32,
    },
    WriteAdmitted {
        operation: u64,
        connection: u64,
        bytes: usize,
        segments: usize,
    },
    WriteCompleted {
        operation: u64,
        bytes: usize,
        certainty: String,
    },
    BrokerRequest {
        connection: u64,
        api: i16,
        version: i16,
        correlation: i32,
        records: Vec<u64>,
    },
    ResponseRead {
        connection: u64,
        correlation: i32,
    },
    ProduceResponse {
        connection: u64,
        correlation: i32,
        token: u64,
        duplicate: bool,
        offset: Option<i64>,
        timestamp: Option<i64>,
    },
    BrokerCommit {
        connection: u64,
        correlation: i32,
        batches: u32,
        records: u32,
    },
    IsolationClosed {
        broker: i32,
        connection: u64,
    },
    BrokerTiming {
        connection: u64,
        broker: i32,
        api: i16,
        correlation: i32,
        frame: u64,
        phase: crate::faults::Phase,
        arrived_ns: u64,
        now_ns: u64,
    },
    FaultDecision(crate::faults::Decision),
    Fault(RealizedFault),
    Delivery {
        token: u64,
        record_id: u64,
        topic: [u8; 16],
        partition: i32,
        outcome: u32,
        reason: u32,
        offset: Option<i64>,
        timestamp: Option<i64>,
        attempts: u32,
    },
    InputReleased {
        lease: u64,
    },
    Flush {
        token: u64,
    },
    FlushDone {
        token: u64,
    },
    TopicReady {
        handle: u32,
        id: [u8; 16],
    },
    TopicFailed {
        handle: u32,
        code: u32,
    },
    Fatal {
        code: u32,
    },
    Closed {
        unknown: u32,
    },
    Credits {
        held: Vec<u64>,
        reserved: Vec<u64>,
        released: Vec<u64>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryEntry {
    pub ordinal: u64,
    pub now_ns: u64,
    pub event: DomainEvent,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainHistory {
    pub version: u32,
    pub entries: Vec<HistoryEntry>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Coverage {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub offered: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub refused: u64,
    pub accepted: u64,
    pub acked: u64,
    pub not_written: u64,
    pub unknown: u64,
    pub partial_writes: u64,
    pub retries: u64,
    pub duplicate_sequences: u64,
    pub leader_moves: u64,
    pub recreates: u64,
    pub expands: u64,
    pub throttles: u64,
    pub capability_rejections: u64,
    pub produce13: u64,
    pub backpressure: u64,
    pub input_releases: u64,
    pub linger_seals: u64,
    pub target_seals: u64,
}
impl Coverage {
    pub fn merge(&mut self, other: &Self) -> Result<(), String> {
        macro_rules! add {($($field:ident),*) => {$(self.$field=self.$field.checked_add(other.$field).ok_or("coverage count overflow")?;)*};}
        add!(
            accepted,
            acked,
            not_written,
            unknown,
            partial_writes,
            retries,
            duplicate_sequences,
            leader_moves,
            recreates,
            expands,
            throttles,
            capability_rejections,
            produce13,
            backpressure,
            input_releases,
            linger_seals,
            target_seals
        );
        Ok(())
    }
    pub fn require_campaign_gates(&self) -> Result<(), String> {
        for (name, count) in [
            ("partial write", self.partial_writes),
            ("retry", self.retries),
            ("duplicate sequence", self.duplicate_sequences),
            ("leader move", self.leader_moves),
            ("recreate", self.recreates),
            ("Produce9 rejection", self.capability_rejections),
            ("Produce13 success", self.produce13),
            ("linger seal", self.linger_seals),
            ("target seal", self.target_seals),
            ("backpressure", self.backpressure),
            ("native release", self.input_releases),
        ] {
            if count == 0 {
                return Err(format!("aggregate coverage gate is zero: {name}"));
            }
        }
        Ok(())
    }
}
#[derive(Default)]
pub(crate) struct Proof {
    pub transmitted: bool,
    pub ambiguous: bool,
    pub response: Option<ObservedResponse>,
    pub parsed_attempts: u32,
    pub rejection: bool,
}
pub(crate) struct Accepted {
    pub id: u64,
    pub topic: [u8; 16],
    pub expected_topic: [u8; 16],
    pub handle: u32,
    pub partition: i32,
    pub key_routed: bool,
    pub consumed: bool,
    pub delivered: bool,
    pub proof: Proof,
}
pub(crate) struct AdmissionRoute {
    pub topic: [u8; 16],
    pub partition: i32,
    pub handle: u32,
    pub resolved: bool,
    pub key_routed: bool,
}
pub(crate) struct Request {
    pub api: i16,
    pub version: i16,
    pub tokens: Vec<(u64, TopicPartition, i32)>,
}
pub(crate) struct Audit {
    pub history: DomainHistory,
    pub oracle: DeliveryOracle,
    pub accepted: BTreeMap<u64, Accepted>,
    pub ids: BTreeMap<u64, u64>,
    pub requests: BTreeMap<(u64, i32), Request>,
    pub leases: BTreeMap<u64, Vec<u64>>,
    pub released_leases: BTreeSet<u64>,
    pub live_leases: BTreeSet<u64>,
    pub undelivered: usize,
    pub pending_tokens: BTreeSet<u64>,
    pub flushes_done: BTreeSet<u64>,
    pub coverage: Coverage,
    pub realized: Vec<RealizedFault>,
    pub error: Option<String>,
    pub maximum: usize,
    pub next_operation: u64,
    pub retained: BTreeSet<u64>,
    pub batch_fill: (u64, u64),
    pub recreated: BTreeSet<[u8; 16]>,
    pub incompatible_produce_advertised: bool,
    link_transitions: Vec<(u64, DomainEvent)>,
    next_link_transition: usize,
    pub request_capture: Option<std::sync::Arc<crate::request_observation::Capture>>,
    request_spare: Vec<(u64, DomainEvent)>,
}
impl Audit {
    pub(crate) fn new(maximum: usize, records: usize) -> Self {
        Self {
            history: DomainHistory {
                version: HISTORY_VERSION,
                entries: Vec::new(),
            },
            oracle: DeliveryOracle::new(OracleLimits {
                records,
                operations: 256,
                retained_bytes: 16 * 1024 * 1024,
                ..OracleLimits::default()
            }),
            accepted: BTreeMap::new(),
            ids: BTreeMap::new(),
            requests: BTreeMap::new(),
            leases: BTreeMap::new(),
            released_leases: BTreeSet::new(),
            live_leases: BTreeSet::new(),
            undelivered: 0,
            pending_tokens: BTreeSet::new(),
            flushes_done: BTreeSet::new(),
            coverage: Coverage::default(),
            realized: Vec::new(),
            error: None,
            maximum,
            next_operation: 1,
            retained: BTreeSet::new(),
            batch_fill: (0, 0),
            recreated: BTreeSet::new(),
            incompatible_produce_advertised: false,
            link_transitions: Vec::new(),
            next_link_transition: 0,
            request_capture: None,
            request_spare: Vec::new(),
        }
    }
    pub(crate) fn configure_links(&mut self, manifest: &crate::ReplayManifest) {
        let mut transitions = Vec::new();
        for (index, window) in manifest.faults.link_outages.iter().enumerate() {
            for (at, state) in [(window.start_ns, Some(window.mode)), (window.end_ns, None)] {
                transitions.push((
                    manifest.start_ns + at,
                    state.is_some(),
                    index,
                    DomainEvent::LinkStateChanged {
                        broker: window.broker,
                        direction: window.direction,
                        state,
                        window: index as u32,
                    },
                ));
            }
        }
        transitions.sort_by_key(|t| (t.0, t.1, t.2));
        self.link_transitions = transitions
            .into_iter()
            .map(|(at, _, _, event)| (at, event))
            .collect();
    }
    pub(crate) fn advance_links(&mut self, now: u64) {
        self.drain_requests();
        self.apply_link_transitions(now);
    }
    fn apply_link_transitions(&mut self, now: u64) {
        while self
            .link_transitions
            .get(self.next_link_transition)
            .is_some_and(|(at, _)| *at <= now)
        {
            let (at, event) = self.link_transitions[self.next_link_transition].clone();
            self.next_link_transition += 1;
            self.record_raw(at, event);
        }
    }
    pub(crate) fn drain_requests(&mut self) {
        let Some(capture) = &self.request_capture else {
            return;
        };
        let mut captured = std::mem::take(&mut self.request_spare);
        if let Err(error) = capture.drain_into(&mut captured) {
            self.request_spare = captured;
            self.fail(error);
            return;
        }
        for (now, mut event) in captured.drain(..) {
            if let DomainEvent::ClientRequestDispatched { tokens, .. } = &mut event {
                for token in tokens {
                    let Some(accepted) = self.ids.get(token) else {
                        self.fail("client dispatched an unaccepted workload ID".into());
                        return;
                    };
                    *token = *accepted;
                }
            }
            self.apply_link_transitions(now);
            self.record_raw(now, event);
        }
        self.request_spare = captured;
    }
    pub(crate) fn record(&mut self, now: u64, event: DomainEvent) -> u64 {
        self.drain_requests();
        // Fixed policy also governs provider callbacks. Insert due transitions
        // before any same-time observation, regardless of which task ran first.
        self.apply_link_transitions(now);
        self.record_raw(now, event)
    }
    fn record_raw(&mut self, now: u64, event: DomainEvent) -> u64 {
        if self.history.entries.len() == self.maximum {
            self.fail("producer domain history capacity exceeded".into());
            return u64::MAX;
        }
        let ordinal = self.history.entries.len() as u64 + 1;
        self.history.entries.push(HistoryEntry {
            ordinal,
            now_ns: now,
            event,
        });
        ordinal
    }
    pub(crate) fn fail(&mut self, error: String) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
    pub(crate) fn accept(
        &mut self,
        now: u64,
        id: u64,
        token: u64,
        route: AdmissionRoute,
        lease: Option<u64>,
    ) -> Result<(), String> {
        let AdmissionRoute {
            topic,
            partition,
            handle,
            resolved,
            key_routed,
        } = route;
        let expected_topic = topic;
        let topic = if resolved { topic } else { [0; 16] };
        let at = self.record(
            now,
            DomainEvent::Accepted {
                record_id: id,
                token,
                topic,
                partition,
                lease,
            },
        );
        let accepted = AcceptedRecord {
            token,
            topic,
            partition,
            lease,
            returned_at: at,
        };
        if key_routed {
            self.oracle.accept_unassigned_partition(accepted)
        } else {
            self.oracle.accept(accepted)
        }
        .map_err(|e| e.to_string())?;
        if self.ids.insert(id, token).is_some() {
            return Err("accepted duplicate workload record".into());
        }
        self.accepted.insert(
            token,
            Accepted {
                id,
                topic,
                expected_topic,
                handle,
                partition,
                key_routed,
                consumed: false,
                delivered: false,
                proof: Proof::default(),
            },
        );
        if let Some(lease) = lease {
            self.leases.entry(lease).or_default().push(token);
            self.live_leases.insert(lease);
        }
        self.undelivered += 1;
        self.pending_tokens.insert(token);
        self.coverage.accepted += 1;
        Ok(())
    }
    pub(crate) fn operation(
        &mut self,
        now: u64,
        connection: u64,
        bytes: usize,
        spans: &[SharedBytes],
    ) -> u64 {
        let id = self.next_operation;
        self.next_operation += 1;
        self.record(
            now,
            DomainEvent::WriteAdmitted {
                operation: id,
                connection,
                bytes,
                segments: spans.len(),
            },
        );
        if !spans.is_empty() {
            if let Err(error) = self.oracle.retain_operation(id, spans) {
                self.fail(error.to_string());
            } else {
                self.retained.insert(id);
            }
        }
        id
    }
    pub(crate) fn complete(
        &mut self,
        now: u64,
        id: u64,
        requested: usize,
        bytes: usize,
        certainty: String,
    ) {
        if bytes > 0 && bytes < requested {
            self.coverage.partial_writes += 1;
        }
        self.record(
            now,
            DomainEvent::WriteCompleted {
                operation: id,
                bytes,
                certainty,
            },
        );
        if let Err(error) = self.oracle.check_retained_operations() {
            self.fail(error.to_string());
        }
        if self.retained.remove(&id)
            && let Err(error) = self.oracle.release_operation(id)
        {
            self.fail(error.to_string());
        }
    }
    pub(crate) fn credits(&mut self, now: u64, credits: &SharedCredits) -> Result<(), String> {
        let pools = credits.snapshot();
        if pools[Resource::ReleaseEvents as usize].held < self.live_leases.len() {
            return Err("C7 native release event lost its ownership credit".into());
        }
        let live = self.undelivered;
        if pools[Resource::Descriptors as usize].held > live
            || pools[Resource::DeliveryEvents as usize].held != live
        {
            return Err(format!(
                "C7 accepted/event ownership mismatch: live {live}, descriptors {}, deliveries {}",
                pools[Resource::Descriptors as usize].held,
                pools[Resource::DeliveryEvents as usize].held
            ));
        }
        for (index, pool) in pools.iter().enumerate() {
            self.oracle
                .credits(CreditObservation {
                    pool: index as u32,
                    capacity: pool.limit as u64,
                    reserved: pool.reserved,
                    released: pool.released,
                    held: pool.held as u64,
                })
                .map_err(|e| e.to_string())?;
        }
        self.record(
            now,
            DomainEvent::Credits {
                held: pools.iter().map(|pool| pool.held as u64).collect(),
                reserved: pools.iter().map(|pool| pool.reserved).collect(),
                released: pools.iter().map(|pool| pool.released).collect(),
            },
        );
        Ok(())
    }
    fn consumed(&mut self, token: u64, ordinal: u64) -> Result<(), String> {
        let accepted = self
            .accepted
            .get_mut(&token)
            .ok_or("consumption of unknown token")?;
        if !accepted.consumed {
            self.oracle
                .input_consumed(token, ordinal)
                .map_err(|e| e.to_string())?;
            accepted.consumed = true;
        }
        Ok(())
    }
    pub(crate) fn event(&mut self, now: u64, event: Event) -> Result<(), String> {
        match event {
            Event::Delivery(event) => {
                let record = self
                    .accepted
                    .get(&event.token.0)
                    .ok_or("C1 delivery for unaccepted token")?;
                if record.delivered {
                    return Err("C1 duplicate delivery".into());
                }
                let unrouted = record.key_routed
                    && event.partition.partition == -1
                    && event.outcome.kind == DeliveryKind::NotWritten
                    && event.attempts == 0
                    && !record.proof.transmitted
                    && !record.proof.ambiguous
                    && record.proof.parsed_attempts == 0
                    && record.proof.response.is_none();
                if record.topic != event.partition.topic.0
                    || (record.partition != event.partition.partition && !unrouted)
                {
                    return Err("C11 changed delivery route".into());
                }
                if self.recreated.contains(&record.topic)
                    && event.outcome.kind == DeliveryKind::NotWritten
                    && event.outcome.reason != FailureReason::TopicDeleted
                {
                    return Err(format!(
                        "C12 recreated topic returned {:?} for token {}",
                        event.outcome.reason, event.token.0
                    ));
                }
                let id = record.id;
                let ordinal = self.record(
                    now,
                    DomainEvent::Delivery {
                        token: event.token.0,
                        record_id: id,
                        topic: event.partition.topic.0,
                        partition: event.partition.partition,
                        outcome: event.outcome.kind as u32,
                        reason: event.outcome.reason as u32,
                        offset: event.base_offset.get(),
                        timestamp: event.timestamp.get(),
                        attempts: event.attempts,
                    },
                );
                self.consumed(event.token.0, ordinal)?;
                let record = self
                    .accepted
                    .get_mut(&event.token.0)
                    .expect("accepted record");
                let outcome = match event.outcome.kind {
                    DeliveryKind::Acked => {
                        self.coverage.acked += 1;
                        ObservedOutcome::Acked
                    }
                    DeliveryKind::NotWritten => {
                        self.coverage.not_written += 1;
                        ObservedOutcome::NotWritten
                    }
                    DeliveryKind::Unknown => {
                        self.coverage.unknown += 1;
                        ObservedOutcome::Unknown
                    }
                };
                self.oracle
                    .delivery(ObservedDelivery {
                        token: event.token.0,
                        topic: event.partition.topic.0,
                        partition: event.partition.partition,
                        outcome,
                        offset: event.base_offset.get(),
                        timestamp: event.timestamp.get(),
                        attempts: event.attempts,
                        parsed_attempts: record.proof.parsed_attempts,
                        at: ordinal,
                        transmitted: record.proof.transmitted,
                        definitive_broker_rejection: record.proof.rejection,
                        prior_ambiguous_attempt: record.proof.ambiguous,
                        response: record.proof.response,
                    })
                    .map_err(|e| e.to_string())?;
                record.delivered = true;
                self.undelivered -= 1;
                self.pending_tokens.remove(&event.token.0);
            }
            Event::InputReleased { lease } => {
                if !self.released_leases.insert(lease.0) {
                    return Err("C2 duplicate native release".into());
                }
                self.live_leases.remove(&lease.0);
                let ordinal = self.record(now, DomainEvent::InputReleased { lease: lease.0 });
                if let Some(tokens) = self.leases.get(&lease.0).cloned() {
                    // The API's release event is the public last-owner witness;
                    // the independent provider-span audit additionally checks C8.
                    for token in tokens {
                        self.consumed(token, ordinal)?;
                    }
                    self.oracle
                        .input_released(lease.0, ordinal + 1)
                        .map_err(|e| e.to_string())?;
                }
                self.coverage.input_releases += 1;
            }
            Event::FlushDone { token } => {
                let at = self.record(now, DomainEvent::FlushDone { token: token.0 });
                self.oracle
                    .flush_done(token.0, at)
                    .map_err(|e| e.to_string())?;
                self.flushes_done.insert(token.0);
            }
            Event::Closed { unresolved } => {
                self.record(
                    now,
                    DomainEvent::Closed {
                        unknown: unresolved,
                    },
                );
                self.oracle.closed().map_err(|e| e.to_string())?;
                if u64::from(unresolved) != self.coverage.unknown.min(u64::from(u32::MAX)) {
                    return Err("C5 reported Unknown count differs".into());
                }
            }
            Event::TopicReady { topic, id, .. } => {
                let at = self.record(
                    now,
                    DomainEvent::TopicReady {
                        handle: topic.0,
                        id: id.0,
                    },
                );
                for (&token, record) in &mut self.accepted {
                    if record.handle != topic.0 || record.delivered {
                        continue;
                    }
                    if record.expected_topic != id.0 {
                        return Err("C11 resolved topic differs from accepted generation".into());
                    }
                    if record.topic == [0; 16] {
                        self.oracle
                            .bind_topic(token, id.0, at)
                            .map_err(|e| e.to_string())?;
                        record.topic = id.0;
                    } else if record.topic != id.0 {
                        return Err("C11 resolved topic rebound".into());
                    }
                }
            }
            Event::TopicFailed { topic, code } => {
                self.record(
                    now,
                    DomainEvent::TopicFailed {
                        handle: topic.0,
                        code,
                    },
                );
            }
            Event::Fatal { code } => {
                self.record(now, DomainEvent::Fatal { code });
                // Discovery is now generic; only the producer's observed fatal
                // event proves that an incompatible advertisement was rejected.
                if code == FailureReason::ProtocolViolation as u32
                    && std::mem::take(&mut self.incompatible_produce_advertised)
                {
                    self.coverage.capability_rejections += 1;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn flush(&mut self, now: u64, token: FlushToken) -> Result<(), String> {
        let at = self.record(now, DomainEvent::Flush { token: token.0 });
        self.oracle.flush(token.0, at).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;

    #[test]
    fn rejection_coverage_requires_advertisement_and_observed_producer_failure() {
        let mut audit = Audit::new(16, 1);
        let rejected = Event::Fatal {
            code: FailureReason::ProtocolViolation as u32,
        };
        audit.event(0, rejected).unwrap();
        assert_eq!(audit.coverage.capability_rejections, 0);
        audit.incompatible_produce_advertised = true;
        audit
            .event(
                1,
                Event::Fatal {
                    code: FailureReason::Transport as u32,
                },
            )
            .unwrap();
        assert_eq!(audit.coverage.capability_rejections, 0);
        audit.event(2, rejected).unwrap();
        assert_eq!(audit.coverage.capability_rejections, 1);
        audit.event(3, rejected).unwrap();
        assert_eq!(audit.coverage.capability_rejections, 1);
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;

    fn acknowledged() -> (Audit, DeliveryEvent) {
        let mut audit = Audit::new(32, 1);
        audit
            .accept(
                0,
                99,
                1,
                AdmissionRoute {
                    topic: [1; 16],
                    partition: 0,
                    handle: 1,
                    resolved: true,
                    key_routed: false,
                },
                None,
            )
            .unwrap();
        let at = audit.record(
            5,
            DomainEvent::ProduceResponse {
                connection: 1,
                correlation: 2,
                token: 1,
                duplicate: false,
                offset: Some(42),
                timestamp: Some(777),
            },
        );
        audit.accepted.get_mut(&1).unwrap().proof = Proof {
            transmitted: true,
            response: Some(ObservedResponse::Success {
                at,
                offset: 42,
                timestamp: Some(777),
            }),
            parsed_attempts: 2,
            ..Proof::default()
        };
        let event = DeliveryEvent {
            token: RecordToken(1),
            user_token: 99,
            topic: TopicHandle(1),
            partition: TopicPartition {
                topic: TopicId([1; 16]),
                partition: 0,
            },
            outcome: DeliveryOutcome::ACKED,
            base_offset: Some(42).into(),
            timestamp: Some(777).into(),
            attempts: 3,
        };
        (audit, event)
    }

    #[test]
    fn domain_history_preserves_public_delivery_metadata_and_response_witness() {
        let (mut audit, event) = acknowledged();
        audit.event(10, Event::Delivery(event)).unwrap();
        assert_eq!(
            audit.history.entries.last().unwrap().event,
            DomainEvent::Delivery {
                token: 1,
                record_id: 99,
                topic: [1; 16],
                partition: 0,
                outcome: DeliveryKind::Acked as u32,
                reason: FailureReason::None as u32,
                offset: Some(42),
                timestamp: Some(777),
                attempts: 3,
            }
        );
        let json = serde_json::to_string(&audit.history).unwrap();
        let restored: DomainHistory = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, audit.history);
    }

    #[test]
    fn audit_compares_public_metadata_with_independent_response_observation() {
        for mutation in 0..3 {
            let (mut audit, mut event) = acknowledged();
            match mutation {
                0 => event.base_offset = Some(43).into(),
                1 => event.timestamp = None.into(),
                _ => event.attempts = 1,
            }
            assert!(audit.event(10, Event::Delivery(event)).is_err());
            assert!(!audit.accepted.get(&1).unwrap().delivered);
        }
    }

    #[test]
    fn public_resolution_binds_only_the_admitted_handle_and_expected_uuid() {
        let mut audit = Audit::new(32, 2);
        for (token, handle) in [(1, 7), (2, 8)] {
            audit
                .accept(
                    0,
                    token,
                    token,
                    AdmissionRoute {
                        topic: [1; 16],
                        partition: 0,
                        handle,
                        resolved: false,
                        key_routed: false,
                    },
                    None,
                )
                .unwrap();
        }
        assert!(
            audit
                .event(
                    1,
                    Event::TopicReady {
                        topic: TopicHandle(7),
                        id: TopicId([2; 16]),
                        partitions: 1,
                    }
                )
                .is_err()
        );
        assert_eq!(audit.accepted[&1].topic, [0; 16]);
        audit
            .event(
                2,
                Event::TopicReady {
                    topic: TopicHandle(7),
                    id: TopicId([1; 16]),
                    partitions: 1,
                },
            )
            .unwrap();
        assert_eq!(audit.accepted[&1].topic, [1; 16]);
        assert_eq!(audit.accepted[&2].topic, [0; 16]);
        assert!(
            audit
                .event(
                    3,
                    Event::TopicReady {
                        topic: TopicHandle(7),
                        id: TopicId([2; 16]),
                        partitions: 1,
                    }
                )
                .is_err()
        );
        assert_eq!(audit.accepted[&1].topic, [1; 16]);
    }
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}
