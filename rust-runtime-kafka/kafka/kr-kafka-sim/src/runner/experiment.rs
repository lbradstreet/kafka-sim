use super::*;
use crate::{ExperimentWorkload, LoadShape, TimedControl};
use std::{collections::BTreeMap, task::Poll};

struct Candidate {
    due_ns: u64,
    offered_ns: u64,
    attempts: u32,
}
struct LoadState {
    next: u32,
    budget: u32,
    outstanding: u32,
    candidate: Option<Candidate>,
    retry_ns: u64,
    done: bool,
}
#[derive(Clone, Copy)]
enum Boundary {
    Link,
    Polling { window: usize, paused: bool },
    Control(usize),
}

pub(super) struct Driver<'a> {
    pub client: &'a ProducerClient,
    pub audit: &'a Rc<RefCell<Audit>>,
    pub credits: &'a kr_kafka_producer::credit::SharedCredits,
    pub handle: &'a RuntimeHandle,
    pub model: &'a Rc<RefCell<BrokerModel>>,
    pub manifest: &'a ReplayManifest,
    pub topics: &'a mut [TopicHandle],
}
impl Driver<'_> {
    pub(super) async fn drive(&mut self) -> Result<bool, String> {
        let config = self
            .manifest
            .experiment
            .as_ref()
            .ok_or("missing experiment")?;
        let mut states: Vec<_> = config
            .loads
            .iter()
            .map(|load| {
                Ok(LoadState {
                    next: 0,
                    budget: load.shape.offer_budget()?,
                    outstanding: 0,
                    candidate: None,
                    retry_ns: load.shape.start_ns(),
                    done: false,
                })
            })
            .collect::<Result<_, String>>()?;
        // (time, class, stable manifest index). End precedes start for touching
        // half-open windows; every polling transition precedes every control.
        let mut boundaries = Vec::new();
        for (window, pause) in config.polling_pauses.iter().enumerate() {
            boundaries.push((
                pause.start_ns,
                1,
                window,
                Boundary::Polling {
                    window,
                    paused: true,
                },
            ));
            boundaries.push((
                pause.end_ns,
                0,
                window,
                Boundary::Polling {
                    window,
                    paused: false,
                },
            ));
        }
        for (index, window) in self.manifest.faults.link_outages.iter().enumerate() {
            boundaries.push((window.start_ns, 0, index, Boundary::Link));
            boundaries.push((window.end_ns, 0, index, Boundary::Link));
        }
        for (index, action) in config.scheduled_actions.iter().enumerate() {
            boundaries.push((action.at_ns, 2, index, Boundary::Control(index)));
        }
        boundaries.sort_by_key(|b| (b.0, b.1, b.2));
        let mut cursor = 0;
        let mut paused = false;
        let mut closing = false;
        let mut broker_ids: Vec<_> = self.manifest.topics.iter().map(|t| t.id).collect();
        let mut owners = BTreeMap::new();
        loop {
            let now = self.handle.now().as_nanos();
            let relative = now - self.manifest.start_ns;
            while cursor < boundaries.len() && boundaries[cursor].0 <= relative {
                match boundaries[cursor].3 {
                    Boundary::Link => self.audit.borrow_mut().advance_links(now),
                    Boundary::Polling {
                        window,
                        paused: next,
                    } => {
                        paused = next;
                        self.audit.borrow_mut().record(
                            now,
                            DomainEvent::PollingChanged {
                                window: window as u32,
                                paused,
                            },
                        );
                    }
                    Boundary::Control(index) => {
                        let action = &config.scheduled_actions[index];
                        self.audit.borrow_mut().record(
                            now,
                            DomainEvent::ScheduledControl {
                                index: index as u32,
                                at_ns: action.at_ns,
                                action: action.action.clone(),
                            },
                        );
                        closing |= self.control(&action.action, &mut broker_ids)?;
                    }
                }
                cursor += 1;
            }
            for (load, state) in config.loads.iter().zip(&mut states) {
                if state.done {
                    continue;
                }
                if closing
                    || load.shape.end_ns().is_some_and(|end| relative >= end)
                        && (!matches!(load.shape, LoadShape::OpenLoop { .. })
                            || state.next == state.budget)
                {
                    self.refuse_pending(
                        load.template.first_id,
                        state,
                        if closing { "Closed" } else { "LoadEnded" },
                    );
                    state.done = true;
                } else if state.next == state.budget {
                    match load.shape {
                        LoadShape::ClosedLoopUntil { .. } => return Err(
                            "closed-loop offer budget exhausted before its required window ended"
                                .into(),
                        ),
                        LoadShape::ClosedLoop { .. } => state.done = true,
                        LoadShape::OpenLoop { .. } => {}
                    }
                }
            }
            if closing
                || (states.iter().all(|s| s.done)
                    && boundaries[cursor..]
                        .iter()
                        .all(|b| matches!(b.3, Boundary::Link)))
            {
                break;
            }
            if relative >= config.offer_deadline_ns {
                return Err("experiment offer deadline exceeded".into());
            }
            let mut work = 0;
            // Pick the oldest due offer, then manifest load index, on every turn.
            // The bounded scan stores only load state, never a payload queue.
            while work < 64 {
                let next = config
                    .loads
                    .iter()
                    .zip(&states)
                    .enumerate()
                    .filter_map(|(index, (load, state))| {
                        Self::next_offer(&load.shape, state).map(|due| (due, index))
                    })
                    .min();
                let Some((_, index)) = next.filter(|(due, _)| *due <= relative) else {
                    break;
                };
                let load = &config.loads[index];
                let state = &mut states[index];
                let record_id = load.template.first_id + u64::from(state.next);
                if state.candidate.is_none() {
                    let due_ns = self.manifest.start_ns
                        + if matches!(load.shape, LoadShape::OpenLoop { .. }) {
                            load.shape.due_ns(state.next)?
                        } else {
                            relative
                        };
                    state.candidate = Some(Candidate {
                        due_ns,
                        offered_ns: now,
                        attempts: 0,
                    });
                    let mut audit = self.audit.borrow_mut();
                    audit.coverage.offered += 1;
                    audit.record(
                        now,
                        DomainEvent::Offered {
                            load: index as u32,
                            record_id,
                            due_ns,
                        },
                    );
                }
                let topic_index = load.template.topic as usize;
                let metadata = self
                    .client
                    .metadata_topic(self.topics[topic_index])
                    .map_err(|e| e.to_string())?;
                let partitions = metadata
                    .as_ref()
                    .filter(|t| t.partitions != 0)
                    .map_or(self.manifest.topics[topic_index].leaders.len(), |t| {
                        t.partitions as usize
                    });
                let record = load.template.materialize(
                    state.next,
                    partitions,
                    self.manifest.producer.lanes,
                )?;
                let (result, lease) = submit(
                    self.client,
                    std::slice::from_ref(&record),
                    self.topics,
                    self.handle.now(),
                )?;
                let candidate = state.candidate.as_mut().expect("logical candidate");
                candidate.attempts = candidate
                    .attempts
                    .checked_add(1)
                    .ok_or("admission attempt overflow")?;
                self.audit.borrow_mut().record(
                    now,
                    DomainEvent::AdmissionAttempt {
                        record_id,
                        attempt: candidate.attempts,
                        error: result.error.map(|e| format!("{e:?}")),
                    },
                );
                if let Some(kr_kafka_producer::admission::AdmissionError::Credit(
                    kr_kafka_producer::credit::CreditError::PartitionPressure(pressure),
                )) = result.error
                {
                    // Simulation admission cannot interleave another metadata
                    // publication between this source snapshot and submit.
                    let partition = (pressure.partition >= 0).then_some(pressure.partition);
                    let topic = partition
                        .and_then(|_| metadata.as_ref().and_then(|t| t.id))
                        .map(|id| id.0);
                    self.audit.borrow_mut().record(
                        now,
                        DomainEvent::DescriptorPressure {
                            record_id,
                            topic,
                            partition,
                            capacity: pressure.capacity as u64,
                            shared_limit: pressure.shared_limit() as u64,
                            total_held: pressure.total_held as u64,
                            class_held: pressure.class_held as u64,
                        },
                    );
                }
                if let Some(token) = result.token(0) {
                    let resolved = self.client.topic_id(self.topics[topic_index]).ok();
                    self.audit.borrow_mut().accept(
                        now,
                        record_id,
                        token.0,
                        crate::history::AdmissionRoute {
                            topic: if self.manifest.produce_max_version < 13 {
                                [0; 16]
                            } else {
                                resolved.map_or(broker_ids[topic_index], |id| id.0)
                            },
                            partition: record.partition,
                            key_routed: record.key_routed,
                            handle: self.topics[topic_index].0,
                            resolved: self.manifest.produce_max_version >= 13 && resolved.is_some(),
                        },
                        lease.map(|l| l.0),
                    )?;
                    owners.insert(token.0, index);
                    state.outstanding += 1;
                    state.next += 1;
                    state.candidate = None;
                    state.retry_ns = relative;
                } else {
                    let error = result.error.ok_or("admission refused without a reason")?;
                    let pressure =
                        matches!(error, AdmissionError::Credit(_) | AdmissionError::BulkLimit);
                    if !pressure
                        && !matches!(
                            error,
                            AdmissionError::Closed
                                | AdmissionError::TopicClosed
                                | AdmissionError::PartitionFailed
                        )
                    {
                        return Err(format!("invalid generated admission: {error}"));
                    }
                    if pressure {
                        self.audit.borrow_mut().coverage.backpressure += 1;
                    }
                    if load.shape.outstanding().is_some() && pressure {
                        state.retry_ns = relative
                            .checked_add(self.manifest.driver.admission_retry_delay_ns)
                            .ok_or("admission retry deadline overflow")?;
                    } else {
                        self.refuse_pending(load.template.first_id, state, &format!("{error:?}"));
                    }
                }
                if let Some(lease) = lease {
                    self.client.release(lease).map_err(|e| e.to_string())?;
                }
                self.audit.borrow_mut().credits(now, self.credits)?;
                work += 1;
            }
            if let Some(error) = &self.audit.borrow().error {
                return Err(error.clone());
            }
            if work == 64 {
                kr_runtime::yield_now().await;
                continue;
            }
            // A completed finite source is retired at the next iteration. Do
            // not park on the hard deadline when the final offer was accepted.
            if config
                .loads
                .iter()
                .zip(&states)
                .any(|(l, s)| !s.done && s.next == s.budget && l.shape.outstanding().is_some())
            {
                continue;
            }
            let mut next_ns = config.offer_deadline_ns;
            if let Some(boundary) = boundaries.get(cursor) {
                next_ns = next_ns.min(boundary.0);
            }
            for (load, state) in config.loads.iter().zip(&states) {
                if state.done {
                    continue;
                }
                if let Some(end) = load.shape.end_ns() {
                    next_ns = next_ns.min(end);
                }
                if let Some(due) = Self::next_offer(&load.shape, state) {
                    next_ns = next_ns.min(due);
                }
            }
            let mut timer = Box::pin(
                self.handle
                    .sleep_until(RuntimeInstant::from_nanos(self.manifest.start_ns + next_ns)),
            );
            let event = poll_fn(|cx| {
                // Timer priority also covers an event already ready exactly at
                // a policy boundary. No client event is consumed while paused.
                if let Poll::Ready(result) = timer.as_mut().poll(cx) {
                    return Poll::Ready(result.map(|()| None).map_err(|e| e.to_string()));
                }
                if paused {
                    return Poll::Pending;
                }
                self.client.poll_event(cx).map(|result| {
                    result.map_err(|e| e.to_string()).and_then(|event| {
                        event
                            .map(Some)
                            .ok_or("experiment event stream ended".into())
                    })
                })
            })
            .await?;
            if let Some(event) = event {
                if let Event::Delivery(delivery) = &event {
                    let index = owners
                        .remove(&delivery.token.0)
                        .ok_or("delivery without generated owner")?;
                    states[index].outstanding = states[index]
                        .outstanding
                        .checked_sub(1)
                        .ok_or("closed-loop outstanding underflow")?;
                }
                let ended = matches!(event, Event::Closed { .. });
                self.audit
                    .borrow_mut()
                    .event(self.handle.now().as_nanos(), event)?;
                self.audit
                    .borrow_mut()
                    .credits(self.handle.now().as_nanos(), self.credits)?;
                if ended {
                    return Err("producer closed before experiment requested close".into());
                }
                // Capacity progress permits a new attempt, still bounded to
                // one attempt per consumed event or retry timer.
                for state in &mut states {
                    if state.candidate.is_some() {
                        state.retry_ns = self.handle.now().as_nanos() - self.manifest.start_ns;
                    }
                }
            }
        }
        let offered = self.audit.borrow().coverage.offered as u32;
        let planned = config.planned_offers()?;
        self.audit.borrow_mut().record(
            self.handle.now().as_nanos(),
            DomainEvent::OffersStopped {
                planned,
                offered,
                cancelled: planned - offered,
            },
        );
        if closing {
            return Ok(false);
        }
        self.settle(config).await?;
        self.client
            .close_at(
                self.handle.now(),
                RuntimeDuration::from_nanos(config.close_timeout_ns),
            )
            .map_err(|e| e.to_string())?;
        Ok(false)
    }

    fn next_offer(shape: &LoadShape, state: &LoadState) -> Option<u64> {
        if state.done
            || state.next == state.budget
            || shape.outstanding().is_some_and(|k| state.outstanding >= k)
        {
            return None;
        }
        Some(
            shape
                .due_ns(state.next)
                .expect("validated bounded offer deadline")
                .max(state.retry_ns),
        )
    }
    fn refuse_pending(&self, first_id: u64, state: &mut LoadState, error: &str) {
        if let Some(candidate) = state.candidate.take() {
            let mut audit = self.audit.borrow_mut();
            audit.coverage.refused += 1;
            audit.record(
                self.handle.now().as_nanos(),
                DomainEvent::Refused {
                    record_id: first_id + u64::from(state.next),
                    due_ns: candidate.due_ns,
                    offered_ns: candidate.offered_ns,
                    error: error.into(),
                },
            );
            state.next += 1;
        }
    }
    async fn settle(&self, config: &ExperimentWorkload) -> Result<(), String> {
        let watermark = self
            .audit
            .borrow()
            .accepted
            .last_key_value()
            .map_or(0, |(token, _)| *token);
        let mut closed = false;
        wait_barrier(
            self.client,
            self.audit,
            self.credits,
            self.handle,
            Barrier {
                count: 0,
                flush: None,
                watermark: Some(watermark),
                timeout_ns: config.settle_timeout_ns,
                require_acked: config.require_acked,
            },
            &mut closed,
        )
        .await
    }
    fn control(&mut self, action: &TimedControl, ids: &mut [[u8; 16]]) -> Result<bool, String> {
        let now = self.handle.now();
        match action {
            TimedControl::CreateTopic { topic } => {
                let spec = &self.manifest.topics[*topic as usize];
                self.model
                    .borrow_mut()
                    .create_topic_with_id(&spec.name, ids[*topic as usize], &spec.leaders)
                    .map_err(|e| e.to_string())?;
            }
            TimedControl::DeleteTopic { topic } => self
                .model
                .borrow_mut()
                .delete_topic(ids[*topic as usize])
                .map_err(|e| e.to_string())?,
            TimedControl::RecreateTopic { topic, new_id } => {
                let spec = &self.manifest.topics[*topic as usize];
                let mut model = self.model.borrow_mut();
                model
                    .delete_topic(ids[*topic as usize])
                    .map_err(|e| e.to_string())?;
                self.audit
                    .borrow_mut()
                    .recreated
                    .insert(ids[*topic as usize]);
                model
                    .create_topic_with_id(&spec.name, *new_id, &spec.leaders)
                    .map_err(|e| e.to_string())?;
                ids[*topic as usize] = *new_id;
            }
            TimedControl::AddPartitions {
                topic,
                additional_leaders,
            } => {
                self.model
                    .borrow_mut()
                    .add_partitions(ids[*topic as usize], additional_leaders)
                    .map_err(|e| e.to_string())?;
                self.audit.borrow_mut().coverage.expands += 1;
            }
            TimedControl::MoveLeader {
                topic,
                partition,
                broker,
            } => {
                self.model
                    .borrow_mut()
                    .move_leader(ids[*topic as usize], *partition, *broker)
                    .map_err(|e| e.to_string())?;
                self.audit.borrow_mut().coverage.leader_moves += 1;
            }
            TimedControl::CloseTopic { topic } => self
                .client
                .close_topic(self.topics[*topic as usize])
                .map_err(|e| e.to_string())?,
            TimedControl::OpenTopic { topic } => {
                // Resolution is asynchronous. Waiting here would stop unrelated
                // load and violate the scheduled-control contract.
                self.topics[*topic as usize] = self
                    .client
                    .open_topic_at(&self.manifest.topics[*topic as usize].name, now)
                    .map_err(|e| e.to_string())?;
            }
            TimedControl::Flush => {
                let token = self.client.flush_at(now).map_err(|e| e.to_string())?;
                self.audit.borrow_mut().flush(now.as_nanos(), token)?;
            }
            TimedControl::Close { deadline_ns } => {
                self.client
                    .close_at(now, RuntimeDuration::from_nanos(*deadline_ns))
                    .map_err(|e| e.to_string())?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}
