//! Compact, absolute-time workload descriptions. All times are relative to the
//! manifest origin; generated ID ranges include reserved, never-offered IDs.
use crate::{RecordSpec, RecordTemplate, ReplayManifest};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum LoadShape {
    OpenLoop {
        start_ns: u64,
        end_ns: u64,
        rate_per_s: u64,
    },
    ClosedLoop {
        start_ns: u64,
        count: u32,
        outstanding: u32,
    },
    ClosedLoopUntil {
        start_ns: u64,
        end_ns: u64,
        max_offers: u32,
        outstanding: u32,
    },
}
impl LoadShape {
    pub fn offer_budget(&self) -> Result<u32, String> {
        match *self {
            Self::OpenLoop {
                start_ns,
                end_ns,
                rate_per_s,
            } => {
                if start_ns >= end_ns || rate_per_s == 0 {
                    return Err("open-loop window/rate".into());
                }
                let count = (u128::from(end_ns - start_ns) * u128::from(rate_per_s))
                    .div_ceil(1_000_000_000);
                u32::try_from(count).map_err(|_| "open-loop offer count overflow".into())
            }
            Self::ClosedLoop { count, .. } => Ok(count),
            Self::ClosedLoopUntil { max_offers, .. } => Ok(max_offers),
        }
    }
    pub fn start_ns(&self) -> u64 {
        match *self {
            Self::OpenLoop { start_ns, .. }
            | Self::ClosedLoop { start_ns, .. }
            | Self::ClosedLoopUntil { start_ns, .. } => start_ns,
        }
    }
    pub fn end_ns(&self) -> Option<u64> {
        match *self {
            Self::OpenLoop { end_ns, .. } | Self::ClosedLoopUntil { end_ns, .. } => Some(end_ns),
            Self::ClosedLoop { .. } => None,
        }
    }
    pub(crate) fn outstanding(&self) -> Option<u32> {
        match *self {
            Self::OpenLoop { .. } => None,
            Self::ClosedLoop { outstanding, .. } | Self::ClosedLoopUntil { outstanding, .. } => {
                Some(outstanding)
            }
        }
    }
    pub fn due_ns(&self, index: u32) -> Result<u64, String> {
        let delta = match *self {
            Self::OpenLoop { rate_per_s: 0, .. } => return Err("zero offer rate".into()),
            Self::OpenLoop { rate_per_s, .. } => {
                u128::from(index) * 1_000_000_000 / u128::from(rate_per_s)
            }
            _ => 0,
        };
        u64::try_from(u128::from(self.start_ns()) + delta)
            .map_err(|_| "offer deadline overflow".into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LoadSpec {
    pub template: RecordTemplate,
    pub shape: LoadShape,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum TimedControl {
    CreateTopic {
        topic: u32,
    },
    DeleteTopic {
        topic: u32,
    },
    RecreateTopic {
        topic: u32,
        new_id: [u8; 16],
    },
    AddPartitions {
        topic: u32,
        additional_leaders: Vec<i32>,
    },
    MoveLeader {
        topic: u32,
        partition: i32,
        broker: i32,
    },
    CloseTopic {
        topic: u32,
    },
    OpenTopic {
        topic: u32,
    },
    Flush,
    Close {
        deadline_ns: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScheduledAction {
    pub at_ns: u64,
    pub action: TimedControl,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PollingPause {
    pub start_ns: u64,
    pub end_ns: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExperimentWorkload {
    pub loads: Vec<LoadSpec>,
    pub scheduled_actions: Vec<ScheduledAction>,
    pub polling_pauses: Vec<PollingPause>,
    /// Last permitted offer time, exclusive, including finite closed loops.
    pub offer_deadline_ns: u64,
    pub settle_timeout_ns: u64,
    pub close_timeout_ns: u64,
    pub require_acked: bool,
}
impl ExperimentWorkload {
    pub fn planned_offers(&self) -> Result<u32, String> {
        self.loads.iter().try_fold(0u32, |sum, load| {
            sum.checked_add(load.shape.offer_budget()?)
                .ok_or("aggregate offer overflow".into())
        })
    }
    pub(crate) fn validate(&self, manifest: &ReplayManifest) -> Result<(), String> {
        if !manifest.workload.is_empty()
            || self.loads.is_empty()
            || self.loads.len() > 64
            || self.scheduled_actions.len() + self.polling_pauses.len() * 2 > 65536
            || self.offer_deadline_ns == 0
            || self.settle_timeout_ns == 0
            || self.close_timeout_ns == 0
            || self
                .offer_deadline_ns
                .checked_add(self.settle_timeout_ns)
                .and_then(|n| n.checked_add(self.close_timeout_ns))
                .is_none_or(|end| end > manifest.limits.elapsed_ns)
            || self.planned_offers()? > manifest.limits.records
        {
            return Err("experiment workload bounds".into());
        }
        let mut ranges = Vec::new();
        for load in &self.loads {
            let count = load.shape.offer_budget()?;
            ranges.push(load.template.id_range(count)?);
            let topic = manifest
                .topics
                .get(load.template.topic as usize)
                .ok_or("template topic")?;
            load.template.validate(
                topic.leaders.len(),
                manifest.producer.lanes,
                manifest.limits.record_bytes,
            )?;
            if load.shape.start_ns() >= self.offer_deadline_ns
                || load
                    .shape
                    .end_ns()
                    .is_some_and(|end| end <= load.shape.start_ns() || end > self.offer_deadline_ns)
                || load
                    .shape
                    .outstanding()
                    .is_some_and(|n| n == 0 || n > manifest.limits.records)
            {
                return Err("experiment load window/outstanding bounds".into());
            }
        }
        ranges.sort_unstable();
        if ranges.windows(2).any(|r| r[0].1 >= r[1].0) {
            return Err("overlapping generated ID ranges".into());
        }
        let mut pauses: Vec<_> = self
            .polling_pauses
            .iter()
            .map(|p| (p.start_ns, p.end_ns))
            .collect();
        pauses.sort_unstable();
        if pauses
            .iter()
            .any(|&(start, end)| start >= end || end > self.offer_deadline_ns)
            || pauses.windows(2).any(|p| p[0].1 > p[1].0)
        {
            return Err("polling window bounds/overlap".into());
        }
        let brokers: BTreeSet<_> = manifest.brokers.iter().map(|b| b.id).collect();
        let mut ids: BTreeSet<_> = manifest.topics.iter().map(|t| t.id).collect();
        let mut partitions: Vec<_> = manifest.topics.iter().map(|t| t.leaders.len()).collect();
        let mut actions: Vec<_> = self.scheduled_actions.iter().enumerate().collect();
        actions.sort_by_key(|(index, a)| (a.at_ns, *index));
        let mut closed = false;
        for (_, action) in actions {
            if closed || action.at_ns > self.offer_deadline_ns {
                return Err("scheduled control lifecycle bounds".into());
            }
            let topic = match action.action {
                TimedControl::CreateTopic { topic }
                | TimedControl::DeleteTopic { topic }
                | TimedControl::RecreateTopic { topic, .. }
                | TimedControl::AddPartitions { topic, .. }
                | TimedControl::MoveLeader { topic, .. }
                | TimedControl::CloseTopic { topic }
                | TimedControl::OpenTopic { topic } => Some(topic as usize),
                _ => None,
            };
            if topic.is_some_and(|t| t >= partitions.len()) {
                return Err("scheduled control topic".into());
            }
            match &action.action {
                TimedControl::RecreateTopic { topic, new_id } => {
                    if *new_id == [0; 16] || !ids.insert(*new_id) {
                        return Err("scheduled recreation identity".into());
                    }
                    partitions[*topic as usize] = manifest.topics[*topic as usize].leaders.len();
                }
                TimedControl::AddPartitions {
                    topic,
                    additional_leaders,
                } => {
                    let count = &mut partitions[*topic as usize];
                    if additional_leaders.is_empty()
                        || *count + additional_leaders.len() > 16
                        || additional_leaders.iter().any(|b| !brokers.contains(b))
                    {
                        return Err("scheduled growth bounds".into());
                    }
                    *count += additional_leaders.len();
                }
                TimedControl::MoveLeader {
                    topic,
                    partition,
                    broker,
                } => {
                    if *partition < 0
                        || *partition as usize >= partitions[*topic as usize]
                        || !brokers.contains(broker)
                    {
                        return Err("scheduled leader bounds".into());
                    }
                }
                TimedControl::Close { deadline_ns } => {
                    if *deadline_ns == 0
                        || action
                            .at_ns
                            .checked_add(*deadline_ns)
                            .is_none_or(|end| end > manifest.limits.elapsed_ns)
                    {
                        return Err("scheduled close deadline".into());
                    }
                    closed = true;
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// Payload identity is independent of current routing. Resolve a single
    /// record without retaining generated payloads for the whole workload.
    pub(crate) fn record(&self, id: u64, manifest: &ReplayManifest) -> Option<RecordSpec> {
        self.loads.iter().find_map(|load| {
            let index = u32::try_from(id.checked_sub(load.template.first_id)?).ok()?;
            (index < load.shape.offer_budget().ok()?)
                .then(|| {
                    load.template
                        .materialize(
                            index,
                            manifest.topics[load.template.topic as usize].leaders.len(),
                            manifest.producer.lanes,
                        )
                        .ok()
                })
                .flatten()
        })
    }
}
