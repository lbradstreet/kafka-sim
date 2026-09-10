//! The ordinary pinned corpus and the larger standalone gate share this driver.
use crate::{faults::*, *};
use kr_kafka_producer::config::Compression;
use kr_runtime::{
    RuntimeDuration,
    rng::{DeterministicRng, RandomStream},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, str::FromStr};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CampaignVariant {
    Legacy,
    Clean,
    FiniteFault,
    Isolation,
}
impl CampaignVariant {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Clean => "clean",
            Self::FiniteFault => "finite-fault",
            Self::Isolation => "isolation",
        }
    }
}
impl FromStr for CampaignVariant {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, String> {
        match value {
            "legacy" => Ok(Self::Legacy),
            "clean" => Ok(Self::Clean),
            "finite-fault" => Ok(Self::FiniteFault),
            "isolation" => Ok(Self::Isolation),
            _ => Err(format!("unknown campaign variant {value}")),
        }
    }
}
/// Fixed regression cases; range expansion never silently replaces these.
pub const PINNED_CASES: &[(u64, CampaignVariant)] = &[
    (0, CampaignVariant::Clean),
    (1, CampaignVariant::FiniteFault),
    (2, CampaignVariant::Isolation),
    (3, CampaignVariant::FiniteFault),
    (4, CampaignVariant::Isolation),
    (5, CampaignVariant::Clean),
    (6, CampaignVariant::Isolation),
    (7, CampaignVariant::FiniteFault),
    (8, CampaignVariant::Clean),
    (9, CampaignVariant::FiniteFault),
    (36, CampaignVariant::Isolation),
    (128, CampaignVariant::Clean),
];
pub fn campaign_cases(start: u64, count: u64) -> Result<Vec<(u64, CampaignVariant)>, String> {
    if count > 4096 {
        return Err("campaign seed count exceeds 4096".into());
    }
    let end = start
        .checked_add(count)
        .ok_or("campaign seed range overflow")?;
    let mut cases: BTreeSet<_> = PINNED_CASES.iter().copied().collect();
    // Retain the named original regressions, including the old test-only seed128.
    for seed in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 36, 128] {
        cases.insert((seed, CampaignVariant::Legacy));
    }
    for seed in start..end {
        for variant in [
            CampaignVariant::Legacy,
            CampaignVariant::Clean,
            CampaignVariant::FiniteFault,
            CampaignVariant::Isolation,
        ] {
            cases.insert((seed, variant));
        }
    }
    Ok(cases.into_iter().collect())
}

pub fn campaign_manifest(seed: u64, variant: CampaignVariant) -> Result<ReplayManifest, String> {
    if variant == CampaignVariant::Legacy {
        return ReplayManifest::from_seed(seed, CampaignLimits::default());
    }
    let mut m = ReplayManifest::from_seed(
        seed,
        CampaignLimits {
            records: 192,
            steps: 2_000_000,
            elapsed_ns: 60_000_000_000,
            history_events: 262144,
            ..CampaignLimits::default()
        },
    )?;
    m.fault_plan.clear();
    m.require_all_acked = true;
    m.fetch_probe = true;
    m.recovery_round_timeout_ns = Some(8_000_000_000);
    m.require_fault_coverage = variant != CampaignVariant::Clean;
    m.producer.max_attempts = 100;
    m.producer.delivery_timeout = ns(60_000_000_000);
    m.producer.topic_resolve_timeout = ns(60_000_000_000);
    m.producer.request_timeout = ns(200_000_000);
    m.producer.retry_backoff_min = ns(10_000_000);
    m.producer.retry_backoff_max = ns(100_000_000);
    m.producer.metadata_max_age = ns(20_000_000);
    m.producer.linger_max =
        ns([0, 1_000_000, 5_000_000, 10_000_000][dimension(seed, 0x4c494e474552, 4)]);
    m.producer.lanes = 1 + dimension(seed, 0x4c414e4553, 4) as u8;
    m.producer.max_in_flight_per_connection = 1 + dimension(seed, 0x57494e444f57, 5) as u8;
    m.producer.batch_target_bytes = [256, 512, 1024, 2048][dimension(seed, 0x4241544348, 4)];
    m.producer.compression = if dimension(seed, 0x434f444543, 2) == 0 {
        Compression::None
    } else {
        Compression::Zstd { level: 1 }
    };
    m.producer.brokers_max = 3;
    m.producer.record_descriptors = 128;
    m.producer.pending_records_per_topic = 128;
    m.producer.delivery_event_capacity = 256;
    m.producer.release_event_capacity = 64;
    m.producer.max_live_leases = 64;
    m.producer.request_max_partitions = 8;
    m.network.connections = 64;
    m.runtime.tasks = 256;
    m.model.log_batches = 1024;
    m.driver.encode_bytes = 512;
    m.driver.encode_cost_ns = 100;
    m.driver.link_latency_ns = 0;
    m.driver.jitter_ns = 100;
    m.driver.chunk_bytes = [64, 257, 4096][dimension(seed, 0x4348554e4b, 3)];
    m.driver.pipe_bytes = 65536;
    m.driver.service_delay_ns = 0;
    m.faults.max_decisions = 262144;
    let brokers = 1 + dimension(seed, 0x42524f4b4552, 3);
    m.brokers = (1..=brokers)
        .map(|index| BrokerSpec {
            id: index as i32,
            host: "model".into(),
            port: 9091 + index as u16,
        })
        .collect();
    let partitions = 1 + dimension(seed, 0x504152544954, 8);
    let first = m.topics[0].clone();
    m.topics = (0..2)
        .map(|index| {
            let mut id = first.id;
            id[0] ^= index as u8 + 1;
            TopicSpec {
                id,
                name: format!("events-{index}"),
                leaders: (0..partitions)
                    .map(|partition| 1 + ((partition + index) % brokers) as i32)
                    .collect(),
            }
        })
        .collect();
    m.faults.services = (1..=brokers)
        .map(|broker| BrokerService {
            broker: broker as i32,
            delay_ns: if broker == 1 { 2_000_000 } else { 200_000 },
        })
        .collect();
    if variant != CampaignVariant::Clean {
        let mut fault_order =
            DeterministicRng::from_root_seed(seed ^ 0x4641554c544f5244, RandomStream::Scenario);
        for round in 1..=6 {
            // A lost first response, request drop and rejection keep at least
            // one request unresolved until the fifth Produce visit, even for a
            // small topology whose entire round fits in one initial request.
            let mut middle = [
                Effects {
                    outcome: Outcome::Drop,
                    ..Effects::default()
                },
                Effects {
                    reject_error: Some(19),
                    ..Effects::default()
                },
            ];
            shuffle(&mut middle, &mut fault_order)?;
            let effects = [
                (
                    Phase::AfterAppend,
                    0,
                    Effects {
                        outcome: Outcome::Drop,
                        ..Effects::default()
                    },
                ),
                (Phase::BeforeAppend, 1, middle[0]),
                (Phase::BeforeAppend, 2, middle[1]),
                // The post-append loss omitted its response hook. Allow one
                // later response through before cutting the following reply.
                (
                    Phase::BeforeResponse,
                    1,
                    Effects {
                        outcome: Outcome::Disconnect,
                        ..Effects::default()
                    },
                ),
                (
                    Phase::BeforeAppend,
                    4,
                    Effects {
                        throttle_ms: 2,
                        ..Effects::default()
                    },
                ),
            ];
            for (phase, skip, effects) in effects {
                m.faults.scripts.push(ScriptRule {
                    matcher: Match {
                        phase,
                        api: Some(0),
                        round: Some(round),
                        ..Default::default()
                    },
                    skip,
                    take: 1,
                    effects,
                });
            }
        }
        // Explicitly fault Metadata as well as Produce; periodic refresh guarantees visits.
        m.faults.scripts.push(ScriptRule {
            matcher: Match {
                phase: Phase::BeforeAppend,
                api: Some(3),
                round: Some(5),
                ..Default::default()
            },
            skip: 0,
            take: 1,
            effects: Effects {
                delay_ns: 5_000_000,
                ..Default::default()
            },
        });
        for (phase, outcome) in [
            (Phase::BeforeAppend, Outcome::Drop),
            (Phase::BeforeResponse, Outcome::Disconnect),
            (Phase::BeforeResponse, Outcome::Continue),
        ] {
            m.faults.random.push(RandomRule {
                matcher: Match {
                    phase,
                    round: Some(6),
                    ..Default::default()
                },
                probability_ppm: 125_000,
                max_delay_ns: 2_000_000,
                outcome,
            });
        }
    }
    if variant == CampaignVariant::Isolation {
        // Warmup finishes first; the post-append lost response remains outstanding.
        m.faults.isolations.push(IsolationWindow {
            broker: 1,
            start_ns: 100_000_000,
            end_ns: 200_000_000,
        });
    }
    let mut records: Vec<_> = m
        .workload
        .iter()
        .filter_map(|op| {
            if let Workload::Submit { records } = op {
                Some(records.clone())
            } else {
                None
            }
        })
        .flatten()
        .collect();
    for record in &mut records {
        record.topic = (record.id % 2) as u32;
        record.key = match record.id % 4 {
            0 => None,
            1 => Some(Vec::new()),
            _ => Some(record.id.to_be_bytes().into()),
        };
        record.value = match record.id % 11 {
            0 => None,
            1 if record.id != 1 => Some(Vec::new()),
            _ => record.value.take(),
        };
        record.timestamp_ms = 10_000 + (record.id % 17) as i64 * 13;
        record.headers.extend([
            HeaderSpec {
                key: "ordinary".into(),
                value: Some(Vec::new()),
            },
            HeaderSpec {
                key: "ordinary".into(),
                value: None,
            },
            HeaderSpec {
                key: "ordinary".into(),
                value: Some(record.id.to_be_bytes().to_vec()),
            },
        ]);
        record.key_routed =
            record.key.as_ref().is_some_and(|key| !key.is_empty()) && record.id.is_multiple_of(3);
        record.partition = if record.key_routed {
            (murmur2(record.key.as_deref().unwrap()) & 0x7fff_ffff) as usize % partitions
        } else {
            record.id as usize % partitions
        } as i32;
        record.lane = record.partition as u8 % m.producer.lanes;
    }
    let warm = 2 * partitions;
    for (index, record) in records[..warm].iter_mut().enumerate() {
        record.topic = (index / partitions) as u32;
        record.partition = (index % partitions) as i32;
        record.key_routed = false;
        record.lane = record.partition as u8 % m.producer.lanes;
    }
    m.workload = vec![
        Workload::BeginRound { round: 0 },
        Workload::Submit {
            records: records[..warm].to_vec(),
        },
        Workload::Flush,
        Workload::AwaitFlush {
            timeout_ns: 2_000_000_000,
            require_acked: true,
        },
    ];
    let mut offset = warm;
    // Separate salted stream: adding payload/profile dimensions does not reshuffle commands.
    let mut commands =
        DeterministicRng::from_root_seed(seed ^ 0x434f4d4d414e4453, RandomStream::Scenario);
    let mut leader = m.topics[0].leaders[0];
    for round in 1..=6 {
        let end = if round == 6 {
            records.len()
        } else {
            warm + (records.len() - warm) * round as usize / 6
        };
        let batch = &records[offset..end];
        let first_cut = batch.len() / 3;
        let second_cut = 2 * batch.len() / 3;
        m.workload.push(Workload::BeginRound { round });
        if round == 5 && variant != CampaignVariant::Clean {
            // Let periodic Metadata reach the explicit control-plane delay rule.
            m.workload.push(Workload::Sleep { nanos: 25_000_000 });
        }
        let mut round_commands = vec![
            Workload::Submit {
                records: batch[..first_cut].to_vec(),
            },
            Workload::Submit {
                records: batch[first_cut..second_cut].to_vec(),
            },
            Workload::Submit {
                records: batch[second_cut..].to_vec(),
            },
            Workload::Flush,
        ];
        if variant != CampaignVariant::Clean && brokers > 1 && round != 1 {
            leader = 1 + leader % brokers as i32;
            round_commands.push(Workload::MoveLeader {
                topic: 0,
                partition: 0,
                broker: leader,
            });
        }
        shuffle(&mut round_commands, &mut commands)?;
        let flush = round_commands
            .iter()
            .position(|command| matches!(command, Workload::Flush))
            .unwrap();
        let last_burst = round_commands
            .iter()
            .rposition(|command| matches!(command, Workload::Submit { .. }))
            .unwrap();
        if flush > last_burst {
            // Every round tests a prefix flush with at least one later admission.
            let command = round_commands.remove(flush);
            round_commands.insert(last_burst, command);
        }
        m.workload.extend(round_commands);
        if round == 1 && variant == CampaignVariant::Isolation {
            // Keep accepted work and live connections across the scheduled outage.
            m.workload.push(Workload::Sleep { nanos: 200_000_000 });
        }
        m.workload.push(Workload::AwaitFlush {
            timeout_ns: 8_000_000_000,
            require_acked: true,
        });
        // A flush excludes later admissions; settle the complete round independently.
        m.workload.push(Workload::Flush);
        m.workload.push(Workload::AwaitFlush {
            timeout_ns: 8_000_000_000,
            require_acked: true,
        });
        m.workload.push(Workload::Settle {
            count: end as u32,
            timeout_ns: 8_000_000_000,
            require_acked: true,
        });
        offset = end;
    }
    m.workload.push(Workload::Close {
        deadline_ns: 2_000_000_000,
    });
    validate_finite_profile(&m)?;
    m.validate()?;
    Ok(m)
}
fn shuffle<T>(values: &mut [T], rng: &mut DeterministicRng) -> Result<(), String> {
    for index in (1..values.len()).rev() {
        let other = rng
            .u64_below(index as u64 + 1)
            .map_err(|error| error.to_string())?;
        values.swap(index, other as usize);
    }
    Ok(())
}

// Independent salts keep profile dimensions from moving in lockstep.
fn dimension(seed: u64, salt: u64, choices: usize) -> usize {
    (DeterministicRng::from_root_seed(seed ^ salt, RandomStream::Scenario).next_u64()
        % choices as u64) as usize
}
fn ns(value: u64) -> RuntimeDuration {
    RuntimeDuration::from_nanos(value)
}

/// Conservative envelope uses one-byte positive progress, never max_chunk_bytes
/// as a minimum. Waiting on unavailable capacity is covered by the finite FIFO.
pub(crate) fn validate_finite_profile(m: &ReplayManifest) -> Result<(), String> {
    // These used to be inherited from general manifest validation. Experiment
    // limits must never silently widen the finite correctness gate.
    if m.faults.crash_on_isolation
        || m.metrics_sampling.is_some()
        || !m.faults.environment.is_empty()
        || m.observe_requests
        || crate::experiment_link::enabled(&m.faults)
        || m.experiment.is_some()
        || m.workload.iter().any(|op| {
            matches!(
                op,
                Workload::SettleAllAccepted { .. } | Workload::SleepUntil { .. }
            )
        })
    {
        return Err("finite fault profile excludes experiment workloads".into());
    }
    if m.limits.records > 256
        || m.limits.elapsed_ns > 60_000_000_000
        || m.limits.history_events > 1_000_000
        || m.limits.steps > 10_000_000
        || m.brokers.len() > 3
        || m.workload.len() > 1024
        || m.faults.max_decisions > 262_144
        || m.faults.services.len() > 64
        || m.faults.isolations.len() > 64
        || m.model.log_batches > 4096
        || m.model.log_records > 65536
        || m.model.log_bytes > 256 * 1024 * 1024
    {
        return Err("finite fault profile resource envelope exceeded".into());
    }

    if m.recovery_round_timeout_ns != Some(8_000_000_000)
        || !m.require_all_acked
        || m.producer.retry_backoff_min.as_nanos() < 10_000_000
        || m.producer.retry_backoff_max.as_nanos() > 100_000_000
        || m.producer.delivery_timeout.as_nanos() < 60_000_000_000
        || m.producer.topic_resolve_timeout.as_nanos() < 60_000_000_000
        || m.faults.budget_per_round != 8
        || m.faults.max_random_effects_per_round > 3
        || m.faults
            .services
            .iter()
            .any(|service| service.delay_ns > 2_000_000)
        || m.faults
            .scripts
            .iter()
            .any(|rule| rule.effects.delay_ns > 5_000_000 || rule.effects.throttle_ms > 2)
        || m.faults
            .random
            .iter()
            .any(|rule| rule.max_delay_ns > 2_000_000)
        || m.faults.isolations.len() > 1
        || m.faults
            .isolations
            .iter()
            .any(|window| window.end_ns - window.start_ns > 100_000_000)
        || m.driver.service_delay_ns > 2_000_000
        || m.driver.encode_cost_ns > 100
        || m.driver.encode_bytes < 512
        || m.producer.linger_max.as_nanos() > 10_000_000
        || m.driver.link_latency_ns > 100
        || m.driver.jitter_ns > 100
    {
        return Err("finite fault profile parameters exceed validated envelope".into());
    }
    let attempts = u64::from(m.faults.budget_per_round)
        + 1
        + 100_000_000 / m.producer.retry_backoff_min.as_nanos()
        + 5;
    let service = m
        .faults
        .services
        .iter()
        .map(|service| service.delay_ns)
        .max()
        .unwrap_or(0)
        .max(m.driver.service_delay_ns);
    let io = u64::from(m.producer.request_hard_bytes)
        * 4
        * (m.driver.link_latency_ns + m.driver.jitter_ns);
    let healthy = u64::from(m.producer.max_in_flight_per_connection) * (service + io) + 10_000_000;
    let recovery = u64::from(m.faults.budget_per_round)
        * (m.producer.request_timeout.as_nanos()
            + 2 * m.producer.retry_backoff_max.as_nanos()
            + healthy)
        + 100_000_000
        + healthy;
    if attempts >= u64::from(m.producer.max_attempts)
        || healthy >= m.producer.request_timeout.as_nanos()
        || recovery >= 8_000_000_000
    {
        return Err("finite fault profile exceeds attempt/recovery envelope".into());
    }
    Ok(())
}

// Independent Java-compatible key-routing reference; never imports producer routing.
pub(crate) fn murmur2(bytes: &[u8]) -> u32 {
    let mut hash = 0x9747_b28c ^ bytes.len() as u32;
    let mut chunks = bytes.chunks_exact(4);
    for chunk in &mut chunks {
        let mut word = u32::from_le_bytes(chunk.try_into().unwrap());
        word = word.wrapping_mul(0x5bd1_e995);
        word ^= word >> 24;
        word = word.wrapping_mul(0x5bd1_e995);
        hash = hash.wrapping_mul(0x5bd1_e995) ^ word;
    }
    let tail = chunks.remainder();
    for (index, byte) in tail.iter().enumerate() {
        hash ^= u32::from(*byte) << (8 * index);
    }
    if !tail.is_empty() {
        hash = hash.wrapping_mul(0x5bd1_e995);
    }
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0x5bd1_e995);
    hash ^ (hash >> 15)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_widened_resource_limit_is_still_rejected_by_the_finite_profile() {
        type Mutation = (&'static str, fn(&mut ReplayManifest));
        let mutations: &[Mutation] = &[
            ("records", |m| m.limits.records = 257),
            ("elapsed", |m| m.limits.elapsed_ns = 60_000_000_001),
            ("history", |m| m.limits.history_events = 1_000_001),
            ("steps", |m| m.limits.steps = 10_000_001),
            ("decisions", |m| m.faults.max_decisions = 262_145),
            ("brokers", |m| {
                while m.brokers.len() < 4 {
                    let mut broker = m.brokers[0].clone();
                    broker.id = m.brokers.len() as i32 + 1;
                    broker.port += broker.id as u16;
                    m.brokers.push(broker);
                }
            }),
            ("workload", |m| {
                m.workload.resize(1025, Workload::Sleep { nanos: 0 })
            }),
            ("services", |m| {
                m.faults.services.resize(
                    65,
                    BrokerService {
                        broker: 1,
                        delay_ns: 0,
                    },
                )
            }),
            ("isolations", |m| {
                m.faults.isolations.resize(
                    65,
                    IsolationWindow {
                        broker: 1,
                        start_ns: 0,
                        end_ns: 1,
                    },
                )
            }),
            ("log batches", |m| m.model.log_batches = 4097),
            ("log records", |m| m.model.log_records = 65537),
            ("log bytes", |m| m.model.log_bytes = 256 * 1024 * 1024 + 1),
        ];
        let base = campaign_manifest(1, CampaignVariant::FiniteFault).unwrap();
        for (name, mutate) in mutations {
            let mut m = base.clone();
            mutate(&mut m);
            assert_eq!(
                validate_finite_profile(&m).unwrap_err(),
                "finite fault profile resource envelope exceeded",
                "{name}"
            );
            assert!(m.validate().is_err(), "{name}");
        }
    }

    #[test]
    fn experiment_bounds_do_not_widen_seed_generation() {
        let mut m = ReplayManifest::from_seed(0, CampaignLimits::default()).unwrap();
        m.limits.records = 1_000_000;
        m.limits.elapsed_ns = 300_000_000_000;
        m.limits.history_events = 8_000_000;
        m.limits.steps = 200_000_000;
        m.model.log_batches = 1_000_000;
        m.model.log_records = 1_000_000;
        m.model.log_bytes = 2 * 1024 * 1024 * 1024;
        m.faults.max_decisions = 2_000_000;
        m.validate().unwrap();
        assert!(ReplayManifest::from_seed(0, m.limits).is_err());
        m.limits.records += 1;
        assert!(m.validate().is_err());
    }

    #[test]
    fn corpus_is_bounded_deduplicated_and_retains_regressions() {
        let cases = campaign_cases(0, 128).unwrap();
        assert!(cases.len() <= 128 * 4 + 25);
        for case in PINNED_CASES {
            assert!(cases.contains(case));
        }
        assert!(cases.contains(&(128, CampaignVariant::Legacy)));
        assert!(campaign_cases(u64::MAX, 1).is_err());
        assert!(campaign_cases(0, 4097).is_err());
        assert_eq!(murmur2(b""), 275646681);
    }
    #[test]
    fn every_generated_profile_has_a_recovery_margin_and_nullable_payloads() {
        for seed in 0..128 {
            let m = campaign_manifest(seed, CampaignVariant::FiniteFault).unwrap();
            validate_finite_profile(&m).unwrap();
            for round in 1..=6 {
                let produce: Vec<_> = m
                    .faults
                    .scripts
                    .iter()
                    .filter(|rule| rule.matcher.round == Some(round) && rule.matcher.api == Some(0))
                    .collect();
                assert_eq!(produce.len(), 5, "five required effects in every round");
                assert!(produce.iter().all(|rule| rule.take == 1));
                assert!(
                    produce
                        .iter()
                        .any(|rule| rule.matcher.phase == Phase::AfterAppend
                            && rule.effects.outcome == Outcome::Drop)
                );
                assert!(
                    produce
                        .iter()
                        .any(|rule| rule.matcher.phase == Phase::BeforeAppend
                            && rule.effects.outcome == Outcome::Drop)
                );
                assert!(
                    produce
                        .iter()
                        .any(|rule| rule.effects.reject_error.is_some())
                );
                assert!(
                    produce
                        .iter()
                        .any(|rule| rule.effects.outcome == Outcome::Disconnect)
                );
                assert!(produce.iter().any(|rule| rule.effects.throttle_ms != 0));
            }
            assert!(m.workload.iter().any(|op| matches!(op,Workload::Submit { records } if records.iter().any(|record| record.value.is_none()))));
            for record in m
                .workload
                .iter()
                .filter_map(|op| {
                    if let Workload::Submit { records } = op {
                        Some(records)
                    } else {
                        None
                    }
                })
                .flatten()
            {
                let ordinary: Vec<_> = record
                    .headers
                    .iter()
                    .filter(|header| header.key == "ordinary")
                    .collect();
                assert_eq!(ordinary.len(), 3);
                assert_eq!(ordinary[0].value, Some(Vec::new()));
                assert_eq!(ordinary[1].value, None);
                assert_eq!(
                    ordinary[2].value.as_deref(),
                    Some(record.id.to_be_bytes().as_slice())
                );
            }
            let mut bad = m;
            bad.producer.max_attempts = 8;
            assert!(validate_finite_profile(&bad).is_err());
        }
    }
    #[test]
    fn finite_profile_rejects_unbounded_fallback_service_and_encoding_delay() {
        let good = campaign_manifest(0, CampaignVariant::Clean).unwrap();
        let mut bad = good.clone();
        bad.faults.budget_per_round = 7;
        assert!(validate_finite_profile(&bad).is_err());
        let mut bad = good.clone();
        bad.faults.services.clear();
        bad.driver.service_delay_ns = 60_000_000_000;
        assert!(validate_finite_profile(&bad).is_err());
        let mut bad = good.clone();
        bad.driver.encode_cost_ns = 60_000_000_000;
        assert!(validate_finite_profile(&bad).is_err());
        let mut bad = good.clone();
        bad.driver.encode_bytes = 1;
        assert!(validate_finite_profile(&bad).is_err());
        let mut bad = good;
        bad.producer.linger_max = ns(60_000_000_000);
        assert!(validate_finite_profile(&bad).is_err());
    }
}
