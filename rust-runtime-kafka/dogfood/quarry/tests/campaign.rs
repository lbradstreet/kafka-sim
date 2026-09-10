use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fs::{self, File};
use std::future::{Future, poll_fn};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;

#[allow(dead_code)]
mod support;

use kr_runtime::rng::RandomStream;
use kr_runtime::test_support::campaign_seed_range;
use kr_runtime::trace::sbe::{SbeRecordingTrace, SbeTraceRetention};
use kr_runtime::{
    DeterminismCheckpoint, Handle, RandomHandle, RuntimeConfig, RuntimeSnapshot, SimDuration,
    SimInstant, SimRuntime,
};
use kr_runtime_trace_tool::{
    TraceArtifactMetadata, validate_sbe_trace_artifact, write_buffered_sbe_trace_artifact,
};
use quarry::{
    AckOutcome, CompletedSnapshot, JobId, JobSnapshot, JobStatus, LeaseToken, LeasedJob,
    NackOutcome, QueueClient, QueueConfig, QueueError, QueueSnapshot, RenewOutcome, RequestId,
    SubmitOutcome, SubmitRequest, WorkerId, start_broker,
};
use support::{below, bounded_ddmin};

const CAMPAIGN_VERSION: u32 = 2;
const CAMPAIGN_SEEDS: u64 = 64;
const CAMPAIGN_STEPS: usize = 64;
const TRACE_PREFIX_CAPACITY_BYTES: usize = 64 * 1_024;
const TRACE_TAIL_CAPACITY_BYTES: usize = 64 * 1_024;
const TRACE_ARTIFACT_DRIVER: &str = "quarry-campaign/1";
const SHRINK_MAX_ATTEMPTS: usize = 64;
const CONCURRENT_ABANDON_SCENARIO_VERSION: u32 = 1;
/// Named, non-regression seeds that keep important broker profiles in every
/// campaign invocation, including sharded or offset runs.
const BROKER_COVERAGE_SEEDS: &[(&str, u64)] = &[
    ("balanced-origin", 0),
    ("midrange-replay", 17),
    ("upper-baseline", 63),
];

static TRACE_ARTIFACT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const QUEUE_CONFIG: QueueConfig = QueueConfig {
    active_capacity: 4,
    max_payload_bytes: 4,
    max_claim_batch: 3,
    completed_history_capacity: 2,
};

#[derive(Clone, Debug)]
enum Op {
    Submit {
        request_slot: u8,
        mode: SubmitMode,
        payload_tag: u8,
        delay_ns: u64,
    },
    Claim {
        worker_slot: u8,
        max_jobs: usize,
        lease_ns: u64,
    },
    Renew {
        job: JobPick,
        token: TokenPick,
        lease_ns: u64,
    },
    Ack {
        job: JobPick,
        token: TokenPick,
    },
    Nack {
        job: JobPick,
        token: TokenPick,
        retry_ns: u64,
    },
    Advance {
        nanos: u64,
    },
    Inspect,
}

#[derive(Clone, Copy, Debug)]
enum SubmitMode {
    NewBody,
    RepeatExact,
    Conflict,
    Oversized,
}

#[derive(Clone, Copy, Debug)]
enum JobPick {
    Leased(u8),
    Active(u8),
    Completed(u8),
    Any(u8),
}

#[derive(Clone, Copy, Debug)]
enum TokenPick {
    Current,
    Previous,
    Foreign,
    Fabricated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SwarmProfile {
    Balanced,
    Submission,
    Leasing,
    Completion,
}

impl SwarmProfile {
    fn choose(random: &RandomHandle) -> Self {
        match below(random, 4) {
            0 => Self::Balanced,
            1 => Self::Submission,
            2 => Self::Leasing,
            _ => Self::Completion,
        }
    }

    fn operation_kind(self, random: &RandomHandle) -> u64 {
        match self {
            Self::Balanced => below(random, 100),
            Self::Submission => below(random, 25),
            Self::Leasing => [25, 43, 75][below(random, 3) as usize],
            Self::Completion => [53, 65, 93][below(random, 3) as usize],
        }
    }
}

#[derive(Clone, Debug)]
struct ModelJob {
    request: SubmitRequest,
    available_at: SimInstant,
    lease: Option<ModelLease>,
}

#[derive(Clone, Copy, Debug)]
struct ModelLease {
    worker_id: WorkerId,
    token: LeaseToken,
    deadline: SimInstant,
}

#[derive(Clone, Debug)]
struct ModelCompleted {
    job_id: JobId,
    request: SubmitRequest,
    ack_token: LeaseToken,
}

#[derive(Debug)]
struct Model {
    config: QueueConfig,
    now: SimInstant,
    active: BTreeMap<JobId, ModelJob>,
    completed: VecDeque<ModelCompleted>,
    seen_job_ids: BTreeSet<JobId>,
    seen_tokens: BTreeSet<LeaseToken>,
    token_history: BTreeMap<JobId, Vec<LeaseToken>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Coverage {
    submitted: u64,
    duplicate_active: u64,
    duplicate_completed: u64,
    request_conflict: u64,
    payload_too_large: u64,
    capacity_reached: u64,
    claimed_jobs: u64,
    leases_expired: u64,
    renewed: u64,
    nacked: u64,
    completed: u64,
    already_completed: u64,
    stale_token: u64,
    completed_evictions: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct CampaignRun {
    coverage: Coverage,
    checkpoint: DeterminismCheckpoint,
    trace_fingerprint: Option<u64>,
}

#[derive(Debug, Eq, PartialEq)]
struct CampaignFailure {
    comparable_report: String,
    checkpoint: Box<DeterminismCheckpoint>,
    diagnostic_report: String,
}

impl std::fmt::Display for CampaignFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.diagnostic_report)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConcurrentAbandonCoverage {
    max_concurrently_admitted: usize,
    abandoned_responses: usize,
    abandoned_effects_observed: usize,
    surviving_responses: usize,
}

#[derive(Debug, Eq, PartialEq)]
struct ConcurrentAbandonArtifact {
    scenario_version: u32,
    seed: u64,
    coverage: ConcurrentAbandonCoverage,
    surviving_outcome: SubmitOutcome,
    snapshot: QueueSnapshot,
    checkpoint: DeterminismCheckpoint,
}

impl Coverage {
    fn merge(&mut self, other: Self) {
        self.submitted += other.submitted;
        self.duplicate_active += other.duplicate_active;
        self.duplicate_completed += other.duplicate_completed;
        self.request_conflict += other.request_conflict;
        self.payload_too_large += other.payload_too_large;
        self.capacity_reached += other.capacity_reached;
        self.claimed_jobs += other.claimed_jobs;
        self.leases_expired += other.leases_expired;
        self.renewed += other.renewed;
        self.nacked += other.nacked;
        self.completed += other.completed;
        self.already_completed += other.already_completed;
        self.stale_token += other.stale_token;
        self.completed_evictions += other.completed_evictions;
    }

    fn assert_meaningful(self) {
        assert!(
            self.submitted > 0,
            "campaign never submitted a job: {self:#?}"
        );
        assert!(
            self.duplicate_active > 0,
            "campaign missed active deduplication: {self:#?}"
        );
        assert!(
            self.duplicate_completed > 0,
            "campaign missed completed deduplication: {self:#?}"
        );
        assert!(
            self.request_conflict > 0,
            "campaign missed request conflicts: {self:#?}"
        );
        assert!(
            self.payload_too_large > 0,
            "campaign missed payload bounds: {self:#?}"
        );
        assert!(
            self.capacity_reached > 0,
            "campaign missed active capacity: {self:#?}"
        );
        assert!(
            self.claimed_jobs > 0,
            "campaign never claimed a job: {self:#?}"
        );
        assert!(
            self.leases_expired > 0,
            "campaign missed lease expiry: {self:#?}"
        );
        assert!(self.renewed > 0, "campaign missed lease renewal: {self:#?}");
        assert!(self.nacked > 0, "campaign missed negative ack: {self:#?}");
        assert!(
            self.completed > 0,
            "campaign missed acknowledgement: {self:#?}"
        );
        assert!(
            self.already_completed > 0,
            "campaign missed idempotent acknowledgement: {self:#?}"
        );
        assert!(
            self.stale_token > 0,
            "campaign missed lease fencing: {self:#?}"
        );
        assert!(
            self.completed_evictions > 0,
            "campaign missed completed-history eviction: {self:#?}"
        );
    }

    fn assert_seed_baseline(self, seed: u64) {
        assert!(
            self.submitted > 0,
            "seed={seed} missed the guaranteed submit baseline: {self:#?}"
        );
        assert!(
            self.duplicate_active > 0,
            "seed={seed} missed the guaranteed active-deduplication baseline: {self:#?}"
        );
        assert!(
            self.request_conflict > 0,
            "seed={seed} missed the guaranteed request-conflict baseline: {self:#?}"
        );
        assert!(
            self.payload_too_large > 0,
            "seed={seed} missed the guaranteed payload-bound baseline: {self:#?}"
        );
        assert!(
            self.claimed_jobs > 0,
            "seed={seed} missed the guaranteed claim baseline: {self:#?}"
        );
        assert!(
            self.leases_expired > 0,
            "seed={seed} missed the guaranteed lease-expiry baseline: {self:#?}"
        );
    }
}

impl Model {
    fn new(config: QueueConfig) -> Self {
        Self {
            config,
            now: SimInstant::ZERO,
            active: BTreeMap::new(),
            completed: VecDeque::new(),
            seen_job_ids: BTreeSet::new(),
            seen_tokens: BTreeSet::new(),
            token_history: BTreeMap::new(),
        }
    }

    fn observe_time(&mut self, now: SimInstant, coverage: &mut Coverage) -> Result<(), String> {
        if now < self.now {
            return Err(format!(
                "virtual time moved backwards: previous={}, next={now}",
                self.now
            ));
        }
        self.now = now;
        for job in self.active.values_mut() {
            if job.lease.is_some_and(|lease| lease.deadline <= now) {
                job.lease = None;
                coverage.leases_expired += 1;
            }
        }
        Ok(())
    }

    fn stored_request(&self, request_id: RequestId) -> Option<&SubmitRequest> {
        self.active
            .values()
            .find(|job| job.request.request_id == request_id)
            .map(|job| &job.request)
            .or_else(|| {
                self.completed
                    .iter()
                    .find(|job| job.request.request_id == request_id)
                    .map(|job| &job.request)
            })
    }

    fn make_request(
        &self,
        request_slot: u8,
        mode: SubmitMode,
        payload_tag: u8,
        delay_ns: u64,
    ) -> SubmitRequest {
        let request_id = RequestId::new(u64::from(request_slot));
        let new_request = || SubmitRequest {
            request_id,
            payload: vec![request_slot, payload_tag],
            not_before: self
                .now
                .checked_add(SimDuration::from_nanos(delay_ns))
                .expect("bounded campaign time cannot overflow"),
        };
        match mode {
            SubmitMode::NewBody => new_request(),
            SubmitMode::RepeatExact => self
                .stored_request(request_id)
                .cloned()
                .unwrap_or_else(new_request),
            SubmitMode::Conflict => {
                let mut request = self
                    .stored_request(request_id)
                    .cloned()
                    .unwrap_or_else(new_request);
                if request.payload.is_empty() {
                    request.payload.push(1);
                } else {
                    request.payload[0] ^= 0xff;
                }
                request
            }
            SubmitMode::Oversized => SubmitRequest {
                request_id,
                payload: vec![payload_tag; self.config.max_payload_bytes + 1],
                not_before: self.now,
            },
        }
    }

    fn check_submit(
        &mut self,
        request: SubmitRequest,
        actual: Result<SubmitOutcome, QueueError>,
        coverage: &mut Coverage,
    ) -> Result<(), String> {
        if request.payload.len() > self.config.max_payload_bytes {
            let expected = Err(QueueError::PayloadTooLarge {
                size: request.payload.len(),
                limit: self.config.max_payload_bytes,
            });
            if actual != expected {
                return Err(format!(
                    "submit mismatch: expected={expected:?}, actual={actual:?}"
                ));
            }
            coverage.payload_too_large += 1;
            return Ok(());
        }

        if let Some((job_id, existing, completed)) = self.request_record(request.request_id) {
            let expected = if existing == &request {
                if completed {
                    Ok(SubmitOutcome::DuplicateCompleted { job_id })
                } else {
                    Ok(SubmitOutcome::DuplicateActive { job_id })
                }
            } else {
                Err(QueueError::RequestConflict {
                    request_id: request.request_id,
                })
            };
            if actual != expected {
                return Err(format!(
                    "submit mismatch: expected={expected:?}, actual={actual:?}"
                ));
            }
            match expected {
                Ok(SubmitOutcome::DuplicateActive { .. }) => coverage.duplicate_active += 1,
                Ok(SubmitOutcome::DuplicateCompleted { .. }) => {
                    coverage.duplicate_completed += 1;
                }
                Err(QueueError::RequestConflict { .. }) => coverage.request_conflict += 1,
                _ => {}
            }
            return Ok(());
        }

        if self.active.len() >= self.config.active_capacity {
            let expected = Err(QueueError::ActiveCapacityReached {
                limit: self.config.active_capacity,
            });
            if actual != expected {
                return Err(format!(
                    "submit mismatch: expected={expected:?}, actual={actual:?}"
                ));
            }
            coverage.capacity_reached += 1;
            return Ok(());
        }

        let Ok(SubmitOutcome::Submitted { job_id }) = actual else {
            return Err(format!(
                "new submit should return Submitted, actual={actual:?}"
            ));
        };
        if !self.seen_job_ids.insert(job_id) {
            return Err(format!("job identifier was reused: {job_id}"));
        }
        self.active.insert(
            job_id,
            ModelJob {
                available_at: request.not_before,
                request,
                lease: None,
            },
        );
        coverage.submitted += 1;
        Ok(())
    }

    fn request_record(&self, request_id: RequestId) -> Option<(JobId, &SubmitRequest, bool)> {
        self.active
            .iter()
            .find(|(_, job)| job.request.request_id == request_id)
            .map(|(job_id, job)| (*job_id, &job.request, false))
            .or_else(|| {
                self.completed
                    .iter()
                    .find(|job| job.request.request_id == request_id)
                    .map(|job| (job.job_id, &job.request, true))
            })
    }

    fn check_claim(
        &mut self,
        worker_id: WorkerId,
        max_jobs: usize,
        lease_for: SimDuration,
        actual: Result<Vec<LeasedJob>, QueueError>,
        coverage: &mut Coverage,
    ) -> Result<(), String> {
        if max_jobs > self.config.max_claim_batch {
            let expected = Err(QueueError::ClaimBatchTooLarge {
                requested: max_jobs,
                limit: self.config.max_claim_batch,
            });
            return equal_result("claim", actual, expected);
        }
        if lease_for == SimDuration::ZERO {
            return equal_result("claim", actual, Err(QueueError::ZeroLeaseDuration));
        }
        let Some(deadline) = self.now.checked_add(lease_for) else {
            return equal_result("claim", actual, Err(QueueError::DeadlineOverflow));
        };
        let eligible: BTreeSet<JobId> = self
            .active
            .iter()
            .filter_map(|(job_id, job)| {
                (job.lease.is_none() && job.available_at <= self.now).then_some(*job_id)
            })
            .collect();
        let leases = actual.map_err(|error| {
            format!("claim unexpectedly failed: error={error:?}, eligible={eligible:?}")
        })?;
        let expected_count = max_jobs.min(eligible.len());
        if leases.len() != expected_count {
            return Err(format!(
                "claim count mismatch: expected={expected_count}, actual={}, eligible={eligible:?}",
                leases.len()
            ));
        }

        let mut claimed = BTreeSet::new();
        for lease in &leases {
            if !claimed.insert(lease.job_id) {
                return Err(format!("claim returned job twice: {}", lease.job_id));
            }
            if !eligible.contains(&lease.job_id) {
                return Err(format!(
                    "claim returned ineligible job: {}, eligible={eligible:?}",
                    lease.job_id
                ));
            }
            let job = self.active.get(&lease.job_id).expect("eligible job exists");
            if lease.request_id != job.request.request_id
                || lease.payload != job.request.payload
                || lease.worker_id != worker_id
                || lease.deadline != deadline
            {
                return Err(format!(
                    "claimed job metadata mismatch: expected_job={job:?}, actual={lease:?}, expected_worker={worker_id}, expected_deadline={deadline}"
                ));
            }
            if self.seen_tokens.contains(&lease.lease_token) {
                return Err(format!("lease token was reused: {}", lease.lease_token));
            }
        }

        for lease in leases {
            self.seen_tokens.insert(lease.lease_token);
            self.token_history
                .entry(lease.job_id)
                .or_default()
                .push(lease.lease_token);
            self.active
                .get_mut(&lease.job_id)
                .expect("claimed job exists")
                .lease = Some(ModelLease {
                worker_id,
                token: lease.lease_token,
                deadline,
            });
            coverage.claimed_jobs += 1;
        }
        Ok(())
    }

    fn check_renew(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        lease_for: SimDuration,
        actual: Result<RenewOutcome, QueueError>,
        coverage: &mut Coverage,
    ) -> Result<(), String> {
        let expected = if lease_for == SimDuration::ZERO {
            Err(QueueError::ZeroLeaseDuration)
        } else if let Some(deadline) = self.now.checked_add(lease_for) {
            match self.active.get(&job_id).and_then(|job| job.lease) {
                None if self.active.contains_key(&job_id) => {
                    Err(QueueError::JobNotLeased { job_id })
                }
                None => Err(QueueError::JobNotFound { job_id }),
                Some(lease) if lease.token != token => Err(QueueError::StaleLeaseToken {
                    job_id,
                    provided: token,
                }),
                Some(_) => Ok(RenewOutcome::Renewed { deadline }),
            }
        } else {
            Err(QueueError::DeadlineOverflow)
        };
        if actual != expected {
            return Err(format!(
                "renew mismatch: expected={expected:?}, actual={actual:?}"
            ));
        }
        if let Ok(RenewOutcome::Renewed { deadline }) = expected {
            self.active
                .get_mut(&job_id)
                .expect("renewed job exists")
                .lease
                .as_mut()
                .expect("renewed lease exists")
                .deadline = deadline;
            coverage.renewed += 1;
        } else if matches!(expected, Err(QueueError::StaleLeaseToken { .. })) {
            coverage.stale_token += 1;
        }
        Ok(())
    }

    fn check_ack(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        actual: Result<AckOutcome, QueueError>,
        coverage: &mut Coverage,
    ) -> Result<(), String> {
        let expected = if let Some(completed) = self
            .completed
            .iter()
            .find(|completed| completed.job_id == job_id)
        {
            if completed.ack_token == token {
                Ok(AckOutcome::AlreadyCompleted)
            } else {
                Err(QueueError::StaleLeaseToken {
                    job_id,
                    provided: token,
                })
            }
        } else {
            match self.active.get(&job_id).and_then(|job| job.lease) {
                None if self.active.contains_key(&job_id) => {
                    Err(QueueError::JobNotLeased { job_id })
                }
                None => Err(QueueError::JobNotFound { job_id }),
                Some(lease) if lease.token != token => Err(QueueError::StaleLeaseToken {
                    job_id,
                    provided: token,
                }),
                Some(_) => Ok(AckOutcome::Completed),
            }
        };
        if actual != expected {
            return Err(format!(
                "ack mismatch: expected={expected:?}, actual={actual:?}"
            ));
        }
        match expected {
            Ok(AckOutcome::Completed) => {
                let job = self.active.remove(&job_id).expect("completed job exists");
                self.completed.push_back(ModelCompleted {
                    job_id,
                    request: job.request,
                    ack_token: token,
                });
                coverage.completed += 1;
                while self.completed.len() > self.config.completed_history_capacity {
                    self.completed.pop_front();
                    coverage.completed_evictions += 1;
                }
            }
            Ok(AckOutcome::AlreadyCompleted) => coverage.already_completed += 1,
            Err(QueueError::StaleLeaseToken { .. }) => coverage.stale_token += 1,
            Err(_) => {}
        }
        Ok(())
    }

    fn check_nack(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        retry_after: SimDuration,
        actual: Result<NackOutcome, QueueError>,
        coverage: &mut Coverage,
    ) -> Result<(), String> {
        let expected = if let Some(available_at) = self.now.checked_add(retry_after) {
            match self.active.get(&job_id).and_then(|job| job.lease) {
                None if self.active.contains_key(&job_id) => {
                    Err(QueueError::JobNotLeased { job_id })
                }
                None => Err(QueueError::JobNotFound { job_id }),
                Some(lease) if lease.token != token => Err(QueueError::StaleLeaseToken {
                    job_id,
                    provided: token,
                }),
                Some(_) => Ok(NackOutcome::Requeued { available_at }),
            }
        } else {
            Err(QueueError::DeadlineOverflow)
        };
        if actual != expected {
            return Err(format!(
                "nack mismatch: expected={expected:?}, actual={actual:?}"
            ));
        }
        if let Ok(NackOutcome::Requeued { available_at }) = expected {
            let job = self.active.get_mut(&job_id).expect("nacked job exists");
            job.lease = None;
            job.available_at = available_at;
            coverage.nacked += 1;
        } else if matches!(expected, Err(QueueError::StaleLeaseToken { .. })) {
            coverage.stale_token += 1;
        }
        Ok(())
    }

    fn resolve_job(&self, pick: JobPick) -> JobId {
        let (mut candidates, ordinal): (Vec<JobId>, u8) = match pick {
            JobPick::Leased(ordinal) => (
                self.active
                    .iter()
                    .filter_map(|(id, job)| job.lease.is_some().then_some(*id))
                    .collect(),
                ordinal,
            ),
            JobPick::Active(ordinal) => (self.active.keys().copied().collect(), ordinal),
            JobPick::Completed(ordinal) => (
                self.completed.iter().map(|job| job.job_id).collect(),
                ordinal,
            ),
            JobPick::Any(ordinal) => (
                self.active
                    .keys()
                    .copied()
                    .chain(self.completed.iter().map(|job| job.job_id))
                    .collect(),
                ordinal,
            ),
        };
        if candidates.is_empty() {
            candidates = self
                .active
                .keys()
                .copied()
                .chain(self.completed.iter().map(|job| job.job_id))
                .collect();
        }
        candidates.sort_unstable();
        candidates.dedup();
        if candidates.is_empty() {
            JobId::new(u64::MAX - u64::from(ordinal))
        } else {
            candidates[usize::from(ordinal) % candidates.len()]
        }
    }

    fn resolve_token(&self, job_id: JobId, pick: TokenPick) -> LeaseToken {
        let current = self
            .active
            .get(&job_id)
            .and_then(|job| job.lease.map(|lease| lease.token))
            .or_else(|| {
                self.completed
                    .iter()
                    .find(|job| job.job_id == job_id)
                    .map(|job| job.ack_token)
            });
        match pick {
            TokenPick::Current => current
                .or_else(|| {
                    self.token_history
                        .get(&job_id)
                        .and_then(|tokens| tokens.last().copied())
                })
                .unwrap_or_else(|| self.fabricated_token(job_id)),
            TokenPick::Previous => self
                .token_history
                .get(&job_id)
                .and_then(|tokens| {
                    tokens
                        .iter()
                        .rev()
                        .copied()
                        .find(|token| Some(*token) != current)
                })
                .unwrap_or_else(|| self.fabricated_token(job_id)),
            TokenPick::Foreign => self
                .active
                .iter()
                .filter(|(other, _)| **other != job_id)
                .find_map(|(_, job)| job.lease.map(|lease| lease.token))
                .or_else(|| {
                    self.seen_tokens
                        .iter()
                        .copied()
                        .find(|token| Some(*token) != current)
                })
                .unwrap_or_else(|| self.fabricated_token(job_id)),
            TokenPick::Fabricated => self.fabricated_token(job_id),
        }
    }

    fn fabricated_token(&self, job_id: JobId) -> LeaseToken {
        let mut raw = u64::MAX.wrapping_sub(job_id.get());
        loop {
            let token = LeaseToken::new(raw);
            if !self.seen_tokens.contains(&token) {
                return token;
            }
            raw = raw.wrapping_sub(1);
        }
    }

    fn snapshot(&self) -> QueueSnapshot {
        let jobs = self
            .active
            .iter()
            .map(|(job_id, job)| JobSnapshot {
                request_id: job.request.request_id,
                job_id: *job_id,
                payload: job.request.payload.clone(),
                status: match job.lease {
                    Some(lease) => JobStatus::Leased {
                        worker_id: lease.worker_id,
                        lease_token: lease.token,
                        deadline: lease.deadline,
                    },
                    None if job.available_at > self.now => JobStatus::Delayed {
                        available_at: job.available_at,
                    },
                    None => JobStatus::Ready,
                },
            })
            .collect();
        let completed = self
            .completed
            .iter()
            .map(|job| CompletedSnapshot {
                request_id: job.request.request_id,
                job_id: job.job_id,
                payload: job.request.payload.clone(),
                not_before: job.request.not_before,
                ack_token: job.ack_token,
            })
            .collect();
        QueueSnapshot {
            now: self.now,
            active_capacity: self.config.active_capacity,
            jobs,
            completed,
        }
    }
}

fn equal_result<T: std::fmt::Debug + PartialEq>(
    operation: &str,
    actual: Result<T, QueueError>,
    expected: Result<T, QueueError>,
) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{operation} mismatch: expected={expected:?}, actual={actual:?}"
        ))
    }
}

async fn execute_op(
    model: &mut Model,
    coverage: &mut Coverage,
    client: &QueueClient,
    handle: &Handle,
    op: &Op,
) -> Result<(), String> {
    model.observe_time(handle.now(), coverage)?;
    match *op {
        Op::Submit {
            request_slot,
            mode,
            payload_tag,
            delay_ns,
        } => {
            let request = model.make_request(request_slot, mode, payload_tag, delay_ns);
            let actual = client.submit(request.clone()).await;
            model.check_submit(request, actual, coverage)?;
        }
        Op::Claim {
            worker_slot,
            max_jobs,
            lease_ns,
        } => {
            let worker_id = WorkerId::new(u64::from(worker_slot));
            let lease_for = SimDuration::from_nanos(lease_ns);
            let actual = client.claim(worker_id, max_jobs, lease_for).await;
            model.check_claim(worker_id, max_jobs, lease_for, actual, coverage)?;
        }
        Op::Renew {
            job,
            token,
            lease_ns,
        } => {
            let job_id = model.resolve_job(job);
            let token = model.resolve_token(job_id, token);
            let lease_for = SimDuration::from_nanos(lease_ns);
            let actual = client.renew_for(job_id, token, lease_for).await;
            model.check_renew(job_id, token, lease_for, actual, coverage)?;
        }
        Op::Ack { job, token } => {
            let job_id = model.resolve_job(job);
            let token = model.resolve_token(job_id, token);
            let actual = client.ack(job_id, token).await;
            model.check_ack(job_id, token, actual, coverage)?;
        }
        Op::Nack {
            job,
            token,
            retry_ns,
        } => {
            let job_id = model.resolve_job(job);
            let token = model.resolve_token(job_id, token);
            let retry_after = SimDuration::from_nanos(retry_ns);
            let actual = client.nack(job_id, token, retry_after).await;
            model.check_nack(job_id, token, retry_after, actual, coverage)?;
        }
        Op::Advance { nanos } => {
            handle
                .sleep(SimDuration::from_nanos(nanos))
                .await
                .map_err(|error| format!("campaign sleep failed: {error}"))?;
            model.observe_time(handle.now(), coverage)?;
        }
        Op::Inspect => {}
    }

    let actual = client
        .snapshot()
        .await
        .map_err(|error| format!("snapshot failed: {error}"))?;
    check_snapshot(model.snapshot(), actual)
        .map_err(|mismatch| format!("{mismatch} after {op:?}\nmodel={model:#?}"))
}

fn normalize_snapshot(mut snapshot: QueueSnapshot) -> QueueSnapshot {
    snapshot.jobs.sort_by_key(|job| job.job_id);
    snapshot.completed.sort_by_key(|job| job.job_id);
    snapshot
}

fn check_snapshot(expected: QueueSnapshot, actual: QueueSnapshot) -> Result<(), String> {
    let expected = normalize_snapshot(expected);
    let actual = normalize_snapshot(actual);
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "snapshot mismatch\nexpected={expected:#?}\nactual={actual:#?}"
        ))
    }
}

fn generate_ops(rng: &RandomHandle, profile: SwarmProfile, steps: usize) -> Vec<Op> {
    let mut ops = vec![
        Op::Submit {
            request_slot: 0,
            mode: SubmitMode::NewBody,
            payload_tag: 1,
            delay_ns: 0,
        },
        Op::Submit {
            request_slot: 0,
            mode: SubmitMode::RepeatExact,
            payload_tag: 0,
            delay_ns: 0,
        },
        Op::Submit {
            request_slot: 0,
            mode: SubmitMode::Conflict,
            payload_tag: 0,
            delay_ns: 0,
        },
        Op::Claim {
            worker_slot: 0,
            max_jobs: 1,
            lease_ns: 2,
        },
        Op::Renew {
            job: JobPick::Leased(0),
            token: TokenPick::Current,
            lease_ns: 4,
        },
        Op::Nack {
            job: JobPick::Leased(0),
            token: TokenPick::Current,
            retry_ns: 2,
        },
        Op::Advance { nanos: 2 },
        Op::Claim {
            worker_slot: 1,
            max_jobs: 1,
            lease_ns: 2,
        },
        Op::Advance { nanos: 2 },
        Op::Claim {
            worker_slot: 2,
            max_jobs: 1,
            lease_ns: 3,
        },
        Op::Ack {
            job: JobPick::Leased(0),
            token: TokenPick::Previous,
        },
        Op::Ack {
            job: JobPick::Leased(0),
            token: TokenPick::Current,
        },
        Op::Ack {
            job: JobPick::Completed(0),
            token: TokenPick::Current,
        },
        Op::Submit {
            request_slot: 0,
            mode: SubmitMode::RepeatExact,
            payload_tag: 0,
            delay_ns: 0,
        },
        Op::Submit {
            request_slot: 1,
            mode: SubmitMode::Oversized,
            payload_tag: 7,
            delay_ns: 0,
        },
        Op::Claim {
            worker_slot: 0,
            max_jobs: QUEUE_CONFIG.max_claim_batch + 1,
            lease_ns: 1,
        },
        Op::Claim {
            worker_slot: 0,
            max_jobs: 0,
            lease_ns: 0,
        },
        Op::Submit {
            request_slot: 2,
            mode: SubmitMode::NewBody,
            payload_tag: 2,
            delay_ns: 0,
        },
        Op::Submit {
            request_slot: 3,
            mode: SubmitMode::NewBody,
            payload_tag: 3,
            delay_ns: 0,
        },
        Op::Claim {
            worker_slot: 1,
            max_jobs: 2,
            lease_ns: 10,
        },
        Op::Ack {
            job: JobPick::Leased(0),
            token: TokenPick::Current,
        },
        Op::Ack {
            job: JobPick::Leased(0),
            token: TokenPick::Current,
        },
        Op::Submit {
            request_slot: 4,
            mode: SubmitMode::NewBody,
            payload_tag: 4,
            delay_ns: 0,
        },
        Op::Claim {
            worker_slot: 2,
            max_jobs: 1,
            lease_ns: 10,
        },
        Op::Ack {
            job: JobPick::Leased(0),
            token: TokenPick::Current,
        },
    ];
    while ops.len() < steps {
        let kind = profile.operation_kind(rng);
        let op = match kind {
            0..=24 => Op::Submit {
                request_slot: below(rng, 10) as u8,
                mode: match below(rng, 100) {
                    0..=44 => SubmitMode::NewBody,
                    45..=69 => SubmitMode::RepeatExact,
                    70..=89 => SubmitMode::Conflict,
                    _ => SubmitMode::Oversized,
                },
                payload_tag: below(rng, 16) as u8,
                delay_ns: below(rng, 9),
            },
            25..=42 => Op::Claim {
                worker_slot: below(rng, 4) as u8,
                max_jobs: below(rng, 5) as usize,
                lease_ns: below(rng, 9),
            },
            43..=52 => Op::Renew {
                job: random_job_pick(rng),
                token: random_token_pick(rng),
                lease_ns: below(rng, 9),
            },
            53..=64 => Op::Ack {
                job: random_job_pick(rng),
                token: random_token_pick(rng),
            },
            65..=74 => Op::Nack {
                job: random_job_pick(rng),
                token: random_token_pick(rng),
                retry_ns: below(rng, 9),
            },
            75..=92 => Op::Advance {
                nanos: below(rng, 7),
            },
            _ => Op::Inspect,
        };
        ops.push(op);
    }
    ops.truncate(steps);
    ops
}

fn random_job_pick(rng: &RandomHandle) -> JobPick {
    let ordinal = below(rng, 16) as u8;
    match below(rng, 100) {
        0..=49 => JobPick::Leased(ordinal),
        50..=69 => JobPick::Active(ordinal),
        70..=89 => JobPick::Completed(ordinal),
        _ => JobPick::Any(ordinal),
    }
}

fn random_token_pick(rng: &RandomHandle) -> TokenPick {
    match below(rng, 100) {
        0..=54 => TokenPick::Current,
        55..=74 => TokenPick::Previous,
        75..=89 => TokenPick::Foreign,
        _ => TokenPick::Fabricated,
    }
}

fn run_seed(seed: u64) -> Result<CampaignRun, CampaignFailure> {
    run_seed_case(seed, false, None, true, false)
}

fn run_seed_traced(seed: u64) -> Result<CampaignRun, CampaignFailure> {
    run_seed_case(seed, true, None, true, true)
}

fn run_seed_case(
    seed: u64,
    traced: bool,
    operations: Option<&[Op]>,
    shrink_failures: bool,
    emit_failure_artifact: bool,
) -> Result<CampaignRun, CampaignFailure> {
    // Every campaign run starts at a seed-derived virtual epoch so an
    // absolute-time assumption in the broker or the model fails a seed
    // instead of hiding behind time zero. The virtual-time budget stays a
    // fixed span above that start.
    let start_time = RuntimeConfig::derived_start_time(seed);
    let max_time = start_time
        .checked_add(SimDuration::from_nanos(10_000))
        .expect("derived start times leave centuries of instant headroom");
    let config = RuntimeConfig {
        seed,
        max_tasks: 256,
        max_timers: 256,
        max_steps_per_run: 100_000,
        max_time: Some(max_time),
        start_time,
    };
    let trace = traced.then(|| {
        Rc::new(SbeRecordingTrace::with_retention(
            SbeTraceRetention::PrefixAndTail {
                prefix_capacity_bytes: TRACE_PREFIX_CAPACITY_BYTES,
                tail_capacity_bytes: TRACE_TAIL_CAPACITY_BYTES,
            },
        ))
    });
    let mut runtime = if let Some(trace) = &trace {
        SimRuntime::with_trace(config, trace.clone())
    } else {
        SimRuntime::new(config)
    };
    let scenario = runtime.random_source(RandomStream::Scenario);
    let profile = SwarmProfile::choose(&scenario);
    drop(scenario);
    let workload = runtime.random_source(RandomStream::Workload);
    let generated_ops = generate_ops(&workload, profile, CAMPAIGN_STEPS);
    let ops = operations.map_or(generated_ops, <[Op]>::to_vec);
    let schedule = runtime.random_source(RandomStream::Schedule);
    let handle = runtime.handle();
    let (client, broker) = match start_broker(handle.clone(), QUEUE_CONFIG, 64) {
        Ok(started) => started,
        Err(error) => {
            let failure_snapshot = runtime.snapshot();
            return Err(campaign_failure(
                seed,
                format!("seed={seed}: broker start failed: {error}"),
                &failure_snapshot,
                trace.as_deref(),
                emit_failure_artifact,
            ));
        }
    };
    let executed_ops = ops.clone();
    let driven = runtime.block_on(async move {
        let mut model = Model::new(QUEUE_CONFIG);
        let mut coverage = Coverage::default();
        let mut failure = None;
        for (step, op) in executed_ops.iter().enumerate() {
            let arrival_jitter = schedule
                .random_below(4)
                .expect("campaign schedule bound is nonzero");
            if arrival_jitter != 0 {
                handle
                    .sleep(SimDuration::from_nanos(arrival_jitter))
                    .await
                    .expect("bounded campaign schedule cannot overflow");
            }
            if let Err(message) = execute_op(&mut model, &mut coverage, &client, &handle, op).await
            {
                failure = Some((step, message, format!("{model:#?}")));
                break;
            }
        }
        let shutdown = client.shutdown().await;
        let joined = broker.await;
        (coverage, failure, shutdown, joined)
    });
    let diagnostic_snapshot = runtime.snapshot();
    let teardown = runtime.shutdown();
    let terminal_snapshot = runtime.snapshot();

    let (coverage, failure, shutdown, joined) = match driven {
        Ok(driven) => driven,
        Err(error) => {
            return Err(campaign_failure(
                seed,
                format!(
                    "seed={seed}; campaign_version={CAMPAIGN_VERSION}; runtime failed: {error:#?}; ops={ops:#?}"
                ),
                &terminal_snapshot,
                trace.as_deref(),
                emit_failure_artifact,
            ));
        }
    };
    if shutdown != Ok(()) {
        return Err(campaign_failure(
            seed,
            format!(
                "seed={seed}; broker shutdown failed: {shutdown:?}; snapshot={diagnostic_snapshot:#?}"
            ),
            &terminal_snapshot,
            trace.as_deref(),
            emit_failure_artifact,
        ));
    }
    if joined != Ok(()) {
        return Err(campaign_failure(
            seed,
            format!(
                "seed={seed}; broker join failed: {joined:?}; snapshot={diagnostic_snapshot:#?}"
            ),
            &terminal_snapshot,
            trace.as_deref(),
            emit_failure_artifact,
        ));
    }
    if let Err(error) = teardown {
        return Err(campaign_failure(
            seed,
            format!(
                "seed={seed}; runtime teardown failed: {error:#?}; snapshot={diagnostic_snapshot:#?}"
            ),
            &terminal_snapshot,
            trace.as_deref(),
            emit_failure_artifact,
        ));
    }
    if let Some((step, message, model)) = failure {
        let prefix_end = (step + 1).min(ops.len());
        let operation_prefix = ops[..prefix_end].to_vec();
        let shrink_report = if shrink_failures {
            // Broker operations resolve missing jobs/tokens to deterministic
            // negative requests, so every subsequence remains executable.
            let shrunk = bounded_ddmin(
                &operation_prefix,
                SHRINK_MAX_ATTEMPTS,
                |_| true,
                |candidate| run_seed_case(seed, traced, Some(candidate), false, false).is_err(),
            );
            format!(
                "\nminimized_operations={:#?}\nshrink_attempts={}; shrink_attempt_limit_reached={}",
                shrunk.minimized, shrunk.attempts, shrunk.attempt_limit_reached
            )
        } else {
            String::new()
        };
        return Err(campaign_failure(
            seed,
            format!(
                "campaign_version={CAMPAIGN_VERSION}; seed={seed}; step={step}; op={:?}; {message}\ncoverage_at_failure={coverage:#?}\nmodel={model}\noperation_prefix={operation_prefix:#?}{shrink_report}\nruntime_snapshot={diagnostic_snapshot:#?}\nreproduce: QUARRY_SEED={seed} cargo test -p quarry --test campaign reproduce_seed_from_environment -- --ignored --exact --nocapture",
                ops[step],
            ),
            &terminal_snapshot,
            trace.as_deref(),
            emit_failure_artifact,
        ));
    }
    Ok(CampaignRun {
        coverage,
        checkpoint: terminal_snapshot.determinism_checkpoint(),
        trace_fingerprint: trace.as_ref().map(|trace| trace.fingerprint()),
    })
}

fn run_concurrent_abandon(seed: u64) -> ConcurrentAbandonArtifact {
    let config = RuntimeConfig {
        seed,
        max_tasks: 16,
        max_timers: 16,
        max_steps_per_run: 1_024,
        max_time: Some(SimInstant::from_nanos(100)),
        start_time: SimInstant::ZERO,
    };
    let mut runtime = SimRuntime::new(config);
    let (client, broker) =
        start_broker(runtime.handle(), QUEUE_CONFIG, 2).expect("start focused broker scenario");
    let request = SubmitRequest {
        request_id: RequestId::new(41),
        payload: b"job".to_vec(),
        not_before: SimInstant::ZERO,
    };

    let (coverage, surviving_outcome, snapshot) = runtime
        .block_on(async move {
            let abandoned_client = client.clone();
            let surviving_client = client.clone();
            let mut abandoned = Box::pin(abandoned_client.submit(request.clone()));
            let mut surviving = Box::pin(surviving_client.submit(request.clone()));
            let (abandoned_pending, surviving_pending) = poll_fn(|context| {
                Poll::Ready((
                    abandoned.as_mut().poll(context).is_pending(),
                    surviving.as_mut().poll(context).is_pending(),
                ))
            })
            .await;
            assert!(
                abandoned_pending && surviving_pending,
                "seed={seed}: both submits must be pending after admission"
            );

            let max_concurrently_admitted = client.pending_commands();
            assert_eq!(
                max_concurrently_admitted, 2,
                "seed={seed}: concurrent admission coverage"
            );
            drop(abandoned);
            assert_eq!(
                client.pending_commands(),
                2,
                "seed={seed}: dropping a response canceled its admitted command"
            );

            let surviving_outcome = surviving.await.expect("surviving submit succeeds");
            let expected_outcome = SubmitOutcome::DuplicateActive {
                job_id: JobId::new(0),
            };
            assert_eq!(
                surviving_outcome, expected_outcome,
                "seed={seed}: abandoned submit did not affect its FIFO successor"
            );

            let snapshot = client.snapshot().await.expect("snapshot after abandonment");
            let expected_snapshot = QueueSnapshot {
                now: SimInstant::ZERO,
                active_capacity: QUEUE_CONFIG.active_capacity,
                jobs: vec![JobSnapshot {
                    request_id: request.request_id,
                    job_id: JobId::new(0),
                    payload: request.payload,
                    status: JobStatus::Ready,
                }],
                completed: Vec::new(),
            };
            check_snapshot(expected_snapshot, snapshot.clone())
                .unwrap_or_else(|mismatch| panic!("seed={seed}: {mismatch}"));

            client.shutdown().await.expect("focused broker shutdown");
            broker.await.expect("focused broker join");
            (
                ConcurrentAbandonCoverage {
                    max_concurrently_admitted,
                    abandoned_responses: 1,
                    abandoned_effects_observed: 1,
                    surviving_responses: 1,
                },
                surviving_outcome,
                snapshot,
            )
        })
        .expect("focused concurrent-abandon scenario completes");
    runtime.shutdown().expect("focused scenario teardown");
    ConcurrentAbandonArtifact {
        scenario_version: CONCURRENT_ABANDON_SCENARIO_VERSION,
        seed,
        coverage,
        surviving_outcome,
        snapshot,
        checkpoint: runtime.snapshot().determinism_checkpoint(),
    }
}

fn trace_tail(trace: Option<&SbeRecordingTrace>) -> Option<Vec<kr_runtime::trace::TraceEvent>> {
    let trace = trace?;
    let events = trace.events();
    Some(events[events.len().saturating_sub(32)..].to_vec())
}

fn campaign_trace_artifact_dir() -> PathBuf {
    std::env::var_os("QUARRY_TRACE_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("quarry-traces"))
}

fn publish_campaign_trace_artifact(
    seed: u64,
    trace: &SbeRecordingTrace,
    snapshot: &RuntimeSnapshot,
) -> Result<PathBuf, Box<dyn Error>> {
    publish_campaign_trace_artifact_to(&campaign_trace_artifact_dir(), seed, trace, snapshot)
}

fn publish_campaign_trace_artifact_to(
    directory: &Path,
    seed: u64,
    trace: &SbeRecordingTrace,
    snapshot: &RuntimeSnapshot,
) -> Result<PathBuf, Box<dyn Error>> {
    fs::create_dir_all(directory)?;
    let ordinal = TRACE_ARTIFACT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = format!(
        "quarry-campaign-v{CAMPAIGN_VERSION}-seed-{seed}-pid-{}-{ordinal}.sbe",
        std::process::id()
    );
    let destination = directory.join(&file_name);
    let temporary = directory.join(format!(".{file_name}.tmp"));

    let publish = (|| -> Result<(), Box<dyn Error>> {
        let file = File::create(&temporary)?;
        let mut writer = BufWriter::new(file);
        write_buffered_sbe_trace_artifact(
            &mut writer,
            trace,
            snapshot,
            TraceArtifactMetadata::new(TRACE_ARTIFACT_DRIVER, "failed"),
        )?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        fs::rename(&temporary, &destination)?;
        Ok(())
    })();

    if let Err(error) = publish {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(destination)
}

fn campaign_failure(
    seed: u64,
    comparable_report: String,
    snapshot: &RuntimeSnapshot,
    trace: Option<&SbeRecordingTrace>,
    emit_failure_artifact: bool,
) -> CampaignFailure {
    let trace_dropped = trace.map(SbeRecordingTrace::dropped);
    let trace_retained_bytes = trace.map(SbeRecordingTrace::retained_bytes);
    let trace_encoding_failures = trace.map(SbeRecordingTrace::encoding_failures);
    let trace_tail = trace_tail(trace);
    let artifact_report = if emit_failure_artifact {
        trace.map_or_else(String::new, |trace| {
            match publish_campaign_trace_artifact(seed, trace, snapshot) {
                Ok(path) => format!("\ntrace_artifact={}", path.display()),
                Err(error) => format!("\ntrace_artifact_error={error}"),
            }
        })
    } else {
        String::new()
    };
    let diagnostic_report = format!(
        "{comparable_report}\ntrace_dropped={trace_dropped:?}\ntrace_retained_bytes={trace_retained_bytes:?}\ntrace_encoding_failures={trace_encoding_failures:?}\ntrace_tail={trace_tail:#?}{artifact_report}"
    );
    CampaignFailure {
        comparable_report,
        checkpoint: Box::new(snapshot.determinism_checkpoint()),
        diagnostic_report,
    }
}

#[test]
fn model_campaign_matches_broker() {
    let mut coverage = Coverage::default();
    for seed in campaign_seed_range("QUARRY_CAMPAIGN", CAMPAIGN_SEEDS) {
        let run = match run_seed(seed) {
            Ok(run) => run,
            Err(untraced_failure) => match run_seed_traced(seed) {
                Err(traced_failure) => {
                    if untraced_failure.comparable_report != traced_failure.comparable_report
                        || untraced_failure.checkpoint != traced_failure.checkpoint
                    {
                        panic!(
                            "tracing changed the campaign failure:\n\nuntraced:\n{untraced_failure}\n\ntraced:\n{traced_failure}\n\nuntraced checkpoint:\n{:#?}\n\ntraced checkpoint:\n{:#?}",
                            untraced_failure.checkpoint, traced_failure.checkpoint,
                        );
                    }
                    panic!(
                        "untraced campaign failed:\n{untraced_failure}\n\ntraced rerun failed equivalently:\n{traced_failure}"
                    );
                }
                Ok(traced_run) => panic!(
                    "untraced campaign failed:\n{untraced_failure}\n\ntraced rerun passed unexpectedly: {traced_run:#?}"
                ),
            },
        };
        assert_eq!(run.trace_fingerprint, None);
        let repeated = run_seed(seed).unwrap_or_else(|failure| {
            panic!("seed {seed} failed only on deterministic rerun: {failure}")
        });
        assert_eq!(run, repeated, "seed {seed} did not replay identically");
        run.coverage.assert_seed_baseline(seed);
        coverage.merge(run.coverage);
    }
    coverage.assert_meaningful();
}

#[test]
fn tracing_is_passive_across_campaign_profiles() {
    const PROFILE_SEEDS: [(u64, SwarmProfile); 4] = [
        (2, SwarmProfile::Balanced),
        (6, SwarmProfile::Submission),
        (0, SwarmProfile::Leasing),
        (17, SwarmProfile::Completion),
    ];

    for (seed, expected_profile) in PROFILE_SEEDS {
        let profile_runtime = SimRuntime::new(RuntimeConfig {
            seed,
            ..RuntimeConfig::default()
        });
        assert_eq!(
            SwarmProfile::choose(&profile_runtime.random_source(RandomStream::Scenario)),
            expected_profile,
            "seed={seed} no longer selects its pinned profile"
        );
        let untraced = run_seed(seed)
            .unwrap_or_else(|failure| panic!("untraced seed {seed} failed: {failure}"));
        let traced = run_seed_traced(seed)
            .unwrap_or_else(|failure| panic!("traced seed {seed} failed: {failure}"));

        assert_eq!(traced.coverage, untraced.coverage, "seed={seed}");
        assert_eq!(traced.checkpoint, untraced.checkpoint, "seed={seed}");
        assert_eq!(untraced.trace_fingerprint, None, "seed={seed}");
        assert!(traced.trace_fingerprint.is_some(), "seed={seed}");
    }
}

#[test]
fn concurrent_submit_abandonment_is_applied_and_replays_identically() {
    const SEED: u64 = 0xabad_1dea;

    let first = run_concurrent_abandon(SEED);
    let repeated = run_concurrent_abandon(SEED);

    assert_eq!(first, repeated, "concurrent abandonment did not replay");
    assert_eq!(
        first.coverage,
        ConcurrentAbandonCoverage {
            max_concurrently_admitted: 2,
            abandoned_responses: 1,
            abandoned_effects_observed: 1,
            surviving_responses: 1,
        },
        "fixed scenario missed its concurrency or cancellation target"
    );
    assert!(
        first.checkpoint.stopped,
        "replay artifact was captured before deterministic teardown"
    );
}

#[test]
fn named_broker_coverage_seed_corpus_replays() {
    for &(name, seed) in BROKER_COVERAGE_SEEDS {
        let first = run_seed(seed)
            .unwrap_or_else(|failure| panic!("corpus case {name} seed={seed} failed: {failure}"));
        let repeated = run_seed(seed).unwrap_or_else(|failure| {
            panic!("corpus case {name} seed={seed} failed on replay: {failure}")
        });
        assert_eq!(first, repeated, "corpus case {name} did not replay");
    }
}

fn oracle_fixture() -> QueueSnapshot {
    QueueSnapshot {
        now: SimInstant::from_nanos(10),
        active_capacity: 4,
        jobs: vec![
            JobSnapshot {
                request_id: RequestId::new(11),
                job_id: JobId::new(1),
                payload: vec![1, 2],
                status: JobStatus::Ready,
            },
            JobSnapshot {
                request_id: RequestId::new(12),
                job_id: JobId::new(2),
                payload: vec![3, 4],
                status: JobStatus::Leased {
                    worker_id: WorkerId::new(7),
                    lease_token: LeaseToken::new(19),
                    deadline: SimInstant::from_nanos(20),
                },
            },
        ],
        completed: vec![CompletedSnapshot {
            request_id: RequestId::new(13),
            job_id: JobId::new(3),
            payload: vec![5, 6],
            not_before: SimInstant::from_nanos(4),
            ack_token: LeaseToken::new(23),
        }],
    }
}

#[test]
fn snapshot_oracle_rejects_missing_observable_state() {
    let expected = oracle_fixture();
    let mut missing = expected.clone();
    missing.jobs.remove(0);

    assert!(check_snapshot(expected, missing).is_err());
}

#[test]
fn snapshot_oracle_rejects_phantom_observable_state() {
    let expected = oracle_fixture();
    let mut phantom = expected.clone();
    phantom.jobs.push(JobSnapshot {
        request_id: RequestId::new(99),
        job_id: JobId::new(99),
        payload: vec![9],
        status: JobStatus::Ready,
    });

    assert!(check_snapshot(expected, phantom).is_err());
}

#[test]
fn snapshot_oracle_rejects_stale_observable_state() {
    let expected = oracle_fixture();
    let mut stale = expected.clone();
    stale.jobs[1].status = JobStatus::Ready;

    assert!(check_snapshot(expected, stale).is_err());
}

#[test]
fn deadline_overflow_oracle_arms_are_exercised() {
    let mut model = Model::new(QUEUE_CONFIG);
    model.now = SimInstant::from_nanos(u64::MAX);
    let mut coverage = Coverage::default();
    let overflow = SimDuration::from_nanos(1);

    model
        .check_claim(
            WorkerId::new(1),
            1,
            overflow,
            Err(QueueError::DeadlineOverflow),
            &mut coverage,
        )
        .expect("claim checker accepts deadline overflow");
    model
        .check_renew(
            JobId::new(1),
            LeaseToken::new(1),
            overflow,
            Err(QueueError::DeadlineOverflow),
            &mut coverage,
        )
        .expect("renew checker accepts deadline overflow");
    model
        .check_nack(
            JobId::new(1),
            LeaseToken::new(1),
            overflow,
            Err(QueueError::DeadlineOverflow),
            &mut coverage,
        )
        .expect("nack checker accepts deadline overflow");
}

#[test]
fn outcome_checkers_reject_deliberately_mutated_results() {
    let mut model = Model::new(QUEUE_CONFIG);
    let mut coverage = Coverage::default();
    let worker = WorkerId::new(1);
    let job = JobId::new(1);
    let token = LeaseToken::new(1);
    let duration = SimDuration::from_nanos(1);

    let request = model.make_request(1, SubmitMode::NewBody, 7, 0);
    let submit = model.check_submit(
        request,
        Err(QueueError::ActiveCapacityReached {
            limit: QUEUE_CONFIG.active_capacity,
        }),
        &mut coverage,
    );
    assert!(
        submit
            .expect_err("submit checker accepted an injected capacity error")
            .contains("new submit should return Submitted")
    );

    let claim = model.check_claim(
        worker,
        QUEUE_CONFIG.max_claim_batch + 1,
        duration,
        Ok(Vec::new()),
        &mut coverage,
    );
    assert!(
        claim
            .expect_err("claim checker accepted success for an oversized batch")
            .contains("claim mismatch")
    );

    let renew = model.check_renew(
        job,
        token,
        duration,
        Ok(RenewOutcome::Renewed {
            deadline: SimInstant::from_nanos(1),
        }),
        &mut coverage,
    );
    assert!(
        renew
            .expect_err("renew checker accepted a missing job")
            .contains("renew mismatch")
    );

    let ack = model.check_ack(job, token, Ok(AckOutcome::Completed), &mut coverage);
    assert!(
        ack.expect_err("ack checker accepted a missing job")
            .contains("ack mismatch")
    );

    let nack = model.check_nack(
        job,
        token,
        duration,
        Ok(NackOutcome::Requeued {
            available_at: SimInstant::from_nanos(1),
        }),
        &mut coverage,
    );
    assert!(
        nack.expect_err("nack checker accepted a missing job")
            .contains("nack mismatch")
    );

    assert_eq!(coverage, Coverage::default());
}

#[test]
fn quarry_campaign_artifact_is_reproducible_and_seed_sensitive() {
    let first = run_seed(17).unwrap();
    let repeated = run_seed(17).unwrap();
    let different = run_seed(18).unwrap();
    let traced = run_seed_traced(17).unwrap();
    let traced_repeated = run_seed_traced(17).unwrap();
    let traced_different = run_seed_traced(18).unwrap();

    assert_eq!(first, repeated);
    assert_eq!(traced, traced_repeated);
    assert_ne!(first.checkpoint, different.checkpoint);
    assert_eq!(first.trace_fingerprint, None);
    assert_eq!(first.checkpoint, traced.checkpoint);
    assert_eq!(different.coverage, traced_different.coverage);
    assert_eq!(different.checkpoint, traced_different.checkpoint);
    assert!(traced.trace_fingerprint.is_some());
    assert_ne!(traced.trace_fingerprint, traced_different.trace_fingerprint);
}

#[test]
fn quarry_sbe_failure_artifact_publishes_atomically_and_validates() {
    let trace = Rc::new(SbeRecordingTrace::with_retention(
        SbeTraceRetention::PrefixAndTail {
            prefix_capacity_bytes: 4 * 1_024,
            tail_capacity_bytes: 4 * 1_024,
        },
    ));
    let mut runtime = SimRuntime::with_trace(
        RuntimeConfig {
            seed: 77,
            ..RuntimeConfig::default()
        },
        trace.clone(),
    );
    runtime.shutdown().expect("fixture runtime shuts down");
    let snapshot = runtime.snapshot();
    let directory = std::env::temp_dir().join(format!(
        "quarry-trace-test-{}-{}",
        std::process::id(),
        TRACE_ARTIFACT_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));

    let artifact = publish_campaign_trace_artifact_to(&directory, 77, &trace, &snapshot)
        .expect("failure artifact publishes");
    let binary = fs::read(&artifact).expect("published artifact is readable");
    assert_eq!(&binary[..8], b"DSTRSBE\0");
    assert!(
        fs::read_dir(&directory)
            .expect("artifact directory is readable")
            .all(|entry| !entry
                .expect("directory entry is readable")
                .path()
                .display()
                .to_string()
                .ends_with(".tmp")),
        "temporary artifact was left behind"
    );

    validate_sbe_trace_artifact(binary.as_slice()).expect("published artifact validates");

    fs::remove_dir_all(&directory).expect("fixture artifact directory is removed");
}

#[test]
#[ignore = "set QUARRY_SEED to reproduce one deterministic campaign case"]
fn reproduce_seed_from_environment() {
    let seed = std::env::var("QUARRY_SEED")
        .expect("set QUARRY_SEED to the failing decimal seed")
        .parse::<u64>()
        .expect("QUARRY_SEED must be a u64");
    let artifact = run_seed_traced(seed).unwrap_or_else(|failure| panic!("{failure}"));
    eprintln!(
        "quarry campaign reproduction passed: version={CAMPAIGN_VERSION}, seed={seed}, artifact={artifact:#?}"
    );
}
