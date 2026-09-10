use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[allow(dead_code)]
mod support;

use kr_runtime::rng::RandomStream;
use kr_runtime::test_support::campaign_seed_range;
use kr_runtime::{
    CompletionCertainty, RandomHandle, RuntimeConfig, SimDuration, SimInstant, SimRuntime,
};
use kr_runtime_ring::{MemoryRing, RingError, RingLimits, RingOperation};
use quarry::{
    AckOutcome, DurableQueue, DurableQueueError, JobId, JobStatus, LeaseToken, QueueConfig,
    QueueError, QueueSnapshot, RecoveryConfig, RequestId, SubmitOutcome, SubmitRequest, WorkerId,
};
use support::fault_ring::{FaultRing, InjectedFault};
use support::{below, bounded_ddmin, complete};

const CAMPAIGN_VERSION: u32 = 4;
const CAMPAIGN_SEEDS: u64 = 16;
const CAMPAIGN_STEPS: usize = 36;
const READ_BATCH_RECORDS: usize = 3;
const SHRINK_MAX_ATTEMPTS: usize = 64;
/// Named, non-regression seeds spanning the forced clean-reopen and crash
/// halves. Add real minimized failures here when found.
const DURABILITY_COVERAGE_SEEDS: &[(&str, u64)] = &[
    ("clean-origin", 0),
    ("clean-boundary", 7),
    ("crash-boundary", 8),
    ("crash-upper", 15),
];

const QUEUE_CONFIG: QueueConfig = QueueConfig {
    active_capacity: 64,
    max_payload_bytes: 8,
    max_claim_batch: 4,
    completed_history_capacity: 64,
};

const RING_LIMITS: RingLimits = RingLimits {
    max_record_bytes: 256,
    max_live_records: 256,
    max_live_payload_bytes: 256 * 256,
    max_read_records: READ_BATCH_RECORDS,
    max_read_bytes: 1_024 * 1_024,
    max_batch_records: 1,
    max_batch_bytes: 256,
};

const RECOVERY_CONFIG: RecoveryConfig = RecoveryConfig::new(
    READ_BATCH_RECORDS,
    RING_LIMITS.max_read_bytes,
    RING_LIMITS.max_live_records,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultPoint {
    Append,
    Sync,
}

impl FaultPoint {
    const fn operation(self) -> RingOperation {
        match self {
            Self::Append => RingOperation::Append,
            Self::Sync => RingOperation::Sync,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Append => 0,
            Self::Sync => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Op {
    Submit,
    Retry {
        ordinal: u64,
    },
    Claim {
        worker: u8,
    },
    Ack {
        ordinal: u64,
    },
    Nack {
        ordinal: u64,
    },
    Restart {
        crash: bool,
    },
    FaultedSubmit {
        point: FaultPoint,
        fault: InjectedFault,
        crash: bool,
    },
    FaultedAck {
        point: FaultPoint,
        fault: InjectedFault,
        crash: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SwarmProfile {
    Balanced,
    QueueOnly,
    Recovery,
    Faults,
}

impl SwarmProfile {
    fn choose(random: &RandomHandle) -> Self {
        match below(random, 4) {
            0 => Self::Balanced,
            1 => Self::QueueOnly,
            2 => Self::Recovery,
            _ => Self::Faults,
        }
    }

    fn operation_kind(self, workload: &RandomHandle) -> u64 {
        match self {
            Self::Balanced => below(workload, 100),
            Self::QueueOnly => below(workload, 62),
            Self::Recovery => 62 + below(workload, 9),
            Self::Faults => 71 + below(workload, 29),
        }
    }
}

#[derive(Clone, Debug)]
struct ModelJob {
    request: SubmitRequest,
}

#[derive(Clone, Debug)]
struct ModelCompleted {
    job_id: JobId,
    request: SubmitRequest,
    ack_token: LeaseToken,
}

#[derive(Clone, Debug)]
enum Mutation {
    Submit {
        job_id: JobId,
        request: SubmitRequest,
    },
    Ack {
        job_id: JobId,
        token: LeaseToken,
    },
}

#[derive(Clone, Debug, Default)]
struct Model {
    next_job_id: u64,
    active: BTreeMap<JobId, ModelJob>,
    completed: VecDeque<ModelCompleted>,
}

impl Model {
    fn apply(&mut self, mutation: Mutation) -> Result<(), String> {
        match mutation {
            Mutation::Submit { job_id, request } => {
                let expected = JobId::new(self.next_job_id);
                if job_id != expected {
                    return Err(format!(
                        "model submit named job {job_id}, expected {expected}"
                    ));
                }
                if self.request_state(request.request_id).is_some() {
                    return Err(format!(
                        "model submit reused retained request {}",
                        request.request_id
                    ));
                }
                self.next_job_id = self
                    .next_job_id
                    .checked_add(1)
                    .ok_or_else(|| "model job identifier exhausted".to_owned())?;
                self.active.insert(job_id, ModelJob { request });
            }
            Mutation::Ack { job_id, token } => {
                let job = self
                    .active
                    .remove(&job_id)
                    .ok_or_else(|| format!("model ack could not find active job {job_id}"))?;
                self.completed.push_back(ModelCompleted {
                    job_id,
                    request: job.request,
                    ack_token: token,
                });
                while self.completed.len() > QUEUE_CONFIG.completed_history_capacity {
                    self.completed.pop_front();
                }
            }
        }
        Ok(())
    }

    fn request_state(&self, request_id: RequestId) -> Option<(JobId, bool)> {
        self.active
            .iter()
            .find_map(|(job_id, job)| {
                (job.request.request_id == request_id).then_some((*job_id, false))
            })
            .or_else(|| {
                self.completed.iter().find_map(|completed| {
                    (completed.request.request_id == request_id).then_some((completed.job_id, true))
                })
            })
    }

    fn retained_request(&self, ordinal: u64) -> Option<(SubmitRequest, JobId, bool)> {
        let retained = self.active.len() + self.completed.len();
        if retained == 0 {
            return None;
        }
        let index = usize::try_from(ordinal % retained as u64).expect("bounded ordinal fits");
        if index < self.active.len() {
            let (job_id, job) = self
                .active
                .iter()
                .nth(index)
                .expect("active index is bounded");
            Some((job.request.clone(), *job_id, false))
        } else {
            let completed = self
                .completed
                .get(index - self.active.len())
                .expect("completed index is bounded");
            Some((completed.request.clone(), completed.job_id, true))
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ModelLease {
    worker_id: WorkerId,
    token: LeaseToken,
    deadline: SimInstant,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Coverage {
    submits: u64,
    claims: u64,
    acks: u64,
    nacks: u64,
    restarts: u64,
    crashes: u64,
    submit_retries: u64,
    ack_retries: u64,
    post_fault_operations: u64,
    stacked_uncertainty_rejections: u64,
    healthy_post_fault_observations: u64,
    legal_state_resolutions: u64,
    faulted_submits: [u64; 8],
    faulted_acks: [u64; 8],
}

impl Coverage {
    fn merge(&mut self, other: Self) {
        self.submits += other.submits;
        self.claims += other.claims;
        self.acks += other.acks;
        self.nacks += other.nacks;
        self.restarts += other.restarts;
        self.crashes += other.crashes;
        self.submit_retries += other.submit_retries;
        self.ack_retries += other.ack_retries;
        self.post_fault_operations += other.post_fault_operations;
        self.stacked_uncertainty_rejections += other.stacked_uncertainty_rejections;
        self.healthy_post_fault_observations += other.healthy_post_fault_observations;
        self.legal_state_resolutions += other.legal_state_resolutions;
        for (total, seed) in self.faulted_submits.iter_mut().zip(other.faulted_submits) {
            *total += seed;
        }
        for (total, seed) in self.faulted_acks.iter_mut().zip(other.faulted_acks) {
            *total += seed;
        }
    }

    fn assert_meaningful(self) {
        assert!(self.submits > 0, "campaign did not submit: {self:#?}");
        assert!(self.claims > 0, "campaign did not claim: {self:#?}");
        assert!(self.acks > 0, "campaign did not ack: {self:#?}");
        assert!(self.nacks > 0, "campaign did not nack: {self:#?}");
        assert!(self.restarts > 0, "campaign did not recover: {self:#?}");
        assert!(self.crashes > 0, "campaign did not crash: {self:#?}");
        assert!(
            self.submit_retries > 0 && self.ack_retries > 0,
            "campaign did not retry both durable operations: {self:#?}"
        );
        assert!(
            self.stacked_uncertainty_rejections > 0,
            "campaign did not prove uncertain mutations fence later operations: {self:#?}"
        );
        assert!(
            self.healthy_post_fault_observations > 0,
            "campaign did not operate after a non-poisoning fault: {self:#?}"
        );
        assert!(
            self.legal_state_resolutions > 0,
            "campaign did not resolve an ambiguous legal-state set: {self:#?}"
        );
        let faulted_operations = self
            .faulted_submits
            .iter()
            .chain(&self.faulted_acks)
            .sum::<u64>();
        assert_eq!(
            self.post_fault_operations, faulted_operations,
            "not every injected fault was followed by a pre-restart operation: {self:#?}"
        );
        for (index, count) in self.faulted_submits.into_iter().enumerate() {
            assert!(
                count > 0,
                "campaign missed fault matrix index {index} for submit: {self:#?}"
            );
        }
        for (index, count) in self.faulted_acks.into_iter().enumerate() {
            assert!(
                count > 0,
                "campaign missed fault matrix index {index} for ack: {self:#?}"
            );
        }
    }

    fn assert_seed_baseline(self, seed: u64) {
        let ack_fault_index = (seed % 8) as usize;
        let submit_fault_index = ((seed + 3) % 8) as usize;
        assert!(
            self.faulted_acks[ack_fault_index] > 0,
            "campaign_version={CAMPAIGN_VERSION}; seed={seed}; assigned ack fault matrix index {ack_fault_index} was not exercised: {self:#?}"
        );
        assert!(
            self.faulted_submits[submit_fault_index] > 0,
            "campaign_version={CAMPAIGN_VERSION}; seed={seed}; assigned submit fault matrix index {submit_fault_index} was not exercised: {self:#?}"
        );
        assert!(
            self.restarts >= 3,
            "campaign_version={CAMPAIGN_VERSION}; seed={seed}; forced fault/restart prefix was not exercised: {self:#?}"
        );
        assert!(
            self.crashes > 0,
            "campaign_version={CAMPAIGN_VERSION}; seed={seed}; forced crash was not exercised: {self:#?}"
        );
        assert!(
            self.restarts > self.crashes,
            "campaign_version={CAMPAIGN_VERSION}; seed={seed}; forced clean reopen was not exercised: {self:#?}"
        );
        assert!(
            self.submit_retries > 0 && self.ack_retries > 0,
            "campaign_version={CAMPAIGN_VERSION}; seed={seed}; forced durable retries were not exercised: {self:#?}"
        );
        assert!(
            self.post_fault_operations >= 2,
            "campaign_version={CAMPAIGN_VERSION}; seed={seed}; operations were not attempted between each forced fault and restart: {self:#?}"
        );
    }
}

struct Harness {
    queue: Option<DurableQueue<FaultRing>>,
    ring: FaultRing,
    model: Model,
    pending: Option<(FaultPoint, Mutation)>,
    leases: BTreeMap<JobId, ModelLease>,
    next_request_id: u64,
    trace: Vec<String>,
    coverage: Coverage,
}

impl Harness {
    fn new() -> Result<Self, String> {
        let ring = FaultRing::new(
            MemoryRing::new(RING_LIMITS)
                .map_err(|error| format!("could not create memory ring: {error}"))?,
        );
        let queue = complete(DurableQueue::recover(
            QUEUE_CONFIG,
            ring.clone(),
            RECOVERY_CONFIG,
        ))
        .map_err(|error| format!("initial recovery failed: {error:?}"))?;
        let mut harness = Self {
            queue: Some(queue),
            ring,
            model: Model::default(),
            pending: None,
            leases: BTreeMap::new(),
            next_request_id: 0,
            trace: vec!["initial recovery -> incarnation=1".to_owned()],
            coverage: Coverage::default(),
        };
        harness.assert_state()?;
        Ok(harness)
    }

    fn execute(&mut self, op: Op) -> Result<(), String> {
        match op {
            Op::Submit => self.normal_submit().map(|_| ()),
            Op::Retry { ordinal } => self.retry_retained(ordinal),
            Op::Claim { worker } => self.claim_one(WorkerId::new(u64::from(worker))).map(|_| ()),
            Op::Ack { ordinal } => self.normal_ack(ordinal),
            Op::Nack { ordinal } => self.normal_nack(ordinal),
            Op::Restart { crash } => self.restart(crash),
            Op::FaultedSubmit {
                point,
                fault,
                crash,
            } => self.faulted_submit(point, fault, crash),
            Op::FaultedAck {
                point,
                fault,
                crash,
            } => self.faulted_ack(point, fault, crash),
        }
    }

    fn queue_mut(&mut self) -> &mut DurableQueue<FaultRing> {
        self.queue.as_mut().expect("campaign queue is open")
    }

    fn fresh_request(&mut self) -> SubmitRequest {
        let id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .expect("bounded campaign request identifiers cannot exhaust");
        SubmitRequest {
            request_id: RequestId::new(id),
            payload: vec![b'q', (id & 0xff) as u8, ((id >> 8) & 0xff) as u8],
            not_before: SimInstant::ZERO,
        }
    }

    fn normal_submit(&mut self) -> Result<(SubmitRequest, JobId), String> {
        let request = self.fresh_request();
        let expected_job = JobId::new(self.model.next_job_id);
        let result = complete(self.queue_mut().submit(request.clone(), SimInstant::ZERO));
        let outcome = result.map_err(|error| format!("normal submit failed: {error:?}"))?;
        if outcome
            != (SubmitOutcome::Submitted {
                job_id: expected_job,
            })
        {
            return Err(format!(
                "normal submit returned {outcome:?}, expected job {expected_job}"
            ));
        }
        self.model.apply(Mutation::Submit {
            job_id: expected_job,
            request: request.clone(),
        })?;
        self.coverage.submits += 1;
        self.trace.push(format!(
            "submit request={} -> job={expected_job}",
            request.request_id
        ));
        self.assert_state()?;
        Ok((request, expected_job))
    }

    fn retry_retained(&mut self, ordinal: u64) -> Result<(), String> {
        let Some((request, job_id, completed)) = self.model.retained_request(ordinal) else {
            self.trace
                .push("retry skipped: no retained request".to_owned());
            return Ok(());
        };
        let outcome = complete(self.queue_mut().submit(request.clone(), SimInstant::ZERO))
            .map_err(|error| format!("retained submit retry failed: {error:?}"))?;
        let expected = if completed {
            SubmitOutcome::DuplicateCompleted { job_id }
        } else {
            SubmitOutcome::DuplicateActive { job_id }
        };
        if outcome != expected {
            return Err(format!(
                "retained request {} retry returned {outcome:?}, expected {expected:?}",
                request.request_id
            ));
        }
        self.coverage.submit_retries += 1;
        self.trace.push(format!(
            "retry request={} -> {outcome:?}",
            request.request_id
        ));
        self.assert_state()
    }

    fn claim_one(&mut self, worker_id: WorkerId) -> Result<Option<(JobId, LeaseToken)>, String> {
        let claimed = self
            .queue_mut()
            .claim(worker_id, 1, SimDuration::from_nanos(100), SimInstant::ZERO)
            .map_err(|error| format!("claim failed: {error:?}"))?;
        let Some(job) = claimed.into_iter().next() else {
            self.trace
                .push(format!("claim worker={worker_id} -> empty"));
            self.assert_state()?;
            return Ok(None);
        };
        if !self.model.active.contains_key(&job.job_id) {
            return Err(format!("claim returned non-model job {}", job.job_id));
        }
        if self.leases.contains_key(&job.job_id) {
            return Err(format!("claim returned already leased job {}", job.job_id));
        }
        if job.lease_token.incarnation() != self.queue_mut().incarnation() {
            return Err(format!(
                "claim token {} does not match queue incarnation {}",
                job.lease_token,
                self.queue_mut().incarnation()
            ));
        }
        self.leases.insert(
            job.job_id,
            ModelLease {
                worker_id,
                token: job.lease_token,
                deadline: job.deadline,
            },
        );
        self.coverage.claims += 1;
        self.trace.push(format!(
            "claim worker={worker_id} -> job={} token={}",
            job.job_id, job.lease_token
        ));
        self.assert_state()?;
        Ok(Some((job.job_id, job.lease_token)))
    }

    fn normal_ack(&mut self, ordinal: u64) -> Result<(), String> {
        let Some((job_id, lease)) = pick_lease(&self.leases, ordinal) else {
            self.trace.push("ack skipped: no lease".to_owned());
            return Ok(());
        };
        let outcome = complete(self.queue_mut().ack(job_id, lease.token, SimInstant::ZERO))
            .map_err(|error| format!("normal ack failed: {error:?}"))?;
        if outcome != AckOutcome::Completed {
            return Err(format!("normal ack returned {outcome:?}"));
        }
        self.model.apply(Mutation::Ack {
            job_id,
            token: lease.token,
        })?;
        self.leases.remove(&job_id);
        self.coverage.acks += 1;
        self.trace.push(format!(
            "ack job={job_id} token={} -> completed",
            lease.token
        ));
        self.assert_state()
    }

    fn normal_nack(&mut self, ordinal: u64) -> Result<(), String> {
        let Some((job_id, lease)) = pick_lease(&self.leases, ordinal) else {
            self.trace.push("nack skipped: no lease".to_owned());
            return Ok(());
        };
        self.queue_mut()
            .nack(job_id, lease.token, SimDuration::ZERO, SimInstant::ZERO)
            .map_err(|error| format!("nack failed: {error:?}"))?;
        self.leases.remove(&job_id);
        self.coverage.nacks += 1;
        self.trace
            .push(format!("nack job={job_id} token={} retry=0", lease.token));
        self.assert_state()
    }

    fn release_all_leases(&mut self) -> Result<(), String> {
        let leases = self
            .leases
            .iter()
            .map(|(job_id, lease)| (*job_id, *lease))
            .collect::<Vec<_>>();
        for (job_id, lease) in leases {
            self.queue_mut()
                .nack(job_id, lease.token, SimDuration::ZERO, SimInstant::ZERO)
                .map_err(|error| format!("pre-fault nack failed: {error:?}"))?;
            self.leases.remove(&job_id);
            self.coverage.nacks += 1;
            self.trace
                .push(format!("prepare nack job={job_id} token={}", lease.token));
        }
        self.assert_state()
    }

    fn faulted_submit(
        &mut self,
        point: FaultPoint,
        fault: InjectedFault,
        crash: bool,
    ) -> Result<(), String> {
        let request = self.fresh_request();
        let job_id = JobId::new(self.model.next_job_id);
        let mutation = Mutation::Submit {
            job_id,
            request: request.clone(),
        };
        self.inject(point, fault)?;
        let result = complete(self.queue_mut().submit(request.clone(), SimInstant::ZERO));
        let error = match result {
            Ok(outcome) => {
                return Err(format!(
                    "faulted submit unexpectedly succeeded with {outcome:?}"
                ));
            }
            Err(error) => error,
        };
        self.check_fault_error(point, fault, &error)?;
        self.stage_fault_effect(point, fault, mutation.clone())?;
        self.coverage.faulted_submits[fault_index(point, fault)] += 1;
        let poisoned = self.queue_mut().recovery_required();
        self.trace.push(format!(
            "faulted submit request={} job={job_id} point={point:?} fault={fault:?} certainty={:?} poisoned={}",
            request.request_id,
            error.certainty(),
            poisoned
        ));
        self.probe_after_fault(&mutation, poisoned)?;

        self.restart(crash)?;
        let durable = self.model.request_state(request.request_id).is_some();
        let retry = complete(self.queue_mut().submit(request.clone(), SimInstant::ZERO))
            .map_err(|error| format!("faulted submit retry failed: {error:?}"))?;
        if durable {
            let expected = SubmitOutcome::DuplicateActive { job_id };
            if retry != expected {
                return Err(format!(
                    "durable faulted submit retry returned {retry:?}, expected {expected:?}"
                ));
            }
        } else {
            let expected = SubmitOutcome::Submitted { job_id };
            if retry != expected {
                return Err(format!(
                    "lost faulted submit retry returned {retry:?}, expected {expected:?}"
                ));
            }
            self.model.apply(Mutation::Submit {
                job_id,
                request: request.clone(),
            })?;
        }
        self.coverage.submit_retries += 1;
        self.trace.push(format!(
            "faulted submit retry request={} durable_before_retry={durable} -> {retry:?}",
            request.request_id
        ));
        self.assert_state()
    }

    fn faulted_ack(
        &mut self,
        point: FaultPoint,
        fault: InjectedFault,
        crash: bool,
    ) -> Result<(), String> {
        if self.model.active.is_empty() {
            self.normal_submit()?;
        }
        self.release_all_leases()?;
        let (job_id, token) = self
            .claim_one(WorkerId::new(250))?
            .ok_or_else(|| "faulted ack could not prepare a lease".to_owned())?;
        let mutation = Mutation::Ack { job_id, token };

        self.inject(point, fault)?;
        let result = complete(self.queue_mut().ack(job_id, token, SimInstant::ZERO));
        let error = match result {
            Ok(outcome) => {
                return Err(format!(
                    "faulted ack unexpectedly succeeded with {outcome:?}"
                ));
            }
            Err(error) => error,
        };
        self.check_fault_error(point, fault, &error)?;
        self.stage_fault_effect(point, fault, mutation.clone())?;
        self.coverage.faulted_acks[fault_index(point, fault)] += 1;
        let poisoned = self.queue_mut().recovery_required();
        self.trace.push(format!(
            "faulted ack job={job_id} token={token} point={point:?} fault={fault:?} certainty={:?} poisoned={}",
            error.certainty(),
            poisoned
        ));
        self.probe_after_fault(&mutation, poisoned)?;

        self.restart(crash)?;
        let ack_is_durable = self
            .model
            .completed
            .iter()
            .any(|completed| completed.job_id == job_id && completed.ack_token == token);
        let retry = complete(self.queue_mut().ack(job_id, token, SimInstant::ZERO));
        let retry_debug = format!("{retry:?}");
        if ack_is_durable {
            if retry != Ok(AckOutcome::AlreadyCompleted) {
                return Err(format!(
                    "durable faulted ack retry returned {retry:?}, expected AlreadyCompleted"
                ));
            }
        } else {
            let error = match retry {
                Ok(outcome) => {
                    return Err(format!(
                        "lost ack retry with old lease unexpectedly returned {outcome:?}"
                    ));
                }
                Err(error) => error,
            };
            if error.certainty() != CompletionCertainty::NotApplied
                || !matches!(
                    error.error(),
                    DurableQueueError::Queue(QueueError::JobNotLeased { job_id: found })
                        if *found == job_id
                )
            {
                return Err(format!(
                    "lost ack retry returned unexpected result: {error:?}"
                ));
            }
            let (claimed_job, new_token) = self
                .claim_one(WorkerId::new(251))?
                .ok_or_else(|| "lost ack job was not reclaimable".to_owned())?;
            if claimed_job != job_id {
                return Err(format!(
                    "lost ack reclaimed job {claimed_job}, expected {job_id}"
                ));
            }
            let outcome = complete(self.queue_mut().ack(job_id, new_token, SimInstant::ZERO))
                .map_err(|error| format!("replacement ack failed: {error:?}"))?;
            if outcome != AckOutcome::Completed {
                return Err(format!("replacement ack returned {outcome:?}"));
            }
            self.model.apply(Mutation::Ack {
                job_id,
                token: new_token,
            })?;
            self.leases.remove(&job_id);
            self.coverage.acks += 1;
        }
        self.coverage.ack_retries += 1;
        self.trace.push(format!(
            "faulted ack retry job={job_id} durable_before_retry={ack_is_durable} -> {retry_debug}"
        ));
        self.assert_state()
    }

    fn inject(&self, point: FaultPoint, fault: InjectedFault) -> Result<(), String> {
        match point {
            FaultPoint::Append => self.ring.inject_append_fault(fault),
            FaultPoint::Sync => self.ring.inject_sync_fault(fault),
        }
        Ok(())
    }

    fn check_fault_error(
        &mut self,
        point: FaultPoint,
        fault: InjectedFault,
        error: &kr_runtime::CompletionError<DurableQueueError>,
    ) -> Result<(), String> {
        let expected_certainty = expected_certainty(point, fault);
        if error.certainty() != expected_certainty {
            return Err(format!(
                "{point:?}/{fault:?} returned certainty {:?}, expected {expected_certainty:?}",
                error.certainty()
            ));
        }
        let durable_error = error.error();
        if !matches!(
            durable_error,
            DurableQueueError::Ring(RingError::BackendFailure { operation, message, .. })
                if *operation == point.operation() && message == "deterministic injected fault"
        ) {
            return Err(format!(
                "{point:?}/{fault:?} returned unexpected error {durable_error:?}"
            ));
        }
        let expected_poisoned = !matches!(
            (point, fault),
            (FaultPoint::Append, InjectedFault::Before) | (FaultPoint::Sync, InjectedFault::After)
        );
        if self.queue_mut().recovery_required() != expected_poisoned {
            return Err(format!(
                "{point:?}/{fault:?} poisoned={}, expected {expected_poisoned}",
                self.queue_mut().recovery_required()
            ));
        }
        Ok(())
    }

    fn stage_fault_effect(
        &mut self,
        point: FaultPoint,
        fault: InjectedFault,
        mutation: Mutation,
    ) -> Result<(), String> {
        if self.pending.is_some() {
            return Err("campaign attempted to stack uncertain mutations".to_owned());
        }
        match expected_certainty(point, fault) {
            CompletionCertainty::NotApplied => {}
            CompletionCertainty::Applied => self.model.apply(mutation)?,
            CompletionCertainty::MayHaveApplied => self.pending = Some((point, mutation)),
            certainty => {
                return Err(format!(
                    "unsupported completion certainty in durability campaign: {certainty:?}"
                ));
            }
        }
        Ok(())
    }

    fn probe_after_fault(&mut self, mutation: &Mutation, poisoned: bool) -> Result<(), String> {
        self.coverage.post_fault_operations += 1;
        if !poisoned {
            self.assert_state()?;
            self.coverage.healthy_post_fault_observations += 1;
            self.trace
                .push("post-fault snapshot succeeded before restart".to_owned());
            return Ok(());
        }

        match mutation {
            Mutation::Submit { request, .. } => expect_recovery_required(
                "submit",
                complete(self.queue_mut().submit(request.clone(), SimInstant::ZERO)),
            )?,
            Mutation::Ack { job_id, token } => expect_recovery_required(
                "ack",
                complete(self.queue_mut().ack(*job_id, *token, SimInstant::ZERO)),
            )?,
        }
        self.coverage.stacked_uncertainty_rejections += 1;
        self.trace
            .push("post-fault mutation rejected by recovery fence".to_owned());
        Ok(())
    }

    fn restart(&mut self, crash: bool) -> Result<(), String> {
        let queue = self.queue.take().expect("campaign queue is open");
        let ring = queue.into_ring();
        let pending = self.pending.take();
        let had_pending = pending.is_some();
        let ambiguous_sync = matches!(pending.as_ref(), Some((FaultPoint::Sync, _)));
        let reopen_result = (crash || ambiguous_sync).then(|| ring.reopen());
        if crash {
            let result = reopen_result.expect("crash reopens the memory ring");
            let maximum_legal_discarded = usize::from(had_pending);
            if result.discarded_records > maximum_legal_discarded {
                return Err(format!(
                    "crash discarded {} records, legal maximum is {maximum_legal_discarded}",
                    result.discarded_records
                ));
            }
            self.coverage.crashes += 1;
            self.trace.push(format!(
                "crash pending={had_pending} discarded_records={}",
                result.discarded_records
            ));
        } else if let Some(result) = reopen_result {
            self.trace.push(format!(
                "ambiguous sync reopen: discarded_records={}",
                result.discarded_records
            ));
        } else {
            self.trace
                .push("clean recovery without process loss".to_owned());
        }

        let recovered = complete(DurableQueue::recover(QUEUE_CONFIG, ring, RECOVERY_CONFIG))
            .map_err(|error| format!("recovery failed: {error:?}"))?;
        self.queue = Some(recovered);
        self.leases.clear();

        let snapshot = self
            .queue_mut()
            .snapshot(SimInstant::ZERO)
            .map_err(|error| format!("post-recovery snapshot failed: {error:?}"))?;
        let mut candidates = Vec::with_capacity(2);
        if let Some((point, mutation)) = pending {
            let mut applied = self.model.clone();
            applied.apply(mutation)?;
            match (point, crash) {
                // Append alone is not a durability fence, so process loss
                // discards any accepted-but-unsynced candidate record.
                (FaultPoint::Append, true) => {
                    candidates.push(("not-applied-after-reopen", self.model.clone()));
                }
                // A clean recovery can fence an ambiguously accepted append.
                // Reopening an ambiguous sync resolves to either complete
                // checkpoint.
                (FaultPoint::Append, false) | (FaultPoint::Sync, _) => {
                    candidates.push(("not-applied", self.model.clone()));
                    candidates.push(("applied", applied));
                }
            }
        } else {
            candidates.push(("current", self.model.clone()));
        }
        let legal_state_count = candidates.len();
        let mut matching = candidates
            .into_iter()
            .filter(|(_, candidate)| {
                check_snapshot_against_model(&snapshot, candidate, &self.leases).is_ok()
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(format!(
                "post-recovery snapshot matched {} of {legal_state_count} legal states: {snapshot:#?}",
                matching.len()
            ));
        }
        let (resolution, model) = matching.pop().expect("one legal state matched");
        self.model = model;
        if legal_state_count > 1 {
            self.coverage.legal_state_resolutions += 1;
        }
        self.trace.push(format!(
            "recovery legal-state resolution={resolution} candidates={legal_state_count}"
        ));

        self.coverage.restarts += 1;
        let incarnation = self.queue_mut().incarnation();
        self.trace.push(format!(
            "recover crash={crash} -> incarnation={}",
            incarnation
        ));
        self.assert_state()
    }

    fn assert_state(&mut self) -> Result<(), String> {
        let snapshot = self
            .queue_mut()
            .snapshot(SimInstant::ZERO)
            .map_err(|error| format!("snapshot failed: {error:?}"))?;
        check_snapshot_against_model(&snapshot, &self.model, &self.leases)
    }
}

fn expect_recovery_required<T>(
    operation: &str,
    result: kr_runtime::CompletionResult<T, DurableQueueError>,
) -> Result<(), String> {
    let error = result.map(|_| ()).expect_err(&format!(
        "post-fault {operation} unexpectedly passed the recovery fence"
    ));
    if error.certainty() != CompletionCertainty::NotApplied
        || error.error() != &DurableQueueError::RecoveryRequired
    {
        return Err(format!(
            "post-fault {operation} returned {error:?}, expected NotApplied RecoveryRequired"
        ));
    }
    Ok(())
}

fn check_snapshot_against_model(
    snapshot: &QueueSnapshot,
    model: &Model,
    leases: &BTreeMap<JobId, ModelLease>,
) -> Result<(), String> {
    if snapshot.now != SimInstant::ZERO {
        return Err(format!(
            "snapshot time {:?} differs from requested {:?}",
            snapshot.now,
            SimInstant::ZERO
        ));
    }
    if snapshot.active_capacity != QUEUE_CONFIG.active_capacity {
        return Err(format!(
            "snapshot active capacity {} differs from model {}",
            snapshot.active_capacity, QUEUE_CONFIG.active_capacity
        ));
    }
    if snapshot.jobs.len() != model.active.len() {
        return Err(format!(
            "active length mismatch: queue={} model={}",
            snapshot.jobs.len(),
            model.active.len()
        ));
    }
    let mut observed_job_ids = BTreeSet::new();
    for job in &snapshot.jobs {
        if !observed_job_ids.insert(job.job_id) {
            return Err(format!(
                "snapshot contains duplicate active job {}",
                job.job_id
            ));
        }
        let expected = model
            .active
            .get(&job.job_id)
            .ok_or_else(|| format!("snapshot contains unknown active job {}", job.job_id))?;
        if job.request_id != expected.request.request_id || job.payload != expected.request.payload
        {
            return Err(format!(
                "active job {} differs: queue={job:?} model={expected:?}",
                job.job_id
            ));
        }
        let expected_status =
            leases
                .get(&job.job_id)
                .map_or(JobStatus::Ready, |lease| JobStatus::Leased {
                    worker_id: lease.worker_id,
                    lease_token: lease.token,
                    deadline: lease.deadline,
                });
        if job.status != expected_status {
            return Err(format!(
                "active job {} status {:?}, expected {expected_status:?}",
                job.job_id, job.status
            ));
        }
    }
    for job_id in model.active.keys() {
        if !observed_job_ids.contains(job_id) {
            return Err(format!("snapshot is missing active job {job_id}"));
        }
    }
    if snapshot.completed.len() != model.completed.len() {
        return Err(format!(
            "completed length mismatch: queue={} model={}",
            snapshot.completed.len(),
            model.completed.len()
        ));
    }
    for (actual, expected) in snapshot.completed.iter().zip(&model.completed) {
        if actual.request_id != expected.request.request_id
            || actual.job_id != expected.job_id
            || actual.payload != expected.request.payload
            || actual.not_before != expected.request.not_before
            || actual.ack_token != expected.ack_token
        {
            return Err(format!(
                "completed state differs: queue={actual:?} model={expected:?}"
            ));
        }
    }
    Ok(())
}

fn pick_lease(leases: &BTreeMap<JobId, ModelLease>, ordinal: u64) -> Option<(JobId, ModelLease)> {
    if leases.is_empty() {
        return None;
    }
    let index = usize::try_from(ordinal % leases.len() as u64).expect("bounded ordinal fits");
    leases
        .iter()
        .nth(index)
        .map(|(job_id, lease)| (*job_id, *lease))
}

const fn expected_certainty(point: FaultPoint, fault: InjectedFault) -> CompletionCertainty {
    match (point, fault) {
        (FaultPoint::Append, InjectedFault::Before) => CompletionCertainty::NotApplied,
        (FaultPoint::Sync, InjectedFault::After) => CompletionCertainty::Applied,
        _ => CompletionCertainty::MayHaveApplied,
    }
}

const fn fault_variant_index(fault: InjectedFault) -> usize {
    match fault {
        InjectedFault::Before => 0,
        InjectedFault::After => 1,
        InjectedFault::MayHaveAppliedBefore => 2,
        InjectedFault::MayHaveAppliedAfter => 3,
    }
}

const fn fault_index(point: FaultPoint, fault: InjectedFault) -> usize {
    point.index() * 4 + fault_variant_index(fault)
}

fn fault_case(index: usize) -> (FaultPoint, InjectedFault) {
    const FAULTS: [InjectedFault; 4] = [
        InjectedFault::Before,
        InjectedFault::After,
        InjectedFault::MayHaveAppliedBefore,
        InjectedFault::MayHaveAppliedAfter,
    ];
    let point = if index / 4 == 0 {
        FaultPoint::Append
    } else {
        FaultPoint::Sync
    };
    (point, FAULTS[index % 4])
}

fn generate_ops(seed: u64) -> Vec<Op> {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed,
        ..RuntimeConfig::default()
    });
    let workload_random = runtime.random_source(RandomStream::Workload);
    let fault_random = runtime.random_source(RandomStream::Fault);
    let scenario_random = runtime.random_source(RandomStream::Scenario);
    let profile = SwarmProfile::choose(&scenario_random);
    drop(scenario_random);
    let (ack_point, ack_fault) = fault_case((seed % 8) as usize);
    let (submit_point, submit_fault) = fault_case(((seed + 3) % 8) as usize);
    let forced_crash = seed >= CAMPAIGN_SEEDS / 2;
    let mut ops = vec![
        Op::Submit,
        Op::Claim { worker: 1 },
        Op::Nack { ordinal: 0 },
        Op::Claim { worker: 2 },
        Op::FaultedAck {
            point: ack_point,
            fault: ack_fault,
            crash: forced_crash,
        },
        Op::FaultedSubmit {
            point: submit_point,
            fault: submit_fault,
            crash: !forced_crash,
        },
        Op::Restart {
            crash: seed.is_multiple_of(2),
        },
    ];
    while ops.len() < CAMPAIGN_STEPS {
        let op = match profile.operation_kind(&workload_random) {
            0..=15 => Op::Submit,
            16..=25 => Op::Retry {
                ordinal: below(&workload_random, 64),
            },
            26..=39 => Op::Claim {
                worker: below(&workload_random, 8) as u8,
            },
            40..=51 => Op::Ack {
                ordinal: below(&workload_random, 64),
            },
            52..=61 => Op::Nack {
                ordinal: below(&workload_random, 64),
            },
            62..=70 => Op::Restart {
                crash: below(&fault_random, 2) == 0,
            },
            71..=85 => {
                let (point, fault) = fault_case(below(&fault_random, 8) as usize);
                Op::FaultedSubmit {
                    point,
                    fault,
                    crash: below(&fault_random, 2) == 0,
                }
            }
            _ => {
                let (point, fault) = fault_case(below(&fault_random, 8) as usize);
                Op::FaultedAck {
                    point,
                    fault,
                    crash: below(&fault_random, 2) == 0,
                }
            }
        };
        ops.push(op);
    }
    drop(workload_random);
    drop(fault_random);
    runtime
        .shutdown()
        .expect("operation generation leaves no runtime tasks");
    ops
}

#[derive(Debug, Eq, PartialEq)]
struct CampaignRun {
    coverage: Coverage,
    trace: Vec<String>,
}

#[derive(Debug)]
struct CampaignFailure {
    step: Option<usize>,
    detail: String,
}

fn run_ops_once(ops: &[Op]) -> Result<CampaignRun, CampaignFailure> {
    let mut harness = Harness::new().map_err(|error| CampaignFailure {
        step: None,
        detail: format!("initialization: {error}"),
    })?;
    for (step, op) in ops.iter().copied().enumerate() {
        if let Err(error) = harness.execute(op) {
            return Err(CampaignFailure {
                step: Some(step),
                detail: format!(
                    "step={step}; op={op:?}; error={error}; coverage_at_failure={:#?}; model={:#?}; ring={:#?}; trace={:#?}",
                    harness.coverage,
                    harness.model,
                    harness.ring.status_now(),
                    harness.trace
                ),
            });
        }
    }
    Ok(CampaignRun {
        coverage: harness.coverage,
        trace: harness.trace,
    })
}

fn run_seed(seed: u64) -> Result<CampaignRun, String> {
    let ops = generate_ops(seed);
    match run_ops_once(&ops) {
        Ok(run) => Ok(run),
        Err(failure) => {
            let failing_ops = failure
                .step
                .map_or_else(|| ops.clone(), |step| ops[..=step].to_vec());
            // Every durability operation either self-prepares or is a safe
            // no-op when its referenced state is absent, so every subsequence
            // is a valid shrink candidate.
            let shrunk = bounded_ddmin(
                &failing_ops,
                SHRINK_MAX_ATTEMPTS,
                |_| true,
                |candidate| run_ops_once(candidate).is_err(),
            );
            Err(format!(
                "campaign_version={CAMPAIGN_VERSION}; seed={seed}; {}\noriginal_operation_prefix={failing_ops:#?}\nminimized_operations={:#?}\nshrink_attempts={}; shrink_attempt_limit_reached={}",
                failure.detail, shrunk.minimized, shrunk.attempts, shrunk.attempt_limit_reached,
            ))
        }
    }
}

#[test]
fn durable_queue_many_seed_crash_and_retry_campaign() {
    let mut coverage = Coverage::default();
    for seed in campaign_seed_range("QUARRY_DURABILITY", CAMPAIGN_SEEDS) {
        let run = run_seed(seed).unwrap_or_else(|failure| panic!("{failure}"));
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
fn named_durability_coverage_seed_corpus_replays() {
    for &(name, seed) in DURABILITY_COVERAGE_SEEDS {
        let first = run_seed(seed)
            .unwrap_or_else(|failure| panic!("corpus case {name} seed={seed} failed: {failure}"));
        let repeated = run_seed(seed).unwrap_or_else(|failure| {
            panic!("corpus case {name} seed={seed} failed on replay: {failure}")
        });
        assert_eq!(first, repeated, "corpus case {name} did not replay");
    }
}

#[test]
fn durability_oracle_rejects_corrupted_missing_and_stale_snapshots() {
    let mut harness = Harness::new().expect("sentinel harness should recover");
    let (_, job_id) = harness
        .normal_submit()
        .expect("sentinel submit should succeed");
    harness
        .normal_submit()
        .expect("second sentinel submit should succeed");
    let baseline = harness
        .queue_mut()
        .snapshot(SimInstant::ZERO)
        .expect("sentinel snapshot should succeed");
    check_snapshot_against_model(&baseline, &harness.model, &harness.leases)
        .expect("oracle must accept an unmodified snapshot");

    let mut corrupted = baseline.clone();
    corrupted.jobs[0].payload.push(b'!');
    let error = check_snapshot_against_model(&corrupted, &harness.model, &harness.leases)
        .expect_err("oracle accepted a deliberately corrupted payload");
    assert!(
        error.contains("differs"),
        "corrupted sentinel tripped the wrong oracle check: {error}"
    );

    let mut missing = baseline.clone();
    missing.jobs.clear();
    let error = check_snapshot_against_model(&missing, &harness.model, &harness.leases)
        .expect_err("oracle accepted a deliberately missing active job");
    assert!(
        error.contains("active length mismatch"),
        "missing sentinel tripped the wrong oracle check: {error}"
    );

    let mut duplicate = baseline.clone();
    duplicate.jobs[1] = duplicate.jobs[0].clone();
    let error = check_snapshot_against_model(&duplicate, &harness.model, &harness.leases)
        .expect_err("oracle accepted one duplicate and one missing active job");
    assert!(
        error.contains("duplicate active job"),
        "duplicate sentinel tripped the wrong oracle check: {error}"
    );

    let claimed = harness
        .claim_one(WorkerId::new(99))
        .expect("sentinel claim should succeed")
        .expect("sentinel active job should be claimable");
    assert_eq!(claimed.0, job_id);
    let error = check_snapshot_against_model(&baseline, &harness.model, &harness.leases)
        .expect_err("oracle accepted a deliberately stale pre-claim snapshot");
    assert!(
        error.contains("status"),
        "stale sentinel tripped the wrong oracle check: {error}"
    );
}
