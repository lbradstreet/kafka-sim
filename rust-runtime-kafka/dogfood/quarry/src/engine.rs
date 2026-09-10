use std::collections::{BTreeMap, VecDeque};

use kr_runtime::{SimDuration, SimInstant};

use crate::types::{
    AckOutcome, CompletedSnapshot, JobId, JobSnapshot, JobStatus, LeaseToken, LeasedJob,
    NackOutcome, QueueConfig, QueueError, QueueSnapshot, RenewOutcome, RequestId, SubmitOutcome,
    SubmitRequest, WorkerId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestSpec {
    payload: Vec<u8>,
    not_before: SimInstant,
}

impl RequestSpec {
    fn matches(&self, request: &SubmitRequest) -> bool {
        self.payload == request.payload && self.not_before == request.not_before
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Lease {
    worker_id: WorkerId,
    token: LeaseToken,
    deadline: SimInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveJob {
    request_id: RequestId,
    job_id: JobId,
    spec: RequestSpec,
    available_at: SimInstant,
    lease: Option<Lease>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompletedJob {
    request_id: RequestId,
    job_id: JobId,
    spec: RequestSpec,
    ack_token: LeaseToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RequestState {
    Active { job_id: JobId },
    Completed(CompletedJob),
}

/// A submit that has passed every fallible queue check but is not yet visible.
///
/// Durable callers can encode and fence the corresponding record before
/// applying this small mutation. Holding a plan is safe across an await because
/// the caller has exclusive access to the queue and applying it performs no
/// fallible work.
pub(crate) struct PlannedSubmit {
    job: ActiveJob,
    next_job_id: u64,
}

impl PlannedSubmit {
    pub(crate) const fn request_id(&self) -> RequestId {
        self.job.request_id
    }

    pub(crate) const fn job_id(&self) -> JobId {
        self.job.job_id
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.job.spec.payload
    }

    pub(crate) const fn not_before(&self) -> SimInstant {
        self.job.spec.not_before
    }
}

pub(crate) enum SubmitPlan {
    Immediate(SubmitOutcome),
    Insert(PlannedSubmit),
}

/// An acknowledgement that has passed every fallible queue check but is not
/// yet visible.
pub(crate) struct PlannedAck {
    job_id: JobId,
    token: LeaseToken,
}

pub(crate) enum AckPlan {
    Immediate(AckOutcome),
    Complete(PlannedAck),
}

/// Passive, bounded in-memory reference implementation of Quarry semantics.
///
/// Callers supply virtual time explicitly. This type performs no scheduling or
/// I/O and remains useful as a reference implementation when durable drivers
/// are added later. Instants supplied to one queue must be monotonically
/// nondecreasing; the direct engine does not attempt to repair a regressing
/// caller clock.
#[derive(Clone)]
pub struct InMemoryQueue {
    config: QueueConfig,
    incarnation: u64,
    jobs: BTreeMap<JobId, ActiveJob>,
    requests: BTreeMap<RequestId, RequestState>,
    completed_by_job: BTreeMap<JobId, RequestId>,
    completed_order: VecDeque<RequestId>,
    next_job_id: u64,
    next_lease_token: u64,
}

impl InMemoryQueue {
    /// Creates an empty queue with fixed resource bounds.
    #[must_use]
    pub fn new(config: QueueConfig) -> Self {
        Self::with_incarnation(config, 0)
    }

    /// Creates an empty queue that issues tokens in `incarnation`.
    #[must_use]
    pub fn with_incarnation(config: QueueConfig, incarnation: u64) -> Self {
        Self {
            config,
            incarnation,
            jobs: BTreeMap::new(),
            requests: BTreeMap::new(),
            completed_by_job: BTreeMap::new(),
            completed_order: VecDeque::new(),
            next_job_id: 0,
            next_lease_token: 0,
        }
    }

    /// Returns the incarnation used for newly issued lease tokens.
    #[must_use]
    pub const fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Selects the incarnation that will fence leases issued after replay.
    pub(crate) fn begin_incarnation(&mut self, incarnation: u64) {
        debug_assert!(
            self.jobs.values().all(|job| job.lease.is_none()),
            "durable replay does not restore ephemeral leases"
        );
        self.incarnation = incarnation;
        self.next_lease_token = 0;
    }

    /// Submits or deduplicates a request at `now`.
    pub fn submit(
        &mut self,
        request: SubmitRequest,
        now: SimInstant,
    ) -> Result<SubmitOutcome, QueueError> {
        self.expire_due(now);
        self.replay_submit(request)
    }

    /// Replays a submit without inventing a recovery-time clock value.
    pub(crate) fn replay_submit(
        &mut self,
        request: SubmitRequest,
    ) -> Result<SubmitOutcome, QueueError> {
        match self.plan_submit(request)? {
            SubmitPlan::Immediate(outcome) => Ok(outcome),
            SubmitPlan::Insert(plan) => Ok(self.apply_submit(plan)),
        }
    }

    /// Validates a submit without making its durable mutation visible.
    pub(crate) fn plan_submit(&self, request: SubmitRequest) -> Result<SubmitPlan, QueueError> {
        if request.payload.len() > self.config.max_payload_bytes {
            return Err(QueueError::PayloadTooLarge {
                size: request.payload.len(),
                limit: self.config.max_payload_bytes,
            });
        }
        if let Some(existing) = self.requests.get(&request.request_id) {
            return match existing {
                RequestState::Active { job_id }
                    if self
                        .jobs
                        .get(job_id)
                        .expect("active request index points to active job")
                        .spec
                        .matches(&request) =>
                {
                    Ok(SubmitPlan::Immediate(SubmitOutcome::DuplicateActive {
                        job_id: *job_id,
                    }))
                }
                RequestState::Completed(completed) if completed.spec.matches(&request) => {
                    Ok(SubmitPlan::Immediate(SubmitOutcome::DuplicateCompleted {
                        job_id: completed.job_id,
                    }))
                }
                RequestState::Active { .. } | RequestState::Completed(_) => {
                    Err(QueueError::RequestConflict {
                        request_id: request.request_id,
                    })
                }
            };
        }
        if self.jobs.len() >= self.config.active_capacity {
            return Err(QueueError::ActiveCapacityReached {
                limit: self.config.active_capacity,
            });
        }

        let job_id = JobId::new(self.next_job_id);
        let next_job_id = self
            .next_job_id
            .checked_add(1)
            .ok_or(QueueError::JobIdentifierExhausted)?;
        let spec = RequestSpec {
            payload: request.payload,
            not_before: request.not_before,
        };
        let available_at = spec.not_before;
        let job = ActiveJob {
            request_id: request.request_id,
            job_id,
            spec,
            available_at,
            lease: None,
        };
        Ok(SubmitPlan::Insert(PlannedSubmit { job, next_job_id }))
    }

    /// Applies a previously validated submit without further failure points.
    pub(crate) fn apply_submit(&mut self, plan: PlannedSubmit) -> SubmitOutcome {
        let PlannedSubmit { job, next_job_id } = plan;
        let request_id = job.request_id;
        let job_id = job.job_id;
        debug_assert_eq!(self.next_job_id, job_id.get());
        debug_assert!(!self.requests.contains_key(&request_id));
        debug_assert!(!self.jobs.contains_key(&job_id));
        self.next_job_id = next_job_id;
        self.jobs.insert(job_id, job);
        self.requests
            .insert(request_id, RequestState::Active { job_id });
        SubmitOutcome::Submitted { job_id }
    }

    /// Claims up to `max_jobs` eligible jobs under new fencing tokens.
    pub fn claim(
        &mut self,
        worker_id: WorkerId,
        max_jobs: usize,
        lease_for: SimDuration,
        now: SimInstant,
    ) -> Result<Vec<LeasedJob>, QueueError> {
        self.expire_due(now);
        if max_jobs > self.config.max_claim_batch {
            return Err(QueueError::ClaimBatchTooLarge {
                requested: max_jobs,
                limit: self.config.max_claim_batch,
            });
        }
        if lease_for == SimDuration::ZERO {
            return Err(QueueError::ZeroLeaseDuration);
        }
        let deadline = now
            .checked_add(lease_for)
            .ok_or(QueueError::DeadlineOverflow)?;
        let eligible: Vec<JobId> = self
            .jobs
            .iter()
            .filter_map(|(job_id, job)| {
                (job.lease.is_none() && job.available_at <= now).then_some(*job_id)
            })
            .take(max_jobs)
            .collect();
        let token_count =
            u64::try_from(eligible.len()).map_err(|_| QueueError::LeaseTokenExhausted)?;
        let next_lease_token = self
            .next_lease_token
            .checked_add(token_count)
            .ok_or(QueueError::LeaseTokenExhausted)?;

        let mut leased = Vec::with_capacity(eligible.len());
        for (offset, job_id) in eligible.into_iter().enumerate() {
            let offset = u64::try_from(offset).map_err(|_| QueueError::LeaseTokenExhausted)?;
            let token = LeaseToken::from_parts(self.incarnation, self.next_lease_token + offset);
            let job = self.jobs.get_mut(&job_id).expect("eligible job exists");
            job.lease = Some(Lease {
                worker_id,
                token,
                deadline,
            });
            leased.push(LeasedJob {
                request_id: job.request_id,
                job_id: job.job_id,
                payload: job.spec.payload.clone(),
                worker_id,
                lease_token: token,
                deadline,
            });
        }
        self.next_lease_token = next_lease_token;
        Ok(leased)
    }

    /// Replaces a live lease deadline with `now + lease_for`.
    pub fn renew(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        lease_for: SimDuration,
        now: SimInstant,
    ) -> Result<RenewOutcome, QueueError> {
        self.expire_due(now);
        if lease_for == SimDuration::ZERO {
            return Err(QueueError::ZeroLeaseDuration);
        }
        let deadline = now
            .checked_add(lease_for)
            .ok_or(QueueError::DeadlineOverflow)?;
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(QueueError::JobNotFound { job_id })?;
        let lease = Self::matching_lease(job, token)?;
        lease.deadline = deadline;
        Ok(RenewOutcome::Renewed { deadline })
    }

    /// Completes the job fenced by `token`.
    pub fn ack(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        now: SimInstant,
    ) -> Result<AckOutcome, QueueError> {
        self.expire_due(now);
        match self.plan_ack(job_id, token, now)? {
            AckPlan::Immediate(outcome) => Ok(outcome),
            AckPlan::Complete(plan) => Ok(self.apply_ack(plan)),
        }
    }

    /// Validates an acknowledgement without making its durable mutation
    /// visible. A lease due at `now` is treated as expired without mutating the
    /// queue, allowing durable callers to defer every visible change until the
    /// durability fence succeeds.
    pub(crate) fn plan_ack(
        &self,
        job_id: JobId,
        token: LeaseToken,
        now: SimInstant,
    ) -> Result<AckPlan, QueueError> {
        if let Some(completed) = self.completed_ack(job_id, token) {
            return completed.map(AckPlan::Immediate);
        }

        let job = self
            .jobs
            .get(&job_id)
            .ok_or(QueueError::JobNotFound { job_id })?;
        let lease = job
            .lease
            .as_ref()
            .ok_or(QueueError::JobNotLeased { job_id })?;
        if lease.deadline <= now {
            return Err(QueueError::JobNotLeased { job_id });
        }
        if lease.token != token {
            return Err(QueueError::StaleLeaseToken {
                job_id,
                provided: token,
            });
        }
        Ok(AckPlan::Complete(PlannedAck { job_id, token }))
    }

    /// Applies a previously validated acknowledgement without further failure
    /// points.
    pub(crate) fn apply_ack(&mut self, plan: PlannedAck) -> AckOutcome {
        let PlannedAck { job_id, token } = plan;
        let job = self.jobs.remove(&job_id).expect("active job exists");
        debug_assert_eq!(job.lease.map(|lease| lease.token), Some(token));
        self.complete_job(job, token)
    }

    /// Replays a durable acknowledgement without requiring its ephemeral
    /// lease to have survived a restart.
    pub(crate) fn replay_ack(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
    ) -> Result<AckOutcome, QueueError> {
        if let Some(completed) = self.completed_ack(job_id, token) {
            return completed;
        }

        let job = self
            .jobs
            .remove(&job_id)
            .ok_or(QueueError::JobNotFound { job_id })?;
        Ok(self.complete_job(job, token))
    }

    /// Resolves an acknowledgement against the completed-history index.
    ///
    /// Returns `None` when the job has not completed, the idempotent
    /// `AlreadyCompleted` outcome when the token matches the recorded
    /// acknowledgement, and a stale-token error otherwise.
    fn completed_ack(
        &self,
        job_id: JobId,
        token: LeaseToken,
    ) -> Option<Result<AckOutcome, QueueError>> {
        let request_id = self.completed_by_job.get(&job_id)?;
        let RequestState::Completed(completed) = self
            .requests
            .get(request_id)
            .expect("completed job index points to request history")
        else {
            unreachable!("completed job index points to active request");
        };
        Some(if completed.ack_token == token {
            Ok(AckOutcome::AlreadyCompleted)
        } else {
            Err(QueueError::StaleLeaseToken {
                job_id,
                provided: token,
            })
        })
    }

    /// Moves an already-removed active job into bounded completed history.
    ///
    /// This is the single place where the completed indexes advance:
    /// `requests`, `completed_by_job`, and `completed_order` move together,
    /// followed by history eviction.
    fn complete_job(&mut self, job: ActiveJob, token: LeaseToken) -> AckOutcome {
        let job_id = job.job_id;
        let completed = CompletedJob {
            request_id: job.request_id,
            job_id,
            spec: job.spec,
            ack_token: token,
        };
        let request_id = completed.request_id;
        self.requests
            .insert(request_id, RequestState::Completed(completed));
        self.completed_by_job.insert(job_id, request_id);
        self.completed_order.push_back(request_id);
        self.evict_completed_history();
        AckOutcome::Completed
    }

    /// Releases a live lease and delays the job by `retry_after`.
    pub fn nack(
        &mut self,
        job_id: JobId,
        token: LeaseToken,
        retry_after: SimDuration,
        now: SimInstant,
    ) -> Result<NackOutcome, QueueError> {
        self.expire_due(now);
        let available_at = now
            .checked_add(retry_after)
            .ok_or(QueueError::DeadlineOverflow)?;
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(QueueError::JobNotFound { job_id })?;
        Self::matching_lease(job, token)?;
        job.lease = None;
        job.available_at = available_at;
        Ok(NackOutcome::Requeued { available_at })
    }

    /// Applies one scheduled expiry when the token and deadline still match.
    pub fn expire(&mut self, job_id: JobId, token: LeaseToken, now: SimInstant) -> bool {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return false;
        };
        if job
            .lease
            .is_some_and(|lease| lease.token == token && lease.deadline <= now)
        {
            job.lease = None;
            true
        } else {
            false
        }
    }

    /// Returns semantic state after expiring every lease due at `now`.
    pub fn snapshot(&mut self, now: SimInstant) -> QueueSnapshot {
        self.expire_due(now);
        let jobs = self
            .jobs
            .values()
            .map(|job| JobSnapshot {
                request_id: job.request_id,
                job_id: job.job_id,
                payload: job.spec.payload.clone(),
                status: match job.lease {
                    Some(lease) => JobStatus::Leased {
                        worker_id: lease.worker_id,
                        lease_token: lease.token,
                        deadline: lease.deadline,
                    },
                    None if job.available_at > now => JobStatus::Delayed {
                        available_at: job.available_at,
                    },
                    None => JobStatus::Ready,
                },
            })
            .collect();
        let completed = self
            .completed_order
            .iter()
            .map(|request_id| {
                let RequestState::Completed(completed) = self
                    .requests
                    .get(request_id)
                    .expect("completed order points to request history")
                else {
                    unreachable!("completed order points to active request");
                };
                CompletedSnapshot {
                    request_id: completed.request_id,
                    job_id: completed.job_id,
                    payload: completed.spec.payload.clone(),
                    not_before: completed.spec.not_before,
                    ack_token: completed.ack_token,
                }
            })
            .collect();
        QueueSnapshot {
            now,
            active_capacity: self.config.active_capacity,
            jobs,
            completed,
        }
    }

    fn matching_lease(job: &mut ActiveJob, token: LeaseToken) -> Result<&mut Lease, QueueError> {
        let Some(lease) = job.lease.as_mut() else {
            return Err(QueueError::JobNotLeased { job_id: job.job_id });
        };
        if lease.token != token {
            return Err(QueueError::StaleLeaseToken {
                job_id: job.job_id,
                provided: token,
            });
        }
        Ok(lease)
    }

    pub(crate) fn expire_due(&mut self, now: SimInstant) {
        for job in self.jobs.values_mut() {
            if job.lease.is_some_and(|lease| lease.deadline <= now) {
                job.lease = None;
            }
        }
    }

    fn evict_completed_history(&mut self) {
        while self.completed_order.len() > self.config.completed_history_capacity {
            let request_id = self
                .completed_order
                .pop_front()
                .expect("history length was nonzero");
            let Some(RequestState::Completed(completed)) = self.requests.remove(&request_id) else {
                unreachable!("completed order points to completed request");
            };
            self.completed_by_job.remove(&completed.job_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: u64, payload: &[u8], not_before: u64) -> SubmitRequest {
        SubmitRequest {
            request_id: RequestId::new(id),
            payload: payload.to_vec(),
            not_before: SimInstant::from_nanos(not_before),
        }
    }

    #[test]
    fn submit_is_bounded_and_idempotent_while_retained() {
        let mut engine = InMemoryQueue::new(QueueConfig {
            active_capacity: 1,
            max_payload_bytes: 3,
            max_claim_batch: 1,
            completed_history_capacity: 1,
        });
        let now = SimInstant::ZERO;
        let submitted = engine.submit(request(1, b"one", 0), now).unwrap();
        let job_id = submitted.job_id();
        assert_eq!(
            engine.submit(request(1, b"one", 0), now),
            Ok(SubmitOutcome::DuplicateActive { job_id })
        );
        assert_eq!(
            engine.submit(request(1, b"two", 0), now),
            Err(QueueError::RequestConflict {
                request_id: RequestId::new(1)
            })
        );
        assert_eq!(
            engine.submit(request(2, b"two", 0), now),
            Err(QueueError::ActiveCapacityReached { limit: 1 })
        );
        assert_eq!(
            engine.submit(request(3, b"four", 0), now),
            Err(QueueError::PayloadTooLarge { size: 4, limit: 3 })
        );
    }

    #[test]
    fn durable_mutation_plans_are_invisible_until_applied() {
        let mut engine = InMemoryQueue::new(QueueConfig::default());
        let submit = match engine
            .plan_submit(request(1, b"job", 0))
            .expect("submit plan validates")
        {
            SubmitPlan::Insert(plan) => plan,
            SubmitPlan::Immediate(outcome) => panic!("unexpected immediate outcome: {outcome:?}"),
        };
        assert!(engine.jobs.is_empty());
        assert!(engine.requests.is_empty());

        let job_id = engine.apply_submit(submit).job_id();
        assert!(engine.jobs.contains_key(&job_id));
        assert!(matches!(
            engine.requests.get(&RequestId::new(1)),
            Some(RequestState::Active { job_id: found }) if *found == job_id
        ));

        let lease = engine
            .claim(
                WorkerId::new(7),
                1,
                SimDuration::from_nanos(5),
                SimInstant::ZERO,
            )
            .expect("claim succeeds")
            .pop()
            .expect("one job is leased");
        let ack = match engine
            .plan_ack(job_id, lease.lease_token, SimInstant::ZERO)
            .expect("ack plan validates")
        {
            AckPlan::Complete(plan) => plan,
            AckPlan::Immediate(outcome) => panic!("unexpected immediate outcome: {outcome:?}"),
        };
        assert!(engine.jobs.contains_key(&job_id));
        assert!(matches!(
            engine.requests.get(&RequestId::new(1)),
            Some(RequestState::Active { .. })
        ));

        assert_eq!(engine.apply_ack(ack), AckOutcome::Completed);
        assert!(!engine.jobs.contains_key(&job_id));
        assert!(matches!(
            engine.requests.get(&RequestId::new(1)),
            Some(RequestState::Completed(_))
        ));
    }

    #[test]
    fn leases_expire_and_stale_tokens_cannot_ack() {
        let mut engine = InMemoryQueue::new(QueueConfig::default());
        let job_id = engine
            .submit(request(1, b"job", 0), SimInstant::ZERO)
            .unwrap()
            .job_id();
        let first = engine
            .claim(
                WorkerId::new(1),
                1,
                SimDuration::from_nanos(5),
                SimInstant::ZERO,
            )
            .unwrap()
            .pop()
            .unwrap();
        let second = engine
            .claim(
                WorkerId::new(2),
                1,
                SimDuration::from_nanos(5),
                SimInstant::from_nanos(5),
            )
            .unwrap()
            .pop()
            .unwrap();
        assert_ne!(first.lease_token, second.lease_token);
        assert_eq!(
            engine.ack(job_id, first.lease_token, SimInstant::from_nanos(5)),
            Err(QueueError::StaleLeaseToken {
                job_id,
                provided: first.lease_token,
            })
        );
        assert_eq!(
            engine.ack(job_id, second.lease_token, SimInstant::from_nanos(5)),
            Ok(AckOutcome::Completed)
        );
        assert_eq!(
            engine.ack(job_id, second.lease_token, SimInstant::from_nanos(5)),
            Ok(AckOutcome::AlreadyCompleted)
        );
    }

    #[test]
    fn lease_tokens_are_scoped_to_the_queue_incarnation() {
        let mut first = InMemoryQueue::with_incarnation(QueueConfig::default(), 7);
        let mut second = InMemoryQueue::with_incarnation(QueueConfig::default(), 8);
        for engine in [&mut first, &mut second] {
            engine
                .submit(request(1, b"job", 0), SimInstant::ZERO)
                .unwrap();
        }

        let first_token = first
            .claim(
                WorkerId::new(1),
                1,
                SimDuration::from_nanos(1),
                SimInstant::ZERO,
            )
            .unwrap()[0]
            .lease_token;
        let second_token = second
            .claim(
                WorkerId::new(1),
                1,
                SimDuration::from_nanos(1),
                SimInstant::ZERO,
            )
            .unwrap()[0]
            .lease_token;

        assert_eq!(first.incarnation(), 7);
        assert_eq!(first_token, LeaseToken::from_parts(7, 0));
        assert_eq!(second_token, LeaseToken::from_parts(8, 0));
        assert_ne!(first_token, second_token);
        assert_eq!(first_token.to_string(), "7:0");
    }

    #[test]
    fn completed_history_is_bounded_and_frees_active_capacity() {
        let mut engine = InMemoryQueue::new(QueueConfig {
            active_capacity: 1,
            completed_history_capacity: 1,
            ..QueueConfig::default()
        });
        let now = SimInstant::ZERO;
        let first = engine.submit(request(1, b"first", 0), now).unwrap();
        let lease = engine
            .claim(WorkerId::new(1), 1, SimDuration::from_nanos(1), now)
            .unwrap()
            .pop()
            .unwrap();
        engine.ack(first.job_id(), lease.lease_token, now).unwrap();
        let second = engine.submit(request(2, b"second", 0), now).unwrap();
        let lease = engine
            .claim(WorkerId::new(1), 1, SimDuration::from_nanos(1), now)
            .unwrap()
            .pop()
            .unwrap();
        engine.ack(second.job_id(), lease.lease_token, now).unwrap();

        assert!(matches!(
            engine.submit(request(1, b"first", 0), now),
            Ok(SubmitOutcome::Submitted { .. })
        ));
        assert_eq!(engine.snapshot(now).completed.len(), 1);
    }

    #[test]
    fn nack_and_renew_use_virtual_time() {
        let mut engine = InMemoryQueue::new(QueueConfig::default());
        let job_id = engine
            .submit(request(1, b"job", 0), SimInstant::ZERO)
            .unwrap()
            .job_id();
        let lease = engine
            .claim(
                WorkerId::new(1),
                1,
                SimDuration::from_nanos(5),
                SimInstant::ZERO,
            )
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            engine.renew(
                job_id,
                lease.lease_token,
                SimDuration::from_nanos(10),
                SimInstant::from_nanos(2),
            ),
            Ok(RenewOutcome::Renewed {
                deadline: SimInstant::from_nanos(12)
            })
        );
        assert_eq!(
            engine.nack(
                job_id,
                lease.lease_token,
                SimDuration::from_nanos(4),
                SimInstant::from_nanos(3),
            ),
            Ok(NackOutcome::Requeued {
                available_at: SimInstant::from_nanos(7)
            })
        );
        assert!(
            engine
                .claim(
                    WorkerId::new(2),
                    1,
                    SimDuration::from_nanos(1),
                    SimInstant::from_nanos(6),
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            engine
                .claim(
                    WorkerId::new(2),
                    1,
                    SimDuration::from_nanos(1),
                    SimInstant::from_nanos(7),
                )
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn token_exhaustion_does_not_partially_lease_jobs() {
        let mut engine = InMemoryQueue::new(QueueConfig::default());
        engine
            .submit(request(1, b"one", 0), SimInstant::ZERO)
            .unwrap();
        engine
            .submit(request(2, b"two", 0), SimInstant::ZERO)
            .unwrap();
        engine.next_lease_token = u64::MAX - 1;

        assert_eq!(
            engine.claim(
                WorkerId::new(1),
                2,
                SimDuration::from_nanos(1),
                SimInstant::ZERO,
            ),
            Err(QueueError::LeaseTokenExhausted)
        );
        assert!(
            engine
                .snapshot(SimInstant::ZERO)
                .jobs
                .iter()
                .all(|job| job.status == JobStatus::Ready)
        );
        assert_eq!(engine.next_lease_token, u64::MAX - 1);
    }
}
