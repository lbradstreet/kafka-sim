use std::collections::BTreeMap;
use std::fmt;

use kr_runtime::{
    JoinHandle, RuntimeDuration, RuntimeHandle, RuntimeInstant, SpawnError, yield_now,
};

use crate::engine::InMemoryQueue;
use crate::mailbox::{OneshotSender, Sender, TrySendError, WeakSender, channel, oneshot};
use crate::types::{
    AckOutcome, JobId, LeaseToken, LeasedJob, NackOutcome, QueueConfig, QueueError, QueueSnapshot,
    RenewOutcome, SubmitOutcome, SubmitRequest, WorkerId,
};

type Reply<T> = OneshotSender<Result<T, QueueError>>;

enum Command {
    Submit {
        request: SubmitRequest,
        reply: Reply<SubmitOutcome>,
    },
    Claim {
        worker_id: WorkerId,
        max_jobs: usize,
        lease_for: RuntimeDuration,
        reply: Reply<Vec<LeasedJob>>,
    },
    Renew {
        job_id: JobId,
        token: LeaseToken,
        lease_for: RuntimeDuration,
        reply: Reply<RenewOutcome>,
    },
    Ack {
        job_id: JobId,
        token: LeaseToken,
        reply: Reply<AckOutcome>,
    },
    Nack {
        job_id: JobId,
        token: LeaseToken,
        retry_after: RuntimeDuration,
        reply: Reply<NackOutcome>,
    },
    Snapshot {
        reply: Reply<QueueSnapshot>,
    },
    Expire {
        job_id: JobId,
        token: LeaseToken,
        generation: ExpiryGeneration,
    },
    Shutdown {
        reply: Reply<()>,
    },
}

/// The task handle for a running broker actor.
pub type BrokerJoin = JoinHandle<()>;

/// A failure that prevents a broker actor from starting.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BrokerStartError {
    /// A zero-capacity command mailbox could never accept work.
    ZeroCommandCapacity,
    /// The runtime rejected the broker task.
    Spawn(SpawnError),
}

impl fmt::Display for BrokerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCommandCapacity => {
                formatter.write_str("broker command capacity must be non-zero")
            }
            Self::Spawn(error) => write!(formatter, "broker task could not start: {error}"),
        }
    }
}

impl std::error::Error for BrokerStartError {}

/// Starts one bounded in-memory queue broker on either executor.
///
/// The handle may come from a deterministic simulation or a host runtime;
/// queue deadlines are measured on that runtime's timeline. The returned
/// client applies immediate backpressure when `command_capacity` commands are
/// already queued. Call [`QueueClient::shutdown`] and await the returned join
/// handle for orderly teardown.
///
/// The broker keeps at most one tracked expiry task per leased job. If the
/// runtime rejects an expiry task at its task limit, expiry remains logically
/// correct but lazy: the next queue operation or snapshot expires every due
/// lease before acting.
pub fn start_broker(
    handle: impl Into<RuntimeHandle>,
    config: QueueConfig,
    command_capacity: usize,
) -> Result<(QueueClient, BrokerJoin), BrokerStartError> {
    let handle = handle.into();
    if command_capacity == 0 {
        return Err(BrokerStartError::ZeroCommandCapacity);
    }
    let engine = InMemoryQueue::new(config);
    let (sender, receiver) = channel(command_capacity);
    let expiry_sender = sender.downgrade();
    let client = QueueClient {
        sender,
        max_payload_bytes: config.max_payload_bytes,
    };
    let actor_handle = handle.clone();
    let join = handle
        .spawn(async move { run_broker(actor_handle, engine, expiry_sender, receiver).await })
        .map_err(BrokerStartError::Spawn)?;
    Ok((client, join))
}

/// A cloneable, bounded command capability for one broker.
#[derive(Clone)]
pub struct QueueClient {
    sender: Sender<Command>,
    max_payload_bytes: usize,
}

impl QueueClient {
    /// Submits or deduplicates one job.
    pub async fn submit(&self, request: SubmitRequest) -> Result<SubmitOutcome, QueueError> {
        if request.payload.len() > self.max_payload_bytes {
            return Err(QueueError::PayloadTooLarge {
                size: request.payload.len(),
                limit: self.max_payload_bytes,
            });
        }
        self.request(|reply| Command::Submit { request, reply })
            .await
    }

    /// Claims up to `max_jobs` currently eligible jobs for `worker_id`.
    pub async fn claim(
        &self,
        worker_id: WorkerId,
        max_jobs: usize,
        lease_for: RuntimeDuration,
    ) -> Result<Vec<LeasedJob>, QueueError> {
        self.request(|reply| Command::Claim {
            worker_id,
            max_jobs,
            lease_for,
            reply,
        })
        .await
    }

    /// Replaces the matching lease deadline with `now + lease_for`.
    pub async fn renew_for(
        &self,
        job_id: JobId,
        token: LeaseToken,
        lease_for: RuntimeDuration,
    ) -> Result<RenewOutcome, QueueError> {
        self.request(|reply| Command::Renew {
            job_id,
            token,
            lease_for,
            reply,
        })
        .await
    }

    /// Completes a job if `token` is its current lease token.
    pub async fn ack(&self, job_id: JobId, token: LeaseToken) -> Result<AckOutcome, QueueError> {
        self.request(|reply| Command::Ack {
            job_id,
            token,
            reply,
        })
        .await
    }

    /// Releases a lease and delays the next claim by `retry_after`.
    pub async fn nack(
        &self,
        job_id: JobId,
        token: LeaseToken,
        retry_after: RuntimeDuration,
    ) -> Result<NackOutcome, QueueError> {
        self.request(|reply| Command::Nack {
            job_id,
            token,
            retry_after,
            reply,
        })
        .await
    }

    /// Returns a semantic snapshot at the broker's current time.
    pub async fn snapshot(&self) -> Result<QueueSnapshot, QueueError> {
        self.request(|reply| Command::Snapshot { reply }).await
    }

    /// Requests orderly broker termination.
    pub async fn shutdown(&self) -> Result<(), QueueError> {
        self.request(|reply| Command::Shutdown { reply }).await
    }

    /// Returns the current number of commands waiting for the broker.
    #[must_use]
    pub fn pending_commands(&self) -> usize {
        self.sender.len()
    }

    /// Returns the broker's command mailbox capacity.
    #[must_use]
    pub fn command_capacity(&self) -> usize {
        self.sender.capacity()
    }

    async fn request<T>(
        &self,
        make_command: impl FnOnce(Reply<T>) -> Command,
    ) -> Result<T, QueueError> {
        let (reply, response) = oneshot();
        let command = make_command(reply);
        match self.sender.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(QueueError::Backpressure {
                    limit: self.sender.capacity(),
                });
            }
            Err(TrySendError::Closed(_)) => return Err(QueueError::BrokerStopped),
        }
        response.await.map_err(|_| QueueError::BrokerStopped)?
    }
}

async fn run_broker(
    handle: RuntimeHandle,
    mut engine: InMemoryQueue,
    expiry_sender: WeakSender<Command>,
    mut receiver: crate::mailbox::Receiver<Command>,
) {
    let mut expiries: BTreeMap<JobId, TrackedExpiry> = BTreeMap::new();
    let mut next_expiry_generation = 0_u64;
    while let Some(command) = receiver.recv().await {
        let now = handle.now();
        match command {
            Command::Submit { request, reply } => {
                let _ = reply.send(engine.submit(request, now));
            }
            Command::Claim {
                worker_id,
                max_jobs,
                lease_for,
                reply,
            } => {
                let result = engine.claim(worker_id, max_jobs, lease_for, now);
                if let Ok(leases) = &result {
                    for lease in leases {
                        replace_expiry(
                            &mut expiries,
                            &mut next_expiry_generation,
                            &handle,
                            expiry_sender.clone(),
                            lease.job_id,
                            lease.lease_token,
                            lease.deadline,
                        );
                    }
                }
                let _ = reply.send(result);
            }
            Command::Renew {
                job_id,
                token,
                lease_for,
                reply,
            } => {
                let result = engine.renew(job_id, token, lease_for, now);
                if let Ok(RenewOutcome::Renewed { deadline }) = result {
                    replace_expiry(
                        &mut expiries,
                        &mut next_expiry_generation,
                        &handle,
                        expiry_sender.clone(),
                        job_id,
                        token,
                        deadline,
                    );
                }
                let _ = reply.send(result);
            }
            Command::Ack {
                job_id,
                token,
                reply,
            } => {
                let result = engine.ack(job_id, token, now);
                if result.is_ok() {
                    cancel_expiry(&mut expiries, job_id, token);
                }
                let _ = reply.send(result);
            }
            Command::Nack {
                job_id,
                token,
                retry_after,
                reply,
            } => {
                let result = engine.nack(job_id, token, retry_after, now);
                if result.is_ok() {
                    cancel_expiry(&mut expiries, job_id, token);
                }
                let _ = reply.send(result);
            }
            Command::Snapshot { reply } => {
                let _ = reply.send(Ok(engine.snapshot(now)));
            }
            Command::Expire {
                job_id,
                token,
                generation,
            } => {
                complete_expiry(&mut expiries, job_id, token, generation);
                let _ = engine.expire(job_id, token, now);
            }
            Command::Shutdown { reply } => {
                drop(expiries);
                let _ = reply.send(Ok(()));
                break;
            }
        }
        yield_now().await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpiryGeneration(u64);

struct TrackedExpiry {
    token: LeaseToken,
    generation: ExpiryGeneration,
    join: JoinHandle<()>,
    abort_on_drop: bool,
}

impl Drop for TrackedExpiry {
    fn drop(&mut self) {
        if self.abort_on_drop {
            self.join.abort();
        }
    }
}

fn replace_expiry(
    expiries: &mut BTreeMap<JobId, TrackedExpiry>,
    next_expiry_generation: &mut u64,
    handle: &RuntimeHandle,
    sender: WeakSender<Command>,
    job_id: JobId,
    token: LeaseToken,
    deadline: RuntimeInstant,
) {
    let Some(generation) = allocate_expiry_generation(next_expiry_generation) else {
        return;
    };
    let Some(join) = spawn_expiry(handle, sender, job_id, token, generation, deadline) else {
        return;
    };
    let _ = expiries.insert(
        job_id,
        TrackedExpiry {
            token,
            generation,
            join,
            abort_on_drop: true,
        },
    );
}

fn allocate_expiry_generation(next: &mut u64) -> Option<ExpiryGeneration> {
    let generation = ExpiryGeneration(*next);
    *next = next.checked_add(1)?;
    Some(generation)
}

fn cancel_expiry(expiries: &mut BTreeMap<JobId, TrackedExpiry>, job_id: JobId, token: LeaseToken) {
    if expiries
        .get(&job_id)
        .is_some_and(|expiry| expiry.token == token)
    {
        let _ = expiries.remove(&job_id);
    }
}

fn complete_expiry(
    expiries: &mut BTreeMap<JobId, TrackedExpiry>,
    job_id: JobId,
    token: LeaseToken,
    generation: ExpiryGeneration,
) {
    if expiries
        .get(&job_id)
        .is_some_and(|expiry| expiry.token == token && expiry.generation == generation)
    {
        let mut expiry = expiries.remove(&job_id).expect("tracked expiry exists");
        expiry.abort_on_drop = false;
    }
}

fn spawn_expiry(
    handle: &RuntimeHandle,
    sender: WeakSender<Command>,
    job_id: JobId,
    token: LeaseToken,
    generation: ExpiryGeneration,
    deadline: RuntimeInstant,
) -> Option<BrokerJoin> {
    let timer = handle.clone();
    handle
        .spawn(async move {
            if timer.sleep_until(deadline).await.is_err() {
                return;
            }
            let _ = sender.try_send(Command::Expire {
                job_id,
                token,
                generation,
            });
        })
        .ok()
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::panic_any;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use kr_runtime::{HostRuntime, RuntimeConfig, SimDuration, SimInstant, SimRuntime};

    use super::*;
    use crate::{RequestId, SubmitRequest};

    struct PanickingWake;

    struct PanickingPayload;

    impl Drop for PanickingPayload {
        fn drop(&mut self) {
            panic!("injected response wake payload destructor panic");
        }
    }

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic_any(PanickingPayload);
        }
    }

    #[test]
    fn host_broker_round_trip_expires_lease_and_shuts_down() {
        let mut runtime = HostRuntime::default();
        let handle = runtime.handle();
        let (client, broker) = start_broker(handle.clone(), QueueConfig::default(), 8).unwrap();

        runtime
            .block_on(async move {
                let submitted = client
                    .submit(SubmitRequest {
                        request_id: RequestId::new(1),
                        payload: b"job".to_vec(),
                        not_before: RuntimeInstant::ZERO,
                    })
                    .await
                    .unwrap();
                let first = client
                    .claim(
                        WorkerId::new(9),
                        1,
                        RuntimeDuration::from_millis(5).expect("duration fits"),
                    )
                    .await
                    .unwrap()
                    .pop()
                    .unwrap();
                assert_eq!(first.job_id, submitted.job_id());

                handle
                    .sleep(RuntimeDuration::from_millis(10).expect("duration fits"))
                    .await
                    .unwrap();
                let second = client
                    .claim(
                        WorkerId::new(10),
                        1,
                        RuntimeDuration::from_secs(60).expect("duration fits"),
                    )
                    .await
                    .unwrap()
                    .pop()
                    .expect("expired lease is available again");
                assert_eq!(second.job_id, first.job_id);
                assert_ne!(second.lease_token, first.lease_token);
                assert_eq!(
                    client.ack(second.job_id, second.lease_token).await,
                    Ok(AckOutcome::Completed)
                );
                client.shutdown().await.unwrap();
                broker.await.unwrap();
            })
            .unwrap();
        runtime.finish().unwrap();
    }

    #[test]
    fn maximum_command_capacity_is_a_logical_bound() {
        let mut runtime = SimRuntime::default();
        let (client, broker) = start_broker(runtime.handle(), QueueConfig::default(), usize::MAX)
            .expect("a logical capacity must not be reserved eagerly");
        assert_eq!(client.command_capacity(), usize::MAX);

        runtime
            .block_on(async move {
                client.shutdown().await.expect("shutdown is admitted");
                broker.await.expect("broker stops cleanly");
            })
            .expect("runtime completes broker shutdown");
    }

    #[test]
    fn accepted_request_survives_client_cancellation_and_mailbox_is_bounded() {
        let mut runtime = SimRuntime::default();
        let (client, broker) = start_broker(runtime.handle(), QueueConfig::default(), 1).unwrap();
        let request = SubmitRequest {
            request_id: RequestId::new(1),
            payload: b"job".to_vec(),
            not_before: SimInstant::ZERO,
        };
        let mut context = Context::from_waker(Waker::noop());

        let mut accepted = Box::pin(client.submit(request.clone()));
        assert_eq!(accepted.as_mut().poll(&mut context), Poll::Pending);
        let mut rejected = Box::pin(client.submit(SubmitRequest {
            request_id: RequestId::new(2),
            payload: b"other".to_vec(),
            not_before: SimInstant::ZERO,
        }));
        assert_eq!(
            rejected.as_mut().poll(&mut context),
            Poll::Ready(Err(QueueError::Backpressure { limit: 1 }))
        );
        drop(accepted);
        drop(rejected);

        runtime
            .block_on(async move {
                assert!(matches!(
                    client.submit(request).await,
                    Ok(SubmitOutcome::DuplicateActive { .. })
                ));
                client.shutdown().await.unwrap();
                broker.await.unwrap();
            })
            .unwrap();
    }

    #[test]
    fn oversized_submit_is_rejected_before_mailbox_admission() {
        let mut runtime = SimRuntime::default();
        let config = QueueConfig {
            max_payload_bytes: 3,
            ..QueueConfig::default()
        };
        let (client, broker) = start_broker(runtime.handle(), config, 1).unwrap();
        let mut context = Context::from_waker(Waker::noop());

        let mut accepted = Box::pin(client.submit(SubmitRequest {
            request_id: RequestId::new(1),
            payload: b"ok".to_vec(),
            not_before: SimInstant::ZERO,
        }));
        assert_eq!(accepted.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(client.pending_commands(), 1);

        let mut oversized = Box::pin(client.submit(SubmitRequest {
            request_id: RequestId::new(2),
            payload: b"huge".to_vec(),
            not_before: SimInstant::ZERO,
        }));
        assert_eq!(
            oversized.as_mut().poll(&mut context),
            Poll::Ready(Err(QueueError::PayloadTooLarge { size: 4, limit: 3 }))
        );
        assert_eq!(
            client.pending_commands(),
            1,
            "an invalid request must not occupy bounded mailbox capacity"
        );

        drop(accepted);
        drop(oversized);
        runtime.step().expect("broker processes accepted submit");
        runtime
            .block_on(async move {
                client.shutdown().await.unwrap();
                broker.await.unwrap();
            })
            .unwrap();
    }

    #[test]
    fn panicking_response_waker_and_payload_drop_do_not_kill_broker() {
        let mut runtime = SimRuntime::default();
        let (client, broker) = start_broker(runtime.handle(), QueueConfig::default(), 4).unwrap();
        let waker = Waker::from(Arc::new(PanickingWake));
        let mut context = Context::from_waker(&waker);
        let mut submit = Box::pin(client.submit(SubmitRequest {
            request_id: RequestId::new(1),
            payload: b"job".to_vec(),
            not_before: SimInstant::ZERO,
        }));

        assert_eq!(submit.as_mut().poll(&mut context), Poll::Pending);
        runtime
            .step()
            .expect("response wake panic is isolated from the broker actor");
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            submit.as_mut().poll(&mut context),
            Poll::Ready(Ok(SubmitOutcome::Submitted { .. }))
        ));
        drop(submit);

        runtime
            .block_on(async move {
                assert_eq!(client.snapshot().await.unwrap().jobs.len(), 1);
                client.shutdown().await.unwrap();
                broker.await.unwrap();
            })
            .unwrap();
    }

    #[test]
    fn renewal_churn_and_shutdown_keep_expiry_tasks_bounded() {
        let mut runtime = SimRuntime::default();
        let handle = runtime.handle();
        let (client, broker) = start_broker(handle.clone(), QueueConfig::default(), 8).unwrap();

        runtime
            .block_on({
                let handle = handle.clone();
                async move {
                    client
                        .submit(SubmitRequest {
                            request_id: RequestId::new(1),
                            payload: b"job".to_vec(),
                            not_before: SimInstant::ZERO,
                        })
                        .await
                        .unwrap();
                    let lease = client
                        .claim(WorkerId::new(1), 1, SimDuration::from_nanos(1_000))
                        .await
                        .unwrap()
                        .pop()
                        .unwrap();

                    for _ in 0..32 {
                        client
                            .renew_for(
                                lease.job_id,
                                lease.lease_token,
                                SimDuration::from_nanos(1_000),
                            )
                            .await
                            .unwrap();
                        let snapshot = handle.snapshot();
                        assert_eq!(snapshot.live_timers, 1);
                        assert_eq!(snapshot.tasks.len(), 3, "root + broker + one expiry");
                    }

                    client.shutdown().await.unwrap();
                    broker.await.unwrap();
                    let snapshot = handle.snapshot();
                    assert_eq!(snapshot.live_timers, 0);
                    assert_eq!(snapshot.tasks.len(), 1, "only the root remains");
                }
            })
            .unwrap();
        assert!(runtime.snapshot().tasks.is_empty());
    }

    #[test]
    fn stale_expiry_does_not_untrack_renewed_lease_timer() {
        let mut runtime = SimRuntime::default();
        let join = runtime
            .handle()
            .spawn(std::future::pending())
            .expect("spawn expiry probe");
        let abort = join.abort_handle();
        let job_id = JobId::new(1);
        let token = LeaseToken::new(1);
        let current_generation = ExpiryGeneration(2);
        let mut expiries = BTreeMap::from([(
            job_id,
            TrackedExpiry {
                token,
                generation: current_generation,
                join,
                abort_on_drop: true,
            },
        )]);

        complete_expiry(&mut expiries, job_id, token, ExpiryGeneration(1));
        assert_eq!(expiries.len(), 1, "stale generation retained current timer");
        complete_expiry(&mut expiries, job_id, token, current_generation);
        assert!(expiries.is_empty());
        assert!(
            !abort.is_abort_requested(),
            "a timer completing itself must not be aborted"
        );

        abort.abort();
        runtime.step().expect("clean up expiry probe");
        assert!(runtime.snapshot().tasks.is_empty());
    }

    #[test]
    fn dropping_last_client_stops_broker_and_aborts_expiry_tasks() {
        let mut runtime = SimRuntime::default();
        let handle = runtime.handle();
        let (client, broker) = start_broker(handle.clone(), QueueConfig::default(), 8).unwrap();

        runtime
            .block_on({
                let handle = handle.clone();
                async move {
                    client
                        .submit(SubmitRequest {
                            request_id: RequestId::new(1),
                            payload: b"job".to_vec(),
                            not_before: SimInstant::ZERO,
                        })
                        .await
                        .unwrap();
                    client
                        .claim(WorkerId::new(1), 1, SimDuration::from_nanos(1_000_000))
                        .await
                        .unwrap();
                    assert_eq!(handle.snapshot().live_timers, 1);

                    drop(client);
                    broker.await.unwrap();
                    let snapshot = handle.snapshot();
                    assert_eq!(snapshot.live_timers, 0);
                    assert_eq!(snapshot.tasks.len(), 1, "only the root remains");
                }
            })
            .unwrap();
        assert!(runtime.snapshot().tasks.is_empty());
    }

    #[test]
    fn expiry_delivery_does_not_spin_when_mailbox_is_full() {
        let mut runtime = SimRuntime::new(RuntimeConfig {
            max_steps_per_run: 32,
            ..RuntimeConfig::default()
        });
        let handle = runtime.handle();
        let (sender, receiver) = channel(1);
        let weak = sender.downgrade();
        let (reply, _response) = oneshot();
        assert!(sender.try_send(Command::Snapshot { reply }).is_ok());
        let expiry = spawn_expiry(
            &RuntimeHandle::Sim(handle),
            weak,
            JobId::new(1),
            LeaseToken::new(1),
            ExpiryGeneration(1),
            SimInstant::from_nanos(1),
        )
        .unwrap();

        runtime
            .block_on(async move {
                expiry.await.unwrap();
                assert_eq!(sender.len(), 1);
                assert_eq!(receiver.len(), 1);
            })
            .unwrap();
    }

    #[test]
    fn one_broker_poll_processes_at_most_one_queued_command() {
        let mut runtime = SimRuntime::default();
        let (client, broker) = start_broker(runtime.handle(), QueueConfig::default(), 4).unwrap();
        let mut context = Context::from_waker(Waker::noop());
        let mut requests: Vec<_> = (0..3)
            .map(|id| {
                Box::pin(client.submit(SubmitRequest {
                    request_id: RequestId::new(id),
                    payload: vec![id as u8],
                    not_before: SimInstant::ZERO,
                }))
            })
            .collect();
        for request in &mut requests {
            assert_eq!(request.as_mut().poll(&mut context), Poll::Pending);
        }
        assert_eq!(client.pending_commands(), 3);

        runtime.step().unwrap();

        assert_eq!(client.pending_commands(), 2);
        drop(requests);
        runtime
            .block_on(async move {
                client.shutdown().await.unwrap();
                broker.await.unwrap();
            })
            .unwrap();
    }

    #[test]
    fn aborting_broker_aborts_tracked_expiry_tasks() {
        let mut runtime = SimRuntime::default();
        let handle = runtime.handle();
        let (client, broker) = start_broker(handle.clone(), QueueConfig::default(), 8).unwrap();

        runtime
            .block_on({
                let handle = handle.clone();
                async move {
                    client
                        .submit(SubmitRequest {
                            request_id: RequestId::new(1),
                            payload: b"job".to_vec(),
                            not_before: SimInstant::ZERO,
                        })
                        .await
                        .unwrap();
                    client
                        .claim(WorkerId::new(1), 1, SimDuration::from_nanos(1_000_000))
                        .await
                        .unwrap();
                    assert_eq!(handle.snapshot().live_timers, 1);

                    broker.abort();
                    assert_eq!(broker.await, Err(kr_runtime::JoinError::Cancelled));
                    let snapshot = handle.snapshot();
                    assert_eq!(snapshot.live_timers, 0);
                    assert_eq!(snapshot.tasks.len(), 1, "only the root remains");
                    assert_eq!(client.snapshot().await, Err(QueueError::BrokerStopped));
                }
            })
            .unwrap();
        assert!(runtime.snapshot().tasks.is_empty());
    }
}
