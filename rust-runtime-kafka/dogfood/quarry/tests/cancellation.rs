use std::cell::RefCell;
use std::rc::Rc;

use kr_runtime::{JoinError, SimDuration, SimInstant, SimRuntime, yield_now};
use quarry::{
    AckOutcome, JobStatus, LeasedJob, QueueConfig, QueueError, RequestId, SubmitRequest, WorkerId,
    start_broker,
};

#[test]
fn cancelled_worker_leaves_lease_to_expire_and_old_token_is_fenced() {
    let mut runtime = SimRuntime::default();
    let handle = runtime.handle();
    let (client, broker) = start_broker(handle.clone(), QueueConfig::default(), 32).unwrap();
    let observed_lease = Rc::new(RefCell::new(None::<LeasedJob>));

    runtime
        .block_on({
            let handle = handle.clone();
            let observed_lease = Rc::clone(&observed_lease);
            async move {
                let submitted = client
                    .submit(SubmitRequest {
                        request_id: RequestId::new(1),
                        payload: b"job".to_vec(),
                        not_before: SimInstant::ZERO,
                    })
                    .await
                    .unwrap();

                let worker_client = client.clone();
                let worker_timer = handle.clone();
                let worker_observation = Rc::clone(&observed_lease);
                let worker = handle
                    .spawn(async move {
                        let lease = worker_client
                            .claim(WorkerId::new(1), 1, SimDuration::from_nanos(10))
                            .await
                            .unwrap()
                            .pop()
                            .unwrap();
                        *worker_observation.borrow_mut() = Some(lease.clone());
                        worker_timer
                            .sleep(SimDuration::from_nanos(100))
                            .await
                            .unwrap();
                        worker_client
                            .ack(lease.job_id, lease.lease_token)
                            .await
                            .unwrap()
                    })
                    .unwrap();

                for _ in 0..16 {
                    if observed_lease.borrow().is_some() {
                        break;
                    }
                    yield_now().await;
                }
                let first = observed_lease
                    .borrow()
                    .clone()
                    .expect("worker did not claim within 16 scheduler yields");
                assert_eq!(first.job_id, submitted.job_id());

                worker.abort();
                assert_eq!(worker.await, Err(JoinError::Cancelled));
                assert!(
                    client
                        .claim(WorkerId::new(2), 1, SimDuration::from_nanos(10))
                        .await
                        .unwrap()
                        .is_empty(),
                    "cancellation must not roll back an accepted lease"
                );

                handle.sleep(SimDuration::from_nanos(10)).await.unwrap();
                let second = client
                    .claim(WorkerId::new(2), 1, SimDuration::from_nanos(10))
                    .await
                    .unwrap()
                    .pop()
                    .expect("expired lease was not redelivered");
                assert_eq!(second.job_id, first.job_id);
                assert_ne!(second.lease_token, first.lease_token);

                assert_eq!(
                    client.ack(first.job_id, first.lease_token).await,
                    Err(QueueError::StaleLeaseToken {
                        job_id: first.job_id,
                        provided: first.lease_token,
                    })
                );
                let snapshot = client.snapshot().await.unwrap();
                assert_eq!(
                    snapshot.jobs[0].status,
                    JobStatus::Leased {
                        worker_id: WorkerId::new(2),
                        lease_token: second.lease_token,
                        deadline: SimInstant::from_nanos(20),
                    }
                );
                assert_eq!(
                    client.ack(second.job_id, second.lease_token).await,
                    Ok(AckOutcome::Completed)
                );
                assert_eq!(
                    client.ack(second.job_id, second.lease_token).await,
                    Ok(AckOutcome::AlreadyCompleted)
                );

                client.shutdown().await.unwrap();
                broker.await.unwrap();
            }
        })
        .unwrap();

    runtime.shutdown().unwrap();
}
