use kr_runtime::{SimDuration, SimInstant, SimRuntime};
use kr_runtime_io::{SimDisk, SimStorageConfig};
use kr_runtime_ring::file::test_support::{
    create_sim_ring as create_ring, open_sim_ring as open_ring, sim_storage_config,
};
use kr_runtime_ring::{RingLimits, file::FileRingConfig};
use quarry::{
    AckOutcome, DurableQueue, QueueConfig, RecoveryConfig, RequestId, SubmitOutcome, SubmitRequest,
    WorkerId,
};

const RECOVERY_READ_BYTES: usize = 512;

fn queue_config() -> QueueConfig {
    QueueConfig {
        active_capacity: 4,
        max_payload_bytes: 64,
        max_claim_batch: 2,
        completed_history_capacity: 4,
    }
}

fn ring_config() -> FileRingConfig {
    FileRingConfig {
        limits: RingLimits {
            max_record_bytes: 512,
            max_live_records: 32,
            max_live_payload_bytes: 32 * 512,
            max_read_records: 4,
            max_read_bytes: RECOVERY_READ_BYTES,
            max_batch_records: 1,
            max_batch_bytes: 512,
        },
        data_capacity_bytes: 32 * 1_024,
        max_io_request_bytes: 256,
        command_queue_capacity: 8,
    }
}

fn storage_config(config: FileRingConfig) -> SimStorageConfig {
    // Short transfers and mismatched chunk sizes keep provider-splitting
    // pressure on the recovery path; the shared scenario defaults are wide
    // enough not to split requests.
    SimStorageConfig {
        max_read_bytes: 256,
        max_write_bytes: 256,
        max_read_chunk: 47,
        max_write_chunk: 43,
        max_in_flight: 16,
        ..sim_storage_config(config, SimDuration::ZERO, 8)
    }
}

#[test]
fn durable_queue_recovers_completed_deduplication_from_file_ring() {
    let mut runtime = SimRuntime::default();
    let disk = SimDisk::default();
    let ring_config = ring_config();
    let storage_config = storage_config(ring_config);
    let recovery = RecoveryConfig::new(2, RECOVERY_READ_BYTES, 32);
    let request = SubmitRequest {
        request_id: RequestId::new(7),
        payload: b"simulated-file-job".to_vec(),
        not_before: SimInstant::ZERO,
    };
    let duplicate_request = request.clone();

    let (ring, storage) = create_ring(&mut runtime, &disk, ring_config, storage_config);
    let mut queue = runtime
        .block_on(DurableQueue::recover(queue_config(), ring, recovery))
        .expect("runtime completes initial queue recovery")
        .expect("empty queue recovers");
    let (returned_queue, submitted) = runtime
        .block_on(async move {
            let submitted = queue.submit(request, SimInstant::ZERO).await;
            (queue, submitted)
        })
        .expect("runtime completes submit");
    queue = returned_queue;
    let submitted = submitted.expect("submit is durable");
    let job_id = submitted.job_id();
    assert_eq!(submitted, SubmitOutcome::Submitted { job_id });

    let leased = queue
        .claim(
            WorkerId::new(1),
            1,
            SimDuration::from_nanos(10),
            SimInstant::ZERO,
        )
        .expect("claim submitted job")
        .pop()
        .expect("one job was eligible");
    let lease_token = leased.lease_token;
    let (returned_queue, acknowledged) = runtime
        .block_on(async move {
            let acknowledged = queue.ack(job_id, lease_token, SimInstant::ZERO).await;
            (queue, acknowledged)
        })
        .expect("runtime completes acknowledgement");
    queue = returned_queue;
    assert_eq!(acknowledged.expect("ack is durable"), AckOutcome::Completed);

    drop(queue.into_ring());
    storage.crash();

    let (ring, _storage) = open_ring(&mut runtime, &disk, ring_config, storage_config);
    let mut recovered = runtime
        .block_on(DurableQueue::recover(queue_config(), ring, recovery))
        .expect("runtime completes queue reopen")
        .expect("durable queue reopens");
    assert_eq!(recovered.incarnation(), 2);

    let (returned_queue, duplicate) = runtime
        .block_on(async move {
            let duplicate = recovered.submit(duplicate_request, SimInstant::ZERO).await;
            (recovered, duplicate)
        })
        .expect("runtime completes duplicate submit");
    recovered = returned_queue;
    assert_eq!(
        duplicate.expect("completed request is deduplicated"),
        SubmitOutcome::DuplicateCompleted { job_id }
    );

    let snapshot = recovered
        .snapshot(SimInstant::ZERO)
        .expect("snapshot recovered queue");
    assert!(snapshot.jobs.is_empty());
    assert_eq!(snapshot.completed.len(), 1);
    assert_eq!(snapshot.completed[0].job_id, job_id);
    assert_eq!(snapshot.completed[0].ack_token, lease_token);
}
