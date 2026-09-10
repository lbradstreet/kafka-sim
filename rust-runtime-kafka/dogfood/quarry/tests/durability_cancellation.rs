#[allow(dead_code)]
mod support;

use std::cell::RefCell;
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use kr_runtime::{CompletionError, CompletionResult, SimDuration, SimInstant};
use kr_runtime_ring::{
    AppendFailure, AppendRequest, AppendSuccess, ReadPage, ReadRequest, RingCursor, RingError,
    RingOperation, RingPosition, RingReader, RingRecord, RingStatus, RingWriter, SyncFailure,
    SyncSuccess, TrimSuccess,
};
use quarry::{
    DEFAULT_MAX_REPLAY_RECORDS, DEFAULT_RECOVERY_READ_BYTES, DurableQueue, DurableQueueError,
    JobId, LeaseToken, QueueConfig, RecoveryConfig, RequestId, SubmitOutcome, SubmitRequest,
    WorkerId,
};
use support::{assert_every_operation_is_fenced, complete};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum PauseAt {
    #[default]
    Nowhere,
    Read,
    Append,
    Sync,
}

#[derive(Default)]
struct ControlledState {
    pause_at: PauseAt,
    append_foreign_before_sync: bool,
    fail_sync_after_apply: bool,
    report_sync_tail_behind: bool,
    records: Vec<Vec<u8>>,
    accepted_head: usize,
    durable_head: usize,
    durable_count: usize,
    read_calls: usize,
    append_calls: usize,
    sync_calls: usize,
}

#[derive(Clone, Default)]
struct ControlledRing {
    shared: Rc<RefCell<ControlledState>>,
}

impl ControlledRing {
    fn pause_at(&self, pause_at: PauseAt) {
        self.shared.borrow_mut().pause_at = pause_at;
    }

    fn append_calls(&self) -> usize {
        self.shared.borrow().append_calls
    }

    fn read_calls(&self) -> usize {
        self.shared.borrow().read_calls
    }

    fn sync_calls(&self) -> usize {
        self.shared.borrow().sync_calls
    }

    fn accepted_and_durable_counts(&self) -> (usize, usize) {
        let state = self.shared.borrow();
        (state.records.len(), state.durable_count)
    }

    fn append_foreign_before_sync(&self) {
        self.shared.borrow_mut().append_foreign_before_sync = true;
    }

    fn fail_sync_after_apply(&self) {
        self.shared.borrow_mut().fail_sync_after_apply = true;
    }

    fn report_sync_tail_behind(&self) {
        self.shared.borrow_mut().report_sync_tail_behind = true;
    }

    fn append_now(&self, request: AppendRequest) -> CompletionResult<AppendSuccess, AppendFailure> {
        let AppendRequest {
            records,
            expected_accepted_tail,
        } = request;
        if records.is_empty() {
            return Err(CompletionError::not_applied(AppendFailure {
                error: RingError::EmptyBatch,
                records,
                accepted_range: None,
            }));
        }

        let mut state = self.shared.borrow_mut();
        let first = match u64::try_from(state.records.len()) {
            Ok(position) => position,
            Err(_) => {
                return Err(CompletionError::not_applied(AppendFailure {
                    error: RingError::PositionExhausted,
                    records,
                    accepted_range: None,
                }));
            }
        };
        let actual = RingCursor::new(first);
        if let Some(expected) = expected_accepted_tail
            && expected != actual
        {
            return Err(CompletionError::not_applied(AppendFailure {
                error: RingError::PositionConflict { expected, actual },
                records,
                accepted_range: None,
            }));
        }

        let count = match u64::try_from(records.len()) {
            Ok(count) => count,
            Err(_) => {
                return Err(CompletionError::not_applied(AppendFailure {
                    error: RingError::PositionExhausted,
                    records,
                    accepted_range: None,
                }));
            }
        };
        let Some(next) = first.checked_add(count) else {
            return Err(CompletionError::not_applied(AppendFailure {
                error: RingError::PositionExhausted,
                records,
                accepted_range: None,
            }));
        };

        state.records.extend(records.iter().cloned());
        Ok(AppendSuccess {
            first_position: RingPosition::new(first),
            next_cursor: RingCursor::new(next),
            records,
        })
    }

    fn sync_now(&self) -> CompletionResult<SyncSuccess, SyncFailure> {
        let mut state = self.shared.borrow_mut();
        if state.append_foreign_before_sync {
            state.records.push(b"foreign".to_vec());
            state.append_foreign_before_sync = false;
        }
        state.durable_count = state.records.len();
        state.durable_head = state.accepted_head;
        let reported_count = if state.report_sync_tail_behind {
            state.report_sync_tail_behind = false;
            state.durable_count.saturating_sub(1)
        } else {
            state.durable_count
        };
        let durable_tail = match u64::try_from(reported_count) {
            Ok(next) => RingCursor::new(next),
            Err(_) => {
                return Err(CompletionError::not_applied(SyncFailure {
                    error: RingError::PositionExhausted,
                    checkpoint: None,
                }));
            }
        };
        let checkpoint = SyncSuccess {
            durable_head: RingCursor::new(
                u64::try_from(state.durable_head).expect("test durable head fits in u64"),
            ),
            durable_tail,
            reclaimed_records: 0,
            reclaimed_payload_bytes: 0,
        };
        if state.fail_sync_after_apply {
            state.fail_sync_after_apply = false;
            return Err(CompletionError::applied(SyncFailure {
                error: RingError::BackendFailure {
                    operation: RingOperation::Sync,
                    raw_os_error: None,
                    message: "deterministic injected fault".to_owned(),
                },
                checkpoint: Some(checkpoint),
            }));
        }
        Ok(checkpoint)
    }

    fn read_now(&self, request: ReadRequest) -> CompletionResult<ReadPage, RingError> {
        if request.max_records == 0 {
            return Err(CompletionError::not_applied(RingError::ZeroReadRecordLimit));
        }
        if request.max_bytes == 0 {
            return Err(CompletionError::not_applied(RingError::ZeroReadByteLimit));
        }

        let state = self.shared.borrow();
        let durable_head = RingCursor::new(
            u64::try_from(state.durable_head).expect("test durable head fits in u64"),
        );
        if request.cursor < durable_head {
            return Err(CompletionError::not_applied(RingError::CursorExpired {
                requested: request.cursor,
                oldest: durable_head,
            }));
        }
        let Ok(start) = usize::try_from(request.cursor.get()) else {
            return Ok(ReadPage {
                records: Vec::new(),
                next_cursor: request.cursor,
                has_more: false,
                payload_bytes: 0,
            });
        };
        if start >= state.durable_count {
            return Ok(ReadPage {
                records: Vec::new(),
                next_cursor: request.cursor,
                has_more: false,
                payload_bytes: 0,
            });
        }
        let record_limit = start
            .saturating_add(request.max_records)
            .min(state.durable_count);
        let mut payload_bytes = 0_usize;
        let mut end = start;
        while end < record_limit {
            let next_size = state.records[end].len();
            let Some(next_payload_bytes) = payload_bytes.checked_add(next_size) else {
                return Err(CompletionError::not_applied(RingError::PayloadSizeOverflow));
            };
            if next_payload_bytes > request.max_bytes {
                if end == start {
                    return Err(CompletionError::not_applied(
                        RingError::ReadBudgetTooSmall {
                            needed: next_size,
                            available: request.max_bytes,
                        },
                    ));
                }
                break;
            }
            payload_bytes = next_payload_bytes;
            end += 1;
        }
        let records = state.records[start..end]
            .iter()
            .enumerate()
            .map(|(offset, buffer)| RingRecord {
                position: RingPosition::new(
                    u64::try_from(start + offset).expect("test ring position fits in u64"),
                ),
                buffer: buffer.clone(),
            })
            .collect::<Vec<_>>();
        let next_cursor =
            RingCursor::new(u64::try_from(end).expect("test continuation fits in u64"));
        Ok(ReadPage {
            records,
            next_cursor,
            has_more: end < state.durable_count,
            payload_bytes,
        })
    }

    fn status_now(&self) -> CompletionResult<RingStatus, RingError> {
        let state = self.shared.borrow();
        let accepted_tail = RingCursor::new(
            u64::try_from(state.records.len()).expect("test accepted tail fits in u64"),
        );
        let durable_tail = RingCursor::new(
            u64::try_from(state.durable_count).expect("test durable tail fits in u64"),
        );
        let accepted_head = RingCursor::new(
            u64::try_from(state.accepted_head).expect("test accepted head fits in u64"),
        );
        let durable_head = RingCursor::new(
            u64::try_from(state.durable_head).expect("test durable head fits in u64"),
        );
        let accepted_payload_bytes = state.records[state.accepted_head..]
            .iter()
            .map(Vec::len)
            .sum();
        let retained_payload_bytes = state.records[state.durable_head..]
            .iter()
            .map(Vec::len)
            .sum();
        Ok(RingStatus {
            accepted_head,
            accepted_tail,
            durable_head,
            durable_tail,
            accepted_live_records: state.records.len() - state.accepted_head,
            accepted_live_payload_bytes: accepted_payload_bytes,
            retained_records: state.records.len() - state.durable_head,
            retained_payload_bytes,
            pending_reclaim_records: state.accepted_head - state.durable_head,
            pending_reclaim_payload_bytes: state.records[state.durable_head..state.accepted_head]
                .iter()
                .map(Vec::len)
                .sum(),
            max_live_records: usize::MAX,
            max_live_payload_bytes: usize::MAX,
            physical: None,
        })
    }
}

/// A one-shot future that either completes with a precomputed result or
/// stays pending forever while holding that result.
enum ControlledFuture<T> {
    Ready(Option<T>),
    Pending { _result: T },
}

impl<T> ControlledFuture<T> {
    fn new(result: T, pause: bool) -> Self {
        if pause {
            Self::Pending { _result: result }
        } else {
            Self::Ready(Some(result))
        }
    }
}

impl<T: Unpin> Future for ControlledFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(output) => {
                Poll::Ready(output.take().expect("future polled after completion"))
            }
            Self::Pending { .. } => Poll::Pending,
        }
    }
}

impl RingReader for ControlledRing {
    type ReadFuture = ControlledFuture<CompletionResult<ReadPage, RingError>>;
    type StatusFuture = Ready<CompletionResult<RingStatus, RingError>>;

    fn read(&self, request: ReadRequest) -> Self::ReadFuture {
        let pause = {
            let mut state = self.shared.borrow_mut();
            state.read_calls += 1;
            state.pause_at == PauseAt::Read
        };
        ControlledFuture::new(self.read_now(request), pause)
    }

    fn status(&self) -> Self::StatusFuture {
        ready(self.status_now())
    }
}

impl RingWriter for ControlledRing {
    type AppendFuture = ControlledFuture<CompletionResult<AppendSuccess, AppendFailure>>;
    type TrimFuture = Ready<CompletionResult<TrimSuccess, RingError>>;
    type SyncFuture = ControlledFuture<CompletionResult<SyncSuccess, SyncFailure>>;

    fn append(&self, request: AppendRequest) -> Self::AppendFuture {
        let pause = {
            let mut state = self.shared.borrow_mut();
            state.append_calls += 1;
            state.pause_at == PauseAt::Append
        };
        ControlledFuture::new(self.append_now(request), pause)
    }

    fn trim(&self, before: RingCursor) -> Self::TrimFuture {
        let mut state = self.shared.borrow_mut();
        let durable_tail = RingCursor::new(
            u64::try_from(state.durable_count).expect("test durable tail fits in u64"),
        );
        if before > durable_tail {
            return ready(Err(CompletionError::not_applied(
                RingError::TrimPastDurableTail {
                    requested: before,
                    durable_tail,
                },
            )));
        }
        let before = usize::try_from(before.get()).expect("bounded trim cursor fits in usize");
        state.accepted_head = state.accepted_head.max(before);
        ready(Ok(TrimSuccess {
            accepted_head: RingCursor::new(
                u64::try_from(state.accepted_head).expect("test accepted head fits in u64"),
            ),
        }))
    }

    fn sync(&self) -> Self::SyncFuture {
        let pause = {
            let mut state = self.shared.borrow_mut();
            state.sync_calls += 1;
            state.pause_at == PauseAt::Sync
        };
        ControlledFuture::new(self.sync_now(), pause)
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn poll_pending_then_drop<F: Future>(future: F) {
    let mut future = Box::pin(future);
    assert!(matches!(poll_once(future.as_mut()), Poll::Pending));
}

fn queue_config() -> QueueConfig {
    QueueConfig {
        active_capacity: 4,
        max_payload_bytes: 64,
        max_claim_batch: 2,
        completed_history_capacity: 4,
    }
}

fn recovery_config(read_batch_records: usize) -> RecoveryConfig {
    RecoveryConfig::new(
        read_batch_records,
        DEFAULT_RECOVERY_READ_BYTES,
        DEFAULT_MAX_REPLAY_RECORDS,
    )
}

fn request(request_id: u64) -> SubmitRequest {
    SubmitRequest {
        request_id: RequestId::new(request_id),
        payload: b"cancel-me".to_vec(),
        not_before: SimInstant::ZERO,
    }
}

fn recovered_queue() -> (DurableQueue<ControlledRing>, ControlledRing) {
    let ring = ControlledRing::default();
    let observer = ring.clone();
    let queue = complete(DurableQueue::recover(
        queue_config(),
        ring,
        recovery_config(4),
    ))
    .unwrap();
    assert_eq!(observer.accepted_and_durable_counts(), (2, 2));
    (queue, observer)
}

fn leased_queue() -> (
    DurableQueue<ControlledRing>,
    ControlledRing,
    JobId,
    LeaseToken,
) {
    let (mut queue, ring) = recovered_queue();
    let submitted = complete(queue.submit(request(1), SimInstant::ZERO)).unwrap();
    let SubmitOutcome::Submitted { job_id } = submitted else {
        panic!("fresh request was unexpectedly deduplicated: {submitted:?}");
    };
    let leased = queue
        .claim(
            WorkerId::new(1),
            1,
            SimDuration::from_nanos(10),
            SimInstant::ZERO,
        )
        .expect("claim succeeds")
        .pop()
        .expect("submitted job is leased");
    (queue, ring, job_id, leased.lease_token)
}

#[test]
fn dropping_submit_while_append_is_pending_fences_the_queue() {
    let (mut queue, ring) = recovered_queue();
    ring.pause_at(PauseAt::Append);
    let calls_before = (ring.append_calls(), ring.sync_calls());

    poll_pending_then_drop(queue.submit(request(1), SimInstant::ZERO));

    assert_eq!(
        (ring.append_calls(), ring.sync_calls()),
        (calls_before.0 + 1, calls_before.1),
        "submit must be abandoned at append, before reaching sync"
    );
    assert_eq!(
        ring.accepted_and_durable_counts(),
        (3, 2),
        "invoking append queues the submit before its response is abandoned"
    );
    assert!(queue.recovery_required());
    assert_every_operation_is_fenced(&mut queue);
}

#[test]
fn dropping_submit_while_sync_is_pending_fences_the_queue() {
    let (mut queue, ring) = recovered_queue();
    ring.pause_at(PauseAt::Sync);
    let calls_before = (ring.append_calls(), ring.sync_calls());

    poll_pending_then_drop(queue.submit(request(1), SimInstant::ZERO));

    assert_eq!(
        (ring.append_calls(), ring.sync_calls()),
        (calls_before.0 + 1, calls_before.1 + 1),
        "append must complete before submit is abandoned at sync"
    );
    assert_eq!(
        ring.accepted_and_durable_counts(),
        (3, 3),
        "invoking sync queues the durability fence before its response is abandoned"
    );
    assert!(queue.recovery_required());
    assert_every_operation_is_fenced(&mut queue);
}

#[test]
fn dropping_ack_while_append_is_pending_fences_the_queue() {
    let (mut queue, ring, job_id, token) = leased_queue();
    ring.pause_at(PauseAt::Append);
    let calls_before = (ring.append_calls(), ring.sync_calls());
    let counts_before = ring.accepted_and_durable_counts();

    poll_pending_then_drop(queue.ack(job_id, token, SimInstant::ZERO));

    assert_eq!(
        (ring.append_calls(), ring.sync_calls()),
        (calls_before.0 + 1, calls_before.1),
        "ack must be abandoned at append, before reaching sync"
    );
    assert_eq!(
        ring.accepted_and_durable_counts(),
        (counts_before.0 + 1, counts_before.1),
        "invoking append queues the acknowledgement before its response is abandoned"
    );
    assert!(queue.recovery_required());
    assert_every_operation_is_fenced(&mut queue);
}

#[test]
fn dropping_ack_while_sync_is_pending_fences_the_queue() {
    let (mut queue, ring, job_id, token) = leased_queue();
    ring.pause_at(PauseAt::Sync);
    let calls_before = (ring.append_calls(), ring.sync_calls());
    let counts_before = ring.accepted_and_durable_counts();

    poll_pending_then_drop(queue.ack(job_id, token, SimInstant::ZERO));

    assert_eq!(
        (ring.append_calls(), ring.sync_calls()),
        (calls_before.0 + 1, calls_before.1 + 1),
        "ack append must complete before cancellation at sync"
    );
    assert_eq!(
        ring.accepted_and_durable_counts(),
        (counts_before.0 + 1, counts_before.1 + 1),
        "invoking sync queues the acknowledgement fence before its response is abandoned"
    );
    assert!(queue.recovery_required());
    assert_every_operation_is_fenced(&mut queue);
}

#[test]
fn dropping_recovery_during_read_leaves_the_ring_recoverable() {
    let (queue, ring) = recovered_queue();
    drop(queue);
    let calls_before = (ring.read_calls(), ring.sync_calls());
    ring.pause_at(PauseAt::Read);

    poll_pending_then_drop(DurableQueue::recover(
        queue_config(),
        ring.clone(),
        recovery_config(1),
    ));

    assert_eq!(ring.read_calls(), calls_before.0 + 1);
    assert_eq!(
        ring.sync_calls(),
        calls_before.1 + 1,
        "recovery fences before entering its read loop"
    );
    ring.pause_at(PauseAt::Nowhere);
    let recovered = complete(DurableQueue::recover(
        queue_config(),
        ring,
        recovery_config(1),
    ))
    .expect("a cancelled read leaves no poisoned recovery state");
    assert!(!recovered.recovery_required());
}

#[test]
fn a_foreign_append_fenced_by_submit_sync_is_detected() {
    let (mut queue, ring) = recovered_queue();
    ring.append_foreign_before_sync();

    let error = complete(queue.submit(request(1), SimInstant::ZERO)).unwrap_err();

    assert_eq!(error.certainty(), kr_runtime::CompletionCertainty::Applied);
    assert!(matches!(
        error.error(),
        DurableQueueError::InvalidHistory { .. }
    ));
    assert_eq!(ring.accepted_and_durable_counts(), (4, 4));
    assert!(queue.recovery_required());
    assert_every_operation_is_fenced(&mut queue);
}

#[test]
fn applied_sync_error_with_a_foreign_fenced_tail_poisoned_the_queue() {
    let (mut queue, ring) = recovered_queue();
    ring.append_foreign_before_sync();
    ring.fail_sync_after_apply();

    let error = complete(queue.submit(request(2), SimInstant::ZERO)).unwrap_err();

    assert_eq!(error.certainty(), kr_runtime::CompletionCertainty::Applied);
    assert!(matches!(
        error.error(),
        DurableQueueError::InvalidHistory { .. }
    ));
    assert_eq!(ring.accepted_and_durable_counts(), (4, 4));
    assert!(queue.recovery_required());
    assert_every_operation_is_fenced(&mut queue);
}

#[test]
fn sync_checkpoint_behind_the_appended_record_is_not_reported_applied() {
    let (mut queue, ring) = recovered_queue();
    ring.report_sync_tail_behind();

    let error = complete(queue.submit(request(3), SimInstant::ZERO)).unwrap_err();

    assert_eq!(
        error.certainty(),
        kr_runtime::CompletionCertainty::MayHaveApplied
    );
    assert!(matches!(
        error.error(),
        DurableQueueError::InvalidHistory { .. }
    ));
    assert!(queue.recovery_required());
    assert_every_operation_is_fenced(&mut queue);
}
