use std::collections::VecDeque;
use std::future::{Ready, ready};
use std::sync::{Arc, Mutex, MutexGuard};

use kr_runtime::{CompletionCertainty, CompletionError, CompletionResult};
use kr_runtime_ring::{
    AppendFailure, AppendRange, AppendRequest, AppendSuccess, CrashResult, MemoryRing, ReadPage,
    ReadRequest, RingCursor, RingError, RingOperation, RingReader, RingStatus, RingWriter,
    SyncFailure, SyncSuccess, TrimSuccess,
};

/// A deterministic terminal fault injected into the next matching operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InjectedFault {
    Before,
    After,
    MayHaveAppliedBefore,
    MayHaveAppliedAfter,
}

impl InjectedFault {
    const fn certainty(self) -> CompletionCertainty {
        match self {
            Self::Before => CompletionCertainty::NotApplied,
            Self::After => CompletionCertainty::Applied,
            Self::MayHaveAppliedBefore | Self::MayHaveAppliedAfter => {
                CompletionCertainty::MayHaveApplied
            }
        }
    }

    const fn applies(self) -> bool {
        matches!(self, Self::After | Self::MayHaveAppliedAfter)
    }
}

#[derive(Default)]
struct FaultQueues {
    read: VecDeque<InjectedFault>,
    append: VecDeque<InjectedFault>,
    sync: VecDeque<InjectedFault>,
    recovery_required: bool,
}

/// Test-local fault injection over the deterministic reference ring.
///
/// Faults are consumed FIFO per operation at method invocation. Clones share
/// both the underlying ring and the injected fault queues.
#[derive(Clone)]
pub(crate) struct FaultRing {
    ring: MemoryRing,
    faults: Arc<Mutex<FaultQueues>>,
}

impl FaultRing {
    pub(crate) fn new(ring: MemoryRing) -> Self {
        Self {
            ring,
            faults: Arc::new(Mutex::new(FaultQueues::default())),
        }
    }

    pub(crate) fn inject_read_fault(&self, fault: InjectedFault) {
        lock_unpoisoned(&self.faults).read.push_back(fault);
    }

    pub(crate) fn inject_append_fault(&self, fault: InjectedFault) {
        lock_unpoisoned(&self.faults).append.push_back(fault);
    }

    pub(crate) fn inject_sync_fault(&self, fault: InjectedFault) {
        lock_unpoisoned(&self.faults).sync.push_back(fault);
    }

    /// Returns the number of faults awaiting one supported operation.
    pub(crate) fn pending_fault_count_for(&self, operation: RingOperation) -> usize {
        let faults = lock_unpoisoned(&self.faults);
        match operation {
            RingOperation::Read => faults.read.len(),
            RingOperation::Append => faults.append.len(),
            RingOperation::Sync => faults.sync.len(),
            RingOperation::Status | RingOperation::Trim => 0,
        }
    }

    /// Observes the underlying memory ring without entering an async runtime.
    pub(crate) fn status_now(&self) -> RingStatus {
        super::complete(self.ring.status()).expect("memory ring status cannot fail")
    }

    pub(crate) fn crash(&self) -> CrashResult {
        self.reopen()
    }

    /// Models closing every clone and reopening the same durable ring.
    pub(crate) fn reopen(&self) -> CrashResult {
        let result = self.ring.crash();
        lock_unpoisoned(&self.faults).recovery_required = false;
        result
    }

    fn take_fault(&self, operation: RingOperation) -> Option<InjectedFault> {
        let mut faults = lock_unpoisoned(&self.faults);
        match operation {
            RingOperation::Read => faults.read.pop_front(),
            RingOperation::Append => faults.append.pop_front(),
            RingOperation::Sync => faults.sync.pop_front(),
            RingOperation::Status | RingOperation::Trim => None,
        }
    }

    fn recovery_required(&self) -> bool {
        lock_unpoisoned(&self.faults).recovery_required
    }

    fn require_recovery(&self) {
        lock_unpoisoned(&self.faults).recovery_required = true;
    }
}

impl RingReader for FaultRing {
    type ReadFuture = Ready<CompletionResult<ReadPage, RingError>>;
    type StatusFuture = Ready<CompletionResult<RingStatus, RingError>>;

    fn read(&self, request: ReadRequest) -> Self::ReadFuture {
        if self.recovery_required() {
            return ready(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )));
        }
        let Some(fault) = self.take_fault(RingOperation::Read) else {
            return self.ring.read(request);
        };
        if !fault.applies() {
            return ready(Err(injected_error(RingOperation::Read, fault)));
        }

        ready(match super::complete(self.ring.read(request)) {
            Ok(_) => Err(injected_error(RingOperation::Read, fault)),
            Err(error) => Err(error),
        })
    }

    fn status(&self) -> Self::StatusFuture {
        if self.recovery_required() {
            return ready(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )));
        }
        self.ring.status()
    }
}

impl RingWriter for FaultRing {
    type AppendFuture = Ready<CompletionResult<AppendSuccess, AppendFailure>>;
    type TrimFuture = Ready<CompletionResult<TrimSuccess, RingError>>;
    type SyncFuture = Ready<CompletionResult<SyncSuccess, SyncFailure>>;

    fn append(&self, request: AppendRequest) -> Self::AppendFuture {
        if self.recovery_required() {
            return ready(Err(CompletionError::not_applied(AppendFailure {
                error: RingError::RecoveryRequired,
                records: request.records,
                accepted_range: None,
            })));
        }
        let Some(fault) = self.take_fault(RingOperation::Append) else {
            return self.ring.append(request);
        };
        if !fault.applies() {
            return ready(Err(CompletionError::new(
                fault.certainty(),
                AppendFailure {
                    error: injected_ring_error(RingOperation::Append),
                    records: request.records,
                    accepted_range: None,
                },
            )));
        }

        ready(match super::complete(self.ring.append(request)) {
            Ok(success) => {
                let accepted_range = AppendRange {
                    first_position: success.first_position,
                    next_cursor: success.next_cursor,
                };
                Err(CompletionError::new(
                    fault.certainty(),
                    AppendFailure {
                        error: injected_ring_error(RingOperation::Append),
                        records: success.records,
                        accepted_range: Some(accepted_range),
                    },
                ))
            }
            Err(error) => Err(error),
        })
    }

    fn trim(&self, before: RingCursor) -> Self::TrimFuture {
        if self.recovery_required() {
            return ready(Err(CompletionError::not_applied(
                RingError::RecoveryRequired,
            )));
        }
        self.ring.trim(before)
    }

    fn sync(&self) -> Self::SyncFuture {
        if self.recovery_required() {
            return ready(Err(CompletionError::not_applied(SyncFailure {
                error: RingError::RecoveryRequired,
                checkpoint: None,
            })));
        }
        let Some(fault) = self.take_fault(RingOperation::Sync) else {
            return self.ring.sync();
        };
        if !fault.applies() {
            if fault.certainty() == CompletionCertainty::MayHaveApplied {
                self.require_recovery();
            }
            return ready(Err(CompletionError::new(
                fault.certainty(),
                SyncFailure {
                    error: injected_ring_error(RingOperation::Sync),
                    checkpoint: None,
                },
            )));
        }

        ready(match super::complete(self.ring.sync()) {
            Ok(checkpoint) => {
                if fault.certainty() == CompletionCertainty::MayHaveApplied {
                    self.require_recovery();
                }
                Err(CompletionError::new(
                    fault.certainty(),
                    SyncFailure {
                        error: injected_ring_error(RingOperation::Sync),
                        checkpoint: Some(checkpoint),
                    },
                ))
            }
            Err(error) => Err(error),
        })
    }
}

fn injected_error(operation: RingOperation, fault: InjectedFault) -> CompletionError<RingError> {
    CompletionError::new(fault.certainty(), injected_ring_error(operation))
}

fn injected_ring_error(operation: RingOperation) -> RingError {
    RingError::BackendFailure {
        operation,
        raw_os_error: None,
        message: "deterministic injected fault".to_owned(),
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
