//! Typed completion dispatch keeps both public response families on the shared
//! completion primitive while one ordered provider queue owns either payload.
use super::{WriteData, WriteOutput, contiguous, vectored};
use crate::completion::{
    LocalAdmission, LocalCell, LocalOperation, SyncCell, SyncOperation, SyncPermit,
};
use crate::network::{NetworkFailure, VectoredWriteFailure, VectoredWriteResult, WriteResult};
use kr_runtime::CompletionResult;
use std::{rc::Rc, sync::Arc};

type ContiguousOutput = CompletionResult<WriteResult, NetworkFailure>;
type VectoredOutput = CompletionResult<VectoredWriteResult, VectoredWriteFailure>;

impl WriteData {
    pub(in crate::network) const fn is_vectored(&self) -> bool {
        matches!(self, Self::Vectored(_))
    }
}
fn output_is_vectored(output: &WriteOutput) -> bool {
    match output {
        Ok(result) => result.data.is_vectored(),
        Err(error) => error.error().data.is_vectored(),
    }
}

pub(in crate::network) enum LocalWriteResponse {
    Contiguous(LocalOperation<ContiguousOutput>),
    Vectored(LocalOperation<VectoredOutput>),
}
#[derive(Clone)]
pub(in crate::network) enum LocalWriteCell {
    Contiguous(Rc<LocalCell<ContiguousOutput>>),
    Vectored(Rc<LocalCell<VectoredOutput>>),
}
impl LocalWriteResponse {
    pub(in crate::network) fn ready(output: WriteOutput) -> Self {
        if output_is_vectored(&output) {
            Self::Vectored(LocalOperation::ready(vectored(output)))
        } else {
            Self::Contiguous(LocalOperation::ready(contiguous(output)))
        }
    }
    pub(in crate::network) fn into_contiguous(self) -> LocalOperation<ContiguousOutput> {
        let Self::Contiguous(response) = self else {
            unreachable!("contiguous submission creates its typed response")
        };
        response
    }
    pub(in crate::network) fn into_vectored(self) -> LocalOperation<VectoredOutput> {
        let Self::Vectored(response) = self else {
            unreachable!("vectored submission creates its typed response")
        };
        response
    }
}
impl LocalWriteCell {
    pub(in crate::network) fn with_delay(
        admission: LocalAdmission,
        is_vectored: bool,
    ) -> (Self, LocalWriteResponse) {
        if is_vectored {
            let cell = Rc::new(LocalCell::with_delay(admission));
            let response = LocalWriteResponse::Vectored(LocalOperation::from_cell(cell.clone()));
            (Self::Vectored(cell), response)
        } else {
            let cell = Rc::new(LocalCell::with_delay(admission));
            let response = LocalWriteResponse::Contiguous(LocalOperation::from_cell(cell.clone()));
            (Self::Contiguous(cell), response)
        }
    }
    pub(in crate::network) fn complete(&self, output: WriteOutput) {
        match self {
            Self::Contiguous(cell) => cell.complete(contiguous(output)),
            Self::Vectored(cell) => cell.complete(vectored(output)),
        }
    }
    pub(in crate::network) fn close_gate(&self) {
        match self {
            Self::Contiguous(cell) => cell.close_gate(),
            Self::Vectored(cell) => cell.close_gate(),
        }
    }
    pub(in crate::network) fn mark_delay_elapsed(&self) {
        match self {
            Self::Contiguous(cell) => cell.mark_delay_elapsed(),
            Self::Vectored(cell) => cell.mark_delay_elapsed(),
        }
    }
    pub(in crate::network) fn complete_immediately(&self, output: WriteOutput) {
        self.complete(output);
        self.mark_delay_elapsed();
    }
}

pub(in crate::network) enum SyncWriteResponse {
    Contiguous(SyncOperation<ContiguousOutput>),
    Vectored(SyncOperation<VectoredOutput>),
}
#[derive(Clone)]
pub(in crate::network) enum SyncWriteCell {
    Contiguous(Arc<SyncCell<ContiguousOutput>>),
    Vectored(Arc<SyncCell<VectoredOutput>>),
}
impl SyncWriteResponse {
    pub(in crate::network) fn ready(output: WriteOutput) -> Self {
        if output_is_vectored(&output) {
            Self::Vectored(SyncOperation::ready(vectored(output)))
        } else {
            Self::Contiguous(SyncOperation::ready(contiguous(output)))
        }
    }
    pub(in crate::network) fn pending(
        permit: SyncPermit,
        is_vectored: bool,
    ) -> (Self, SyncWriteCell) {
        if is_vectored {
            let (response, cell) = SyncOperation::pending(Some(permit));
            (Self::Vectored(response), SyncWriteCell::Vectored(cell))
        } else {
            let (response, cell) = SyncOperation::pending(Some(permit));
            (Self::Contiguous(response), SyncWriteCell::Contiguous(cell))
        }
    }
    pub(in crate::network) fn into_contiguous(self) -> SyncOperation<ContiguousOutput> {
        let Self::Contiguous(response) = self else {
            unreachable!("contiguous submission creates its typed response")
        };
        response
    }
    pub(in crate::network) fn into_vectored(self) -> SyncOperation<VectoredOutput> {
        let Self::Vectored(response) = self else {
            unreachable!("vectored submission creates its typed response")
        };
        response
    }
}
