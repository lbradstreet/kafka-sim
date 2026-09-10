//! Bounded, opportunistic preparation; final request admission stays in dispatch.
use super::*;
use dispatch_size::RequestSize;

impl ProducerEngine {
    pub(super) fn scheduler_prepare_neighbors(
        &mut self,
        head: BatchKey,
        now: RuntimeInstant,
        budget: WorkBudget,
        progress: &mut Progress,
    ) -> Result<bool> {
        let partition = self.batches.get(head).expect("dispatch head").partition();
        let route = self.destination(partition).expect("dispatch route");
        let limit = self
            .config
            .request_batching_policy
            .partition_limit(self.config.request_max_partitions);
        let mut measure =
            RequestSize::new(self.config.client_id.len()).ok_or(EngineError::AllocationFailed)?;
        measure.commit(self.scheduler_measure_for_gather(&measure, head)?);
        let visits = self
            .scheduler
            .ready
            .route_len(route)
            .min((budget.items - progress.items) as usize);
        let mut members = 1;
        let mut prepared = false;
        for _ in 0..visits {
            if members == limit || measure.bytes() >= self.config.request_target_bytes as usize {
                break;
            }
            let Some(candidate) = self.scheduler.ready.gather_after(route, partition) else {
                break;
            };
            progress.items += 1;
            if candidate == partition
                || self.destination(candidate) != Some(route)
                || self
                    .partitions
                    .get(&candidate)
                    .is_none_or(|queue| queue.retry_at > now)
            {
                continue;
            }
            let Some(key) = self.dispatch_candidate(candidate, now, true) else {
                continue;
            };
            let addition = self.scheduler_measure_for_gather(&measure, key)?;
            if addition.bytes > self.config.request_hard_bytes as usize {
                continue;
            }
            measure.commit(addition);
            members += 1;
            let batch = self.batches.get_mut(key).expect("gather candidate");
            if batch.state() == BatchState::Open {
                batch.gather_attempted = true;
                batch.seal_at(SealReason::RequestGather, now);
                self.refresh_batch_deadline(key, now);
                self.encoder_refresh_batch(key, true);
                self.metrics_batch(key);
                prepared = true;
            }
        }
        Ok(prepared)
    }

    fn scheduler_measure_for_gather(
        &self,
        measure: &RequestSize,
        key: BatchKey,
    ) -> Result<dispatch_size::Addition> {
        let batch = self.batches.get(key).expect("gather candidate");
        let bytes = batch
            .wire_bytes()
            .unwrap_or_else(|| usize::try_from(batch.estimated_wire_bytes()).unwrap_or(usize::MAX));
        // Estimates only bound speculative preparation. Final dispatch measures
        // the completed frame and rechecks all wire, segment, credit and order
        // constraints. No codec calls or new transform reservations occur here.
        measure
            .preview(
                batch.partition().topic,
                bytes,
                batch.chunk_count().unwrap_or(3),
            )
            .ok_or(EngineError::AllocationFailed)
    }
}
