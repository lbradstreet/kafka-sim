//! Destination fences and resumable retry deadline updates. A failure captures
//! its time and jitter once; later owner polls never move that baseline forward.
use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};

type Destination = (i32, u8);
struct RetryJob {
    failure_at: RuntimeInstant,
    jitter: u64,
    cursor: Option<TopicPartition>,
    end: Option<TopicPartition>,
    topology: u64,
    restart: bool,
}
#[derive(Default)]
pub(super) struct RetryWork {
    jobs: BTreeMap<Destination, RetryJob>,
    cursor: Option<Destination>,
    topology: u64,
}
impl ProducerEngine {
    pub(super) fn retry_pending(&self, broker: i32, lane: u8) -> bool {
        self.retry.jobs.contains_key(&(broker, lane))
    }
    pub(super) fn has_retry_work(&self) -> bool {
        !self.retry.jobs.is_empty()
    }
    /// Call only when a metadata application changed identity/partition routing
    /// or an engine partition was inserted. Repeated identical metadata is inert.
    pub(super) fn retry_topology_changed(&mut self) {
        if self.retry.jobs.is_empty() {
            return;
        }
        if let Some(next) = self.retry.topology.checked_add(1) {
            self.retry.topology = next;
        } else {
            self.fail(FailureReason::ResourceExhausted);
        }
    }
    pub(super) fn schedule_connection_retry(&mut self, key: ConnectionKey, now: RuntimeInstant) {
        let Some(connection) = self.connections.get_mut(Slot::from_packed(key.0)) else {
            return;
        };
        if connection.retry_scheduled || self.failed.is_some() {
            return;
        }
        connection.retry_scheduled = true;
        let destination = (connection.broker, connection.lane);
        if let Some(job) = self.retry.jobs.get_mut(&destination) {
            if now >= job.failure_at {
                job.failure_at = now;
                job.jitter = self.retry_jitter;
            }
            job.restart = true;
            return;
        }
        if self.retry.jobs.len() >= self.validated.max_connections {
            // An impossible capacity state must never let an unfenced route
            // dispatch. Producer failure installs its own immediate global fence.
            self.fail(FailureReason::ResourceExhausted);
            return;
        }
        self.retry.jobs.insert(
            destination,
            RetryJob {
                failure_at: now,
                jitter: self.retry_jitter,
                cursor: None,
                end: self.partitions.last_key_value().map(|(key, _)| *key),
                topology: self.retry.topology,
                restart: false,
            },
        );
        self.scheduler_connection_changed(destination.0, destination.1);
        self.encoder_dispatch_changed(destination.0, destination.1);
    }
    fn finish_retry_job(&mut self, destination: Destination) {
        self.retry.jobs.remove(&destination);
        self.scheduler_connection_changed(destination.0, destination.1);
        self.encoder_dispatch_changed(destination.0, destination.1);
        self.complete_fences();
    }
    /// One raw partition visit, one empty/end visit, or one job removal. Neither
    /// unrelated partitions nor deleted keys can escape the caller's item quota.
    pub(super) fn retry_step(&mut self) -> bool {
        let destination = match self.retry.cursor {
            Some(cursor) => self
                .retry
                .jobs
                .range((Excluded(cursor), Unbounded))
                .next()
                .or_else(|| self.retry.jobs.first_key_value()),
            None => self.retry.jobs.first_key_value(),
        }
        .map(|(destination, _)| *destination);
        let Some(destination) = destination else {
            return false;
        };
        self.retry.cursor = Some(destination);
        if self.failed.is_some()
            || self.closed
            || self
                .close
                .as_ref()
                .is_some_and(|close| self.tracker.completed_through().0 >= close.watermark.0)
        {
            self.finish_retry_job(destination);
            return true;
        }
        let job = &self.retry.jobs[&destination];
        let next = job.end.and_then(|end| {
            self.partitions
                .range((job.cursor.map_or(Unbounded, Excluded), Included(end)))
                .next()
                .map(|(key, _)| *key)
        });
        let Some(partition) = next else {
            let end = self.partitions.last_key_value().map(|(key, _)| *key);
            let job = self
                .retry
                .jobs
                .get_mut(&destination)
                .expect("selected retry");
            if job.restart || job.topology != self.retry.topology {
                job.cursor = None;
                job.end = end;
                job.topology = self.retry.topology;
                job.restart = false;
            } else {
                self.finish_retry_job(destination);
            }
            return true;
        };
        let job = self
            .retry
            .jobs
            .get_mut(&destination)
            .expect("selected retry");
        job.cursor = Some(partition);
        let (failure_at, jitter) = (job.failure_at, job.jitter);
        if self.destination(partition) == Some(destination) {
            let attempts = self
                .ledger
                .as_ref()
                .and_then(|ledger| ledger.max_attempts(partition).ok())
                .unwrap_or(0)
                .max(1);
            let at =
                Self::deadline_after(failure_at, delay(&self.config, u64::from(attempts), jitter));
            let queue = self
                .partitions
                .get_mut(&partition)
                .expect("visited retry partition");
            queue.retry_at = queue.retry_at.max(at);
            self.deadlines
                .set(DeadlineKey::Retry(partition), queue.retry_at);
            self.scheduler_mark(partition);
        }
        true
    }
}

pub(super) fn delay(config: &ProducerConfig, attempt: u64, jitter: u64) -> RuntimeDuration {
    let shift = u32::try_from(attempt.saturating_sub(1).min(63)).unwrap_or(63);
    let base = config
        .retry_backoff_min
        .as_nanos()
        .saturating_mul(1u64 << shift)
        .min(config.retry_backoff_max.as_nanos());
    let jitter = if base == 0 { 0 } else { jitter % base };
    RuntimeDuration::from_nanos(base.saturating_add(jitter))
}

#[cfg(test)]
mod tests;
