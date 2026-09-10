//! ABI v2 immutable, bounded metadata readers. No returned pointer borrows Rust.
use super::*;
use kr_kafka_producer::{
    client::ClientTopic,
    topic::{MetadataSnapshot, TopicState},
    types::FailureReason,
};
use std::sync::Arc;

pub(super) struct Snapshots {
    next: u64,
    slots: Vec<Option<(u64, Arc<MetadataSnapshot>)>>,
}
impl Snapshots {
    pub(super) fn storage_bytes(count: usize) -> usize {
        count * size_of::<Option<(u64, Arc<MetadataSnapshot>)>>()
    }
    pub(super) fn new(count: usize) -> Result<Self, i32> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(count)
            .map_err(|_| KR_ERR_EXHAUSTED)?;
        slots.resize_with(count, || None);
        Ok(Self { next: 1, slots })
    }
    fn insert(&mut self, snapshot: Arc<MetadataSnapshot>) -> Result<u64, i32> {
        let next = self.next.checked_add(1).ok_or(KR_ERR_EXHAUSTED)?;
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or(KR_ERR_EXHAUSTED)?;
        let handle = self.next;
        *slot = Some((handle, snapshot));
        self.next = next;
        Ok(handle)
    }
    fn get(&self, handle: u64) -> Result<Arc<MetadataSnapshot>, i32> {
        self.slots
            .iter()
            .flatten()
            .find(|(id, _)| *id == handle)
            .map(|(_, snapshot)| snapshot.clone())
            .ok_or(KR_ERR_INVALID)
    }
    fn remove(&mut self, handle: u64) -> Result<(), i32> {
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|(id, _)| *id == handle))
            .ok_or(KR_ERR_INVALID)?;
        *slot = None;
        Ok(())
    }
}
fn snapshot(p: &KrProducer, handle: u64) -> Result<Arc<MetadataSnapshot>, i32> {
    p.snapshots
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(handle)
}
fn status(topic: Option<&ClientTopic>) -> KrTopicStatus {
    let mut out = KrTopicStatus {
        struct_size: size_of::<KrTopicStatus>() as u32,
        status: 5,
        ..Default::default()
    };
    if let Some(topic) = topic {
        out.status = if topic.closing {
            4
        } else if topic.state == TopicState::Ready && topic.metadata_invalidated {
            6
        } else {
            match topic.state {
                TopicState::Resolving => 0,
                TopicState::Ready => 1,
                TopicState::Failed => 2,
                TopicState::Deleted => 3,
            }
        };
        out.generation = topic
            .snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.generation);
        out.topic_id = topic.id.unwrap_or_default().0;
        out.partition_count = topic.partitions;
        out.reason = if topic.failure_reason != 0 {
            topic.failure_reason
        } else {
            match topic.state {
                TopicState::Failed => FailureReason::TopicResolution as u32,
                TopicState::Deleted => FailureReason::TopicDeleted as u32,
                _ => 0,
            }
        };
    }
    out
}
/// # Safety
/// Producer and independent output remain live for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_owner_status(producer: *mut KrProducer, out: *mut u32) -> i32 {
    // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
    code(with!(producer, true, |p| unsafe {
        memory::write(out, p.client.owner_status())
    }))
}
/// # Safety
/// Output has initialized matching struct_size and writable independent storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_topic_get_status(
    producer: *mut KrProducer,
    topic: u32,
    out: *mut KrTopicStatus,
) -> i32 {
    code(with!(producer, true, |p| {
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::versioned(out) }?;
        let topic = p
            .client
            .metadata_topic(TopicHandle(topic))
            .map_err(client_error)?;
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(out, status(topic.as_ref())) }
    }))
}
/// # Safety
/// Producer is live and excludes destruction throughout the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_topic_refresh(producer: *mut KrProducer, topic: u32) -> i32 {
    code(with!(producer, false, |p| p
        .client
        .refresh_topic(TopicHandle(topic))
        .map_err(client_error)))
}
/// # Safety
/// Output has initialized matching struct_size and writable independent storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_metadata_acquire(
    producer: *mut KrProducer,
    topic: u32,
    out: *mut KrMetadataSnapshot,
) -> i32 {
    code(with!(producer, true, |p| {
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::versioned(out) }?;
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe {
            memory::write(
                out,
                KrMetadataSnapshot {
                    struct_size: size_of::<KrMetadataSnapshot>() as u32,
                    ..Default::default()
                },
            )
        }?;
        let topic = p
            .client
            .metadata_topic(TopicHandle(topic))
            .map_err(client_error)?;
        let info = status(topic.as_ref());
        if info.status != 1 {
            return Err(if matches!(info.status, 0 | 6) {
                KR_ERR_NOT_READY
            } else {
                KR_ERR_CLOSED
            });
        }
        let snapshot = topic
            .and_then(|topic| topic.snapshot)
            .ok_or(KR_ERR_NOT_READY)?;
        let brokers = snapshot.brokers.rows.len() as u32;
        let handle = p
            .snapshots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(snapshot)?;
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe {
            memory::write(
                out,
                KrMetadataSnapshot {
                    struct_size: size_of::<KrMetadataSnapshot>() as u32,
                    status: info.status,
                    generation: info.generation,
                    topic_id: info.topic_id,
                    partition_count: info.partition_count,
                    reason: info.reason,
                    snapshot: handle,
                    broker_count: brokers,
                },
            )
        }
    }))
}
/// # Safety
/// Producer is live. A released snapshot handle is rejected without effect.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_metadata_release(producer: *mut KrProducer, handle: u64) -> i32 {
    code(with!(producer, true, |p| p
        .snapshots
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(handle)))
}
fn page(
    length: usize,
    start: u32,
    capacity: u32,
    maximum: u32,
) -> Result<std::ops::Range<usize>, i32> {
    if capacity > maximum || start as usize > length {
        return Err(KR_ERR_INVALID);
    }
    let begin = start as usize;
    Ok(begin..length.min(begin + capacity as usize))
}
unsafe fn versioned_rows<T: Copy>(out: *mut T, capacity: u32) -> Result<(), i32> {
    memory::check(out, capacity as usize)?;
    for index in 0..capacity as usize {
        // SAFETY: the caller provides initialized rows; the index is within checked capacity.
        unsafe { memory::versioned(out.add(index)) }?;
    }
    Ok(())
}
/// # Safety
/// Every row has initialized struct_size. Rows and written are writable and disjoint.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_metadata_brokers(
    producer: *mut KrProducer,
    handle: u64,
    start: u32,
    out: *mut KrMetadataBroker,
    capacity: u32,
    written: *mut u32,
) -> i32 {
    code(with!(producer, true, |p| {
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, 0) }?;
        let data = snapshot(p, handle)?;
        let range = page(data.brokers.rows.len(), start, capacity, 1024)?;
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { versioned_rows(out, capacity) }?;
        for (index, row) in data.brokers.rows[range.clone()].iter().enumerate() {
            // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
            unsafe {
                memory::write(
                    out.add(index),
                    KrMetadataBroker {
                        struct_size: size_of::<KrMetadataBroker>() as u32,
                        id: row.id,
                        port: u32::from(row.port),
                        host_len: row.host.len() as u32,
                        rack_len: row.rack.as_ref().map_or(0, |rack| rack.len() as u32),
                        rack_present: u32::from(row.rack.is_some()),
                    },
                )
            }?;
        }
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, range.len() as u32) }
    }))
}
/// # Safety
/// Every row has initialized struct_size. Rows and written are writable and disjoint.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_metadata_partitions(
    producer: *mut KrProducer,
    handle: u64,
    start: u32,
    out: *mut KrMetadataPartition,
    capacity: u32,
    written: *mut u32,
) -> i32 {
    code(with!(producer, true, |p| {
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, 0) }?;
        let data = snapshot(p, handle)?;
        let range = page(data.partitions.len(), start, capacity, 1024)?;
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { versioned_rows(out, capacity) }?;
        for (index, row) in data.partitions[range.clone()].iter().enumerate() {
            // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
            unsafe {
                memory::write(
                    out.add(index),
                    KrMetadataPartition {
                        struct_size: size_of::<KrMetadataPartition>() as u32,
                        partition: row.index,
                        leader: row.metadata.leader,
                        leader_epoch: row.metadata.leader_epoch,
                        error_code: i32::from(row.error_code),
                        replica_count: row.replicas.len() as u32,
                        isr_count: row.isr.len() as u32,
                        offline_count: row.offline.len() as u32,
                    },
                )
            }?;
        }
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, range.len() as u32) }
    }))
}
/// # Safety
/// Capacity node IDs and written are writable and disjoint for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_metadata_nodes(
    producer: *mut KrProducer,
    handle: u64,
    partition: u32,
    kind: u32,
    start: u32,
    out: *mut i32,
    capacity: u32,
    written: *mut u32,
) -> i32 {
    code(with!(producer, true, |p| {
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, 0) }?;
        let data = snapshot(p, handle)?;
        let row = data
            .partitions
            .get(partition as usize)
            .ok_or(KR_ERR_INVALID)?;
        let nodes = match kind {
            0 => &row.replicas,
            1 => &row.isr,
            2 => &row.offline,
            _ => return Err(KR_ERR_INVALID),
        };
        let range = page(nodes.len(), start, capacity, 1024)?;
        memory::check(out, capacity as usize)?;
        for (index, value) in nodes[range.clone()].iter().enumerate() {
            // SAFETY: the caller provides writable rows; this page is within checked capacity.
            unsafe { memory::write(out.add(index), *value) }?;
        }
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, range.len() as u32) }
    }))
}
/// # Safety
/// Capacity bytes and written are writable and disjoint for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_metadata_string(
    producer: *mut KrProducer,
    handle: u64,
    broker: u32,
    kind: u32,
    start: u32,
    out: *mut u8,
    capacity: u32,
    written: *mut u32,
) -> i32 {
    code(with!(producer, true, |p| {
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, 0) }?;
        let data = snapshot(p, handle)?;
        let broker = data
            .brokers
            .rows
            .get(broker as usize)
            .ok_or(KR_ERR_INVALID)?;
        let bytes = match kind {
            0 => broker.host.as_bytes(),
            1 => broker.rack.as_deref().unwrap_or("").as_bytes(),
            _ => return Err(KR_ERR_INVALID),
        };
        let range = page(bytes.len(), start, capacity, 65536)?;
        memory::check(out, capacity as usize)?;
        for (index, value) in bytes[range.clone()].iter().enumerate() {
            // SAFETY: the caller provides writable bytes; this page is within checked capacity.
            unsafe { memory::write(out.add(index), *value) }?;
        }
        // SAFETY: the ABI caller keeps independent initialized storage live; bounds and versions are checked before writing.
        unsafe { memory::write(written, range.len() as u32) }
    }))
}

#[cfg(all(test, feature = "binding-test-hooks"))]
mod tests;
