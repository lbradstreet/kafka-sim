//! Incremental exact Produce-v13 framing size for a bounded candidate set.
use super::*;

pub(super) struct RequestSize {
    topics: BTreeMap<TopicId, usize>,
    bytes: usize,
    segments: usize,
    partitions: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Addition {
    pub(super) bytes: usize,
    pub(super) segments: usize,
    pub(super) partitions: usize,
    topic: TopicId,
    topic_partitions: usize,
}

impl RequestSize {
    pub(super) fn new(client_id_bytes: usize) -> Option<Self> {
        Some(Self {
            topics: BTreeMap::new(),
            // Frame + request header + Produce scalar fields + compact topics.
            bytes: 24usize.checked_add(client_id_bytes)?,
            segments: 1,
            partitions: 0,
        })
    }

    pub(super) fn preview(
        &self,
        topic: TopicId,
        record_bytes: usize,
        chunks: usize,
    ) -> Option<Addition> {
        let prior = self.topics.get(&topic).copied().unwrap_or(0);
        let next = prior.checked_add(1)?;
        let partition_bytes = 5usize
            .checked_add(varint_len(record_bytes.checked_add(1)?))?
            .checked_add(record_bytes)?;
        let topic_bytes = if prior == 0 {
            let topics = self.topics.len();
            17usize
                .checked_add(varint_len(2))?
                .checked_add(varint_len(topics.checked_add(2)?))?
                .checked_sub(varint_len(topics.checked_add(1)?))?
        } else {
            varint_len(next.checked_add(1)?) - varint_len(next)
        };
        Some(Addition {
            bytes: self
                .bytes
                .checked_add(partition_bytes)?
                .checked_add(topic_bytes)?,
            segments: self.segments.checked_add(chunks)?,
            partitions: self.partitions.checked_add(1)?,
            topic,
            topic_partitions: next,
        })
    }

    pub(super) fn commit(&mut self, addition: Addition) {
        self.topics
            .insert(addition.topic, addition.topic_partitions);
        self.bytes = addition.bytes;
        self.segments = addition.segments;
        self.partitions = addition.partitions;
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

fn varint_len(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 128 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_sizes_match_the_independent_full_group_measure_at_varint_boundaries() {
        for topics in [1, 2, 126, 127, 128] {
            let mut size = RequestSize::new(7).unwrap();
            let mut reference: BTreeMap<TopicId, Vec<usize>> = BTreeMap::new();
            for index in 0..1024 {
                let topic = TopicId(
                    ((index % topics as usize) as u64)
                        .to_be_bytes()
                        .repeat(2)
                        .try_into()
                        .unwrap(),
                );
                let bytes = [1, 126, 127, 128, 16382, 16383, 16384][index % 7];
                let addition = size.preview(topic, bytes, 3).unwrap();
                reference.entry(topic).or_default().push(bytes);
                let expected = 23
                    + 7
                    + varint_len(reference.len() + 1)
                    + reference
                        .values()
                        .map(|partitions| {
                            17 + varint_len(partitions.len() + 1)
                                + partitions
                                    .iter()
                                    .map(|bytes| 5 + varint_len(bytes + 1) + bytes)
                                    .sum::<usize>()
                        })
                        .sum::<usize>();
                assert_eq!(addition.bytes, expected);
                assert_eq!(addition.segments, 1 + 3 * (index + 1));
                size.commit(addition);
                assert_eq!(size.bytes(), expected);
            }
        }
    }

    #[test]
    fn rejected_candidate_preview_and_overflow_leave_the_accumulator_unchanged() {
        let topic = TopicId([1; 16]);
        let mut size = RequestSize::new(0).unwrap();
        let accepted = size.preview(topic, 100, 2).unwrap();
        size.commit(accepted);
        let before = size.bytes();
        assert!(size.preview(topic, usize::MAX, 2).is_none());
        let rejected = size.preview(TopicId([2; 16]), 1024, 3).unwrap();
        assert!(rejected.bytes > before);
        assert_eq!(size.bytes(), before);
        assert_eq!(size.topics.len(), 1);
        assert_eq!(size.partitions, 1);
        assert!(RequestSize::new(usize::MAX).is_none());
    }
}
