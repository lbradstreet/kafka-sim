//! Compact deterministic payload recipes. No runtime RNG stream is consumed.
use crate::{RecordSpec, identity_headers};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Partitioning {
    RoundRobin,
    Fixed { partition: i32 },
    Keyed { keys: u32, skew_ppm: u32 },
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LanePolicy {
    Fixed(u8),
    ByPartition,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValuePattern {
    #[default]
    Compressible,
    Incompressible {
        salt: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordTemplate {
    pub first_id: u64,
    pub topic: u32,
    pub partitioning: Partitioning,
    pub value_bytes: u32,
    pub key_bytes: u32,
    pub lane: LanePolicy,
    pub native: bool,
    #[serde(default)]
    pub value_pattern: ValuePattern,
}
impl RecordTemplate {
    /// Inclusive ID range; a singleton at u64::MAX is valid.
    pub fn id_range(&self, count: u32) -> Result<(u64, u64), String> {
        if self.first_id == 0 || count == 0 {
            return Err("template requires a nonzero ID and count".into());
        }
        Ok((
            self.first_id,
            self.first_id
                .checked_add(u64::from(count - 1))
                .ok_or("template ID range overflow")?,
        ))
    }
    pub fn validate(&self, partitions: usize, lanes: u8, record_bytes: u32) -> Result<(), String> {
        if partitions == 0
            || partitions > 1024
            || lanes == 0
            || self.value_bytes > record_bytes
            || self.key_bytes > record_bytes
            || matches!(self.lane, LanePolicy::Fixed(lane) if lane >= lanes)
        {
            return Err("template payload/topology/lane bounds".into());
        }
        match self.partitioning {
            Partitioning::Fixed { partition }
                if partition < 0 || partition as usize >= partitions =>
            {
                Err("template fixed partition bounds".into())
            }
            Partitioning::Keyed { keys, skew_ppm }
                if keys == 0 || keys > 65536 || skew_ppm > 1_000_000 || self.key_bytes < 8 =>
            {
                Err("template key/skew bounds".into())
            }
            _ => Ok(()),
        }
    }
    pub fn materialize(
        &self,
        index: u32,
        partitions: usize,
        lanes: u8,
    ) -> Result<RecordSpec, String> {
        self.validate(partitions, lanes, 4096)?;
        let id = self
            .first_id
            .checked_add(u64::from(index))
            .ok_or("template ID overflow")?;
        if id == 0 {
            return Err("zero template ID".into());
        }
        let key_number = match self.partitioning {
            Partitioning::Keyed { keys, skew_ppm } => {
                // Evenly distributed ranks make skew reproducible without an RNG.
                if (u64::from(index) * 618_033) % 1_000_000 < u64::from(skew_ppm) {
                    0
                } else {
                    u64::from(index % keys)
                }
            }
            _ => u64::from(index),
        };
        let key = (self.key_bytes != 0).then(|| {
            let mut bytes = vec![0; self.key_bytes as usize];
            let encoded = key_number.to_be_bytes();
            for (position, byte) in bytes.iter_mut().enumerate() {
                *byte = encoded[position % 8];
            }
            bytes
        });
        let partition = match self.partitioning {
            Partitioning::RoundRobin => (index as usize % partitions) as i32,
            Partitioning::Fixed { partition } => partition,
            Partitioning::Keyed { .. } => {
                ((crate::campaign::murmur2(key.as_deref().unwrap()) & 0x7fff_ffff) as usize
                    % partitions) as i32
            }
        };
        let mut value = vec![0x61; self.value_bytes as usize];
        if let ValuePattern::Incompressible { salt } = self.value_pattern {
            for (chunk, bytes) in value.chunks_mut(8).enumerate() {
                let mut word = salt
                    .wrapping_add(id)
                    .wrapping_add((chunk as u64).wrapping_mul(0x9e3779b97f4a7c15));
                word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
                word ^= word >> 31;
                bytes.copy_from_slice(&word.to_le_bytes()[..bytes.len()]);
            }
        }
        Ok(RecordSpec {
            id,
            topic: self.topic,
            partition,
            key_routed: matches!(self.partitioning, Partitioning::Keyed { .. }),
            lane: match self.lane {
                LanePolicy::Fixed(lane) => lane,
                LanePolicy::ByPartition => partition as u8 % lanes,
            },
            key,
            value: Some(value),
            timestamp_ms: 1_000 + i64::from(index),
            native: self.native,
            headers: identity_headers(id),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn templates_reproduce_full_width_identity_and_do_not_embed_payloads_in_json() {
        let mut template = RecordTemplate {
            first_id: u64::MAX,
            topic: 0,
            partitioning: Partitioning::RoundRobin,
            value_bytes: 2048,
            key_bytes: 8,
            lane: LanePolicy::ByPartition,
            native: false,
            value_pattern: ValuePattern::Incompressible { salt: 17 },
        };
        assert_eq!(template.id_range(1).unwrap(), (u64::MAX, u64::MAX));
        assert!(template.id_range(2).is_err());
        let record = template.materialize(0, 6, 4).unwrap();
        assert_eq!(
            crate::record_id(
                record
                    .headers
                    .iter()
                    .map(|h| (h.key.as_str(), h.value.as_deref()))
            )
            .unwrap(),
            u64::MAX
        );
        let encoded = serde_json::to_string(&template).unwrap();
        assert!(encoded.len() < 512);
        let decoded: RecordTemplate = serde_json::from_str(&encoded).unwrap();
        assert_eq!(record, decoded.materialize(0, 6, 4).unwrap());
        template.partitioning = Partitioning::Keyed {
            keys: 64,
            skew_ppm: 500_000,
        };
        template.first_id = 1;
        for index in 0..128 {
            let record = template.materialize(index, 12, 4).unwrap();
            assert!(record.key_routed);
            assert_eq!(record.lane, record.partition as u8 % 4);
            assert_eq!(
                record.partition,
                kr_kafka_producer::routing::keyed_partition(record.key.as_deref().unwrap(), 12)
                    .unwrap()
            );
        }
    }
}
