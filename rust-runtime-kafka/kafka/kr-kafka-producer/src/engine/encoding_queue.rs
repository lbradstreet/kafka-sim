//! Two-level raw-byte DRR. One visit always advances both stable cursors.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Default)]
struct Lane {
    ready: BTreeSet<TopicPartition>,
    cursor: Option<TopicPartition>,
    deficit: usize,
}
#[derive(Default)]
struct Member {
    lane: u8,
    deficit: usize,
    codec_turn: bool,
}
#[derive(Default)]
pub(super) struct RawDrr {
    lanes: [Lane; 4],
    members: BTreeMap<TopicPartition, Member>,
    next_lane: usize,
    active_lanes: u8,
}
pub(super) struct Visit {
    pub partition: TopicPartition,
    pub codec_turn: bool,
    pub raw_bytes: usize,
}
impl RawDrr {
    pub(super) fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
    pub(super) fn insert(&mut self, partition: TopicPartition, lane: u8) {
        if let Some(member) = self.members.get(&partition) {
            assert_eq!(member.lane, lane, "partition lane is immutable");
            return;
        }
        let state = &mut self.lanes[usize::from(lane)];
        if state.ready.is_empty() {
            self.active_lanes += 1;
        }
        state.ready.insert(partition);
        self.members.insert(
            partition,
            Member {
                lane,
                ..Member::default()
            },
        );
    }
    pub(super) fn remove(&mut self, partition: TopicPartition) {
        let Some(member) = self.members.remove(&partition) else {
            return;
        };
        let lane = &mut self.lanes[usize::from(member.lane)];
        assert!(lane.ready.remove(&partition));
        if lane.ready.is_empty() {
            lane.deficit = 0;
            self.active_lanes -= 1;
        }
    }
    pub(super) fn visit(&mut self, quantum: usize, cap: usize, maximum: usize) -> Option<Visit> {
        if self.is_empty() || maximum == 0 {
            return None;
        }
        // The lane count is a validated fixed four, not a topology walk.
        let index = (0..4)
            .map(|offset| (self.next_lane + offset) % 4)
            .find(|&index| !self.lanes[index].ready.is_empty())
            .expect("active member has a lane");
        self.next_lane = (index + 1) % 4;
        let grant = (quantum / usize::from(self.active_lanes)).max(1).min(cap);
        let lane = &mut self.lanes[index];
        let partition = lane
            .cursor
            .and_then(|after| {
                lane.ready
                    .range((Excluded(after), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| lane.ready.first().copied())
            .expect("selected nonempty lane");
        lane.cursor = Some(partition);
        lane.deficit = lane.deficit.saturating_add(grant).min(cap);
        let member = self.members.get_mut(&partition).expect("ready member");
        member.deficit = member.deficit.saturating_add(grant).min(cap);
        let codec_turn = member.codec_turn;
        member.codec_turn = !codec_turn;
        Some(Visit {
            partition,
            codec_turn,
            raw_bytes: maximum.min(lane.deficit).min(member.deficit),
        })
    }
    pub(super) fn charge(&mut self, partition: TopicPartition, raw: usize) {
        let member = self
            .members
            .get_mut(&partition)
            .expect("visited member retained until charge");
        let lane = &mut self.lanes[usize::from(member.lane)];
        member.deficit = member.deficit.checked_sub(raw).expect("raw quota checked");
        lane.deficit = lane
            .deficit
            .checked_sub(raw)
            .expect("lane raw quota checked");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(index: i32) -> TopicPartition {
        TopicPartition {
            topic: TopicId([1; 16]),
            partition: index,
        }
    }
    #[test]
    fn hot_lane_cardinality_cannot_displace_a_cold_lanes_turn() {
        let mut queue = RawDrr::default();
        for partition in 0..64 {
            queue.insert(key(partition), 0);
        }
        queue.insert(key(64), 1);
        for turn in 0..512 {
            let visit = queue.visit(1024, 65536, 1024).unwrap();
            assert_eq!(visit.partition == key(64), turn % 2 == 1);
            assert!(visit.raw_bytes > 0 && visit.raw_bytes <= 1024);
            queue.charge(visit.partition, visit.raw_bytes);
        }
    }
    #[test]
    fn raw_consumption_and_zero_input_seals_keep_distinct_deficit_accounting() {
        let mut queue = RawDrr::default();
        queue.insert(key(0), 0);
        let first = queue.visit(100, 1000, 1000).unwrap();
        assert!(!first.codec_turn);
        queue.charge(key(0), 0); // descriptor append or zero-input seal
        assert_eq!(queue.members[&key(0)].deficit, 100);
        let second = queue.visit(100, 1000, 1000).unwrap();
        assert!(second.codec_turn);
        assert_eq!(second.raw_bytes, 200);
        queue.charge(key(0), 137); // actual input, independent of output size
        assert_eq!(queue.members[&key(0)].deficit, 63);
        assert_eq!(queue.lanes[0].deficit, 63);
        queue.remove(key(0));
        assert!(queue.is_empty());
        queue.insert(key(0), 0);
        assert_eq!(queue.visit(100, 1000, 1000).unwrap().raw_bytes, 100);
    }
}
