//! Indexed wire service: lane rotation and bounded deadline/age priority turns.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

type Priority = (RuntimeInstant, RuntimeInstant, TopicPartition);
type Route = (i32, u8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Ready {
    pub(super) route: Option<Route>,
    pub(super) lane: u8,
    pub(super) deadline: RuntimeInstant,
    pub(super) accepted_at: RuntimeInstant,
}

#[derive(Default)]
struct Lane {
    ordered: BTreeSet<TopicPartition>,
    priority: BTreeSet<Priority>,
    cursor: Option<TopicPartition>,
    priority_turn: bool,
    deficit: usize,
}

#[derive(Default)]
pub(super) struct WireReady {
    members: BTreeMap<TopicPartition, Ready>,
    routes: BTreeMap<Route, BTreeSet<TopicPartition>>,
    destinations: BTreeMap<TopicPartition, Route>,
    route_cursors: BTreeMap<Route, TopicPartition>,
    lanes: [Lane; 4],
    next_lane: usize,
    active_lanes: u8,
}

impl WireReady {
    pub(super) fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub(super) fn insert(&mut self, partition: TopicPartition, ready: Ready) {
        if self.members.get(&partition) == Some(&ready) {
            return;
        }
        self.remove(partition);
        let lane = &mut self.lanes[usize::from(ready.lane)];
        if lane.ordered.is_empty() {
            self.active_lanes += 1;
        }
        lane.ordered.insert(partition);
        lane.priority
            .insert((ready.deadline, ready.accepted_at, partition));
        self.track_route(partition, ready.route);
        self.members.insert(partition, ready);
    }

    pub(super) fn remove(&mut self, partition: TopicPartition) -> Option<Ready> {
        let ready = self.members.remove(&partition)?;
        let lane = &mut self.lanes[usize::from(ready.lane)];
        assert!(lane.ordered.remove(&partition));
        assert!(
            lane.priority
                .remove(&(ready.deadline, ready.accepted_at, partition))
        );
        if lane.ordered.is_empty() {
            self.active_lanes -= 1;
        }
        Some(ready)
    }

    /// Route candidates include dirty and parked nonempty owners. Gathering
    /// validates one candidate per item; it never scans unrelated destinations.
    pub(super) fn track_route(&mut self, partition: TopicPartition, route: Option<Route>) {
        if self.destinations.get(&partition).copied() == route {
            return;
        }
        if let Some(old) = self.destinations.remove(&partition) {
            let members = self.routes.get_mut(&old).expect("indexed destination");
            assert!(members.remove(&partition));
            if members.is_empty() {
                self.routes.remove(&old);
                self.route_cursors.remove(&old);
            }
        }
        if let Some(route) = route {
            self.destinations.insert(partition, route);
            self.routes.entry(route).or_default().insert(partition);
        }
    }

    pub(super) fn forget(&mut self, partition: TopicPartition) {
        self.remove(partition);
        self.track_route(partition, None);
    }

    /// A priority visit changes service order, never its byte charge. Every
    /// second visit in each lane uses its independent round-robin cursor.
    pub(super) fn visit(
        &mut self,
        target_bytes: usize,
        cap: usize,
    ) -> Option<(TopicPartition, usize)> {
        if self.members.is_empty() {
            return None;
        }
        let index = (0..4)
            .map(|offset| (self.next_lane + offset) % 4)
            .find(|&index| !self.lanes[index].ordered.is_empty())
            .expect("indexed active lane");
        self.next_lane = (index + 1) % 4;
        let quantum = (target_bytes / usize::from(self.active_lanes))
            .max(1)
            .min(cap);
        let lane = &mut self.lanes[index];
        let partition = if lane.priority_turn {
            lane.priority.first().expect("priority membership").2
        } else {
            lane.cursor
                .and_then(|after| {
                    lane.ordered
                        .range((Excluded(after), Unbounded))
                        .next()
                        .copied()
                })
                .or_else(|| lane.ordered.first().copied())
                .expect("round-robin membership")
        };
        lane.priority_turn = !lane.priority_turn;
        // Only the ordinary turn moves this cursor, so repeated priority wins
        // cannot move it backwards and strand the next ordinary candidate.
        if lane.priority_turn {
            lane.cursor = Some(partition);
        }
        lane.deficit = lane.deficit.saturating_add(quantum).min(cap);
        Some((partition, quantum))
    }

    /// Gathering visits only the selected broker/lane's active index. The
    /// caller charges every returned candidate against its outer item budget.
    pub(super) fn gather_after(
        &mut self,
        route: Route,
        after: TopicPartition,
    ) -> Option<TopicPartition> {
        let members = self.routes.get(&route)?;
        let cursor = self.route_cursors.get(&route).copied().unwrap_or(after);
        let next = members
            .range((Excluded(cursor), Unbounded))
            .next()
            .copied()
            .or_else(|| members.first().copied())?;
        self.route_cursors.insert(route, next);
        Some(next)
    }

    pub(super) fn route_len(&self, route: Route) -> usize {
        self.routes.get(&route).map_or(0, BTreeSet::len)
    }

    pub(super) fn lane_deficit(&self, lane: u8) -> usize {
        self.lanes[usize::from(lane)].deficit
    }

    pub(super) fn charge(&mut self, lane: u8, wire_bytes: usize) {
        let state = &mut self.lanes[usize::from(lane)];
        state.deficit = state
            .deficit
            .checked_sub(wire_bytes)
            .expect("wire quota reserved");
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
    fn ready(lane: u8, deadline: u64, age: u64) -> Ready {
        Ready {
            route: Some((0, lane)),
            lane,
            deadline: RuntimeInstant::from_nanos(deadline),
            accepted_at: RuntimeInstant::from_nanos(age),
        }
    }

    #[test]
    fn repeated_deadline_priority_cannot_starve_round_robin_or_another_lane() {
        let mut queue = WireReady::default();
        for partition in 0..32 {
            queue.insert(key(partition), ready(0, 100, partition as u64));
        }
        queue.insert(key(0), ready(0, 1, 0));
        queue.insert(key(32), ready(1, 1000, 0));
        let mut ordinary = BTreeSet::new();
        for turn in 0..128 {
            let (partition, quantum) = queue.visit(100, 1000).unwrap();
            assert_eq!(partition == key(32), turn % 2 == 1);
            if turn % 4 == 0 {
                ordinary.insert(partition);
            } else if turn % 4 == 2 {
                assert_eq!(partition, key(0));
            }
            queue.charge(if partition == key(32) { 1 } else { 0 }, quantum);
        }
        assert_eq!(ordinary.len(), 32);
    }

    #[test]
    fn priority_is_deadline_then_age_and_every_priority_byte_is_debited() {
        let mut queue = WireReady::default();
        queue.insert(key(0), ready(0, 100, 1));
        queue.insert(key(1), ready(0, 10, 9));
        queue.insert(key(2), ready(0, 10, 2));
        assert_eq!(queue.visit(100, 1000).unwrap().0, key(0));
        assert_eq!(queue.visit(100, 1000).unwrap().0, key(2));
        assert_eq!(queue.lane_deficit(0), 200);
        queue.charge(0, 175);
        assert_eq!(queue.lane_deficit(0), 25);
        assert_eq!(queue.visit(100, 1000).unwrap().0, key(1));
        assert_eq!(queue.lane_deficit(0), 125);
    }

    #[test]
    fn gathering_and_removal_use_only_the_selected_route_and_release_every_index() {
        let mut queue = WireReady::default();
        for index in 0..32 {
            let mut state = ready((index % 2) as u8, 100, 0);
            state.route = Some((index % 3, state.lane));
            queue.insert(key(index), state);
        }
        let count = queue.route_len((0, 0));
        let mut selected = BTreeSet::new();
        for _ in 0..count {
            selected.insert(queue.gather_after((0, 0), key(0)).unwrap());
        }
        assert_eq!(selected.len(), count);
        assert!(selected.iter().all(|key| key.partition % 6 == 0));
        for index in 0..32 {
            queue.forget(key(index));
        }
        assert!(queue.is_empty());
        assert!(queue.routes.is_empty());
        assert!(queue.route_cursors.is_empty());
        assert_eq!(queue.active_lanes, 0);
        assert!(
            queue
                .lanes
                .iter()
                .all(|lane| lane.ordered.is_empty() && lane.priority.is_empty())
        );
    }
}
