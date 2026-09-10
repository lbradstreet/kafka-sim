//! Opt-in peer-visibility timing. Ordinary SimNetwork pairs do not allocate
//! arrival queues, spawn propagation tasks, or change completion scheduling.
use super::*;
use kr_runtime::{JoinHandle, SimInstant};

/// A half-open interval of simulated absolute time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PropagationWindow {
    pub start: SimInstant,
    pub end: SimInstant,
}

/// Peer visibility policy for one direction of an explicitly opted-in pair.
/// Local completion latency remains the independent `LinkConfig::latency`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PropagationProfile {
    pub latency: SimDuration,
    /// Bytes whose visibility deadline falls in a window wait until its end.
    /// Already visible bytes are unaffected. Touching windows act continuously.
    pub black_holes: Vec<PropagationWindow>,
    /// Either direction's failure retires the whole pair, including pending
    /// operations and bytes. A pair cannot be created during these windows.
    pub fail_fast: Vec<PropagationWindow>,
}
impl PropagationProfile {
    fn validate(&self, now: SimInstant) -> Result<(), NetworkError> {
        if self.black_holes.len() + self.fail_fast.len() > 256
            || now
                .as_nanos()
                .checked_add(self.latency.as_nanos())
                .is_none()
        {
            return Err(invalid("propagation profile bounds"));
        }
        let mut windows: Vec<_> = self.black_holes.iter().chain(&self.fail_fast).collect();
        windows.sort_by_key(|w| w.start);
        if windows.iter().any(|w| w.start >= w.end)
            || windows.windows(2).any(|pair| pair[0].end > pair[1].start)
        {
            return Err(invalid("propagation windows overlap or are empty"));
        }
        Ok(())
    }
    fn arrival(&self, now: SimInstant) -> Result<SimInstant, NetworkError> {
        let mut at = now
            .as_nanos()
            .checked_add(self.latency.as_nanos())
            .ok_or_else(|| invalid("propagation deadline overflow"))?;
        for window in &self.black_holes {
            if (window.start.as_nanos()..window.end.as_nanos()).contains(&at) {
                at = window.end.as_nanos();
            }
        }
        Ok(SimInstant::from_nanos(at))
    }
}
fn invalid(reason: &'static str) -> NetworkError {
    NetworkError::InvalidConfig { reason }
}

impl SimNetwork {
    /// Creates a pair with bounded in-pipe propagation queues. The first profile
    /// is left-to-right; the second is right-to-left. Policy is fixed for the
    /// pair's lifetime, so same-time traffic cannot outrun an outage callback.
    ///
    /// # Errors
    /// Rejects invalid/overlapping windows, exhausted provider/runtime capacity,
    /// or setup during a FailFast window. Black-hole setup deadlines belong to
    /// the connector; once created, setup bytes obey these same visibility rules.
    pub fn connected_pair_with_propagation(
        &self,
        left: NodeId,
        right: NodeId,
        mut forward: PropagationProfile,
        mut reverse: PropagationProfile,
    ) -> Result<(SimStream, SimStream), NetworkError> {
        let mut state = self.state.borrow_mut();
        let now = state.handle.now();
        for (profile, link) in [
            (
                &mut forward,
                LinkKey {
                    from: left,
                    to: right,
                },
            ),
            (
                &mut reverse,
                LinkKey {
                    from: right,
                    to: left,
                },
            ),
        ] {
            profile.validate(now)?;
            profile.black_holes.sort_by_key(|w| w.start);
            profile.fail_fast.sort_by_key(|w| w.start);
            if profile
                .fail_fast
                .iter()
                .any(|w| w.start <= now && now < w.end)
            {
                return Err(NetworkError::Partitioned { link });
            }
        }
        let connection = state.create_connection(left, right, None)?;
        for (direction, profile) in [
            (Direction::LeftToRight, forward),
            (Direction::RightToLeft, reverse),
        ] {
            let fail_at = profile
                .fail_fast
                .iter()
                .find(|w| w.start >= now)
                .map(|w| w.start);
            direction
                .pipe_mut(
                    state
                        .connections
                        .get_mut(&connection)
                        .expect("created pair"),
                )
                .propagation = Some(Propagation {
                profile,
                fail_at,
                arrivals: VecDeque::new(),
                hidden: 0,
                wake: None,
                state: Rc::downgrade(&self.state),
            });
            if let Err(error) = state.schedule_propagation(connection, direction) {
                state.retire_propagating_pair(connection);
                return Err(error);
            }
        }
        Ok((
            SimStream::new(self.state.clone(), connection, Side::Left),
            SimStream::new(self.state.clone(), connection, Side::Right),
        ))
    }
}

struct Arrival {
    at: SimInstant,
    bytes: usize,
}
struct Wake {
    at: SimInstant,
    task: Option<JoinHandle<()>>,
}
impl Drop for Wake {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
pub(super) struct Propagation {
    profile: PropagationProfile,
    fail_at: Option<SimInstant>,
    arrivals: VecDeque<Arrival>,
    hidden: usize,
    wake: Option<Wake>,
    state: Weak<RefCell<NetworkState>>,
}
impl Pipe {
    pub(super) fn visible_bytes(&self) -> usize {
        self.bytes.len() - self.propagation.as_ref().map_or(0, |p| p.hidden)
    }
}
impl NetworkState {
    /// Drop the propagation state before calling ordinary closure so recursive
    /// servicing sees a fully retired pair, never another propagation callback.
    pub(super) fn retire_propagating_pair(&mut self, connection: u64) {
        let Some(pair) = self.connections.get_mut(&connection) else {
            return;
        };
        for pipe in [&mut pair.left_to_right, &mut pair.right_to_left] {
            pipe.propagation = None;
            pipe.bytes.clear();
            pipe.receiver_open = false;
        }
        self.close_side_immediately(connection, Side::Left);
        self.close_side_immediately(connection, Side::Right);
    }
    pub(super) fn advance_propagation(&mut self, connection: u64) {
        let Some(pair) = self.connections.get_mut(&connection) else {
            return;
        };
        if pair.left_to_right.propagation.is_none() && pair.right_to_left.propagation.is_none() {
            return;
        }
        let now = self.handle.now();
        if [&pair.left_to_right, &pair.right_to_left].iter().any(|p| {
            p.propagation
                .as_ref()
                .is_some_and(|p| p.fail_at.is_some_and(|at| at <= now))
        }) {
            self.retire_propagating_pair(connection);
            return;
        }
        for pipe in [&mut pair.left_to_right, &mut pair.right_to_left] {
            if let Some(p) = &mut pipe.propagation {
                while p.arrivals.front().is_some_and(|arrival| arrival.at <= now) {
                    p.hidden -= p.arrivals.pop_front().expect("due arrival").bytes;
                }
            }
        }
    }
    /// Reserve metadata before copying bytes so admission failure never leaves
    /// untracked possibly-visible data. At most one queue item per resident byte.
    pub(super) fn prepare_propagation(
        &mut self,
        connection: u64,
        direction: Direction,
        bytes: usize,
    ) -> Result<(), NetworkError> {
        let pipe = direction.pipe_mut(self.connections.get_mut(&connection).expect("live pair"));
        let Some(p) = &mut pipe.propagation else {
            return Ok(());
        };
        let now = self.handle.now();
        if bytes == 0 {
            return Ok(());
        }
        let at = p.profile.arrival(now)?;
        if at <= now {
            return Ok(());
        }
        p.arrivals
            .try_reserve(1)
            .map_err(|_| NetworkError::ResourceExhausted {
                resource: "propagation queue",
                limit: pipe.capacity,
            })?;
        p.arrivals.push_back(Arrival { at, bytes });
        p.hidden += bytes;
        // A failed scheduler rolls back the reservation before any bytes copy.
        if let Err(error) = self.schedule_propagation(connection, direction) {
            let p = direction
                .pipe_mut(self.connections.get_mut(&connection).expect("live pair"))
                .propagation
                .as_mut()
                .expect("propagation pair");
            p.arrivals.pop_back();
            p.hidden -= bytes;
            return Err(error);
        }
        Ok(())
    }
    pub(super) fn rollback_propagation(
        &mut self,
        connection: u64,
        direction: Direction,
        bytes: usize,
    ) {
        let now = self.handle.now();
        let pipe = direction.pipe_mut(self.connections.get_mut(&connection).expect("live pair"));
        if let Some(p) = &mut pipe.propagation
            && p.profile.arrival(now).is_ok_and(|at| at > now)
            && bytes != 0
        {
            p.arrivals.pop_back();
            p.hidden -= bytes;
        }
    }
    pub(super) fn schedule_propagation(
        &mut self,
        connection: u64,
        direction: Direction,
    ) -> Result<(), NetworkError> {
        let pipe = direction.pipe_mut(self.connections.get_mut(&connection).expect("live pair"));
        let Some(p) = &mut pipe.propagation else {
            return Ok(());
        };
        let at = p
            .arrivals
            .front()
            .map(|a| a.at)
            .into_iter()
            .chain(p.fail_at)
            .min();
        if at == p.wake.as_ref().map(|w| w.at) {
            return Ok(());
        }
        let Some(at) = at else {
            p.wake = None;
            return Ok(());
        };
        let handle = self.handle.clone();
        let weak = p.state.clone();
        let task = self
            .handle
            .spawn(async move {
                let elapsed = handle.sleep_until(at).await;
                if let Some(state) = weak.upgrade() {
                    let mut state = state.borrow_mut();
                    if let Some(pair) = state.connections.get_mut(&connection)
                        && let Some(p) = &mut direction.pipe_mut(pair).propagation
                    {
                        // Do not request our own cancellation while finishing.
                        if let Some(mut wake) = p.wake.take() {
                            wake.task.take();
                        }
                    }
                    if elapsed.is_err() {
                        state.retire_propagating_pair(connection);
                        return;
                    }
                    state.advance_propagation(connection);
                    state.service_direction(connection, direction, None);
                    if state.connections.contains_key(&connection)
                        && state.schedule_propagation(connection, direction).is_err()
                    {
                        state.retire_propagating_pair(connection);
                    }
                }
            })
            .map_err(|_| NetworkError::CompletionDriverUnavailable)?;
        p.wake = Some(Wake {
            at,
            task: Some(task),
        });
        Ok(())
    }
}
