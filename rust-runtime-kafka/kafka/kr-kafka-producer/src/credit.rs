//! Atomic multi-pool reservations and explicit ownership of resource obligations.
mod partition;
use crate::config::DescriptorAdmissionPolicy;
pub(crate) use partition::configured_storage_bytes;
pub use partition::{DescriptorClass, PartitionPressure};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

/// Shared admission authority. A single lock covers each complete reservation;
/// the engine and event consumer use the same ledger, including during teardown.
#[derive(Clone, Debug)]
pub struct SharedCredits(Arc<Mutex<CreditLedger>>);
impl SharedCredits {
    /// Clones name the same authority; equal limits do not establish ownership.
    pub(crate) fn same_authority(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// # Errors
    /// Rejects invalid lane counts before constructing the authority.
    pub fn new(limits: [usize; Resource::COUNT], lanes: u8) -> Result<Self, CreditError> {
        Self::with_descriptor_policy(limits, lanes, DescriptorAdmissionPolicy::Shared)
    }
    /// Creates one immutable admission policy for every user of this authority.
    /// # Errors
    /// Rejects invalid limits or failed bounded account-table allocation.
    pub fn with_descriptor_policy(
        limits: [usize; Resource::COUNT],
        lanes: u8,
        policy: DescriptorAdmissionPolicy,
    ) -> Result<Self, CreditError> {
        let mut ledger = CreditLedger::new(limits, lanes)?;
        if policy == DescriptorAdmissionPolicy::PartitionPressure {
            ledger.partition = Some(Box::new(partition::PartitionCredits::new(
                limits[Resource::Descriptors as usize],
            )?));
        }
        Ok(Self(Arc::new(Mutex::new(ledger))))
    }
    #[must_use]
    pub fn descriptor_policy(&self) -> DescriptorAdmissionPolicy {
        if self
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .partition
            .is_some()
        {
            DescriptorAdmissionPolicy::PartitionPressure
        } else {
            DescriptorAdmissionPolicy::Shared
        }
    }
    /// # Errors
    /// Returns the first reservation error without partial acquisition. Empty
    /// and single-claim reservations allocate no token backing; larger public
    /// transactions preserve arbitrary duplicate claims in one reserved Vec.
    pub fn reserve(&self, claims: &[Claim]) -> Result<HeldCredits, CreditError> {
        self.reserve_class(claims, DescriptorClass::Unclassified)
    }
    pub(crate) fn reserve_class(
        &self,
        claims: &[Claim],
        class: DescriptorClass,
    ) -> Result<HeldCredits, CreditError> {
        let credits = self
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reserve_held(claims, class)?;
        Ok(HeldCredits {
            ledger: self.clone(),
            credits,
        })
    }
    #[must_use]
    pub fn snapshot(&self) -> [PoolStatus; Resource::COUNT] {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).snapshot()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).is_empty()
    }
}

/// Resources held for exactly this object's lifetime. Moving the guard transfers
/// ownership. An admitted I/O/job stores its guard at the provider, so dropping
/// the observing future cannot return credits early.
#[derive(Debug)]
pub struct HeldCredits {
    ledger: SharedCredits,
    credits: GuardCredits,
}

// These tokens cannot escape their authority-owning HeldCredits. Keeping one
// authority in the guard avoids a redundant Arc and full-width Resource in
// every token. Guarded claims never expose a token ID, so their IDs need not be
// stored; admission still consumes exactly the same checked counter range.
// Public Credit retains its identity and foreign/double-release validation.
#[derive(Debug)]
struct GuardCredit {
    amount: usize,
    resource: u8,
    lane: u8,
    // One-based stable account slot; zero means no scoped descriptor charge.
    // Fits the existing padding, including in inline delivery-event guards.
    descriptor_slot: u32,
}
const _: () = assert!(Resource::COUNT <= u8::MAX as usize + 1);
impl GuardCredit {
    fn new(claim: Claim) -> Self {
        Self {
            amount: claim.amount,
            resource: claim.resource as u8,
            lane: claim.lane,
            descriptor_slot: 0,
        }
    }
    fn claim(&self) -> Claim {
        Claim {
            resource: Resource::ALL[self.resource as usize],
            amount: self.amount,
            lane: self.lane,
        }
    }
}

#[derive(Debug, Default)]
enum GuardCredits {
    #[default]
    Empty,
    One(GuardCredit),
    Many(Vec<GuardCredit>),
}
impl GuardCredits {
    fn as_slice(&self) -> &[GuardCredit] {
        match self {
            Self::Empty => &[],
            Self::One(credit) => std::slice::from_ref(credit),
            Self::Many(credits) => credits,
        }
    }
    fn as_mut_slice(&mut self) -> &mut [GuardCredit] {
        match self {
            Self::Empty => &mut [],
            Self::One(credit) => std::slice::from_mut(credit),
            Self::Many(credits) => credits,
        }
    }
    fn from_vec(mut credits: Vec<GuardCredit>) -> Self {
        match credits.len() {
            0 => Self::Empty,
            1 => Self::One(credits.pop().expect("one credit")),
            _ => Self::Many(credits),
        }
    }
    fn remove_released(&mut self) {
        if let Self::Many(credits) = self {
            credits.retain(|c| c.amount != 0);
        }
        if matches!(self, Self::One(credit) if credit.amount == 0) {
            *self = Self::Empty;
        } else if matches!(self, Self::Many(credits) if credits.len() <= 1)
            && let Self::Many(credits) = std::mem::take(self)
        {
            *self = Self::from_vec(credits);
        }
    }
    fn take(&mut self, resource: Resource) -> Self {
        let selected = self
            .as_slice()
            .iter()
            .filter(|c| c.claim().resource == resource)
            .count();
        if selected == 0 {
            return Self::Empty;
        }
        if selected == self.as_slice().len() {
            return std::mem::take(self);
        }
        let Self::Many(credits) = self else {
            unreachable!("partial selection requires many credits")
        };
        if selected == 1 {
            let index = credits
                .iter()
                .position(|c| c.claim().resource == resource)
                .expect("one selected credit");
            let result = Self::One(credits.remove(index));
            self.remove_released();
            return result;
        }
        if credits.len() - selected == 1 {
            let index = credits
                .iter()
                .position(|c| c.claim().resource != resource)
                .expect("one retained credit");
            let retained = Self::One(credits.remove(index));
            return std::mem::replace(self, retained);
        }
        // Public callers may submit arbitrary duplicate resources. Splitting
        // two multi-token owners requires a second backing allocation; the
        // producer's bounded distinct-resource transactions never use this path.
        // `take` preserves its existing infallible collection-allocation policy.
        let mut taken = Vec::with_capacity(selected);
        credits.retain_mut(|credit| {
            if credit.claim().resource == resource {
                taken.push(std::mem::replace(
                    credit,
                    GuardCredit {
                        amount: 0,
                        resource: 0,
                        lane: 0,
                        descriptor_slot: 0,
                    },
                ));
                false
            } else {
                true
            }
        });
        Self::Many(taken)
    }
}
impl HeldCredits {
    /// Separate retained token backing, excluding this guard's inline value
    /// and its shared ledger allocation. Single/empty guards retain zero bytes.
    #[must_use]
    pub fn metadata_capacity_bytes(&self) -> usize {
        match &self.credits {
            GuardCredits::Many(credits) => credits.capacity() * size_of::<GuardCredit>(),
            _ => 0,
        }
    }

    /// Atomically moves record-owned reservations to their resolved partition's
    /// lane. A full destination leaves every source reservation unchanged.
    /// Shared native allocations retain their acquisition owner's separate guard.
    /// # Errors
    /// Rejects foreign authorities, invalid lanes or fair-pool limits. No
    /// temporary token/reference allocation is needed for the transaction.
    pub fn transfer_lane_group(guards: &mut [&mut Self], lane: u8) -> Result<(), CreditError> {
        let Some(first) = guards.first() else {
            return Ok(());
        };
        let authority = first.ledger.clone();
        if guards
            .iter()
            .any(|guard| !Arc::ptr_eq(&authority.0, &guard.ledger.0))
        {
            return Err(CreditError::ForeignCredit);
        }
        authority
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .transfer_group(guards, lane)
    }
    #[must_use]
    pub fn amount(&self, resource: Resource) -> usize {
        self.credits
            .as_slice()
            .iter()
            .filter(|c| c.claim().resource == resource)
            .map(|c| c.amount)
            .sum()
    }
    /// Also checks empty guards, whose source authority remains meaningful.
    pub(crate) fn belongs_to(&self, authority: &SharedCredits) -> bool {
        self.ledger.same_authority(authority)
    }
    /// Moves one resource's obligations into a separately owned guard.
    /// Taking no/all tokens or a single selected/remaining token allocates no
    /// backing storage. A public multi-duplicate split may allocate one Vec,
    /// following this method's existing infallible allocation behavior.
    #[must_use]
    pub fn take(&mut self, resource: Resource) -> Self {
        Self {
            ledger: self.ledger.clone(),
            credits: self.credits.take(resource),
        }
    }
    /// Releases one resource at its documented terminal point.
    pub fn release(&mut self, resource: Resource) {
        let mut ledger = self.ledger.0.lock().unwrap_or_else(|p| p.into_inner());
        for credit in self.credits.as_mut_slice() {
            if credit.claim().resource == resource {
                ledger
                    .release_guard(credit, credit.amount)
                    .expect("private guard owns live credits from this ledger");
                credit.amount = 0;
            }
        }
        self.credits.remove_released();
    }
    /// Returns unused capacity after a bounded transform seals. This never
    /// creates a new obligation or consumes another credit token ID.
    /// # Errors
    /// Growing a reservation is rejected without changing any owned credit.
    pub fn shrink(&mut self, resource: Resource, keep: usize) -> Result<(), CreditError> {
        if keep > self.amount(resource) {
            return Err(CreditError::InvalidAmount);
        }
        let mut remaining = keep;
        let mut ledger = self.ledger.0.lock().unwrap_or_else(|p| p.into_inner());
        for credit in self.credits.as_mut_slice() {
            if credit.claim().resource == resource {
                let amount = remaining.min(credit.amount);
                remaining -= amount;
                ledger.release_guard(credit, credit.amount - amount)?;
                credit.amount = amount;
            }
        }
        self.credits.remove_released();
        Ok(())
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.credits.as_slice().is_empty()
    }
}
impl Drop for HeldCredits {
    fn drop(&mut self) {
        let mut ledger = self.ledger.0.lock().unwrap_or_else(|p| p.into_inner());
        for credit in self.credits.as_slice() {
            ledger
                .release_guard(credit, credit.amount)
                .expect("private guard owns live credits from this ledger");
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum Resource {
    Mailbox,
    Descriptors,
    InputBytes,
    ReleaseEvents,
    DeliveryEvents,
    CodecContexts,
    CompressedBytes,
    StagingBytes,
    RequestSlots,
    WireWindow,
    RxBytes,
    ControlReserve,
    WorkerJobs,
    ControlEvents,
    RequestMetadata,
    TlsBytes,
}
impl Resource {
    pub const COUNT: usize = 16;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Mailbox,
        Self::Descriptors,
        Self::InputBytes,
        Self::ReleaseEvents,
        Self::DeliveryEvents,
        Self::CodecContexts,
        Self::CompressedBytes,
        Self::StagingBytes,
        Self::RequestSlots,
        Self::WireWindow,
        Self::RxBytes,
        Self::ControlReserve,
        Self::WorkerJobs,
        Self::ControlEvents,
        Self::RequestMetadata,
        Self::TlsBytes,
    ];
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Mailbox => "mailbox",
            Self::Descriptors => "descriptors",
            Self::InputBytes => "input_bytes",
            Self::ReleaseEvents => "release_events",
            Self::DeliveryEvents => "delivery_events",
            Self::CodecContexts => "codec_contexts",
            Self::CompressedBytes => "compressed_bytes",
            Self::StagingBytes => "staging_bytes",
            Self::RequestSlots => "request_slots",
            Self::WireWindow => "wire_window",
            Self::RxBytes => "rx_bytes",
            Self::ControlReserve => "control_reserve",
            Self::WorkerJobs => "worker_jobs",
            Self::ControlEvents => "control_events",
            Self::RequestMetadata => "request_metadata",
            Self::TlsBytes => "tls_bytes",
        }
    }
    #[must_use]
    pub const fn fair(self) -> bool {
        matches!(
            self,
            Self::Descriptors | Self::InputBytes | Self::DeliveryEvents
        )
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CreditError {
    PartitionPressure(PartitionPressure),
    ResourceExhausted {
        resource: &'static str,
        limit: usize,
    },
    InvalidLane {
        lane: u8,
        lanes: u8,
    },
    InvalidAmount,
    TokenExhausted,
    CounterExhausted {
        resource: &'static str,
    },
    AllocationFailed,
    ForeignCredit,
    AlreadyReleased,
    ConservationViolation,
}
impl fmt::Display for CreditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartitionPressure(pressure) => {
                write!(f, "partition descriptor pressure: {pressure:?}")
            }
            Self::ResourceExhausted { resource, limit } => {
                write!(f, "{resource} exhausted (limit {limit})")
            }
            Self::InvalidLane { lane, lanes } => write!(f, "lane {lane} is outside 0..{lanes}"),
            Self::InvalidAmount => f.write_str("credit amount must be nonzero"),
            Self::TokenExhausted => f.write_str("credit token space exhausted"),
            Self::CounterExhausted { resource } => {
                write!(f, "{resource} cumulative credit counter exhausted")
            }
            Self::AllocationFailed => f.write_str("credit allocation failed"),
            Self::ForeignCredit => f.write_str("credit belongs to another ledger"),
            Self::AlreadyReleased => f.write_str("credit was already released"),
            Self::ConservationViolation => f.write_str("credit conservation violated"),
        }
    }
}
impl std::error::Error for CreditError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Claim {
    pub resource: Resource,
    pub amount: usize,
    pub lane: u8,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolStatus {
    pub limit: usize,
    pub held: usize,
    pub peak_held: usize,
    /// Cumulative successful ownership acquisition/release, excluding relabeling.
    pub reserved: u64,
    pub released: u64,
    pub shared_held: usize,
    pub lane_held: [usize; 4],
    pub lane_borrowed: [usize; 4],
    pub guaranteed_per_lane: usize,
}

/// A non-Copy obligation. Explicit release is required, including teardown.
#[derive(Debug)]
pub struct Credit {
    owner: Arc<()>,
    id: u64,
    claim: Claim,
    active: bool,
}
impl Credit {
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }
    #[must_use]
    pub const fn claim(&self) -> Claim {
        self.claim
    }
    #[must_use]
    pub const fn is_released(&self) -> bool {
        !self.active
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        if self.active && cfg!(debug_assertions) && !std::thread::panicking() {
            panic!(
                "unreleased {} credit {} ({} units)",
                self.claim.resource.name(),
                self.id,
                self.claim.amount
            );
        }
    }
}

/// Owner-local ledger. Shared admission holds its outer mutex for the complete
/// transaction, so no caller observes a partial cross-pool reservation.
#[derive(Debug)]
pub struct CreditLedger {
    owner: Arc<()>,
    pools: [PoolStatus; Resource::COUNT],
    lanes: u8,
    next_id: u64,
    partition: Option<Box<partition::PartitionCredits>>,
}
impl CreditLedger {
    fn transfer_group(
        &mut self,
        guards: &mut [&mut HeldCredits],
        lane: u8,
    ) -> Result<(), CreditError> {
        if lane >= self.lanes {
            return Err(CreditError::InvalidLane {
                lane,
                lanes: self.lanes,
            });
        }
        let mut next = self.pools;
        for credit in guards.iter().flat_map(|guard| guard.credits.as_slice()) {
            if credit.amount == 0 {
                return Err(CreditError::AlreadyReleased);
            }
            let claim = credit.claim();
            let old = claim.lane as usize;
            let pool = &mut next[claim.resource as usize];
            if pool.held < claim.amount || pool.lane_held[old] < claim.amount {
                return Err(CreditError::ConservationViolation);
            }
            if claim.resource.fair() {
                let borrowed = claim.amount.min(pool.lane_borrowed[old]);
                pool.lane_borrowed[old] -= borrowed;
                pool.shared_held -= borrowed;
            }
            pool.lane_held[old] -= claim.amount;
            pool.held -= claim.amount;
        }
        for credit in guards.iter().flat_map(|guard| guard.credits.as_slice()) {
            self.add_claim(
                &mut next,
                Claim {
                    lane,
                    ..credit.claim()
                },
            )?;
        }
        self.pools = next;
        for credit in guards
            .iter_mut()
            .flat_map(|guard| guard.credits.as_mut_slice())
        {
            credit.lane = lane;
        }
        Ok(())
    }
    /// Half of each fair pool is guaranteed equally to lanes; the remainder is
    /// borrowable. A lane cannot consume another lane's idle guarantee.
    ///
    /// # Errors
    /// Rejects a lane count outside 1..=4.
    pub fn new(limits: [usize; Resource::COUNT], lanes: u8) -> Result<Self, CreditError> {
        if !(1..=4).contains(&lanes) {
            return Err(CreditError::InvalidLane {
                lane: lanes,
                lanes: 4,
            });
        }
        let pools = std::array::from_fn(|i| PoolStatus {
            limit: limits[i],
            guaranteed_per_lane: if Resource::ALL[i].fair() {
                limits[i] / (usize::from(lanes) * 2)
            } else {
                0
            },
            ..Default::default()
        });
        Ok(Self {
            owner: Arc::new(()),
            pools,
            lanes,
            next_id: 1,
            partition: None,
        })
    }
    #[must_use]
    pub fn status(&self, resource: Resource) -> PoolStatus {
        self.pools[resource as usize]
    }
    #[must_use]
    pub fn snapshot(&self) -> [PoolStatus; Resource::COUNT] {
        self.pools
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pools.iter().all(|p| p.held == 0)
            && self.partition.as_ref().is_none_or(|p| p.is_empty())
    }
    /// # Errors
    /// Validates every claim and the ID counter before changing any pool.
    pub fn reserve_many(&mut self, claims: &[Claim]) -> Result<Vec<Credit>, CreditError> {
        let (next, end) = self.prepare_reservation(claims)?;
        let mut credits = Vec::new();
        credits
            .try_reserve_exact(claims.len())
            .map_err(|_| CreditError::AllocationFailed)?;
        for (i, &claim) in claims.iter().enumerate() {
            credits.push(Credit {
                owner: self.owner.clone(),
                id: self.next_id + i as u64,
                claim,
                active: true,
            });
        }
        self.pools = next;
        self.next_id = end;
        Ok(credits)
    }
    fn prepare_reservation(
        &self,
        claims: &[Claim],
    ) -> Result<([PoolStatus; Resource::COUNT], u64), CreditError> {
        let mut next = self.pools;
        for &claim in claims {
            self.add_claim(&mut next, claim)?;
            Self::count_reservation(&mut next, claim)?;
        }
        let count = u64::try_from(claims.len()).map_err(|_| CreditError::TokenExhausted)?;
        let end = self
            .next_id
            .checked_add(count)
            .ok_or(CreditError::TokenExhausted)?;
        Ok((next, end))
    }
    fn reserve_held(
        &mut self,
        claims: &[Claim],
        class: DescriptorClass,
    ) -> Result<GuardCredits, CreditError> {
        let (next, end) = self.prepare_reservation(claims)?;
        let descriptors = next[Resource::Descriptors as usize].held
            - self.pools[Resource::Descriptors as usize].held;
        if let Some(partition) = &self.partition {
            partition.check(
                class,
                self.pools[Resource::Descriptors as usize].held,
                descriptors,
            )?;
        }
        let mut credits = match claims {
            [] => GuardCredits::Empty,
            [claim] => GuardCredits::One(GuardCredit::new(*claim)),
            _ => {
                let mut credits = Vec::new();
                credits
                    .try_reserve_exact(claims.len())
                    .map_err(|_| CreditError::AllocationFailed)?;
                for &claim in claims {
                    credits.push(GuardCredit::new(claim));
                }
                GuardCredits::Many(credits)
            }
        };
        if descriptors != 0
            && let Some(partition) = &mut self.partition
        {
            let slot = partition.acquire(class, descriptors);
            for credit in credits.as_mut_slice() {
                if credit.claim().resource == Resource::Descriptors {
                    credit.descriptor_slot = slot;
                }
            }
        }
        self.pools = next;
        self.next_id = end;
        Ok(credits)
    }
    /// # Errors
    /// Same atomic failure guarantees as [`Self::reserve_many`].
    pub fn reserve(
        &mut self,
        resource: Resource,
        amount: usize,
        lane: u8,
    ) -> Result<Credit, CreditError> {
        let claim = Claim {
            resource,
            amount,
            lane,
        };
        let (next, end) = self.prepare_reservation(&[claim])?;
        let credit = Credit {
            owner: self.owner.clone(),
            id: self.next_id,
            claim,
            active: true,
        };
        self.pools = next;
        self.next_id = end;
        Ok(credit)
    }
    /// Checks a complete prospective transaction without consuming IDs or credits.
    ///
    /// # Errors
    /// Returns the first exhausted pool, malformed claim, or exhausted token space.
    pub fn can_reserve(&self, claims: &[Claim]) -> Result<(), CreditError> {
        self.next_id
            .checked_add(claims.len() as u64)
            .ok_or(CreditError::TokenExhausted)?;
        let mut pools = self.pools;
        for &claim in claims {
            self.add_claim(&mut pools, claim)?;
            Self::count_reservation(&mut pools, claim)?;
        }
        Ok(())
    }
    fn count_reservation(
        pools: &mut [PoolStatus; Resource::COUNT],
        claim: Claim,
    ) -> Result<(), CreditError> {
        let error = CreditError::CounterExhausted {
            resource: claim.resource.name(),
        };
        let amount = u64::try_from(claim.amount).map_err(|_| error)?;
        let pool = &mut pools[claim.resource as usize];
        pool.reserved = pool.reserved.checked_add(amount).ok_or(error)?;
        pool.peak_held = pool.peak_held.max(pool.held);
        Ok(())
    }
    fn count_release(pool: &mut PoolStatus, amount: usize) -> Result<(), CreditError> {
        let amount = u64::try_from(amount).map_err(|_| CreditError::ConservationViolation)?;
        let released = pool
            .released
            .checked_add(amount)
            .filter(|released| *released <= pool.reserved)
            .ok_or(CreditError::ConservationViolation)?;
        pool.released = released;
        Ok(())
    }
    fn add_claim(
        &self,
        pools: &mut [PoolStatus; Resource::COUNT],
        claim: Claim,
    ) -> Result<(), CreditError> {
        if claim.lane >= self.lanes {
            return Err(CreditError::InvalidLane {
                lane: claim.lane,
                lanes: self.lanes,
            });
        }
        if claim.amount == 0 {
            return Err(CreditError::InvalidAmount);
        }
        let pool = &mut pools[claim.resource as usize];
        let lane = claim.lane as usize;
        let full = CreditError::ResourceExhausted {
            resource: claim.resource.name(),
            limit: pool.limit,
        };
        let held = pool.held.checked_add(claim.amount).ok_or(full)?;
        if held > pool.limit {
            return Err(full);
        }
        if claim.resource.fair() {
            let own_held = pool.lane_held[lane] - pool.lane_borrowed[lane];
            let own_available = pool.guaranteed_per_lane - own_held;
            let borrowed = claim.amount.saturating_sub(own_available);
            let shared_limit = pool.limit - pool.guaranteed_per_lane * usize::from(self.lanes);
            if borrowed > shared_limit - pool.shared_held {
                return Err(full);
            }
            pool.lane_borrowed[lane] += borrowed;
            pool.shared_held += borrowed;
        }
        pool.lane_held[lane] += claim.amount;
        pool.held = held;
        Ok(())
    }
    /// Explicitly releases a token. Borrowed units are returned before the lane's
    /// guarantee, even when tokens were acquired in another order.
    ///
    /// # Errors
    /// Rejects foreign/already-released tokens without changing either ledger.
    pub fn release(&mut self, credit: &mut Credit) -> Result<(), CreditError> {
        if !Arc::ptr_eq(&self.owner, &credit.owner) {
            return Err(CreditError::ForeignCredit);
        }
        if !credit.active {
            return Err(CreditError::AlreadyReleased);
        }
        self.release_claim(credit.claim)?;
        credit.active = false;
        Ok(())
    }
    fn release_claim(&mut self, claim: Claim) -> Result<(), CreditError> {
        let lane = claim.lane as usize;
        let pool = &mut self.pools[claim.resource as usize];
        if claim.amount > pool.held || claim.amount > pool.lane_held[lane] {
            return Err(CreditError::ConservationViolation);
        }
        Self::count_release(pool, claim.amount)?;
        if claim.resource.fair() {
            let borrowed = claim.amount.min(pool.lane_borrowed[lane]);
            pool.lane_borrowed[lane] -= borrowed;
            pool.shared_held -= borrowed;
        }
        pool.held -= claim.amount;
        pool.lane_held[lane] -= claim.amount;
        Ok(())
    }
    fn release_guard(&mut self, credit: &GuardCredit, amount: usize) -> Result<(), CreditError> {
        self.release_claim(Claim {
            amount,
            ..credit.claim()
        })?;
        if credit.descriptor_slot != 0 && amount != 0 {
            self.partition
                .as_mut()
                .expect("scoped guard has a ledger")
                .release(credit.descriptor_slot, amount);
        }
        Ok(())
    }
    /// # Errors
    /// Rejects foreign/released credits and growth without mutation.
    pub fn shrink(&mut self, credit: &mut Credit, keep: usize) -> Result<(), CreditError> {
        if !Arc::ptr_eq(&self.owner, &credit.owner) {
            return Err(CreditError::ForeignCredit);
        }
        if !credit.active {
            return Err(CreditError::AlreadyReleased);
        }
        if keep > credit.claim.amount {
            return Err(CreditError::InvalidAmount);
        }
        if keep == 0 {
            return self.release(credit);
        }
        let returned = credit.claim.amount - keep;
        let lane = credit.claim.lane as usize;
        let pool = &mut self.pools[credit.claim.resource as usize];
        if returned > pool.held || returned > pool.lane_held[lane] {
            return Err(CreditError::ConservationViolation);
        }
        Self::count_release(pool, returned)?;
        if credit.claim.resource.fair() {
            let borrowed = returned.min(pool.lane_borrowed[lane]);
            pool.lane_borrowed[lane] -= borrowed;
            pool.shared_held -= borrowed;
        }
        pool.held -= returned;
        pool.lane_held[lane] -= returned;
        credit.claim.amount = keep;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inline_guard_fits_the_former_vector_guard_without_inflating_event_storage() {
        assert!(size_of::<HeldCredits>() <= size_of::<(SharedCredits, Vec<Credit>)>());
        assert!(size_of::<GuardCredit>() < size_of::<Credit>());
    }
    fn backing(guard: &HeldCredits) -> Option<(*const GuardCredit, usize)> {
        match &guard.credits {
            GuardCredits::Many(credits) => Some((credits.as_ptr(), credits.capacity())),
            _ => None,
        }
    }
    #[test]
    fn single_claims_and_unique_resource_splits_keep_no_detached_vector_backing() {
        let authority = SharedCredits::new([100; Resource::COUNT], 2).unwrap();
        let claim = |resource| Claim {
            resource,
            amount: 1,
            lane: 0,
        };
        let mut one = authority.reserve(&[claim(Resource::InputBytes)]).unwrap();
        assert!(matches!(one.credits, GuardCredits::One(_)));
        assert_eq!(one.metadata_capacity_bytes(), 0);
        let mut extracted = one.take(Resource::InputBytes);
        assert!(matches!(one.credits, GuardCredits::Empty));
        assert!(matches!(extracted.credits, GuardCredits::One(_)));
        HeldCredits::transfer_lane_group(&mut [&mut one, &mut extracted], 1).unwrap();
        extracted.release(Resource::InputBytes);
        assert!(matches!(extracted.credits, GuardCredits::Empty));

        let mut group = authority
            .reserve(&[
                claim(Resource::Descriptors),
                claim(Resource::InputBytes),
                claim(Resource::DeliveryEvents),
                claim(Resource::Mailbox),
            ])
            .unwrap();
        let allocation = backing(&group).unwrap();
        assert_eq!(
            group.metadata_capacity_bytes(),
            allocation.1 * size_of::<GuardCredit>()
        );
        let input = group.take(Resource::InputBytes);
        assert!(backing(&input).is_none());
        assert_eq!(backing(&group), Some(allocation));
        group.release(Resource::Descriptors);
        assert_eq!(backing(&group), Some(allocation));
        let delivery = group.take(Resource::DeliveryEvents);
        assert!(matches!(delivery.credits, GuardCredits::One(_)));
        assert!(matches!(group.credits, GuardCredits::One(_)));
        assert_eq!(group.metadata_capacity_bytes(), 0);
        drop((input, delivery, group));
        assert!(authority.is_empty());
    }
    #[test]
    fn arbitrary_duplicate_claims_preserve_ids_and_conservation_through_splits() {
        let authority = SharedCredits::new([10_000; Resource::COUNT], 4).unwrap();
        let claims: Vec<_> = (0..Resource::COUNT * 4 + 3)
            .map(|index| Claim {
                resource: if index % 3 == 0 {
                    Resource::Descriptors
                } else {
                    Resource::InputBytes
                },
                amount: index % 5 + 1,
                lane: (index % 4) as u8,
            })
            .collect();
        let mut group = authority.reserve(&claims).unwrap();
        assert_eq!(
            group
                .credits
                .as_slice()
                .iter()
                .map(GuardCredit::claim)
                .collect::<Vec<_>>(),
            claims
        );
        let old_backing = backing(&group).unwrap();
        let before = authority.snapshot();
        let mut input = group.take(Resource::InputBytes);
        assert_eq!(authority.snapshot(), before);
        assert_eq!(backing(&group), Some(old_backing));
        let taken_claims: Vec<_> = input
            .credits
            .as_slice()
            .iter()
            .map(GuardCredit::claim)
            .collect();
        assert_eq!(
            taken_claims,
            claims
                .iter()
                .copied()
                .filter(|c| c.resource == Resource::InputBytes)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            input.amount(Resource::InputBytes),
            claims
                .iter()
                .filter(|c| c.resource == Resource::InputBytes)
                .map(|c| c.amount)
                .sum()
        );
        HeldCredits::transfer_lane_group(&mut [&mut group, &mut input], 2).unwrap();
        input.shrink(Resource::InputBytes, 7).unwrap();
        let status = authority.snapshot()[Resource::InputBytes as usize];
        assert_eq!(status.lane_held, [0, 0, 7, 0]);
        input.release(Resource::InputBytes);
        assert!(input.is_empty());
        drop(group);
        assert!(authority.is_empty());
        let mut ledger = authority.0.lock().unwrap();
        let mut next = ledger.reserve(Resource::Mailbox, 1, 0).unwrap();
        assert_eq!(next.id(), claims.len() as u64 + 1);
        ledger.release(&mut next).unwrap();
        for status in ledger.snapshot() {
            assert_eq!(status.reserved, status.released);
        }
    }
    #[test]
    fn duplicate_claim_failures_and_foreign_transfers_leave_owners_unchanged() {
        let authority = SharedCredits::new([10; Resource::COUNT], 2).unwrap();
        let first = Claim {
            resource: Resource::InputBytes,
            amount: 3,
            lane: 0,
        };
        for last in [
            Claim { amount: 8, ..first },
            Claim { amount: 0, ..first },
            Claim { lane: 2, ..first },
        ] {
            let before = authority.snapshot();
            assert!(authority.reserve(&[first, last]).is_err());
            assert_eq!(authority.snapshot(), before);
            assert_eq!(authority.0.lock().unwrap().next_id, 1);
        }
        let other = SharedCredits::new([10; Resource::COUNT], 2).unwrap();
        let mut left = authority.reserve(&[first, first]).unwrap();
        let mut right = other.reserve(&[first]).unwrap();
        let before = authority.snapshot();
        let other_before = other.snapshot();
        assert_eq!(
            HeldCredits::transfer_lane_group(&mut [&mut left, &mut right], 1),
            Err(CreditError::ForeignCredit)
        );
        assert_eq!(authority.snapshot(), before);
        assert_eq!(other.snapshot(), other_before);
        let all = left.take(Resource::InputBytes);
        assert!(left.is_empty());
        assert_eq!(all.amount(Resource::InputBytes), 6);
        drop((left, all, right));
        assert!(authority.is_empty() && other.is_empty());
    }
    #[test]
    fn inline_and_many_reservations_preserve_counter_exhaustion_atomicity() {
        let authority = SharedCredits::new([100; Resource::COUNT], 1).unwrap();
        authority.0.lock().unwrap().next_id = u64::MAX - 1;
        let claim = Claim {
            resource: Resource::InputBytes,
            amount: 1,
            lane: 0,
        };
        let before = authority.snapshot();
        assert_eq!(
            authority.reserve(&[claim, claim]).unwrap_err(),
            CreditError::TokenExhausted
        );
        assert_eq!(authority.snapshot(), before);
        let one = authority.reserve(&[claim]).unwrap();
        assert!(matches!(one.credits, GuardCredits::One(_)));
        let full = authority.snapshot();
        assert_eq!(
            authority.reserve(&[claim]).unwrap_err(),
            CreditError::TokenExhausted
        );
        assert_eq!(authority.snapshot(), full);
        assert!(authority.reserve(&[]).unwrap().is_empty());
        drop(one);
        assert!(authority.is_empty());
    }
    #[test]
    fn cumulative_counters_are_atomic_and_lane_transfer_is_not_new_ownership() {
        let c = SharedCredits::new([100; Resource::COUNT], 2).unwrap();
        let mut guard = c
            .reserve(&[Claim {
                resource: Resource::InputBytes,
                amount: 30,
                lane: 0,
            }])
            .unwrap();
        HeldCredits::transfer_lane_group(&mut [&mut guard], 1).unwrap();
        let pool = c.snapshot()[Resource::InputBytes as usize];
        assert_eq!((pool.reserved, pool.released, pool.held), (30, 0, 30));
        guard.shrink(Resource::InputBytes, 11).unwrap();
        let pool = c.snapshot()[Resource::InputBytes as usize];
        assert_eq!((pool.reserved, pool.released, pool.held), (30, 19, 11));
        drop(guard);
        let pool = c.snapshot()[Resource::InputBytes as usize];
        assert_eq!((pool.reserved, pool.released, pool.held), (30, 30, 0));
        assert_eq!(pool.peak_held, 30);
        let mut ledger = CreditLedger::new([100; Resource::COUNT], 1).unwrap();
        ledger.pools[Resource::InputBytes as usize].reserved = u64::MAX;
        ledger.pools[Resource::InputBytes as usize].released = u64::MAX;
        let before = ledger.snapshot();
        let claims = [
            Claim {
                resource: Resource::Descriptors,
                amount: 1,
                lane: 0,
            },
            Claim {
                resource: Resource::InputBytes,
                amount: 1,
                lane: 0,
            },
        ];
        assert!(matches!(
            ledger.can_reserve(&claims),
            Err(CreditError::CounterExhausted { .. })
        ));
        assert!(matches!(
            ledger.reserve_many(&claims),
            Err(CreditError::CounterExhausted { .. })
        ));
        assert_eq!(ledger.snapshot(), before);
        assert_eq!(ledger.next_id, 1);
    }
    #[test]
    fn partition_lane_transfer_is_atomic_across_input_and_completion_pools() {
        let c = SharedCredits::new([100; Resource::COUNT], 2).unwrap();
        let mut input = c
            .reserve(&[Claim {
                resource: Resource::InputBytes,
                amount: 30,
                lane: 0,
            }])
            .unwrap();
        let mut delivery = c
            .reserve(&[Claim {
                resource: Resource::DeliveryEvents,
                amount: 30,
                lane: 0,
            }])
            .unwrap();
        let hot = c
            .reserve(&[Claim {
                resource: Resource::DeliveryEvents,
                amount: 70,
                lane: 1,
            }])
            .unwrap();
        let before = c.snapshot();
        assert!(HeldCredits::transfer_lane_group(&mut [&mut input, &mut delivery], 1).is_err());
        assert_eq!(c.snapshot(), before);
        drop(hot);
        HeldCredits::transfer_lane_group(&mut [&mut input, &mut delivery], 1).unwrap();
        for resource in [Resource::InputBytes, Resource::DeliveryEvents] {
            assert_eq!(c.snapshot()[resource as usize].lane_held, [0, 30, 0, 0]);
        }
        drop(input);
        drop(delivery);
        assert!(c.is_empty());
    }
    #[test]
    fn shrinking_shared_guard_returns_slack_without_reallocating_credit_ids() {
        let credits = SharedCredits::new([100; Resource::COUNT], 2).unwrap();
        let mut held = credits
            .reserve(&[Claim {
                resource: Resource::InputBytes,
                amount: 75,
                lane: 0,
            }])
            .unwrap();
        assert_eq!(
            held.shrink(Resource::InputBytes, 76),
            Err(CreditError::InvalidAmount)
        );
        held.shrink(Resource::InputBytes, 40).unwrap();
        let status = credits.snapshot()[Resource::InputBytes as usize];
        assert_eq!(status.held, 40);
        assert_eq!(status.shared_held, 15);
        let held = Arc::new(held);
        let provider = held.clone();
        drop(held);
        assert_eq!(credits.snapshot()[Resource::InputBytes as usize].held, 40);
        drop(provider);
        assert!(credits.is_empty());
    }
    #[test]
    fn cross_pool_failure_leaves_every_pool_and_id_unchanged() {
        let mut ledger = CreditLedger::new([10; Resource::COUNT], 2).unwrap();
        let before = ledger.snapshot();
        assert!(
            ledger
                .reserve_many(&[
                    Claim {
                        resource: Resource::Descriptors,
                        amount: 4,
                        lane: 0
                    },
                    Claim {
                        resource: Resource::InputBytes,
                        amount: 11,
                        lane: 0
                    }
                ])
                .is_err()
        );
        assert_eq!(ledger.snapshot(), before);
        let mut token = ledger.reserve(Resource::Mailbox, 1, 0).unwrap();
        assert_eq!(token.id(), 1);
        ledger.release(&mut token).unwrap();
        assert!(ledger.is_empty());
    }
    #[test]
    fn a_hot_lane_cannot_take_a_cold_lanes_guarantee() {
        let mut ledger = CreditLedger::new([100; Resource::COUNT], 2).unwrap();
        let mut hot = ledger.reserve(Resource::Descriptors, 75, 0).unwrap();
        assert!(ledger.reserve(Resource::Descriptors, 1, 0).is_err());
        let mut cold = ledger.reserve(Resource::Descriptors, 25, 1).unwrap();
        ledger.release(&mut hot).unwrap();
        assert_eq!(ledger.status(Resource::Descriptors).held, 25);
        ledger.release(&mut cold).unwrap();
        assert!(ledger.is_empty());
    }
    #[test]
    fn returning_old_allowance_credits_refills_shared_before_guarantees() {
        let mut ledger = CreditLedger::new([100; Resource::COUNT], 2).unwrap();
        let mut a = ledger.reserve(Resource::InputBytes, 25, 0).unwrap();
        let mut b = ledger.reserve(Resource::InputBytes, 25, 0).unwrap();
        ledger.release(&mut a).unwrap();
        let status = ledger.status(Resource::InputBytes);
        assert_eq!(status.shared_held, 0);
        assert_eq!(status.lane_held[0], 25);
        ledger.release(&mut b).unwrap();
    }
    #[test]
    fn foreign_tokens_and_double_release_are_rejected_without_mutation() {
        let mut a = CreditLedger::new([10; Resource::COUNT], 1).unwrap();
        let mut b = CreditLedger::new([10; Resource::COUNT], 1).unwrap();
        let mut token = a.reserve(Resource::Mailbox, 1, 0).unwrap();
        assert_eq!(b.release(&mut token), Err(CreditError::ForeignCredit));
        assert!(b.is_empty());
        a.release(&mut token).unwrap();
        assert_eq!(a.release(&mut token), Err(CreditError::AlreadyReleased));
    }
    #[test]
    fn token_exhaustion_preserves_all_credit_counts() {
        let mut ledger = CreditLedger::new([10; Resource::COUNT], 1).unwrap();
        ledger.next_id = u64::MAX;
        let before = ledger.snapshot();
        assert_eq!(
            ledger.reserve(Resource::Mailbox, 1, 0).unwrap_err(),
            CreditError::TokenExhausted
        );
        assert_eq!(ledger.snapshot(), before);
    }
    #[test]
    fn deterministic_multi_lane_credit_campaign_conserves_owners() {
        for seed in 1u64..64 {
            let mut state = seed;
            let mut ledger = CreditLedger::new([64; Resource::COUNT], 4).unwrap();
            let mut held = Vec::new();
            for step in 0..512 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if state & 1 == 0 {
                    if let Ok(token) = ledger.reserve(
                        Resource::Descriptors,
                        1 + (state as usize % 11),
                        ((state >> 8) % 4) as u8,
                    ) {
                        held.push(token);
                    }
                } else if !held.is_empty() {
                    let mut token = held.swap_remove(state as usize % held.len());
                    ledger.release(&mut token).unwrap();
                }
                let status = ledger.status(Resource::Descriptors);
                assert_eq!(
                    status.held,
                    held.iter().map(|c| c.claim.amount).sum::<usize>(),
                    "seed={seed} step={step}"
                );
                assert_eq!(status.held, status.lane_held.iter().sum());
                assert_eq!(status.shared_held, status.lane_borrowed.iter().sum());
                for lane in 0..4 {
                    assert!(
                        status.lane_held[lane] - status.lane_borrowed[lane]
                            <= status.guaranteed_per_lane
                    );
                }
            }
            for mut token in held {
                ledger.release(&mut token).unwrap();
            }
            assert!(ledger.is_empty());
        }
    }
}
