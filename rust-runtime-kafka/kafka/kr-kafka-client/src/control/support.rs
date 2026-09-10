//! Bounded scratch owned by one synchronous control-frame parse.
use super::{BrokerNode, ControlError, ControlLimits, Result};
use std::mem::size_of;
mod index;
pub use index::{Index, unique};
pub fn throttle(value: i32) -> Result<u32> {
    u32::try_from(value).map_err(|_| ControlError::Invalid("negative throttle"))
}
pub struct OwnedBudget {
    pub(super) limits: ControlLimits,
    used: usize,
}
impl OwnedBudget {
    pub fn new(limits: ControlLimits) -> Self {
        Self { limits, used: 0 }
    }
    /// Preflight requested scratch, then account any allocator-reported spare
    /// capacity before retaining it. Arithmetic and allocation failures are typed.
    pub fn vec<T>(&mut self, count: usize, maximum: usize, label: &'static str) -> Result<Vec<T>> {
        self.array::<T>(count, maximum, label)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| ControlError::Limit("allocation failed"))?;
        self.charge(
            values
                .capacity()
                .saturating_sub(count)
                .checked_mul(size_of::<T>())
                .ok_or(ControlError::Limit("owned bytes"))?,
        )?;
        Ok(values)
    }
    pub fn charge(&mut self, bytes: usize) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or(ControlError::Limit("owned bytes"))?;
        if self.used > self.limits.owned_bytes {
            return Err(ControlError::Limit("owned bytes"));
        }
        Ok(())
    }
    pub fn array<T>(&mut self, count: usize, maximum: usize, resource: &'static str) -> Result<()> {
        if count > maximum {
            return Err(ControlError::Limit(resource));
        }
        self.charge(
            count
                .checked_mul(size_of::<T>())
                .ok_or(ControlError::Limit("owned bytes"))?,
        )
    }
    pub fn string(&mut self, value: &str) -> Result<String> {
        if value.len() > self.limits.string_bytes {
            return Err(ControlError::Limit("string bytes"));
        }
        self.charge(value.len())?;
        Ok(value.to_owned())
    }
    pub fn optional_string(&mut self, value: Option<&str>) -> Result<Option<String>> {
        value.map(|s| self.string(s)).transpose()
    }
    pub fn node(
        &mut self,
        id: i32,
        host: &str,
        port: i32,
        rack: Option<&str>,
    ) -> Result<BrokerNode> {
        if id < 0 || host.is_empty() || !(1..=u16::MAX as i32).contains(&port) {
            return Err(ControlError::Invalid("broker endpoint"));
        }
        Ok(BrokerNode {
            id,
            host: self.string(host)?,
            port: port as u16,
            rack: self.optional_string(rack)?,
        })
    }
}
