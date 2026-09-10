//! Exact retained element capacity without eager element initialization.
//!
//! `try_reserve_exact` may return excess capacity. A successful fixed-storage
//! constructor explicitly verifies that it did not; otherwise the unpublished
//! reservation is dropped and the caller returns its allocation-failure error.
//! Rejected reservations are startup transients. Allocator bookkeeping/rounding
//! that is not exposed as collection capacity is outside this element bound.
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CapacityError;

fn check_capacity<T>(requested: usize, actual: usize) -> Result<(), CapacityError> {
    // ZST collections allocate no element backing and may report usize::MAX.
    // Their public owners enforce the configured logical limit separately.
    if size_of::<T>() == 0 || actual == requested {
        Ok(())
    } else {
        Err(CapacityError)
    }
}

pub(crate) fn try_vec<T>(capacity: usize) -> Result<Vec<T>, CapacityError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| CapacityError)?;
    check_capacity::<T>(capacity, values.capacity())?;
    Ok(values)
}

pub(crate) fn try_deque<T>(capacity: usize) -> Result<VecDeque<T>, CapacityError> {
    let mut values = VecDeque::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| CapacityError)?;
    check_capacity::<T>(capacity, values.capacity())?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_are_empty_exact_and_overflow_checked() {
        for capacity in [0, 1, 3, 7, 32, 1_000] {
            let values = try_vec::<u64>(capacity).unwrap();
            let queue = try_deque::<u64>(capacity).unwrap();
            assert!(values.is_empty());
            assert!(queue.is_empty());
            assert_eq!(values.capacity(), capacity);
            assert_eq!(queue.capacity(), capacity);
        }
        assert_eq!(try_vec::<u64>(usize::MAX), Err(CapacityError));
        assert_eq!(try_deque::<u64>(usize::MAX), Err(CapacityError));
    }

    #[test]
    fn reported_excess_capacity_is_rejected_without_assuming_allocator_layout() {
        // Inject an actual larger reservation into the same check used before
        // publication; no global allocator replacement or platform assumption.
        let values = try_vec::<u64>(8).unwrap();
        let queue = try_deque::<u64>(8).unwrap();
        assert_eq!(
            check_capacity::<u64>(7, values.capacity()),
            Err(CapacityError)
        );
        assert_eq!(
            check_capacity::<u64>(7, queue.capacity()),
            Err(CapacityError)
        );
        assert_eq!(check_capacity::<u64>(8, 7), Err(CapacityError));
        assert!(try_vec::<()>(3).unwrap().is_empty());
        assert!(try_deque::<()>(3).unwrap().is_empty());
    }
}
