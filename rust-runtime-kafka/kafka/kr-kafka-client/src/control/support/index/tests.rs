use super::*;
use crate::control::ControlLimits;
use std::{cell::Cell, cmp::Ordering};

#[test]
fn permutation_cycles_restore_independent_expected_order_and_mark_exactly_once() {
    for seed in 1u64..=128 {
        let mut keys: Vec<u32> = (0..257).collect();
        let mut draw = seed;
        for i in (1..keys.len()).rev() {
            draw ^= draw << 13;
            draw ^= draw >> 7;
            draw ^= draw << 17;
            keys.swap(i, draw as usize % (i + 1));
        }
        let mut budget = OwnedBudget::new(ControlLimits::default());
        let mut index = Index::new(
            keys.iter().copied(),
            &mut budget,
            257,
            "keys",
            ControlError::UnexpectedTopic,
        )
        .unwrap();
        for (original, key) in keys.iter().enumerate() {
            let (slot, found) = index.get(key).unwrap();
            assert_eq!(found, original);
            assert!(index.mark(slot));
            assert!(!index.mark(slot));
        }
        let mut sorted: Vec<u32> = (0..257).collect();
        index.restore_order(&mut sorted);
        assert_eq!(sorted, keys, "seed={seed}");
    }
}

#[derive(Clone, Copy)]
struct Counted<'a>(u32, &'a Cell<usize>);
impl PartialEq for Counted<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Counted<'_> {}
impl PartialOrd for Counted<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Counted<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.set(self.1.get() + 1);
        self.0.cmp(&other.0)
    }
}
#[test]
fn large_permuted_index_has_logarithmic_lookups_and_checked_scratch_bound() {
    let comparisons = Cell::new(0);
    let mut budget = OwnedBudget::new(ControlLimits::default());
    let index = Index::new(
        (0..4096).map(|i| Counted((i * 719) % 4096, &comparisons)),
        &mut budget,
        4096,
        "keys",
        ControlError::UnexpectedTopic,
    )
    .unwrap();
    assert!(comparisons.get() < 4096 * 32);
    comparisons.set(0);
    for key in 0..4096 {
        assert!(index.get(&Counted(key, &comparisons)).is_some());
    }
    assert!(comparisons.get() <= 4096 * 14);
    let mut tiny = OwnedBudget::new(ControlLimits {
        owned_bytes: 1,
        ..ControlLimits::default()
    });
    let allocations = allocation_counter::measure(|| {
        assert!(matches!(
            Index::new(0..2, &mut tiny, 2, "keys", ControlError::UnexpectedTopic),
            Err(ControlError::Limit("owned bytes"))
        ));
    });
    assert_eq!(
        allocations.count_total, 0,
        "preflight must precede allocation"
    );
}
