use super::*;
use std::collections::BTreeMap;

fn check(index: &ScopeIndex, expected: &BTreeMap<Scope, usize>) {
    fn visit(index: &ScopeIndex, root: Option<usize>, keys: &mut Vec<(Scope, usize)>) -> u8 {
        let Some(root) = root else { return 0 };
        let node = &index.nodes[root];
        let left = visit(index, node.left, keys);
        keys.push((node.scope, root));
        let right = visit(index, node.right, keys);
        assert!(left.abs_diff(right) <= 1);
        assert_eq!(node.height, left.max(right) + 1);
        node.height
    }
    let mut actual = Vec::new();
    let height = visit(index, index.root, &mut actual);
    assert_eq!(
        actual,
        expected.iter().map(|(&k, &v)| (k, v)).collect::<Vec<_>>()
    );
    for (&key, &token) in expected {
        let (found, visits) = index.lookup(key);
        assert_eq!(found, Some(token));
        assert_eq!(index.get(token), Some(&key));
        assert!(visits <= usize::from(height));
    }
}

#[test]
fn rotations_preserve_stable_tokens_against_an_independent_ordered_map() {
    const COUNT: usize = 4096;
    for order in 0..4 {
        let mut index = ScopeIndex::new(COUNT).unwrap();
        let capacity = index.capacity();
        assert_eq!(capacity, COUNT);
        let mut expected = BTreeMap::new();
        for i in 0..COUNT {
            let key = match order {
                0 => i,
                1 => COUNT - i - 1,
                2 => {
                    if i % 2 == 0 {
                        i / 2
                    } else {
                        COUNT - 1 - i / 2
                    }
                }
                _ => (i * 2791) % COUNT,
            };
            let scope = match key % 2 {
                0 => Scope::Broker(key as i32),
                _ => Scope::Partition {
                    topic_id: (key as u128).to_be_bytes(),
                    partition: (key % 17) as i32,
                },
            };
            assert_eq!(index.find(scope), None);
            let token = index.insert_new(scope);
            assert_eq!(token, i);
            assert!(expected.insert(scope, token).is_none());
            assert_eq!(index.capacity(), capacity);
            if i % 257 == 0 {
                check(&index, &expected);
            }
        }
        check(&index, &expected);
    }
}

#[test]
fn maximum_cardinality_lookup_and_misses_have_logarithmic_visit_bounds() {
    let count = 1 + 2 * usize::from(u16::MAX);
    let mut index = ScopeIndex::new(count).unwrap();
    let capacity = index.capacity();
    assert_eq!(capacity, count);
    assert_eq!(index.insert_new(Scope::Global), 0);
    for key in 0..u16::MAX {
        index.insert_new(Scope::Broker(i32::from(key)));
        index.insert_new(Scope::Partition {
            topic_id: [7; 16],
            partition: i32::from(key),
        });
    }
    let logarithmic_bound = 2 * (usize::BITS - count.leading_zeros()) as usize;
    assert!(usize::from(index.height(index.root)) <= logarithmic_bound);
    for key in (0..u16::MAX).step_by(13) {
        let scope = Scope::Partition {
            topic_id: [7; 16],
            partition: i32::from(key),
        };
        let (token, visits) = index.lookup(scope);
        assert_eq!(token, Some(2 + 2 * usize::from(key)));
        assert!(visits <= logarithmic_bound);
        let (missing, visits) = index.lookup(Scope::Partition {
            topic_id: [8; 16],
            partition: i32::from(key),
        });
        assert!(missing.is_none());
        assert!(visits <= logarithmic_bound);
    }
    assert_eq!(index.nodes.len(), count);
    assert_eq!(index.capacity(), capacity);
    assert!(ScopeIndex::configured_storage_bytes(usize::MAX).is_none());
}
