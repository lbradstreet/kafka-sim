use super::*;
use std::collections::BTreeSet;

fn check(
    set: &HandleSet,
    index: Option<usize>,
    low: Option<TopicHandle>,
    high: Option<TopicHandle>,
) -> (u8, usize) {
    let Some(index) = index else { return (0, 0) };
    let node = set.nodes[index];
    assert!(low.is_none_or(|low| low < node.key));
    assert!(high.is_none_or(|high| high > node.key));
    let (left, left_count) = check(set, node.left, low, Some(node.key));
    let (right, right_count) = check(set, node.right, Some(node.key), high);
    assert!(left.abs_diff(right) <= 1);
    assert_eq!(node.height, left.max(right) + 1);
    (node.height, left_count + right_count + 1)
}

#[test]
fn ordered_index_churn_matches_set_and_preserves_height_and_preallocation() {
    for seed in 1u64..=64 {
        let mut draw = seed;
        let mut set = HandleSet::new(64).unwrap();
        let mut model = BTreeSet::new();
        let capacities = (set.nodes.capacity(), set.free.capacity());
        for step in 0..2048 {
            draw ^= draw << 13;
            draw ^= draw >> 7;
            draw ^= draw << 17;
            let key = TopicHandle((draw % 97) as u32);
            if draw & 256 == 0 {
                let expected = if model.contains(&key) {
                    Ok(false)
                } else if model.len() == 64 {
                    Err(FailureReason::ResourceExhausted)
                } else {
                    model.insert(key);
                    Ok(true)
                };
                assert_eq!(set.insert(key), expected, "seed={seed} step={step}");
            } else {
                set.remove(key);
                model.remove(&key);
            }
            assert_eq!(check(&set, set.root, None, None).1, model.len());
            assert_eq!(set.nodes.len() - set.free.len(), model.len());
            assert_eq!((set.nodes.capacity(), set.free.capacity()), capacities);
            for key in 0..97 {
                assert_eq!(
                    set.contains(TopicHandle(key)),
                    model.contains(&TopicHandle(key))
                );
            }
        }
    }
}

#[test]
fn fifo_groups_split_without_remainder_copy_and_members_survive_current_work() {
    let mut queue = Queue::new(8).unwrap();
    queue
        .push_metadata(vec![TopicHandle(3), TopicHandle(1), TopicHandle(3)])
        .unwrap();
    queue.push_identity(None).unwrap();
    queue
        .push_metadata(vec![TopicHandle(1), TopicHandle(2)])
        .unwrap();
    assert!(matches!(queue.take(1), Some(Queued::Metadata(1))));
    assert_eq!(queue.pop_handle(), TopicHandle(3));
    queue
        .push_metadata(vec![TopicHandle(3), TopicHandle(4)])
        .unwrap();
    assert!(matches!(queue.take(1), Some(Queued::Metadata(1))));
    assert_eq!(queue.pop_handle(), TopicHandle(1));
    assert!(matches!(queue.take(1), Some(Queued::Identity(None))));
    assert!(matches!(queue.take(8), Some(Queued::Metadata(2))));
    assert_eq!(queue.pop_handle(), TopicHandle(2));
    assert_eq!(queue.pop_handle(), TopicHandle(4));
    assert!(queue.take(8).is_none());
    queue.complete(&[
        TopicHandle(3),
        TopicHandle(1),
        TopicHandle(2),
        TopicHandle(4),
    ]);
    queue.push_metadata(vec![TopicHandle(3)]).unwrap();
    assert!(matches!(queue.take(8), Some(Queued::Metadata(1))));
}

#[test]
fn failed_bulk_admission_rolls_back_membership_and_preserves_pending_fifo() {
    let mut queue = Queue::new(3).unwrap();
    queue
        .push_metadata(vec![TopicHandle(1), TopicHandle(2)])
        .unwrap();
    assert_eq!(
        queue.push_metadata(vec![TopicHandle(2), TopicHandle(3), TopicHandle(4)]),
        Err(FailureReason::ResourceExhausted)
    );
    assert_eq!(queue.members.nodes.len() - queue.members.free.len(), 2);
    assert!(!queue.members.contains(TopicHandle(3)));
    queue.push_metadata(vec![TopicHandle(3)]).unwrap();
    assert!(matches!(queue.take(3), Some(Queued::Metadata(3))));
    for key in 1..=3 {
        assert_eq!(queue.pop_handle(), TopicHandle(key));
    }
    queue.clear();
    assert!(queue.is_empty());
    assert!(queue.members.root.is_none());
}

#[test]
fn queue_admission_rotation_and_release_reuse_the_preallocated_backing() {
    let mut queue = Queue::new(1024).unwrap();
    let batches: Vec<Vec<TopicHandle>> = (0..32)
        .map(|round| {
            (0..1024)
                .map(|key| TopicHandle(round * 1024 + key))
                .collect()
        })
        .collect();
    let mut active = Vec::with_capacity(1024);
    let allocations = allocation_counter::measure(|| {
        for batch in batches {
            queue.push_metadata(batch).unwrap();
            assert!(matches!(queue.take(1024), Some(Queued::Metadata(1024))));
            for _ in 0..1024 {
                active.push(queue.pop_handle());
            }
            queue.complete(&active);
            active.clear();
        }
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(allocations.bytes_total, 0);
    assert!(queue.is_empty());
    assert_eq!(check(&queue.members, queue.members.root, None, None).1, 0);
}
