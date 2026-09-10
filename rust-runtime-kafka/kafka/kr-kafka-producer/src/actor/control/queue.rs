//! Fixed backing for FIFO control groups and duplicate membership. The AVL
//! index reuses retired nodes, so enqueue never allocates or shifts a backlog.
use super::{FailureReason, ProducerIdentity, Result, TopicHandle};
use std::{cmp::Ordering, collections::VecDeque};

#[derive(Clone, Copy, Debug)]
pub(super) enum Queued {
    Metadata(usize),
    Identity(Option<ProducerIdentity>),
}
pub(super) struct Queue {
    groups: VecDeque<Queued>,
    handles: VecDeque<TopicHandle>,
    members: HandleSet,
}
impl Queue {
    pub(super) fn storage_bytes(limit: usize) -> Option<usize> {
        limit
            .checked_mul(size_of::<TopicHandle>() + size_of::<Node>() + size_of::<usize>())?
            .checked_add(3 * size_of::<Queued>())
    }
    pub(super) fn new(limit: usize) -> Result<Self> {
        let mut groups = VecDeque::new();
        let mut handles = VecDeque::new();
        groups
            .try_reserve_exact(3)
            .map_err(|_| FailureReason::ResourceExhausted)?;
        handles
            .try_reserve_exact(limit)
            .map_err(|_| FailureReason::ResourceExhausted)?;
        Ok(Self {
            groups,
            handles,
            members: HandleSet::new(limit)?,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.groups.len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
    pub(super) fn identity(&self) -> Option<Option<ProducerIdentity>> {
        self.groups.iter().find_map(|group| match group {
            Queued::Identity(identity) => Some(*identity),
            Queued::Metadata(_) => None,
        })
    }
    pub(super) fn push_identity(&mut self, identity: Option<ProducerIdentity>) -> Result<()> {
        if self.groups.len() == 3 {
            return Err(FailureReason::ResourceExhausted);
        }
        self.groups.push_back(Queued::Identity(identity));
        Ok(())
    }
    /// One O(log limit) lookup/insertion per supplied handle. The actor submits
    /// singleton engine metadata orders; passive bulk callers pay their input
    /// length. Failure rolls back membership without changing queued FIFO work.
    pub(super) fn push_metadata(&mut self, mut input: Vec<TopicHandle>) -> Result<()> {
        if input.capacity() > self.members.limit {
            return Err(FailureReason::ResourceExhausted);
        }
        let mut unique = 0;
        for read in 0..input.len() {
            let handle = input[read];
            match self.members.insert(handle) {
                Ok(true) => {
                    input[unique] = handle;
                    unique += 1;
                }
                Ok(false) => {}
                Err(error) => {
                    for handle in &input[..unique] {
                        self.members.remove(*handle);
                    }
                    return Err(error);
                }
            }
        }
        if unique == 0 {
            return Ok(());
        }
        if let Some(Queued::Metadata(count)) = self.groups.back_mut() {
            *count += unique;
        } else {
            if self.groups.len() == 3 {
                for handle in &input[..unique] {
                    self.members.remove(*handle);
                }
                return Err(FailureReason::ResourceExhausted);
            }
            self.groups.push_back(Queued::Metadata(unique));
        }
        self.handles.extend(input[..unique].iter().copied());
        Ok(())
    }
    /// Splits a group by changing its count; no remainder is copied or moved.
    pub(super) fn take(&mut self, maximum: usize) -> Option<Queued> {
        match self.groups.front_mut()? {
            Queued::Metadata(count) if *count > maximum => {
                *count -= maximum;
                Some(Queued::Metadata(maximum))
            }
            _ => self.groups.pop_front(),
        }
    }
    pub(super) fn pop_handle(&mut self) -> TopicHandle {
        self.handles
            .pop_front()
            .expect("selected metadata group has handles")
    }
    pub(super) fn complete(&mut self, handles: &[TopicHandle]) {
        for handle in handles {
            self.members.remove(*handle);
        }
    }
    pub(super) fn clear(&mut self) {
        self.groups.clear();
        self.handles.clear();
        self.members.clear();
    }
}

#[derive(Clone, Copy)]
struct Node {
    key: TopicHandle,
    left: Option<usize>,
    right: Option<usize>,
    height: u8,
}
struct HandleSet {
    nodes: Vec<Node>,
    free: Vec<usize>,
    root: Option<usize>,
    limit: usize,
}
impl HandleSet {
    fn new(limit: usize) -> Result<Self> {
        let mut nodes = Vec::new();
        let mut free = Vec::new();
        nodes
            .try_reserve_exact(limit)
            .map_err(|_| FailureReason::ResourceExhausted)?;
        free.try_reserve_exact(limit)
            .map_err(|_| FailureReason::ResourceExhausted)?;
        Ok(Self {
            nodes,
            free,
            root: None,
            limit,
        })
    }
    fn contains(&self, key: TopicHandle) -> bool {
        let mut current = self.root;
        while let Some(index) = current {
            let node = self.nodes[index];
            current = match key.cmp(&node.key) {
                Ordering::Less => node.left,
                Ordering::Greater => node.right,
                Ordering::Equal => return true,
            };
        }
        false
    }
    fn insert(&mut self, key: TopicHandle) -> Result<bool> {
        if self.contains(key) {
            return Ok(false);
        }
        let node = Node {
            key,
            left: None,
            right: None,
            height: 1,
        };
        let inserted = if let Some(index) = self.free.pop() {
            self.nodes[index] = node;
            index
        } else {
            if self.nodes.len() == self.limit {
                return Err(FailureReason::ResourceExhausted);
            }
            let index = self.nodes.len();
            self.nodes.push(node);
            index
        };
        self.root = Some(self.insert_at(self.root, inserted));
        Ok(true)
    }
    fn insert_at(&mut self, root: Option<usize>, inserted: usize) -> usize {
        let Some(root) = root else { return inserted };
        if self.nodes[inserted].key < self.nodes[root].key {
            self.nodes[root].left = Some(self.insert_at(self.nodes[root].left, inserted));
        } else {
            self.nodes[root].right = Some(self.insert_at(self.nodes[root].right, inserted));
        }
        self.balance(root)
    }
    fn remove(&mut self, key: TopicHandle) {
        self.root = self.remove_at(self.root, key);
    }
    fn remove_at(&mut self, root: Option<usize>, key: TopicHandle) -> Option<usize> {
        let root = root?;
        match key.cmp(&self.nodes[root].key) {
            Ordering::Less => self.nodes[root].left = self.remove_at(self.nodes[root].left, key),
            Ordering::Greater => {
                self.nodes[root].right = self.remove_at(self.nodes[root].right, key)
            }
            Ordering::Equal => {
                if self.nodes[root].left.is_none() || self.nodes[root].right.is_none() {
                    self.free.push(root);
                    return self.nodes[root].left.or(self.nodes[root].right);
                }
                let mut successor = self.nodes[root].right.unwrap();
                while let Some(left) = self.nodes[successor].left {
                    successor = left;
                }
                let key = self.nodes[successor].key;
                self.nodes[root].key = key;
                self.nodes[root].right = self.remove_at(self.nodes[root].right, key);
            }
        }
        Some(self.balance(root))
    }
    fn height(&self, index: Option<usize>) -> u8 {
        index.map_or(0, |i| self.nodes[i].height)
    }
    fn update(&mut self, root: usize) {
        self.nodes[root].height = self
            .height(self.nodes[root].left)
            .max(self.height(self.nodes[root].right))
            + 1;
    }
    fn rotate_left(&mut self, root: usize) -> usize {
        let next = self.nodes[root].right.unwrap();
        self.nodes[root].right = self.nodes[next].left;
        self.nodes[next].left = Some(root);
        self.update(root);
        self.update(next);
        next
    }
    fn rotate_right(&mut self, root: usize) -> usize {
        let next = self.nodes[root].left.unwrap();
        self.nodes[root].left = self.nodes[next].right;
        self.nodes[next].right = Some(root);
        self.update(root);
        self.update(next);
        next
    }
    fn balance(&mut self, root: usize) -> usize {
        self.update(root);
        let left = self.height(self.nodes[root].left);
        let right = self.height(self.nodes[root].right);
        if left > right + 1 {
            let child = self.nodes[root].left.unwrap();
            if self.height(self.nodes[child].right) > self.height(self.nodes[child].left) {
                self.nodes[root].left = Some(self.rotate_left(child));
            }
            return self.rotate_right(root);
        }
        if right > left + 1 {
            let child = self.nodes[root].right.unwrap();
            if self.height(self.nodes[child].left) > self.height(self.nodes[child].right) {
                self.nodes[root].right = Some(self.rotate_right(child));
            }
            return self.rotate_left(root);
        }
        root
    }
    fn clear(&mut self) {
        self.root = None;
        self.nodes.clear();
        self.free.clear();
    }
}

#[cfg(test)]
mod tests;
