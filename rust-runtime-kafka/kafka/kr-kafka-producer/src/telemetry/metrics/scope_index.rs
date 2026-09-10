//! Append-only AVL nodes in one preallocated allocation. Rotations change links,
//! never node positions, so a node index is a permanent histogram scope token.
use super::{MetricsError, Scope};

struct Node {
    scope: Scope,
    left: Option<usize>,
    right: Option<usize>,
    height: u8,
}
#[derive(Default)]
pub(super) struct ScopeIndex {
    nodes: Vec<Node>,
    root: Option<usize>,
    limit: usize,
}
impl std::fmt::Debug for ScopeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeIndex")
            .field("nodes", &self.nodes.len())
            .field("limit", &self.limit)
            .finish()
    }
}
impl ScopeIndex {
    pub(super) fn configured_storage_bytes(slots: usize) -> Option<usize> {
        slots.checked_mul(size_of::<Node>())
    }
    pub(super) fn new(limit: usize) -> Result<Self, MetricsError> {
        let nodes = crate::fixed::try_vec(limit).map_err(|_| MetricsError::AllocationFailed)?;
        Ok(Self {
            nodes,
            root: None,
            limit,
        })
    }
    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.nodes.capacity()
    }
    pub(super) fn get(&self, index: usize) -> Option<&Scope> {
        self.nodes.get(index).map(|node| &node.scope)
    }
    pub(super) fn find(&self, scope: Scope) -> Option<usize> {
        self.lookup(scope).0
    }
    // Returning visits gives deterministic work evidence in tests without a
    // clock, allocation counter, tracing event or mutable recording state.
    fn lookup(&self, scope: Scope) -> (Option<usize>, usize) {
        let mut current = self.root;
        let mut visits = 0;
        while let Some(index) = current {
            visits += 1;
            let node = &self.nodes[index];
            current = match scope.cmp(&node.scope) {
                std::cmp::Ordering::Less => node.left,
                std::cmp::Ordering::Greater => node.right,
                std::cmp::Ordering::Equal => return (Some(index), visits),
            };
        }
        (None, visits)
    }
    /// Caller already proved absence and admitted this scope under its separate
    /// broker/partition quota. No reservation or allocation occurs here.
    pub(super) fn insert_new(&mut self, scope: Scope) -> usize {
        assert!(
            self.nodes.len() < self.limit,
            "scope quota precedes insertion"
        );
        let index = self.nodes.len();
        self.nodes.push(Node {
            scope,
            left: None,
            right: None,
            height: 1,
        });
        self.root = Some(self.insert_at(self.root, index));
        index
    }
    fn height(&self, index: Option<usize>) -> u8 {
        index.map_or(0, |index| self.nodes[index].height)
    }
    fn update_height(&mut self, index: usize) {
        self.nodes[index].height = self
            .height(self.nodes[index].left)
            .max(self.height(self.nodes[index].right))
            + 1;
    }
    fn rotate_left(&mut self, root: usize) -> usize {
        let next = self.nodes[root].right.expect("right-heavy subtree");
        self.nodes[root].right = self.nodes[next].left;
        self.nodes[next].left = Some(root);
        self.update_height(root);
        self.update_height(next);
        next
    }
    fn rotate_right(&mut self, root: usize) -> usize {
        let next = self.nodes[root].left.expect("left-heavy subtree");
        self.nodes[root].left = self.nodes[next].right;
        self.nodes[next].right = Some(root);
        self.update_height(root);
        self.update_height(next);
        next
    }
    fn insert_at(&mut self, root: Option<usize>, inserted: usize) -> usize {
        let Some(root) = root else { return inserted };
        match self.nodes[inserted].scope.cmp(&self.nodes[root].scope) {
            std::cmp::Ordering::Less => {
                self.nodes[root].left = Some(self.insert_at(self.nodes[root].left, inserted));
            }
            std::cmp::Ordering::Greater => {
                self.nodes[root].right = Some(self.insert_at(self.nodes[root].right, inserted));
            }
            std::cmp::Ordering::Equal => panic!("scope lookup precedes unique insertion"),
        }
        self.update_height(root);
        let left_height = self.height(self.nodes[root].left);
        let right_height = self.height(self.nodes[root].right);
        if left_height > right_height + 1 {
            let left = self.nodes[root].left.expect("left-heavy subtree");
            if self.height(self.nodes[left].right) > self.height(self.nodes[left].left) {
                self.nodes[root].left = Some(self.rotate_left(left));
            }
            return self.rotate_right(root);
        }
        if right_height > left_height + 1 {
            let right = self.nodes[root].right.expect("right-heavy subtree");
            if self.height(self.nodes[right].left) > self.height(self.nodes[right].right) {
                self.nodes[root].right = Some(self.rotate_right(right));
            }
            return self.rotate_left(root);
        }
        root
    }
}

#[cfg(test)]
mod tests;
