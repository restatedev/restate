// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hash::BuildHasherDefault;
use std::iter::FusedIterator;

use ahash::AHasher;
use itertools::Itertools;
use rand::prelude::*;

use crate::PlainNodeId;

// Why? Over 50% faster in iteration than HashSet and ~40% faster than default RandomState for
// contains() and set intersection operations. Additionally, it's 300% faster when created from
// iterators than HashSet with default RandomState.
type IndexSet<T> = indexmap::IndexSet<T, BuildHasherDefault<AHasher>>;

/// A type that represents a unique set of nodes. NodeSet maintains the order of the nodes in the
/// set and provides efficient set operations. Note that the order across serialization and
/// deserialization is only guaranteed if the underlying format maintains the order as well.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Default,
    Eq,
    PartialEq,
    derive_more::Index,
    derive_more::IntoIterator,
    derive_more::From,
)]
pub struct NodeSet(IndexSet<PlainNodeId>);

impl NodeSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_single(node: impl Into<PlainNodeId>) -> Self {
        let mut set = IndexSet::with_capacity_and_hasher(1, BuildHasherDefault::default());
        set.insert(node.into());
        Self(set)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(IndexSet::with_capacity_and_hasher(
            capacity,
            BuildHasherDefault::default(),
        ))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return true if the node is in the nodeset
    pub fn contains(&self, node: impl Into<PlainNodeId>) -> bool {
        self.0.contains(&node.into())
    }

    /// Returns true if any node of the input iterator exist in the nodeset
    pub fn contains_any(&self, mut nodes: impl Iterator<Item = PlainNodeId>) -> bool {
        nodes.any(|node| self.0.contains(&node))
    }

    /// Returns true if all nodes of the input iterator are in the nodeset
    pub fn contains_all(&self, mut nodes: impl Iterator<Item = PlainNodeId>) -> bool {
        nodes.all(|node| self.0.contains(&node))
    }

    /// Returns true if this node didn't already exist in the nodeset
    pub fn insert(&mut self, node: impl Into<PlainNodeId>) -> bool {
        self.0.insert(node.into())
    }
    /// Adds a value to the set, replacing the existing value, if any, that is equal
    /// to the given one, without altering its insertion order. Returns the replaced node.
    ///
    /// Computes in O(1) time (average).
    pub fn replace(&mut self, node: impl Into<PlainNodeId>) -> Option<PlainNodeId> {
        self.0.replace(node.into())
    }

    /// Clears the nodeset, removes all nodes.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Remove the last node from the set and return it.
    pub fn pop(&mut self) -> Option<PlainNodeId> {
        self.0.pop()
    }

    pub fn remove(&mut self, node: impl Into<PlainNodeId>) {
        let node = node.into();
        self.0.retain(|x| x != &node);
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.0.iter()
    }

    /// Creates a new nodeset that excludes the nodes in the provided set
    /// Iterator over values that are in self but not in `other`. Values produced in the same order
    /// as they appear in `self`.
    pub fn difference<'a>(
        &'a self,
        other: &'a NodeSet,
    ) -> impl DoubleEndedIterator<Item = PlainNodeId> + FusedIterator + Clone + 'a {
        self.0.difference(&other.0).copied()
    }

    /// Return an iterator over the values that are in both self and `other`.
    /// Values are produced in the same order that they appear in self.
    pub fn intersect<'a>(
        &'a self,
        other: &'a NodeSet,
    ) -> impl DoubleEndedIterator<Item = PlainNodeId> + FusedIterator + Clone + 'a {
        self.0.intersection(&other.0).copied()
    }

    /// Return an iterator over all values that are in self or `other`.
    /// Values from self are produced in their original order, followed by values that are unique to other in their original order.
    pub fn union<'a, I>(
        &'a self,
        other: &'a NodeSet,
    ) -> impl DoubleEndedIterator<Item = PlainNodeId> + FusedIterator + Clone + 'a {
        self.0.union(&other.0).copied()
    }

    /// Returns true if all nodes of self are contained in other.
    pub fn is_subset(&self, other: &NodeSet) -> bool {
        self.0.is_subset(&other.0)
    }

    /// Returns true if all elements of other are contained in self.
    pub fn is_superset(&self, other: &NodeSet) -> bool {
        self.0.is_superset(&other.0)
    }

    pub fn as_slice(&self) -> &indexmap::set::Slice<PlainNodeId> {
        self.0.as_slice()
    }

    pub fn into_boxed_slice(self) -> Box<indexmap::set::Slice<PlainNodeId>> {
        self.0.into_boxed_slice()
    }

    /// Sort based on node ids
    pub fn sort(&mut self) {
        self.0.sort()
    }

    /// Shuffles the node set in place.
    pub fn shuffle<R: Rng + ?Sized>(&mut self, rng: &mut R) {
        use rand::distributions::Standard;
        self.0
            .sort_by_cached_key(|_| rng.sample::<usize, Standard>(Standard));
    }
}

impl<I> PartialEq<I> for NodeSet
where
    I: std::ops::Deref<Target = [PlainNodeId]>,
{
    fn eq(&self, other: &I) -> bool {
        Iterator::eq(self.iter(), other.iter())
    }
}

impl std::iter::Extend<PlainNodeId> for NodeSet {
    fn extend<T: IntoIterator<Item = PlainNodeId>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}

impl<const N: usize> From<[PlainNodeId; N]> for NodeSet {
    fn from(value: [PlainNodeId; N]) -> Self {
        Self(IndexSet::from_iter(value))
    }
}

impl<const N: usize> From<[u32; N]> for NodeSet {
    fn from(value: [u32; N]) -> Self {
        Self(value.into_iter().map(PlainNodeId::from).collect())
    }
}

impl From<NodeSet> for Vec<PlainNodeId> {
    fn from(value: NodeSet) -> Self {
        value.0.into_iter().collect()
    }
}

impl From<NodeSet> for Vec<u32> {
    fn from(value: NodeSet) -> Self {
        value.0.into_iter().map(Into::into).collect()
    }
}

impl From<Vec<u32>> for NodeSet {
    fn from(value: Vec<u32>) -> Self {
        Self(value.into_iter().map(PlainNodeId::from).collect())
    }
}

impl From<NodeSet> for Box<[PlainNodeId]> {
    fn from(value: NodeSet) -> Self {
        value.0.into_iter().collect()
    }
}

impl<A: Into<PlainNodeId>> FromIterator<A> for NodeSet {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        Self(IndexSet::from_iter(iter.into_iter().map(Into::into)))
    }
}

impl std::fmt::Debug for NodeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.iter()).finish()
    }
}

impl std::fmt::Display for NodeSet {
    /// The alternate format displays a *sorted* list of short-form plain node ids, suitable for human-friendly output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match f.alternate() {
            false => write_nodes(self, f),
            true => write_nodes_sorted(self, f),
        }
    }
}

fn write_nodes(node_set: &NodeSet, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "[")?;
    let mut nodes = node_set.0.iter();
    if let Some(node) = nodes.next() {
        write!(f, "{node}")?;
        for node in nodes {
            write!(f, ", {node}")?;
        }
    }
    write!(f, "]")
}

fn write_nodes_sorted(node_set: &NodeSet, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "[")?;
    let mut nodes = node_set.0.iter().sorted();
    if let Some(node) = nodes.next() {
        write!(f, "{node}")?;
        for node in nodes {
            write!(f, ", {node}")?;
        }
    }
    write!(f, "]")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn nodeset_order_from_iterator() {
        // nodeset created from a vec should maintain the original order
        let nodes = NodeSet::from_iter([2, 4, 3, 1, 5]);
        assert_eq!(
            nodes.iter().map(|n| u32::from(*n)).collect::<Vec<_>>(),
            vec![2, 4, 3, 1, 5]
        );

        // indexing
        assert_eq!(nodes[0], PlainNodeId::from(2));
        assert_eq!(nodes[4], PlainNodeId::from(5));
    }

    #[test]
    fn nodeset_insertion_and_sorting() {
        let mut nodes = NodeSet::default();
        assert_eq!(nodes.len(), 0);
        nodes.insert(18);
        nodes.insert(2);
        assert_eq!(nodes.len(), 2);
        assert!(!nodes.insert(2));
        assert_eq!(nodes.len(), 2);
        nodes.insert(1);
        assert_eq!(nodes.len(), 3);

        assert_eq!(
            nodes.iter().map(|n| u32::from(*n)).collect::<Vec<_>>(),
            vec![18, 2, 1]
        );
        // sorting should change the order
        nodes.sort();

        assert_eq!(
            nodes.iter().map(|n| u32::from(*n)).collect::<Vec<_>>(),
            vec![1, 2, 18]
        );

        assert_eq!(nodes.pop(), Some(PlainNodeId::from(18)));
        assert_eq!(nodes.pop(), Some(PlainNodeId::from(2)));
        assert_eq!(nodes.pop(), Some(PlainNodeId::from(1)));
        assert_eq!(nodes.pop(), None);
        nodes.extend(NodeSet::from([1, 2, 3]));
        assert_eq!(
            nodes,
            vec![
                PlainNodeId::from(1),
                PlainNodeId::from(2),
                PlainNodeId::from(3)
            ]
        );
    }

    #[test]
    fn nodeset_intersection() {
        let nodes1 = NodeSet::from_iter([1, 2, 3, 4, 5]);
        let nodes2 = NodeSet::from_iter([3, 4, 5, 6, 7]);
        let nodes3 = NodeSet::from_iter([1, 2, 3, 4, 5]);
        let intersection = nodes1.intersect(&nodes2).collect::<NodeSet>();
        assert_eq!(intersection.len(), 3);
        assert_eq!(intersection, NodeSet::from_iter([3, 4, 5]));
        let intersection = nodes1.intersect(&nodes3).collect::<NodeSet>();
        assert_eq!(intersection.len(), 5);
        assert_eq!(intersection, NodeSet::from(vec![1, 2, 3, 4, 5]));
    }
}
