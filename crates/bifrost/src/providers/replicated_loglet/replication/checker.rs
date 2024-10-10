// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{hash_map, HashMap};

use restate_types::nodes_config::{NodesConfiguration, StorageState};
use restate_types::replicated_loglet::{NodeSet, ReplicationProperty};
use restate_types::PlainNodeId;

/// Trait for merging two attributes
pub trait Merge {
    fn merge(&mut self, other: Self);
}

impl Merge for bool {
    fn merge(&mut self, other: Self) {
        *self |= other;
    }
}

/// NodeSetChecker maintains a set of nodes that can be tagged with
/// an attribute, and provides an API for querying the replication properties of
/// the subset of nodes with a certain values for this attribute, given a
/// replication requirement across several failure domains.
///
/// The checker is created with default value of Attribute set on all nodes.
///
/// **NOTE:** Currently, this will not perform any failure-domain-aware quorum
/// checks, this will be implemented in the near future.
///
/// The utility provides two methods:
/// - `check_write_quorum()`: Can be used to check if it'd be possible to replicate a record on the
///                           subset of nodes that have a certain value for the attribute
/// - `check_fmajority()`: Used to check if enough nodes have certain values for the
///                        attribute so that that set of nodes is an f-majority for at
///                        least one of the scope for which there is a replication
///                        requirement.
///                        An example usage of this method is during loglet seal which
///                        each node gets tagged with "SEALED" if that node has been sealed.
///                        The seal operation is able to know if it can consider the seal
///                        to be completed or not.
///
/// Note that this doesn't track changes that happen to the storage-states after instantiation.
/// For a fresh view, rebuild this with a new nodes configuration.
pub struct NodeSetChecker<'a, Attribute> {
    node_attribute: HashMap<PlainNodeId, Attribute>,
    /// Mapping between node-id and its log-server storage state
    storage_states: HashMap<PlainNodeId, StorageState>,
    replication_property: &'a ReplicationProperty,
}

impl<'a, Attribute> NodeSetChecker<'a, Attribute> {
    // Note that this doesn't track changes that happen to the storage-states after instantiation.
    // For a fresh view, rebuild this with a new nodes configuration.
    pub fn new(
        nodeset: &NodeSet,
        nodes_config: &NodesConfiguration,
        replication_property: &'a ReplicationProperty,
    ) -> Self
    where
        Attribute: Default,
    {
        Self::with_factory(nodeset, nodes_config, replication_property, |_| {
            Default::default()
        })
    }

    pub fn with_factory(
        nodeset: &NodeSet,
        nodes_config: &NodesConfiguration,
        replication_property: &'a ReplicationProperty,
        attribute_factory: impl Fn(PlainNodeId) -> Attribute,
    ) -> Self {
        let storage_states: HashMap<_, _> = nodeset
            .iter()
            .filter_map(|n| {
                match nodes_config.get_log_server_storage_state(n) {
                    // storage states. Only include nodes that enable reads or above.
                    storage_state if !storage_state.empty() => Some((*n, storage_state)),
                    // node is not readable or doesn't exist. Treat as DISABLED
                    _ => None,
                }
            })
            .collect();

        let node_attribute: HashMap<_, _> = storage_states
            .keys()
            .map(|node_id| (*node_id, attribute_factory(*node_id)))
            .collect();

        Self {
            node_attribute,
            storage_states,
            replication_property,
        }
    }

    pub fn len(&self) -> usize {
        self.node_attribute.len()
    }

    pub fn is_empty(&self) -> bool {
        self.node_attribute.is_empty()
    }

    /// resets all attributes for all nodes to this value
    pub fn reset_with(&mut self, attribute: Attribute)
    where
        Attribute: Clone,
    {
        for (_, v) in self.node_attribute.iter_mut() {
            *v = attribute.clone();
        }
    }

    /// resets all attributes for all nodes with the default value of Attribute
    pub fn reset_with_default(&mut self)
    where
        Attribute: Default,
    {
        for (_, v) in self.node_attribute.iter_mut() {
            *v = Default::default();
        }
    }

    /// Set the attribute value of a node. Note that a node can only be
    /// associated with one attribute value at a time, so if the node has an
    /// existing attribute value, the value will be cleared.
    ///
    /// Returns the old attribute if it was set
    pub fn set_attribute(
        &mut self,
        node_id: PlainNodeId,
        attribute: Attribute,
    ) -> Option<Attribute> {
        // ignore if the node is not in the original nodeset
        if self.storage_states.contains_key(&node_id) {
            self.node_attribute.insert(node_id, attribute)
        } else {
            None
        }
    }

    pub fn set_attribute_on_each<'b>(
        &mut self,
        nodes: impl IntoIterator<Item = &'b PlainNodeId>,
        f: impl Fn() -> Attribute,
    ) {
        for node in nodes.into_iter() {
            // ignore if the node is not in the original nodeset
            if self.storage_states.contains_key(node) {
                self.node_attribute.insert(*node, f());
            }
        }
    }

    pub fn remove_attribute(&mut self, node_id: &PlainNodeId) -> Option<Attribute> {
        self.node_attribute.remove(node_id)
    }

    pub fn merge_attribute(&mut self, node_id: PlainNodeId, attribute: Attribute)
    where
        Attribute: Merge,
    {
        match self.node_attribute.entry(node_id) {
            hash_map::Entry::Occupied(mut existing) => {
                existing.get_mut().merge(attribute);
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert(attribute);
            }
        }
    }

    pub fn get_attribute(&mut self, node_id: &PlainNodeId) -> Option<&Attribute> {
        self.node_attribute.get(node_id)
    }

    /// Check if nodes that match the predicate meet the write-quorum rules according to the
    /// replication property. For instance, if replication property is set to {node: 3, zone: 2}
    /// then this function will return `True` if nodes that match the predicate are spread across 2
    /// zones.
    pub fn check_write_quorum<Predicate>(&self, predicate: Predicate) -> bool
    where
        Predicate: Fn(&Attribute) -> bool,
    {
        let filtered = self.node_attribute.iter().filter(|(node_id, v)| {
            predicate(v)
                && self
                    .storage_states
                    .get(node_id)
                    .expect("node must be in node-set")
                    // only consider nodes that are writeable.
                    .can_write_to()
        });
        // todo(asoli): Location-aware quorum check
        filtered.count() >= self.replication_property.num_copies().into()
    }

    /// Does any node matches the predicate?
    pub fn any(&self, predicate: impl Fn(&Attribute) -> bool) -> bool {
        self.node_attribute.values().any(predicate)
    }

    /// Do all nodes match the predicate?
    pub fn all(&self, predicate: impl Fn(&Attribute) -> bool) -> bool {
        self.node_attribute.values().all(predicate)
    }

    // Does any node matches the predicate?
    pub fn filter(
        &self,
        predicate: impl Fn(&Attribute) -> bool,
    ) -> impl Iterator<Item = (&PlainNodeId, &Attribute)> {
        self.node_attribute
            .iter()
            .filter(move |(_, attribute)| predicate(attribute))
    }

    /// Check if enough nodes have certain values for the attribute so that that
    /// set of nodes is an f-majority for at least one of the scope for which
    /// there is a replication requirement.
    ///
    /// Two ways to form a mental model about this:
    /// 1) Nodes that match the predicate (storage-state considered) will form an f-majority.
    /// 2) Do we lose quorum-read availability if we lost all nodes that match the predicate?
    pub fn check_fmajority<Predicate>(&self, predicate: Predicate) -> FMajorityResult
    where
        Predicate: Fn(&Attribute) -> bool,
    {
        let filtered = self
            .node_attribute
            .iter()
            .filter(|(_, v)| predicate(v))
            // `node_attribute` nodes must be in storage_states
            .map(|(node_id, _)| self.storage_states.get(node_id).unwrap());

        let mut authoritative = 0;
        let mut non_authoritative = 0;
        for state in filtered {
            // at the moment, data-loss is the only non-authoritative state
            if state.is_data_loss() {
                non_authoritative += 1;
            } else {
                authoritative += 1;
            }
        }

        if self.storage_states.len() < usize::from(self.replication_property.num_copies()) {
            // short-circuit to avoid overflow on subtraction
            return FMajorityResult::None;
        }

        // todo(asoli): Location-aware quorum check
        let fmajority_requires: usize =
            self.storage_states.len() - usize::from(self.replication_property.num_copies()) + 1;

        if non_authoritative + authoritative < fmajority_requires {
            // not enough nodes to form an f-majority
            return FMajorityResult::None;
        }

        if non_authoritative > 0 {
            // either BestEffort or SuccessWithRisk depends on how many authoritative nodes
            if authoritative >= fmajority_requires {
                return FMajorityResult::SuccessWithRisk;
            }
            return FMajorityResult::BestEffort;
        }
        FMajorityResult::Success
    }
}

/// Possible results of f-majority checks for a subset of the NodeSet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FMajorityResult {
    /// The subset of nodes neither satisfies the authoritative f-majority
    /// property, nor does it contain all authoritative nodes.
    ///
    /// * Bad. No f-majority is possible.
    None,
    /// there are enough node with `DataLoss` that prevent us from
    /// authoritatively deciding the replication state of records. Because of this,
    /// the subset of nodes does not satisfy the authoritative f-majority property
    /// However, the subset of nodes already contain all authoritative
    /// nodes in the NodeSet. As a best effort, the subset of nodes is
    /// considered to be non-authoritative f-majority.
    /// * Bad but with chance of success
    BestEffort,
    /// the subset of nodes satisfy the authoritative f-majority property, and it
    /// has _all_ authoritative nodes in the NodeSet.
    ///
    /// * Good
    Success,
    /// the subset of nodes satisfies the authoritative f-majority property, and
    /// it has suffcient but _not_ all authoritative nodes in the
    /// NodeSet.
    ///
    /// * Good
    SuccessWithRisk,
}

impl FMajorityResult {
    pub fn passed(&self) -> bool {
        matches!(
            self,
            FMajorityResult::Success | FMajorityResult::SuccessWithRisk
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;

    use restate_types::Version;

    use crate::providers::replicated_loglet::test_util::{
        generate_logserver_node, generate_logserver_nodes_config,
    };

    #[test]
    fn test_replication_checker_basics() -> Result<()> {
        // all_authoritative
        let nodes_config = generate_logserver_nodes_config(10, StorageState::ReadWrite);

        let nodeset: NodeSet = (1..=5).collect();
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // all nodes in the nodeset are authoritative
        assert_that!(checker.len(), eq(5));
        // all nodes are false by default. Can't establish write quorum.
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));

        checker.set_attribute_on_each(
            &[
                PlainNodeId::new(1),
                PlainNodeId::new(2),
                PlainNodeId::new(4),
            ],
            || true,
        );
        // all nodes are false by default. Can't establish write-quorum.
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));

        // 2 nodes are false in this node-set, not enough for write-quorum
        assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(false));

        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // we only have 2 nodes with false, impossible to achieve fmajority.
        assert_that!(
            checker.check_fmajority(|attr| !(*attr)),
            eq(FMajorityResult::None)
        );
        Ok(())
    }

    #[test]
    fn test_replication_checker_mixed() -> Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        nodes_config.upsert_node(generate_logserver_node(1, StorageState::Disabled));
        nodes_config.upsert_node(generate_logserver_node(2, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_logserver_node(3, StorageState::ReadOnly));
        nodes_config.upsert_node(generate_logserver_node(4, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_logserver_node(5, StorageState::DataLoss));
        nodes_config.upsert_node(generate_logserver_node(6, StorageState::DataLoss));

        // effective will be [2-6] because 1 is disabled (authoritatively drained)
        let nodeset: NodeSet = (1..=6).collect();
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // 1 is removed
        assert_that!(checker.len(), eq(5));

        checker.set_attribute_on_each(
            &[
                // validates that we actually ignore this
                PlainNodeId::new(1),
                PlainNodeId::new(2),
                PlainNodeId::new(3),
                PlainNodeId::new(4),
                PlainNodeId::new(5),
            ],
            || true,
        );
        // we cannot write on nodes 3, 5. This should fail the write quorum check because we only have
        // 2 nodes that pass the predicate *and* are writeable (2, 4) and we need 3 for replication.
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));

        // do we have f-majority?
        // [nodeset]         2   3   4   5   6
        // [predicate]       x   x   x   x   x
        // [storage-state]   RW  RO  RW  DL  DL
        //
        // We need 3 nodes of authoritative nodes for successful f-majority. Yes, we have them (2, 3, 4).
        // But some nodes are non-authoritative, so we should observe
        // FMajorityResult::SuccessWithRisk
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::SuccessWithRisk)
        );

        // Can we lose Node 3? No.
        checker.set_attribute(PlainNodeId::new(3), false);

        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::BestEffort)
        );
        assert!(!checker.check_fmajority(|attr| *attr).passed());

        Ok(())
    }

    #[test]
    fn test_replication_single_copy_single_node() -> Result<()> {
        let nodes_config = generate_logserver_nodes_config(1, StorageState::ReadWrite);

        let replication = ReplicationProperty::new(1.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> = NodeSetChecker::new(
            &NodeSet::from_single(PlainNodeId::new(1)),
            &nodes_config,
            &replication,
        );
        assert_that!(checker.len(), eq(1));
        checker.set_attribute(PlainNodeId::new(1), true);

        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        Ok(())
    }

    #[test]
    fn test_dont_panic_on_replication_factor_exceeding_nodeset_size() {
        let nodes_config = generate_logserver_nodes_config(3, StorageState::ReadWrite);

        let nodeset: NodeSet = (1..=3).collect();
        let replication = ReplicationProperty::new(5.try_into().unwrap());

        // replication > nodeset size; could happen as misconfiguration or by nodes getting removed in operation.
        assert_that!(replication.num_copies() as usize, gt(nodeset.len()));

        let checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        assert_that!(
            checker.check_fmajority(|attr| !(*attr)),
            eq(FMajorityResult::None)
        );
    }
}
