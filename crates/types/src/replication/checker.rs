// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use itertools::Itertools;
use tracing::trace;

use crate::Merge;
use crate::PlainNodeId;
use crate::locality::{LocationScope, NodeLocation};
use crate::nodes_config::NodesConfigError;
use crate::nodes_config::{NodesConfiguration, StorageState};

use super::DecoratedNodeSet;
use super::{NodeSet, ReplicationProperty};

type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

/// Possible results of f-majority checks for a subset of the NodeSet.
/// Read variant docs for details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FMajorityResult {
    /// The subset of nodes neither satisfies the authoritative f-majority
    /// property, nor does it contain sufficient authoritative nodes.
    ///
    /// * Bad. No f-majority is possible.
    None,
    /// No f-majority, but all authoritative nodes already matches the attribute and predicate.
    /// This indicates that it's the best possible achievable f-majority and it's up to the user to
    /// decide whether it to consider this a "safe" majority or not. By design, `passed()` return
    /// false for this one.
    ///
    /// * Bad, but it's the best possible majority given the nodeset status.
    BestEffort,
    /// the subset of nodes satisfies the f-majority property, but not enough nodes are
    /// authoritative in that matching subset.
    ///
    /// * Okay (depending on the situation)
    SuccessWithRisk,
    /// the subset of nodes satisfy the authoritative f-majority property, and it
    /// has enough authoritative in that matching subset.
    ///
    /// * Good
    Success,
}

impl FMajorityResult {
    pub fn passed(&self) -> bool {
        matches!(
            self,
            FMajorityResult::Success | FMajorityResult::SuccessWithRisk
        )
    }

    pub fn is_authoritative_complete(&self) -> bool {
        matches!(self, FMajorityResult::Success)
    }
}

/// Run quorum checks on a set of nodes tagged with certain attribute
///
/// It maintains a set of nodes that can be tagged with an attribute,
/// and provides an API for querying the replication properties of the subset of nodes
/// with a certain values for this attribute, given a replication requirement across
/// several failure domains.
///
///
/// The checker is created without any values assigned to any nodes. Note that `check_write_quorum`
/// and `check_fmajority` will only consider nodes with an attribute assigned to them.
///
/// If an attribute is needed to be set by default on all nodes, then use `fill_with` to assign the
/// attribute to all nodes. It's best to do this only if [`NodeSetChecker`] is kept for long-ish
/// period of time.
///
/// The utility provides two methods:
/// - `check_write_quorum()`: Can be used to check if the subset of nodes that are tagged with an attribute
///   matching a predicate may form a legal write quorum. Note that this function doesn't care whether
///   those tagged nodes are writeable or not. It's your responsibility to mark the correct nodes if
///   you want to take StorageState into account.
///
/// - `check_fmajority()`: Used to check if enough nodes have certain values for the attribute so
///   that that set of nodes is an f-majority for at least one of the scope for which there is a
///   replication requirement. An example usage of this method is during loglet seal which each node
///   gets tagged with "SEALED" if that node has been sealed.The seal operation is able to know if
///   it can consider the seal to be completed or not.
///
/// Note that at the moment this doesn't track changes that happen to the storage-states after instantiation.
/// For a fresh view, rebuild this with a new nodes configuration. This might change in the future
/// with a method to refresh the nodes configuration's view.
///
///
/// ## F-Majority and authoritative-ness
///
/// The concept of "f-majority" can be defined as a set of nodes that intersects every possible
/// legal write quorum. Nodes are assigned a [`StorageState`] that define their authoritative-ness.
/// An authoritative node is one that has not lost data. Empty nodes are those that we guarantee
/// that they have never received any writes and will never participate in future writes.
/// A node is considered non-authoritative if it's in [`StorageState::DataLoss`] which means it
/// has lost data and it cannot be considered a reliable participant in some quorum checks. That
/// said, dataloss doesn't include "corruption". It only means that if a node responds negatively
/// to a read request that we cannot confidently assume that the data was never written. Conversely,
/// if the node responds with a record/data, we can use this data safely.
pub struct NodeSetChecker<Attr> {
    // This is a btree-map to leverage its great cache-locality on small sets like this one.
    // this could also be Box<[(PlainNodeId, LocationScopeState<Attr>)]> but btreemap is nicer to
    // use.
    scopes: BTreeMap<LocationScope, LocationScopeState<Attr>>,
    node_to_attr: HashMap<PlainNodeId, Attr>,
    /// Mapping between node-id and its log-server storage state. Note that we keep all nodes even
    /// unreadable ones here because they might become readable after a nodes configuration
    /// refresh. That said. Node-ids that have been deleted in nodes-configuration or that has
    /// storage state `Disabled` will not appear here.
    node_to_storage_state: HashMap<PlainNodeId, StorageState>,
}

impl<Attr: Eq + Hash + Clone + std::fmt::Debug> NodeSetChecker<Attr> {
    // Note that this doesn't track changes that happen to the storage-states after instantiation.
    // For a fresh view, rebuild this with a new nodes configuration.
    pub fn new(
        nodeset: &NodeSet,
        nodes_config: &NodesConfiguration,
        replication_property: &ReplicationProperty,
    ) -> Self {
        let mut scope = LocationScope::Root;
        let mut scopes = BTreeMap::new();
        while let Some(current_scope) = scope.next_smaller_scope() {
            // we are not interested in the scope if replication property doesn't
            // specify a value for it.
            if let Some(replication) = replication_property.copies_at_scope(current_scope) {
                scopes.insert(current_scope, LocationScopeState::new(replication));
            }
            scope = current_scope;
        }

        // we must have at least node-level replication defined
        assert!(!scopes.is_empty());

        let storage_states = HashMap::with_capacity(nodeset.len());
        let node_attribute = HashMap::with_capacity(nodeset.len());

        let mut checker = Self {
            scopes,
            node_to_attr: node_attribute,
            node_to_storage_state: storage_states,
        };

        for node_id in nodeset.iter() {
            match nodes_config.find_node_by_id(*node_id) {
                Ok(config) => checker.add_node(
                    *node_id,
                    config.log_server_config.storage_state,
                    &config.location,
                ),
                Err(NodesConfigError::Deleted(_)) => {
                    // deleted nodes are implicitly considered disabled (auth empty)
                }
                Err(NodesConfigError::UnknownNodeId(_)) => {
                    // Unknown nodes must be also considered as Provisioning because we might be
                    // operating on a nodeset that's out of sync with the nodes configuration
                    // (nodeset is newer).
                    checker.add_node(*node_id, StorageState::Provisioning, &NodeLocation::new());
                }
                Err(NodesConfigError::GenerationMismatch { .. }) => {
                    unreachable!("impossible nodes-configuration errors")
                }
            }
        }

        checker
    }

    /// The number of authoritative nodes in this node set
    pub fn count_authoritative_nodes(&self) -> u32 {
        u32::try_from(
            self.node_to_storage_state
                .values()
                .filter(|s| StorageState::is_authoritative(s))
                .count(),
        )
        .expect("number of nodes in a cluster must fit in u32")
    }

    /// The number of nodes in that nodeset that matches the storage state predicate
    pub fn count_nodes(&self, predicate: impl Fn(&StorageState) -> bool) -> u32 {
        u32::try_from(
            self.node_to_storage_state
                .values()
                .filter(|s| predicate(s))
                .count(),
        )
        .expect("number of nodes in a cluster must fit in u32")
    }

    /// Checks if the set of nodes with attribute matching the predicate are in fact, all authoritative nodes.
    pub fn is_complete_set<Predicate>(&self, predicate: &Predicate) -> bool
    where
        Predicate: Fn(&Attr) -> bool,
    {
        // count complete domains
        let node_scope = self
            .scopes
            .get(&LocationScope::Node)
            .expect("node scope must be set");

        // A shortcut to get the number of authoritative nodes that have this attribute matching
        let (num_matching_auth_nodes, _) = node_scope.count_matching_domains(&predicate);
        // did all authoritative nodes have this attribute?
        num_matching_auth_nodes == self.count_authoritative_nodes()
    }

    /// How many nodes have attributes set. Note that this doesn't count nodes which has not
    /// received an attribute yet (set_attribute was not called on such nodes)
    pub fn len(&self) -> usize {
        self.node_to_attr.len()
    }

    /// Returns true if no attributes were set to any nodes
    pub fn is_empty(&self) -> bool {
        self.node_to_attr.is_empty()
    }

    /// Sets this attribute value on every node of the input iterator
    pub fn set_attribute_on_each(
        &mut self,
        nodes: impl IntoIterator<Item = impl Into<PlainNodeId>>,
        attribute: Attr,
    ) {
        for node in nodes.into_iter() {
            // ignore if the node is not in the original nodeset
            self.set_attribute(node, attribute.clone());
        }
    }

    /// sets all nodes in the node-set to this attribute
    pub fn fill_with(&mut self, attribute: Attr) {
        let node_ids = self.node_to_storage_state.keys().copied().collect_vec();
        self.set_attribute_on_each(node_ids, attribute);
    }

    /// sets all attributes for all nodes with the default value of Attribute
    pub fn fill_with_default(&mut self)
    where
        Attr: Default,
    {
        self.fill_with(Default::default());
    }

    /// Set the attribute value of a node. Note that a node can only be
    /// associated with one attribute value at a time, so if the node has an
    /// existing attribute value, the value will be swapped with this value.
    pub fn set_attribute(&mut self, node_id: impl Into<PlainNodeId>, attribute: Attr) {
        let node_id = node_id.into();
        // ignore if the node is not in the original nodeset
        let storage_state = self
            .node_to_storage_state
            .get(&node_id)
            .copied()
            .unwrap_or(StorageState::Disabled);
        if storage_state.empty() {
            return;
        }
        let old_attribute = self.node_to_attr.insert(node_id, attribute.clone());
        if let Some(old_attribute) = old_attribute {
            if old_attribute == attribute {
                // nothing to be done here.
                return;
            }
            // update references for old attribute
            self.rm_attr_internal(node_id, &old_attribute, storage_state);
        }
        self.add_attr_internal(node_id, attribute, storage_state);
    }

    /// Merges the input attributes with the already assigned attribute if set, otherwise, it'll
    /// merge with the default value of this attribute type.
    pub fn merge_attribute(&mut self, node_id: impl Into<PlainNodeId>, attribute: Attr)
    where
        Attr: Merge + Default,
    {
        let node_id = node_id.into();
        let storage_state = self
            .node_to_storage_state
            .get(&node_id)
            .copied()
            .unwrap_or(StorageState::Disabled);
        if storage_state.empty() {
            return;
        }
        let existing = self.node_to_attr.get(&node_id);
        match existing {
            Some(existing) => {
                let mut new = existing.clone();
                if new.merge(attribute) {
                    // only attempt to set if the merge resulting in a change
                    self.set_attribute(node_id, new);
                }
            }
            None => {
                let mut new = Attr::default();
                new.merge(attribute);
                // we set unconditionally to ensure the attribute is actually assigned to the node
                self.set_attribute(node_id, new);
            }
        }
    }

    pub fn get_attribute(&mut self, node_id: &PlainNodeId) -> Option<&Attr> {
        self.node_to_attr.get(node_id)
    }

    /// Does any node matches the predicate?
    pub fn any(&self, predicate: impl Fn(&Attr) -> bool) -> bool {
        self.node_to_attr.values().any(predicate)
    }

    /// Do all nodes match the predicate?
    pub fn all(&self, predicate: impl Fn(&Attr) -> bool) -> bool {
        self.node_to_attr.values().all(predicate)
    }

    /// Does any node matches the predicate?
    pub fn filter(
        &self,
        predicate: impl Fn(&Attr) -> bool,
    ) -> impl Iterator<Item = (&PlainNodeId, &Attr)> {
        self.node_to_attr
            .iter()
            .filter(move |(_, attribute)| predicate(attribute))
    }

    /// Check if nodes that match the predicate meet the write-quorum rules according to the
    /// replication property. For instance, if replication property is set to {node: 3, zone: 2}
    /// then this function will return `True` if nodes that match the predicate are spread across 2
    /// zones.
    pub fn check_write_quorum<Predicate>(&self, predicate: Predicate) -> bool
    where
        Predicate: Fn(&Attr) -> bool,
    {
        // To check for write-quorum, the quorum-check needs to pass on *all* scopes. The check on
        // every scope ensures we have enough domains to replicate a record according to the
        // configured replication property _at_ the specified scope.
        for (_scope, scope_state) in self.scopes.iter() {
            if !scope_state.check_write_quorum(&predicate) {
                // todo(asoli): change return type to encode failed-scope information so upper layers can
                // log useful information about why write-quorum cannot be achieved.
                return false;
            }
        }
        true
    }

    /// Checks if nodes with attribute that matches `predicate` form a legal f-majority.
    ///
    /// F-majority is possible when any f-majority is achieved on at least one of the scopes
    /// for which there is a replication requirement.
    ///
    /// Two ways to form a mental model about this:
    /// 1) Nodes that match the predicate (storage-state considered) will form an f-majority.
    /// 2) Do we lose quorum-read availability if we lost all nodes that match the predicate?
    pub fn check_fmajority<Predicate>(&self, predicate: Predicate) -> FMajorityResult
    where
        Predicate: Fn(&Attr) -> bool,
    {
        // we have an f-majority if at least one scope has f-majority
        let mut f_majority = false;
        let mut sufficient_auth_domains = false;
        //` if you need the scope for debugging.
        // for (scope, scope_state) in self.scopes.iter() {
        for scope_state in self.scopes.values() {
            let candidate_domains = u32::try_from(scope_state.failure_domains.len())
                .expect("total domains must fit into u32");
            debug_assert!(scope_state.replication_factor > 0);
            let f_majority_needed =
                candidate_domains.saturating_sub(u32::from(scope_state.replication_factor)) + 1;

            let (auth_domains, non_auth_domains) = scope_state.count_matching_domains(&predicate);
            let total = auth_domains + non_auth_domains;
            // Intentionally left as future reference for debugging
            // let replication_at_scope = scope_state.replication_factor;
            // println!(
            //     "[{scope}] replication_at_scope={replication_at_scope} f_majority_needed={f_majority_needed} num_non_empty={candidate_domains} auth_domains={} non_auth_domains={} fmajority? {}",
            //     auth_domains, non_auth_domains,
            //     total > 0 && auth_domains + non_auth_domains >= f_majority_needed
            // );
            if total >= f_majority_needed {
                f_majority = true;
                sufficient_auth_domains = auth_domains >= f_majority_needed;
                break;
            }
        }

        if f_majority && sufficient_auth_domains {
            FMajorityResult::Success
        } else if f_majority {
            // not enough nodes were fully-authoritative but overall we have f-majority
            FMajorityResult::SuccessWithRisk
        } else if self.is_complete_set(&predicate) {
            FMajorityResult::BestEffort
        } else {
            FMajorityResult::None
        }
    }

    /// Should be called only at creation time, to populate the internal state of the checker.
    /// Keep this as a private method to avoid misuse.
    fn add_node(
        &mut self,
        node_id: PlainNodeId,
        storage_state: StorageState,
        location: &NodeLocation,
    ) {
        // we only consider nodes that might have data, everything else is ignored.
        if storage_state.empty() {
            return;
        }
        self.node_to_storage_state.insert(node_id, storage_state);

        for (scope, scope_state) in self.scopes.iter_mut() {
            let domain_name = if *scope == LocationScope::Node {
                // We don't use `domain_string(*scope, Some(node_id))` to avoid allocating long
                // strings at this scope (the node scope has the most number of domains)
                node_id.to_string()
            } else if location.is_scope_defined(*scope) {
                location.domain_string(*scope, None)
            } else {
                // node doesn't have location information at this scope level
                // this is trace because it can very noisy if a single node in the cluster in this
                // state. We don't have a good way of avoiding log-spamming yet.
                trace!(
                    "Node {} doesn't have location information at location scope {:?} although replication is configured at this scope",
                    node_id, scope
                );
                continue;
            };
            // add this node to the correct domain
            let fd = scope_state
                .failure_domains
                .entry(domain_name.into())
                .or_insert_with(|| Box::new(FailureDomainState::default()));
            fd.increment();
            scope_state.node_to_fd.insert(node_id, HashPtr::new(fd));
        }
    }

    fn rm_attr_internal(&mut self, node_id: PlainNodeId, attr: &Attr, storage_state: StorageState) {
        // Remove this attribute on every scope
        for scope_state in self.scopes.values_mut() {
            let Some(fd) = scope_state.node_to_fd.get_mut(&node_id) else {
                // the node's location isn't defined at this scope
                continue;
            };

            let Some(counters) = fd.as_mut_ref().per_attribute_counter.get_mut(attr) else {
                continue;
            };

            counters.decrement(storage_state);

            // last node in this domain.
            if counters.is_empty() {
                let replication_set = scope_state.replication_fds.get_mut(attr);
                let is_replication_set_empty = if let Some(replication_set) = replication_set {
                    replication_set.remove(fd);
                    replication_set.is_empty()
                } else {
                    false
                };
                if is_replication_set_empty {
                    // this domain won't be considered in the replication set for this attribute
                    // value. It has no nodes that matches this attribute.
                    scope_state.replication_fds.remove(attr);
                }
                fd.as_mut_ref().per_attribute_counter.remove(attr);
            }
        }
    }

    fn add_attr_internal(&mut self, node_id: PlainNodeId, attr: Attr, storage_state: StorageState) {
        // Apply this attribute on every scope
        for scope_state in self.scopes.values_mut() {
            let Some(fd) = scope_state.node_to_fd.get_mut(&node_id) else {
                // the node's location isn't defined at this scope
                continue;
            };
            let counters = fd
                .as_mut_ref()
                .per_attribute_counter
                .entry(attr.clone())
                .or_insert(Count::default());
            counters.increment(storage_state);
            let counters_num_authoritative_nodes = counters.num_authoritative_nodes;
            let counters_num_readable_nodes = counters.num_readable_nodes;

            // it's the first time we see this attribute on this scope, let's add the
            // domain into the replication_fds.
            // it's cheaper to do this check vs. always attempting to lookup+insert
            if counters_num_readable_nodes == 1 {
                let replicate_fds = scope_state.replication_fds.entry(attr.clone()).or_default();
                replicate_fds.insert(fd.clone());
            }

            if storage_state.is_authoritative() {
                debug_assert!(counters_num_authoritative_nodes <= counters_num_readable_nodes);
                debug_assert!(counters_num_authoritative_nodes <= fd.as_ref().num_nodes);
            }
        }
    }
}

impl<Attr: Debug> Debug for NodeSetChecker<Attr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.node_to_attr.fmt(f)
    }
}

impl<Attr: Display> Display for NodeSetChecker<Attr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use itertools::Position;
        write!(f, "[")?;
        for (pos, (node_id, attr)) in self
            .node_to_attr
            .iter()
            .sorted_by_key(|v| v.0)
            .with_position()
        {
            match pos {
                Position::Only | Position::Last => write!(f, "{node_id}({attr})")?,
                Position::First | Position::Middle => write!(f, "{node_id}({attr}), ")?,
            }
        }
        write!(f, "]")
    }
}

/// Store the number of nodes for each attribute value in a failure domain
#[derive(Default, Debug)]
struct Count {
    /// Total number of readable nodes for this attribute
    /// read-write + read-only + data-loss
    num_readable_nodes: u32,
    /// Number of nodes in an authoritative storage state (didn't lose data and not empty)
    /// read-write + read-only
    num_authoritative_nodes: u32,
}

impl Count {
    /// No nodes with this attribute value in this domain
    fn is_empty(&self) -> bool {
        self.num_readable_nodes == 0
    }

    /// Number of nodes that are not authoritative
    fn num_non_auth_nodes(&self) -> u32 {
        self.num_readable_nodes - self.num_authoritative_nodes
    }

    /// Increments the corresponding counters according to this storage state
    fn increment(&mut self, storage_state: StorageState) {
        if storage_state.can_read_from() {
            self.num_readable_nodes += 1;
        }

        if storage_state.is_authoritative() {
            self.num_authoritative_nodes += 1;
        }
    }

    /// Increments the corresponding counters according to this storage state
    fn decrement(&mut self, storage_state: StorageState) {
        if self.num_readable_nodes == 0 {
            return;
        }

        if storage_state.can_read_from() {
            self.num_readable_nodes -= 1;
        }
        if storage_state.is_authoritative() {
            self.num_authoritative_nodes -= 1;
        }
    }
}

#[derive(Debug)]
struct FailureDomainState<Attr> {
    /// number of non-empty nodes in this domain
    num_nodes: u32,
    // for each attribute, count the number of nodes that have it set.
    per_attribute_counter: HashMap<Attr, Count>,
}

impl<Attr: Eq + Hash> FailureDomainState<Attr> {
    fn increment(&mut self) {
        self.num_nodes += 1;
    }

    fn non_empty_nodes(&self) -> u32 {
        self.num_nodes
    }

    /// Is the entire domain empty?
    fn is_empty(&self) -> bool {
        self.num_nodes == 0
    }
}

impl<Attr> Default for FailureDomainState<Attr> {
    fn default() -> Self {
        Self {
            num_nodes: 0,
            per_attribute_counter: Default::default(),
        }
    }
}

/// A wrapper type to implement Hash and Eq on a raw pointer allocation, equality
/// is based on the underlying allocation pointer.
#[derive(Debug, Clone)]
struct HashPtr<Attr>(*mut FailureDomainState<Attr>);

/// **Safety**: HashPtr is safe to send as it's strictly private to [`NodeSetChecker`] and is tied to
/// a heap-allocation that's also self-owned.
unsafe impl<Attr: Send> Send for HashPtr<Attr> {}

impl<Attr> HashPtr<Attr> {
    fn as_ref(&self) -> &FailureDomainState<Attr> {
        // Safety: We are the only one who can create [`HashPtr`] and we guarantee that the
        // underlying allocation is valid.
        unsafe { &*self.0 }
    }
    fn as_mut_ref(&mut self) -> &mut FailureDomainState<Attr> {
        // Safety: We are the only one who can create [`HashPtr`] and we guarantee that the
        // underlying allocation is valid.
        unsafe { &mut *self.0 }
    }
}

impl<Attr> Hash for HashPtr<Attr> {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.0.hash(hasher)
    }
}

impl<Attr> Eq for HashPtr<Attr> {}
impl<Attr> PartialEq for HashPtr<Attr> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<Attr> HashPtr<Attr> {
    fn new(value: &mut Box<FailureDomainState<Attr>>) -> Self {
        // Critical: We want the address of the underlying allocation, not the box itself so the
        // double dereferencing is essential.
        //
        // We could have made new() accept &mut FailureDomainState<Attr> but it would have made it
        // easy to pass non-boxed references which will move.
        let value: *mut _ = &mut **value;
        Self(value)
    }
}

#[derive(Debug)]
struct LocationScopeState<Attr> {
    /// domains at that scope
    failure_domains: HashMap<SmartString, Box<FailureDomainState<Attr>>>,
    /// maps between node-id to the domain it belongs to for fast lookups
    node_to_fd: HashMap<PlainNodeId, HashPtr<Attr>>,
    /// replication factor at that scope
    replication_factor: u8,
    /// For each Attribute, set of the domains at that scope that have at least one node with the
    /// attribute. Those are the failure domains considered for replication/write-quorum.
    replication_fds: HashMap<Attr, HashSet<HashPtr<Attr>>>,
}

impl<Attr: Eq + Hash + Clone + std::fmt::Debug> LocationScopeState<Attr> {
    /// Checks write-quorum at this specific scope
    fn check_write_quorum<Predicate>(&self, predicate: &Predicate) -> bool
    where
        Predicate: Fn(&Attr) -> bool,
    {
        // For each attribute, merge the set of FailureDomainState that have at least one node
        // with the attribute here. We stop merging immediately after the set's size reaches
        // the replication target.
        let mut matches = HashSet::with_capacity(self.failure_domains.len());
        for (attribute, domains) in self.replication_fds.iter() {
            if predicate(attribute) {
                for domain in domains {
                    matches.insert(domain);
                    // we have sufficient domains that match the predicate to satisfy replication
                    if matches.len() >= usize::from(self.replication_factor) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Counts the number of domains within this location scope that has
    /// authoritative nodes with the attribute matching the input predicate and the domains with
    /// non-authoritative nodes with the attribute matching the input. This excludes domains that
    /// has no matches or domains that are all empty
    /// Returns (authoritative, non_authoritative)
    fn count_matching_domains<Predicate>(&self, predicate: &Predicate) -> (u32, u32)
    where
        Predicate: Fn(&Attr) -> bool,
    {
        // For all failure domains within this scope; check if it's a complete domain given the
        // input predicate
        let mut num_completed_auth_domains = 0;
        let mut num_completed_non_auth_domains = 0;
        for domain in self.failure_domains.values() {
            let mut num_auth_nodes = 0;
            let mut num_non_auth_nodes = 0;
            if domain.is_empty() {
                num_completed_non_auth_domains += 1;
                continue;
            }

            for (attr, counters) in domain.per_attribute_counter.iter() {
                if predicate(attr) {
                    num_auth_nodes += counters.num_authoritative_nodes;
                    num_non_auth_nodes += counters.num_non_auth_nodes();
                }
            }

            if num_auth_nodes > 0 && num_auth_nodes >= domain.non_empty_nodes() {
                num_completed_auth_domains += 1;
            } else if num_non_auth_nodes > 0 && num_non_auth_nodes >= domain.non_empty_nodes() {
                num_completed_non_auth_domains += 1;
            }
        }
        (num_completed_auth_domains, num_completed_non_auth_domains)
    }
}

impl<Attr: Eq + Hash> LocationScopeState<Attr> {
    fn new(replication: u8) -> Self {
        Self {
            replication_factor: replication,
            failure_domains: HashMap::default(),
            node_to_fd: HashMap::default(),
            replication_fds: HashMap::default(),
        }
    }
}

impl<'a, Attr> IntoIterator for &'a NodeSetChecker<Attr> {
    type Item = (&'a PlainNodeId, &'a Attr);

    type IntoIter = <&'a HashMap<PlainNodeId, Attr> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.node_to_attr.iter()
    }
}

impl<Attr> From<NodeSetChecker<Attr>> for DecoratedNodeSet<Attr> {
    fn from(val: NodeSetChecker<Attr>) -> DecoratedNodeSet<Attr> {
        DecoratedNodeSet::from_iter(val.node_to_attr)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use googletest::prelude::*;

    use crate::nodes_config::{
        LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
    };
    use crate::{GenerationalNodeId, PlainNodeId};

    fn generate_logserver_node(
        id: impl Into<PlainNodeId>,
        storage_state: StorageState,
        location: &str,
    ) -> NodeConfig {
        let id: PlainNodeId = id.into();
        NodeConfig::builder()
            .name(format!("node-{id}"))
            .current_generation(GenerationalNodeId::new(id.into(), 1))
            .location(location.parse().unwrap())
            .address(format!("unix:/tmp/my_socket-{id}").parse().unwrap())
            .roles(Role::LogServer.into())
            .log_server_config(LogServerConfig { storage_state })
            .build()
    }

    #[test]
    fn nodeset_checker_basics() -> Result<()> {
        const NUM_NODES: u32 = 10;
        const LOCATION: &str = "";
        // all_authoritative, all flat structure (no location)
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // all_authoritative
        for i in 1..=NUM_NODES {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                LOCATION,
            ));
        }

        let nodeset: NodeSet = (1..=5).collect();
        // flat node-level replication
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // all nodes in the nodeset are authoritative
        assert_that!(checker.count_nodes(StorageState::is_authoritative), eq(5));
        // all nodes are false by default. Can't establish write quorum.
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));

        checker.set_attribute_on_each([1, 2, 4], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));

        // 2 nodes are false in this node-set, not enough for write-quorum
        assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(false));

        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // we only have 2 nodes with false, fmajority not possible
        assert_that!(
            checker.check_fmajority(|attr| !(*attr)),
            eq(FMajorityResult::None)
        );
        Ok(())
    }

    #[test]
    fn nodeset_checker_tight() -> Result<()> {
        const NUM_NODES: u32 = 2;
        const LOCATION: &str = "";
        // all_authoritative, all flat structure (no location)
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // all_authoritative
        for i in 1..=NUM_NODES {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                LOCATION,
            ));
        }

        let nodeset: NodeSet = (1..=2).collect();
        // flat node-level replication
        let replication = ReplicationProperty::new(2.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // all nodes in the nodeset are authoritative
        assert_that!(checker.count_nodes(StorageState::is_authoritative), eq(2));

        // checker.set_attribute_on_each([1, 2, 4], true);
        // assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));
        //
        // // 2 nodes are false in this node-set, not enough for write-quorum
        // assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(false));

        // Assuming we have [N1, N2] and replication={node: 2}
        // Assuming that true means SEALED and false means Open and unset means that a node didn't
        // respond yet.
        //
        // F-majority of SEALED means we are sealed. In this case we are not sealed.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        // Making sure that we are not interpreting unset as false.
        assert_that!(
            checker.check_fmajority(|attr| !(*attr)),
            eq(FMajorityResult::None)
        );

        // A loglet is Open if write-quorum of nodes is open. We are unset, we shouldn't consider
        // this as equivalent to Open (false).
        assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(false));

        // # Scenario A: 1 Node is SEALED. The other node didn't respond.
        checker.set_attribute(1, true);
        // F-Majority if sealed.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );
        // Flip-side, we can't say we have write quorum
        assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(false));

        // # Scenario B: 1 Node is OPEN, The node didn't respond.
        checker.set_attribute(1, false);
        // F-Majority is NOT sealed.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        // And we don't have enough responses to say that the loglet is open
        assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(false));
        // Only when N2 is Open we can say it's open
        checker.set_attribute(2, false);
        assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(true));
        // making sure that we are not mistakingly saying that it's sealed
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        // Scenario C: Both nodes are sealed.
        checker.set_attribute(1, true);
        checker.set_attribute(2, true);
        // checking if write-quorum nodes are open, NO.
        assert_that!(checker.check_write_quorum(|attr| !(*attr)), eq(false));
        // checking if f-majority is sealed, YES.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        Ok(())
    }

    #[test]
    fn nodeset_checker_non_existent_nodes() -> Result<()> {
        const LOCATION: &str = "";
        // all_authoritative, all flat structure (no location)
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // add 2 nodes to config N1, N2
        for i in 1..=2 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                LOCATION,
            ));
        }

        // NodeSet has 3 nodes N1, N2, N3. (N3 doesn't exist in config)
        let nodeset: NodeSet = (1..=3).collect();
        let replication = ReplicationProperty::new(2.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // all nodes in the nodeset are authoritative
        assert_that!(checker.count_nodes(StorageState::is_authoritative), eq(3));
        // all nodes are false by default. Can't establish write quorum.
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));

        // Because N3 is not found, we should not consider it as auth empty, because it might show
        // up in future nodes configuration update, we *must* consider it as Provisioning (fully
        // auth). This means that we must acquire 2 nodes to achieve f-majority.
        checker.set_attribute_on_each([1], true);
        assert_that!(
            checker.check_fmajority(|attr| !(*attr)),
            eq(FMajorityResult::None)
        );

        checker.set_attribute_on_each([1, 2], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // 2 nodes is also what we need to write quorum
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));
        Ok(())
    }

    #[test]
    fn nodeset_checker_mixed() -> Result<()> {
        const LOCATION: &str = "";
        // still flat
        let mut nodes_config = NodesConfiguration::new_for_testing();
        nodes_config.upsert_node(generate_logserver_node(1, StorageState::Disabled, LOCATION));
        nodes_config.upsert_node(generate_logserver_node(
            2,
            StorageState::ReadWrite,
            LOCATION,
        ));
        nodes_config.upsert_node(generate_logserver_node(3, StorageState::ReadOnly, LOCATION));
        nodes_config.upsert_node(generate_logserver_node(
            4,
            StorageState::ReadWrite,
            LOCATION,
        ));
        nodes_config.upsert_node(generate_logserver_node(5, StorageState::DataLoss, LOCATION));
        nodes_config.upsert_node(generate_logserver_node(6, StorageState::DataLoss, LOCATION));

        // effective will be [2-6] because 1 is disabled (authoritatively drained)
        let nodeset: NodeSet = (1..=6).collect();
        // flat node-level replication
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // len only returns nodes with attribute "set" on them
        assert_that!(checker.len(), eq(0));
        assert_that!(checker.count_authoritative_nodes(), eq(3));

        checker.set_attribute_on_each([1, 2, 3, 4], true);
        // 1 is ignored as it's disabled.
        assert_that!(checker.len(), eq(3));
        // Despite that node 3 is ReadOnly, check_write_quorum doesn't care about this, it doesn't
        // answer the question of whether a record "can" be replicated or not, but if a record has
        // been already "replicated" on those nodes, do those node form a legal write-quorum or
        // not.
        //
        // The caller should only set the attribute on nodes with `StorageState::can_write_to() ==
        // true` if they want to restrict this check.
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));

        // do we have f-majority?
        // [nodeset]         2   3   4   5   6
        // [predicate]       x   x   x   -   -
        // [storage-state]   RW  RO  RW  DL  DL
        //
        // We need 3 nodes of authoritative nodes for successful f-majority. Yes, we have them (2, 3, 4).
        // FMajorityResult::Success
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        checker.set_attribute(3, false);
        // Can we lose Node 3? No.
        // [nodeset]         2   3   4   5   6
        // [predicate]       x   -   x   -   -
        // [storage-state]   RW  RO  RW  DL  DL
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        assert!(!checker.check_fmajority(|attr| *attr).passed());

        // What if we have 6?
        // [nodeset]         2   3   4   5   6
        // [predicate]       x   -   x   -   x
        // [storage-state]   RW  RO  RW  DL  DL
        checker.set_attribute(6, true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::SuccessWithRisk)
        );

        Ok(())
    }

    #[test]
    fn nodeset_checker_non_authoritative() -> Result<()> {
        const LOCATION: &str = "";
        // still flat
        let mut nodes_config = NodesConfiguration::new_for_testing();
        nodes_config.upsert_node(generate_logserver_node(1, StorageState::Disabled, LOCATION));
        nodes_config.upsert_node(generate_logserver_node(
            2,
            StorageState::ReadWrite,
            LOCATION,
        ));
        nodes_config.upsert_node(generate_logserver_node(3, StorageState::ReadOnly, LOCATION));
        nodes_config.upsert_node(generate_logserver_node(4, StorageState::DataLoss, LOCATION));
        nodes_config.upsert_node(generate_logserver_node(5, StorageState::DataLoss, LOCATION));
        nodes_config.upsert_node(generate_logserver_node(6, StorageState::DataLoss, LOCATION));
        // effective will be 5 nodes [2-6] because 1 is disabled (authoritatively drained)
        let nodeset: NodeSet = (1..=6).collect();
        // flat node-level replication
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        checker.set_attribute_on_each([2, 3], true);
        // do we have f-majority?
        // [nodeset]         2   3   4   5   6
        // [predicate]       x   x   -   -   -
        // [storage-state]   RW  RO  DL  DL  DL
        //
        // We need 3 nodes of authoritative nodes for successful f-majority. No, but that's the
        // widest majority for authoritative nodes.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::BestEffort)
        );

        checker.set_attribute(5, true);

        // do we have f-majority?
        // [nodeset]         2   3   4   5   6
        // [predicate]       x   x   -   x   -
        // [storage-state]   RW  RO  DL  DL  DL
        //
        // We need 3 nodes of authoritative nodes for a risk-free f-majority.
        // But here, we only have 2,3 authoritative and 5 has the attribute but not authoritative
        let f_majority = checker.check_fmajority(|attr| *attr);
        assert_that!(f_majority, eq(FMajorityResult::SuccessWithRisk));
        assert_that!(f_majority.is_authoritative_complete(), eq(false));
        assert_that!(f_majority.passed(), eq(true));

        // Can we lose Node 3? No.
        checker.set_attribute(3, false);

        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        assert!(!checker.check_fmajority(|attr| *attr).passed());

        checker.fill_with_default();
        checker.set_attribute_on_each([5, 6], true);
        // do we have f-majority?
        // [nodeset]         2   3   4   5   6
        // [predicate]       -   -   -   x   6
        // [storage-state]   RW  RO  DL  DL  DL
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        checker.set_attribute(4, true);

        // do we have f-majority?
        // [nodeset]         2   3   4   5   6
        // [predicate]       -   -   x   x   x
        // [storage-state]   RW  RO  DL  DL  DL
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::SuccessWithRisk)
        );

        checker.set_attribute(2, true);
        checker.set_attribute(3, true);
        // do we have f-majority?
        // [nodeset]         2   3   4   5   6
        // [predicate]       x   x   x   x   x
        // [storage-state]   RW  RO  DL  DL  DL
        //
        // Why is still with risk? because we don't have enough authoritative nodes that match the
        // predicate. We need 3. It's also not BestEffort because the total number of matches meets
        // the majority requirements.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::SuccessWithRisk)
        );

        Ok(())
    }

    #[test]
    fn nodeset_checker_single_copy_single_node() -> Result<()> {
        const NUM_NODES: u32 = 1;
        const LOCATION: &str = "";
        // all_authoritative, all flat structure (no location)
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // all_authoritative
        for i in 1..=NUM_NODES {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                LOCATION,
            ));
        }

        let replication = ReplicationProperty::new(1.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&NodeSet::from_single(1), &nodes_config, &replication);
        assert_that!(checker.count_authoritative_nodes(), eq(1));
        checker.set_attribute(1, true);

        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        Ok(())
    }

    #[test]
    fn nodeset_checker_dont_panic_on_replication_factor_exceeding_nodeset_size() {
        const NUM_NODES: u32 = 3;
        const LOCATION: &str = "";
        // all_authoritative, all flat structure (no location)
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // all_authoritative
        for i in 1..=NUM_NODES {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                LOCATION,
            ));
        }

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

    // Failure-domain-aware tests
    #[test]
    fn nodeset_checker_fd_basics() -> Result<()> {
        // 3 nodes in each zone, and 3 zones in total in two regions
        // additionally, we have 1 node without any region assignment
        // total = 10 nodes
        // region1
        //      .az1  [N1, N2, N3]
        //      .az2  [N4, N5, N6]
        // region2
        //      .az1  [N7, N8, N9]
        // -         [N10(Provisioning/Not In Config) N11(D)]  - D = Disabled
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // all authoritative
        // region1.az1
        for i in 1..=3 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region1.az1",
            ));
        }
        // region1.az2
        for i in 4..=6 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region1.az2",
            ));
        }
        // region2.az1
        for i in 7..=9 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region2.az1",
            ));
        }
        // N10 - Provisioning - Not in config
        // N11 - Disabled
        nodes_config.upsert_node(generate_logserver_node(11, StorageState::Disabled, ""));

        // # Scenario 1
        // - Nodeset with all nodes = 11 nodes (N11 is effectively discounted), effective=10
        // - Replication is {node: 3}
        let nodeset: NodeSet = (1..=11).collect();
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);
        // all nodes in the nodeset are authoritative
        assert_that!(checker.count_nodes(StorageState::is_authoritative), eq(10));
        // all nodes are false by default. Can't establish write quorum.
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        // node 10 is provisioning, we can mark it but we shouldn't if we didn't actually write
        // records to it.
        checker.set_attribute_on_each([1, 2], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        // having 3 will satisfy our quorum needs
        checker.set_attribute(3, true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));

        // We need 8 authoritative non-empty nodes (anywhere) for f-majority, current [1,2,3] are marked.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        // total 8 nodes marked but 11 is empty/disabled, not enough.
        checker.set_attribute_on_each([4, 5, 7, 8, 11], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        checker.set_attribute(9, true);
        // now we have the required 8 nodes
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // # Scenario 2
        // - Nodeset with all nodes = 10 nodes
        // - Replication is {zone: 2}
        let nodeset: NodeSet = (1..=11).collect();
        let replication = ReplicationProperty::from_str("{zone: 2}").unwrap();
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        // two nodes but on the same zone, can't write.
        checker.set_attribute_on_each([1, 2], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        checker.fill_with_default();
        // N10 is not on any zone, so we can only consider its domain on the node scope but not
        // on the zone.
        checker.set_attribute_on_each([1, 10], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        checker.fill_with_default();
        // finally, two nodes across two zones = SUCCESS
        checker.set_attribute_on_each([1, 4], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));

        // F-majority checks.
        checker.fill_with_default();
        // region1.az1 is marked. Not enough for f-majority
        checker.set_attribute_on_each([1, 2, 3], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        // most of region1.az2 is marked. Not enough for f-majority
        checker.set_attribute_on_each([4, 5], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        // now all of region1.az2 is marked. this is Enough for f-majority
        checker.set_attribute_on_each([6], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // Another pattern, marking 2 nodes on every zone and the entirety of region1.az2
        checker.fill_with_default();
        checker.set_attribute_on_each([1, 2, 4, 5, 6, 7, 8], true);
        // region1
        //      .az1  [N1(x), N2(x), N3]
        //      .az2  [N4(x), N5(x), N6(x)]
        // region2
        //      .az1  [N7(x), N8(x), N9]
        // -         [N10]
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        // Would N3 gets us to f-majority? (region1.az1, region1.az2 are all marked)
        checker.set_attribute_on_each([3], true);
        // region1
        //      .az1  [N1(x), N2(x), N3(x)]
        //      .az2  [N4(x), N5(x), N6(x)]
        // region2
        //      .az1  [N7(x), N8(x), N9]
        // -         [N10]
        // Yes because the remaining N9 and N10 cannot form write-quorum. (N10 is not in a zone)
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );
        // now that N10 is marked
        checker.set_attribute_on_each([10], true);
        // region1
        //      .az1  [N1(x), N2(x), N3(x)]
        //      .az2  [N4(x), N5(x), N6(x)]
        // region2
        //      .az1  [N7(x), N8(x), N9]
        // -         [N10(x)]
        // We have f-majority
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        Ok(())
    }

    #[test]
    fn nodeset_checker_fd_non_authoritative_nodeset() -> Result<()> {
        // Did the nodeset become non-authoritative? (many nodes in DataLoss state)
        // 3 nodes in each zone, and 3 zones in total in two regions
        // additionally, we have 1 node without any region assignment
        // The issue here is that we have region1.az1 + N5 in DL which is enough to make the entire nodeset
        // non-authoritative; meaning that we cannot achieve better than
        // `FMajority::SuccessWithRisk` and when marking only authoritative_nodes, we can't achieve
        // f-majority (BestEffort case)
        // In other words, all authoritative_nodes < f_majority
        // total = 10 nodes - DL == DataLoss
        // replication = {zone: 2}
        // region1
        //      .az1  [N1(DL), N2(DL), N3(DL)]
        //      .az2  [N4, N5(DL), N6]
        // region2
        //      .az1  [N7, N8, N9]
        // -         [N10, N11(P)] - P = Provisioning
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // region1.az1
        nodes_config.upsert_node(generate_logserver_node(
            1,
            StorageState::DataLoss,
            "region1.az1",
        ));
        nodes_config.upsert_node(generate_logserver_node(
            2,
            StorageState::DataLoss,
            "region1.az1",
        ));
        nodes_config.upsert_node(generate_logserver_node(
            3,
            StorageState::DataLoss,
            "region1.az1",
        ));
        // region1.az2
        nodes_config.upsert_node(generate_logserver_node(
            4,
            StorageState::ReadWrite,
            "region1.az2",
        ));
        nodes_config.upsert_node(generate_logserver_node(
            5,
            StorageState::DataLoss,
            "region1.az2",
        ));
        nodes_config.upsert_node(generate_logserver_node(
            6,
            StorageState::ReadWrite,
            "region1.az2",
        ));
        // region2.az1
        for i in 7..=9 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region2.az1",
            ));
        }
        // N10
        nodes_config.upsert_node(generate_logserver_node(10, StorageState::ReadWrite, ""));
        // N11
        nodes_config.upsert_node(generate_logserver_node(11, StorageState::Provisioning, ""));

        let nodeset: NodeSet = (1..=11).collect();
        let replication = ReplicationProperty::from_str("{zone: 2}").unwrap();
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // marked every possible node
        checker.set_attribute_on_each([4, 6, 7, 8, 9, 10, 11], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::BestEffort)
        );

        Ok(())
    }

    #[test]
    fn nodeset_checker_fd_three_levels() -> Result<()> {
        // total = 11 nodes across 3 regions.
        // replication = {region: 2, zone: 3}
        // region1
        //      .az1  [N1, N2, N3, N4]
        //      .az2  [N5, N6, N7]
        // region2
        //      .az1  [N8, N9, N10]
        // region3
        //      .az1  [N11]
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // region1.az1
        for i in 1..=4 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region1.az1",
            ));
        }
        // region1.az2
        for i in 5..=7 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region1.az2",
            ));
        }
        // region2.az1
        for i in 8..=10 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region2.az1",
            ));
        }
        // region3.az1
        nodes_config.upsert_node(generate_logserver_node(
            11,
            StorageState::ReadWrite,
            "region3.az1",
        ));

        let nodeset: NodeSet = (1..=11).collect();
        let replication = ReplicationProperty::from_str("{region: 2, zone: 3}").unwrap();
        assert_that!(
            replication.copies_at_scope(LocationScope::Node),
            some(eq(3))
        );
        assert_that!(
            replication.copies_at_scope(LocationScope::Zone),
            some(eq(3))
        );
        assert_that!(
            replication.copies_at_scope(LocationScope::Region),
            some(eq(2))
        );
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // # Write-quorum checks
        // Three nodes on two regions but on across 2 zones only
        // region1
        //      .az1  [N1, N2, N3, N4]
        //      .az2  [N5, N6, N7]
        // region2
        //      .az1  [N8, N9, N10]
        // region3
        //      .az1  [N11]
        // No, can't do.
        checker.set_attribute_on_each([1, 2, 8], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        // replacing N2 with N5 should fix it
        checker.set_attribute(2, false);
        checker.set_attribute(5, true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));

        // # F-majority checks
        checker.fill_with_default();
        // losing 1 region is okay (marking everything else)
        checker.set_attribute_on_each([8, 9, 10, 11], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // we can't afford to lose _any_ other node.
        checker.set_attribute_on_each([10], false);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        Ok(())
    }

    #[test]
    fn nodeset_checker_fd_empty_domains() -> Result<()> {
        // total = 12 nodes across 4 regions (effectively 3 regions, region4 is disabled)
        // replication = {region: 2, zone: 3}
        // region1
        //      .az1  [N1, N2(P), N3(D), N4(D)]
        //      .az2  [N5(D), N6(D), N7(D)]  - nodes are Disabled
        // region2
        //      .az1  [N8, N9, N10]
        // region3
        //      .az1  [N11]
        // region4 -- region is Disabled
        //      .az1  [N12(D)]
        let mut nodes_config = NodesConfiguration::new_for_testing();
        // region1.az1
        nodes_config.upsert_node(generate_logserver_node(
            1,
            StorageState::ReadWrite,
            "region1.az1",
        ));
        nodes_config.upsert_node(generate_logserver_node(
            2,
            StorageState::Provisioning,
            "region1.az1",
        ));
        nodes_config.upsert_node(generate_logserver_node(
            3,
            StorageState::Disabled,
            "region1.az1",
        ));
        nodes_config.upsert_node(generate_logserver_node(
            4,
            StorageState::Disabled,
            "region1.az1",
        ));
        // region1.az2
        for i in 5..=7 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::Disabled,
                "region1.az2",
            ));
        }
        // region2.az1
        for i in 8..=10 {
            nodes_config.upsert_node(generate_logserver_node(
                i,
                StorageState::ReadWrite,
                "region2.az1",
            ));
        }
        // region3.az1
        nodes_config.upsert_node(generate_logserver_node(
            11,
            StorageState::ReadWrite,
            "region3.az1",
        ));
        // region4.az1
        nodes_config.upsert_node(generate_logserver_node(
            12,
            StorageState::Disabled,
            "region4.az1",
        ));

        let nodeset: NodeSet = (1..=12).collect();
        let replication = ReplicationProperty::from_str("{region: 2, zone: 3}").unwrap();
        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);

        // # Write-quorum checks
        // Can we write to N2, N5, N11? No.
        // region1
        //      .az1  [N1, N2(P), N3(D), N4(D)]
        //      .az2  [N5(D), N6(D), N7(D)]  - nodes are Disabled
        // region2
        //      .az1  [N8, N9, N10]
        // region3
        //      .az1  [N11]
        // region4
        //      .az1  [N12(D)]
        checker.set_attribute_on_each([2, 5, 11], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        // if we add N1? we wouldn't have enough zones still
        checker.set_attribute_on_each([1], true);
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(false));
        // but adding any node from region2 should fix it
        checker.set_attribute_on_each([10], true);
        // region1
        //      .az1  [N1(x), N2(x), N3(D), N4(D)]
        //      .az2  [N5(D), N6(D), N7(D)]  - nodes are Disabled
        // region2
        //      .az1  [N8, N9, N10(x)]
        // region3
        //      .az1  [N11(x)]
        // region4
        //      .az1  [N12(D)]
        assert_that!(checker.check_write_quorum(|attr| *attr), eq(true));

        let mut checker: NodeSetChecker<bool> =
            NodeSetChecker::new(&nodeset, &nodes_config, &replication);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        // # F-majority checks
        // non_empty_regions = 3, non_empty_zones = 3
        // region_f_majority = 3-2+1=2  zone_f_majority=3-3+1=1
        // In other words, have f-majority if we have one full zone, or any 2 non-empty regions.
        //
        // region1
        //      .az1  [N1, N2(P), N3(D), N4(D)]
        //      .az2  [N5(D), N6(D), N7(D)]  - nodes are Disabled
        // region2
        //      .az1  [N8, N9, N10]
        // region3
        //      .az1  [N11]
        // region4
        //      .az1  [N12(D)]

        checker.set_attribute_on_each([1, 2], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );
        // revert that.
        checker.set_attribute_on_each([1, 2], false);

        // N10 on region2.az1 is not sufficient since other nodes on the same zone are not marked
        checker.set_attribute_on_each([10], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        // now let's add the rest of the nodes in this zone
        checker.set_attribute_on_each([8, 9], true);
        // Yay!
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // Another more stringent example is region3.az1. Only N11 should be sufficient for
        // f-majority in this setup.
        checker.fill_with_default();
        checker.set_attribute_on_each([11], true);
        // Flexible quorums FTW.
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::Success)
        );

        // but not for N9
        checker.fill_with_default();
        checker.set_attribute_on_each([9], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );

        // nor N12 because it's empty (not member of the nodeset)
        checker.fill_with_default();
        checker.set_attribute_on_each([12], true);
        assert_that!(
            checker.check_fmajority(|attr| *attr),
            eq(FMajorityResult::None)
        );
        Ok(())
    }
}
