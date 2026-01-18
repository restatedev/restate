// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hasher;

use tracing::{debug, error};
use xxhash_rust::xxh3::Xxh3;

use crate::PlainNodeId;
use crate::locality::{LocationScope, NodeLocation};
use crate::logs::metadata::NodeSetSize;
use crate::nodes_config::{NodeConfig, NodesConfiguration};
use crate::replication::{NodeSet, ReplicationProperty};

// A virtual safety net, nodeset that big can cause all sorts of problems. I hope that
// we don't hit this limit, ever. That said, even if the entire cluster is in the nodeset, a
// cluster bigger than this number of nodes is worth breaking into smaller ones.
//
// This value is not stored on disk and can be changed in the future if needed.
pub const MAX_NODESET_SIZE: u32 = NodeSetSize::MAX.as_u32();

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeSelectorError {
    #[error(
        "not enough candidate nodes to satisfy the replication factor; available={nodeset_len} nodes but replication={replication_factor}"
    )]
    InsufficientNodes {
        nodeset_len: usize,
        replication_factor: u8,
    },
    #[error(
        "node location for node {node_id} is missing. Node locations are required if domain-aware replication is enabled. Domain-aware replication is enabled due to replication-property set to {replication}"
    )]
    NodeLocationMissing {
        node_id: PlainNodeId,
        replication: ReplicationProperty,
    },
    #[error(
        "node location for node {node_id} is configured to be '{location}', but is not defined at scope {missing_scope}. The scope is required because replication {replication} is defined at this scope."
    )]
    IncompleteNodeLocation {
        node_id: PlainNodeId,
        location: NodeLocation,
        missing_scope: LocationScope,
        replication: ReplicationProperty,
    },
}

#[derive(Debug, Clone)]
pub struct NodeSetSelectorOptions<'a> {
    /// For instance, the loglet-id, log-id, or the partition-id we are selecting for. This value
    /// is used to generate repeatable rank of nodes that's different for every id. For instance,
    /// if you want to reduce data scatter for loglets of a given log-id, then it's best to use the
    /// log-id as the hashing_id here.
    hashing_id: u64,
    /// Salt/seed for RNG/hash functions used by the nodeset selector
    salt: u64,
    target_size: NodeSetSize,
    /// enabled by default, disable if you don't want repetable generation
    consistent_hashing: bool,
    top_priority_node: Option<PlainNodeId>,
    preferred_nodes: Option<&'a NodeSet>,
}

impl<'a> NodeSetSelectorOptions<'a> {
    /// id can be log-id, loglet-id, partition-id, or anything you like.
    pub fn new(id: u64) -> Self {
        Self {
            hashing_id: id,
            ..Default::default()
        }
    }

    pub fn id(&self) -> u64 {
        self.hashing_id
    }

    pub fn with_target_size(mut self, target_size: NodeSetSize) -> Self {
        self.target_size = target_size;
        self
    }

    pub fn target_size(&self) -> NodeSetSize {
        self.target_size
    }

    pub fn with_salt(mut self, salt: u64) -> Self {
        self.salt = salt;
        self
    }

    pub fn salt(&self) -> u64 {
        self.salt
    }

    pub fn with_consistent_hashing(mut self, consistent_hashing: bool) -> Self {
        self.consistent_hashing = consistent_hashing;
        self
    }

    pub fn consistent_hashing(&self) -> bool {
        self.consistent_hashing
    }

    pub fn with_preferred_nodes(mut self, preferred_nodes: &'a NodeSet) -> Self {
        self.preferred_nodes = Some(preferred_nodes);
        self
    }

    pub fn with_preferred_nodes_opt(mut self, preferred_nodes: Option<&'a NodeSet>) -> Self {
        self.preferred_nodes = preferred_nodes;
        self
    }

    pub fn preferred_nodes(&self) -> Option<&NodeSet> {
        self.preferred_nodes
    }

    /// A node that's considered a top priority to be included in this nodeset. This node will rank
    /// the highest in its domain (if it passes the candidacy filter) and its domain will be
    /// scanned first. The node will be picked even if it doesn't pass `is_writeable` filter.
    pub fn with_top_priority_node(mut self, node: impl Into<PlainNodeId>) -> Self {
        self.top_priority_node = Some(node.into());
        self
    }

    /// A node that's considered a top priority to be included in this nodeset.
    pub fn top_priority_node(&self) -> Option<PlainNodeId> {
        self.top_priority_node
    }
}

impl Default for NodeSetSelectorOptions<'_> {
    fn default() -> Self {
        Self {
            hashing_id: 0,
            target_size: NodeSetSize::default(),
            salt: 14712721395741015273,
            consistent_hashing: true,
            preferred_nodes: None,
            top_priority_node: None,
        }
    }
}

impl NodeSetSelectorOptions<'_> {
    fn hash_node_id(&self, node_id: PlainNodeId) -> u64 {
        if self.consistent_hashing {
            let mut hasher = Xxh3::with_seed(self.salt);
            hasher.write_u64(self.hashing_id);
            hasher.write_u64(u32::from(node_id) as u64);
            hasher.finish()
        } else {
            rand::random()
        }
    }

    fn hash_domain(&self, domain_path: &DomainPath) -> u64 {
        if self.consistent_hashing {
            let mut hasher = Xxh3::with_seed(self.salt);
            hasher.write_u64(self.hashing_id);
            match domain_path {
                DomainPath::Root => hasher.write_u64(0),
                DomainPath::Specific(domain) => hasher.write(domain.as_bytes()),
            }
            hasher.finish()
        } else {
            rand::random()
        }
    }
}

/// Picks a set of nodes out of the available pool.
///
/// Node selector can be used to generate a set of candidates for partition placement, or for
/// replicated loglet. The user of the API decides the meaning of "writeable" node and
/// "preferred_nodes" as they see fit. For instance, whether the user wants to consider
/// ClusterState as a signal to consider a node as "writeable" or not is independent from the
/// selection algorithm.
///
/// Note: This nodeset selector won't check if the returned nodeset is valid or not as this
/// depends on who is using it. Therefore, it's the responsibility of the caller to validate its
/// output if needed. That said, it will fail to generate a nodeset if it cannot find enough nodes
/// to satisfy the number of copies defined in replication_property.
///
/// ## Working principle
///
/// We try to build a nodeset that's spread out as evenly as possible across domains, it's focus is
/// to collect enough nodes that span the widest defined scope in replication property without
/// giving much attention to intermediate levels in the hierarchy. This approach is proven to be
/// simple, efficient, and reliable.
///
/// It forms a nodeset by repeatedly popping entries from a shuffled node list of the domain with
/// the smallest number of picked nodes. It tries to generates repeatable nodeset unless `consistent_hashing`
/// is set to false. Additionally, when visiting every domain to choose nodes, we give the highest priority
/// to the set of `preferred_nodes` to give them the best chance of being picked up.
///
/// **Handling of temporarily unwritable nodes:**
///
/// If there are temporarily unwritable nodes based on the result of `is_writeable` filtering
/// function, we may pick some of them, but only _in addition_ to a full-size nodeset of writable nodes.
///
/// E.g. if a zone is unwritable, we'll pick a nodeset as if that zone didn't exist,
/// then add a few nodes from the zone, ending up with a nodeset bigger than [`NodeSetSelectorOptions::target_size`].
///
/// Picking unwritable nodes is needed to allow these node to participate in the cluster after
/// coming back and becoming writeable again. Picking a full-size nodeset of writable nodes is still
/// needed to make sure the data distribution is good until the unwritable nodes become writable
/// again
///
///
/// ## Definitions
///
/// **Preferred nodes**: The set of nodes which we prefer to see in the output iff they pass
/// is_candidate filter. Preferred nodes are used as a hint but it doesn't impact the balance of
/// domains being picked.
/// **Top priority node**: An optional single node that will promote itself and _its domain_ to the top of
/// the selection list. The top-priority node must pass the candidacy filter, otherwise, it's
/// ignored.
/// **Candidate node**: Any node that passes the `is_candidate` filter lambda passed by the user.
/// **Writeable node**: Any node that passes the `is_writeable` filter lambda passed by the user.
pub struct DomainAwareNodeSetSelector;

impl DomainAwareNodeSetSelector {
    /// Note: determinism of nodeset generation depends on all inputs, including the
    /// nodes_configuration, and all options in [`NodeSetSelectorOptions`]. If the list of
    /// preferred_nodes was changed, the output of generation will be different.
    pub fn select(
        nodes_config: &NodesConfiguration,
        replication_property: &ReplicationProperty,
        is_candidate: impl Fn(PlainNodeId, &NodeConfig) -> bool,
        is_writeable: impl Fn(PlainNodeId, &NodeConfig) -> bool,
        options: NodeSetSelectorOptions<'_>,
    ) -> Result<NodeSet, NodeSelectorError> {
        let replication_factors = replication_property.distinct_replication_factors();
        debug_assert!(!replication_factors.is_empty());
        // Explanation:
        // Our goal is to figure out what's the minimum number of domains we need to pick and at
        // which location scope.
        //
        // 1. If widest replication scope is at the node level, we can't have locality affinity so
        //    we'll pick nodes that span the entire topology (replicate_across_scope == Root). In
        //    that case, at minimum we need enough domains (nodes) to satisfy replication.
        // 2. If the widest replication is higher, we aim to find equal number of domains within
        //    this location scope. (e.g. {zone: 2, node: 5}, then replicate_across_scope=zone, and
        //    the minimum is (5 - 2 + 1 = 4) nodes from every zone. This is the minimum because
        //    it's valid to replicate 4 copies in one zone + 1 copy in another zone.
        let (replicate_across_scope, min_nodes_per_domain) = {
            let (replicate_across_scope, factor) = replication_factors.first().unwrap();
            if let LocationScope::Node = *replicate_across_scope {
                // no locality affinity
                (LocationScope::Root, *factor as u32)
            } else {
                let min_nodes_per_domain: u32 =
                    replication_factors.last().unwrap().1 as u32 - (*factor as u32) + 1;
                (*replicate_across_scope, min_nodes_per_domain)
            }
        };

        // Try and pick at least this many nodes in total. If target size of the nodeset is too
        // small compared to replication factor, we increase it to 2 * replication_factor -1,
        // which, in some sense, provides equal write and read availability similar to
        // simple-majority quorums.
        let min_total_nodes: u32 = std::cmp::max(
            options.target_size().as_u32(),
            replication_property.num_copies() as u32 * 2 - 1,
        );

        // Try and not go under 2 nodes (mainly for replication_factor=1 case, we try and pick 2 if
        // possible)
        let min_total_nodes: u32 = std::cmp::max(
            replication_property.num_copies() as u32 + 1,
            min_total_nodes,
        );

        let mut domains: HashMap<DomainPath, Domain> =
            HashMap::with_capacity(min_total_nodes as usize);
        // populating candidate nodes. We start by writeable nodes.
        for (node_id, node_config) in nodes_config.iter() {
            if !is_candidate(node_id, node_config) {
                continue;
            }
            let domain_path = if let LocationScope::Root = replicate_across_scope {
                // flat replication, don't spend the energy
                DomainPath::Root
            } else {
                // [validation]
                // Being defensive to simplify the logic. If locality is required, all
                // nodes should have a location string and they need to be defined at the widest
                // replication requested in the replication factor (if replication factor asks for
                // zone-level replication, then the zone of all nodes must be known.
                if node_config.location.is_empty() {
                    return Err(NodeSelectorError::NodeLocationMissing {
                        node_id,
                        replication: replication_property.clone(),
                    });
                }
                if !node_config
                    .location
                    .is_scope_defined(replicate_across_scope)
                {
                    return Err(NodeSelectorError::IncompleteNodeLocation {
                        node_id,
                        location: node_config.location.clone(),
                        missing_scope: replicate_across_scope,
                        replication: replication_property.clone(),
                    });
                }
                DomainPath::Specific(
                    node_config
                        .location
                        .domain_string(replicate_across_scope, None),
                )
            };

            let selection_priority = if options.top_priority_node.is_some_and(|p| node_id == p) {
                CandidatePriority::Top
            } else if options.preferred_nodes.is_some_and(|p| p.contains(node_id)) {
                CandidatePriority::Preferred
            } else {
                CandidatePriority::Normal
            };
            let candidate = CandidateNode {
                node_id,
                hash: options.hash_node_id(node_id),
                selection_priority,
                is_writeable: is_writeable(node_id, node_config),
            };
            let domain = domains
                .entry(domain_path)
                .or_insert_with_key(|path| Domain::new(options.hash_domain(path)));
            domain.candidates.push(candidate);
            if let CandidatePriority::Top = selection_priority {
                domain.is_top_priority = true;
            }
        }
        // Now that we have all candidate domains. Let's sort nodes in every domain by hash value
        // to provide a consistent order. Additionally, we order those domains consistently (if
        // consistent_hashing is set in [`NodeSetSelectorOptions`]
        for domain in domains.values_mut() {
            // note that preferred candidates (and top-priority node) will sit at the back end of
            // this vector.
            domain.candidates.sort();
        }

        let mut output_nodeset = NodeSet::with_capacity(min_total_nodes as usize);
        // Now that we have all potential candidates, we repeatedly pop entries from the domain
        // with the smallest number of picked nodes. If there is a tie, we choose at random
        // (consistently). This means our nodeset is distributed as evenly and we stop after
        // accumulating enough "good" nodes.
        let mut choose_domains = |writeable_only: bool| {
            let mut queue = BinaryHeap::with_capacity(min_nodes_per_domain as usize);
            // total number of "good" nodes picked so far in this phase (writeable or from all)
            let mut picked_so_far = 0;
            for domain in domains.values_mut() {
                picked_so_far += domain.count_picked(writeable_only);
                if !domain.candidates.is_empty() {
                    queue.push(OrdDomain::new(writeable_only, domain));
                }
            }

            // Pick the nodes from the domain with the smallest number picked first
            while let Some(domain) = queue.pop() {
                let picked_in_domain = domain.inner.count_picked(writeable_only);
                if picked_in_domain >= min_nodes_per_domain && picked_so_far >= min_total_nodes {
                    // The nodeset is big enough and has enough "good" nodes from each domain.
                    break;
                }
                if output_nodeset.len() >= MAX_NODESET_SIZE as usize {
                    debug!(
                        "Hit the maximum allowed nodeset size {} when selecting nodeset for id '{}', will truncate.",
                        MAX_NODESET_SIZE, options.hashing_id
                    );
                    break;
                }

                if !writeable_only && output_nodeset.len() >= MAX_NODESET_SIZE as usize / 2 {
                    // We are still selecting from all nodes but we are getting dangerously close to
                    // the maximum allowed nodeset size. Let's skip to second stage to make sure we
                    // pick enough writeable nodes.
                    break;
                }
                // this happens only after we choose from all nodes, so it's safe to drop the rest
                // of the candidates in the domain
                if writeable_only {
                    // skip unwriteable nodes
                    domain.inner.candidates.retain(|c| c.is_writeable);
                    // the domain has nothing left for us?
                    if domain.inner.candidates.is_empty() {
                        // move to the next domain
                        continue;
                    }
                }

                domain.inner.num_total_picked += 1;
                // we are confident that we have at least a node, otherwise we wouldn't have landed
                // here.
                let node = domain.inner.candidates.pop().unwrap();
                if let CandidatePriority::Top = node.selection_priority {
                    // we only have one node as top priority, therefore the domain is no longer a
                    // top-priority.
                    domain.inner.is_top_priority = false;
                }
                if node.is_writeable {
                    domain.inner.num_writeable_nodes_picked += 1;
                }
                output_nodeset.insert(node.node_id);
                picked_so_far += 1;
                // the domain is not exhausted yet, put it back to the min-heap so it gets its new
                // position according to its exhaustion levels
                if !domain.inner.is_empty() {
                    queue.push(OrdDomain::new(writeable_only, domain.inner));
                }
            }
        };

        choose_domains(false);
        choose_domains(true);
        if output_nodeset.len() < replication_property.num_copies().into() {
            return Err(NodeSelectorError::InsufficientNodes {
                nodeset_len: output_nodeset.len(),
                replication_factor: replication_property.num_copies(),
            });
        }
        Ok(output_nodeset)
    }
}

#[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
enum CandidatePriority {
    /// Only one candidate can be top-priority.
    Top,
    /// Node is preferred in its domain but that doesn't mean that the parent domain in itself is preferred.
    Preferred,
    #[default]
    Normal,
}

#[derive(Eq, Debug)]
struct CandidateNode {
    node_id: PlainNodeId,
    selection_priority: CandidatePriority,
    hash: u64,
    is_writeable: bool,
}

impl PartialEq for CandidateNode {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl PartialOrd for CandidateNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Candidates are ordered by their hash where preferred nodes rank the highest in this comparison.
impl Ord for CandidateNode {
    fn cmp(&self, other: &Self) -> Ordering {
        use CandidatePriority::*;
        match (self.selection_priority, other.selection_priority) {
            (Normal, Normal) | (Preferred, Preferred) => self.hash.cmp(&other.hash),
            (Preferred, Normal) => Ordering::Greater,
            (Normal, Preferred) => Ordering::Less,
            (Top, _) => Ordering::Greater,
            (_, Top) => Ordering::Less,
        }
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
enum DomainPath {
    /// The root domain, no locality affinity
    Root,
    /// A specific domain path
    Specific(String),
}

#[derive(Default, Debug)]
struct Domain {
    /// the list of nodes sorted by hash, we pick nodes by popping the last item for O(1) removals
    candidates: Vec<CandidateNode>,
    /// a hash value that is used to order domains consistently
    priority: u64,
    is_top_priority: bool,
    /// how many nodes did we already pick from this domain
    num_writeable_nodes_picked: u32,
    num_total_picked: u32,
}

impl Domain {
    fn new(priority: u64) -> Self {
        Self {
            priority,
            ..Default::default()
        }
    }

    fn count_picked(&self, writeable: bool) -> u32 {
        if writeable {
            self.num_writeable_nodes_picked
        } else {
            self.num_total_picked
        }
    }

    fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }
}

struct OrdDomain<'a> {
    ord_by_writeable: bool,
    inner: &'a mut Domain,
}

impl<'a> OrdDomain<'a> {
    fn new(ord_by_writeable: bool, domain: &'a mut Domain) -> Self {
        Self {
            ord_by_writeable,
            inner: domain,
        }
    }

    fn priority(&self) -> u64 {
        self.inner.priority
    }

    fn num_picked(&self) -> u32 {
        if self.ord_by_writeable {
            self.inner.num_writeable_nodes_picked
        } else {
            self.inner.num_total_picked
        }
    }
}

impl PartialEq for OrdDomain<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.inner.is_top_priority != other.inner.is_top_priority {
            return false;
        }
        if self.ord_by_writeable {
            self.inner.num_writeable_nodes_picked == other.inner.num_writeable_nodes_picked
        } else {
            self.inner.priority == other.inner.priority
        }
    }
}

impl Eq for OrdDomain<'_> {}

impl PartialOrd for OrdDomain<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Note that this orders domains in reverse order for min-heap uses
impl Ord for OrdDomain<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Note that this is reverse ordering only for priority and num_writeable_nodes_picked,
        // but a top-priority domain (a domain with the top-priority node) goes to the top of
        // the heap.
        match (self.inner.is_top_priority, other.inner.is_top_priority) {
            (true, _) => Ordering::Greater,
            (_, true) => Ordering::Less,
            _ => {
                // Reverse ordering here, so domain with least amount of nodes picked sits
                // at the top of the min-heap. If there is a tie, we order by the normal hash priority.
                // the number of picked depends on whether we are are ordering by writeable nodes
                // or not.
                (other.num_picked(), other.priority()).cmp(&(self.num_picked(), self.priority()))
            }
        }
    }
}

#[cfg(test)]
pub mod tests {

    use crate::locality::LocationScope;
    use crate::nodes_config::{
        LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
    };
    use crate::replication::{NodeSet, ReplicationProperty};
    use crate::{GenerationalNodeId, PlainNodeId};

    use super::*;

    pub fn logserver_candidate_filter(_node_id: PlainNodeId, config: &NodeConfig) -> bool {
        matches!(
            config.log_server_config.storage_state,
            StorageState::Provisioning | StorageState::ReadWrite
        )
    }

    pub fn logserver_writeable_node_filter(_node_id: PlainNodeId, config: &NodeConfig) -> bool {
        matches!(
            config.log_server_config.storage_state,
            StorageState::ReadWrite
        )
    }

    pub fn worker_candidate_filter(_node_id: PlainNodeId, config: &NodeConfig) -> bool {
        config.has_role(Role::Worker)
    }

    /// Generate a test address that works on the current platform
    fn test_address(id: PlainNodeId) -> String {
        #[cfg(unix)]
        {
            format!("unix:/tmp/my_socket-{id}")
        }
        #[cfg(windows)]
        {
            // Use HTTP addresses on Windows since UDS is not supported
            format!("http://127.0.0.1:{}", 10000 + u32::from(id))
        }
    }

    fn generate_node(
        id: impl Into<PlainNodeId>,
        storage_state: StorageState,
        role: Role,
        location: &str,
    ) -> NodeConfig {
        let id: PlainNodeId = id.into();
        NodeConfig::builder()
            .name(format!("node-{id}"))
            .current_generation(GenerationalNodeId::new(id.into(), 1))
            .location(location.parse().unwrap())
            .address(test_address(id).parse().unwrap())
            .roles(role.into())
            .log_server_config(LogServerConfig { storage_state })
            .build()
    }

    #[test]
    fn select_log_servers_insufficient_capacity() {
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(generate_node(0, StorageState::Disabled, Role::Admin, ""));
        nodes_config.upsert_node(generate_node(
            1,
            StorageState::Disabled,
            Role::LogServer,
            "",
        ));
        nodes_config.upsert_node(generate_node(
            2,
            StorageState::Provisioning,
            Role::LogServer,
            "",
        ));

        let nodeset = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            Default::default(),
        );

        // provisioning node can be selected, but it's not enough to satisfy total number of
        // copies.
        assert_eq!(
            nodeset,
            Err(NodeSelectorError::InsufficientNodes {
                nodeset_len: 1,
                replication_factor: 2
            })
        );
    }

    /// Replicated loglets should work just fine in single-node clusters, as long as the replication factor is set to 1.
    #[test]
    fn select_log_servers_single_node_cluster() {
        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(generate_node(
            1,
            StorageState::ReadWrite,
            Role::LogServer,
            "",
        ));

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 1.try_into().unwrap());

        let preferred_nodes = NodeSet::from_single(1);
        let options = NodeSetSelectorOptions::new(5).with_preferred_nodes(&preferred_nodes);
        let selection = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            options,
        );

        assert_eq!(
            selection.unwrap(),
            NodeSet::from([1]),
            "A single-node cluster is possible with replication factor of 1"
        );
    }

    #[test]
    fn select_workers_and_alive_nodes() {
        let mut nodes_config = NodesConfiguration::default();
        for i in 1..=100 {
            nodes_config.upsert_node(generate_node(i, StorageState::Disabled, Role::Worker, ""));
        }

        // replication = 2, we'll try an have at least 2 writeable nodes in the nodeset. So, we
        // should expect 5, 8 both to be here.
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());
        // simulates that we consider 5, 8 are writeable nodes via cluster_state, 5,6 are alive, we prefer to see them in nodeset
        let cluster_state = NodeSet::from_iter([5, 8]);

        let preferred_nodes = NodeSet::from_single(1);
        let options = NodeSetSelectorOptions::new(5).with_preferred_nodes(&preferred_nodes);
        let nodeset = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            |node_id, _| cluster_state.contains(node_id),
            options.clone(),
        )
        .unwrap();

        assert!(nodeset.contains(5));
        assert!(nodeset.contains(8));
        assert!(nodeset.contains(1));
        assert_eq!(nodeset.len(), 5);

        // We can also generate bigger nodesets other than the default size
        let options = options.with_target_size(NodeSetSize::new(10).expect("to be valid"));
        let nodeset = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            |node_id, _| cluster_state.contains(node_id),
            options,
        )
        .unwrap();

        assert!(nodeset.contains(5));
        assert!(nodeset.contains(8));
        assert!(nodeset.contains(1));
        // target size is not a strict number, but we try to stay near it.
        assert_eq!(nodeset.len(), 12);
    }

    #[test]
    fn select_fd_select_nodes() {
        let mut nodes_config = NodesConfiguration::default();
        for i in 1..=12 {
            // three regions, 2 zones per region
            let region = i % 3;
            let zone = i % 2;
            nodes_config.upsert_node(generate_node(
                i,
                StorageState::ReadWrite,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }

        // zone:2. we have 3 * 2 = 6 zones in total.
        let replication: ReplicationProperty = "{zone: 2}".parse().unwrap();

        // we like node 1
        let preferred_nodes = NodeSet::from_single(1);
        let options = NodeSetSelectorOptions::new(5).with_preferred_nodes(&preferred_nodes);
        let nodeset = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            options,
        )
        .unwrap();

        assert!(nodeset.contains(1));
        assert_eq!(nodeset.len(), 6);
    }

    #[test]
    fn selection_with_preferred_previous_nodeset() {
        let mut nodes_config = NodesConfiguration::default();
        // we start by 6 nodes
        // - region-0
        //     zone-0  N0
        //     zone-1  N3
        //-  region1
        //     zone-0  N4
        //     zone-1  N1
        //-  region2
        //     zone-0  N2
        //     zone-1  N5
        for i in 0..6 {
            // three regions, 2 zones per region
            let region = i % 3;
            let zone = i % 2;
            nodes_config.upsert_node(generate_node(
                i,
                StorageState::ReadWrite,
                Role::LogServer,
                &format!("region-{region}.zone-{zone}"),
            ));
        }
        // zone:2. we have 3 * 2 = 6 zones in total.
        let replication: ReplicationProperty = "{zone: 2}".parse().unwrap();

        // we like node 1 but it's not top priority
        let preferred_nodes = NodeSet::from_single(1);
        let log_id = 5;
        let options = NodeSetSelectorOptions::new(log_id).with_preferred_nodes(&preferred_nodes);
        let nodeset1 = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            options,
        )
        .unwrap();

        assert_eq!(nodeset1.len(), 6);

        // let's add 4 more nodes to this cluster without touching existing nodes
        for i in 6..=10 {
            // three regions, 2 zones per region
            let region = i % 3;
            let zone = i % 2;
            nodes_config.upsert_node(generate_node(
                i,
                StorageState::ReadWrite,
                Role::LogServer,
                &format!("region-{region}.zone-{zone}"),
            ));
        }

        // the new nodeset have the entirety of the old nodeset as preferred
        let options = NodeSetSelectorOptions::new(log_id).with_preferred_nodes(&nodeset1);
        let nodeset2 = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            options,
        )
        .unwrap();

        // nodeset2 is identical to nodeset1 because we have no reason to skip any of the
        // previously chosen (and preferred) nodes.
        assert_eq!(nodeset2.len(), 6);
        assert_eq!(nodeset2, nodeset1);

        // Now if we regenerate but assuming that enough nodes became unwriteable in nodeset1.
        // 4 nodes updated. 0 became Disabled (not candidates), and the rest became read-only
        for i in 0..5 {
            // three regions, 2 zones per region
            let region = i % 3;
            let zone = i % 2;
            let state = if i == 0 {
                StorageState::Disabled
            } else {
                // read-only can be considered a signal to avoid picking this node in nodesets, it
                // essentially means that we are draining this node.
                StorageState::ReadOnly
            };
            nodes_config.upsert_node(generate_node(
                i,
                state,
                Role::LogServer,
                &format!("region-{region}.zone-{zone}"),
            ));
        }

        // Now, regenerating...
        let options = NodeSetSelectorOptions::new(log_id).with_preferred_nodes(&nodeset1);
        let nodeset2 = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            options,
        )
        .unwrap();

        assert_eq!(nodeset2.len(), 6);
        // N5 is still there since it's the last one remaining that was not disabled or became
        // unwriteable
        assert!(nodeset2.contains(5));
    }

    #[test]
    fn selection_with_preferred_and_top_priority() {
        // In this test we want to validate that the nodeset selector will choose the domain where
        // the top priority node lives even if the total number of domains significantly exceeds the
        // total number of nodes/domains that we need. We don't want the top-priority node to be
        // lost in the ocean of nodes.
        let mut nodes_config = NodesConfiguration::default();
        // 250 nodes across 9 regions and 5 zones each = 45 zones in total
        for i in 0..250 {
            let region = i % 9;
            let zone = i % 5;
            nodes_config.upsert_node(generate_node(
                i,
                StorageState::ReadWrite,
                Role::LogServer,
                &format!("region-{region}.zone-{zone}"),
            ));
        }
        // zone:2. we have 9 * 5 = 45 zones in total.
        let replication: ReplicationProperty = "{zone: 2}".parse().unwrap();

        // we like node 16 so we'd like to see it in output but it's not as important as the
        // top-priority node (e.g. the sequencer node)
        let preferred_nodes = NodeSet::from_single(16);
        let log_id = 5;
        let options = NodeSetSelectorOptions::new(log_id)
            .with_preferred_nodes(&preferred_nodes)
            // node 98 is the top priority, unless it's disabled (it's not), we
            // should see it in output.
            .with_top_priority_node(98);
        let nodeset1 = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            options,
        )
        .unwrap();

        assert!(nodeset1.contains(98));
        assert!(nodeset1.contains(16));
        assert_eq!(nodeset1.len(), 45);

        // the new nodeset have the entirety of the old nodeset as preferred
        let options = NodeSetSelectorOptions::new(log_id)
            .with_preferred_nodes(&nodeset1)
            // we like the old nodeset, but 12 really needs to be here.
            .with_top_priority_node(12);
        let nodeset2 = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            logserver_candidate_filter,
            logserver_writeable_node_filter,
            options,
        )
        .unwrap();

        assert!(nodeset2.contains(12));
        // it's the first node
        assert_eq!(nodeset2.get(0), Some(PlainNodeId::from(12)));
        assert_eq!(nodeset2.len(), 45);
        // the only difference is the first node N12, in fact the set of the nodeset should _near_
        // identical order of nodes.
        let delta: NodeSet = nodeset2.difference(&nodeset1).collect();
        assert_eq!(delta, NodeSet::from_single(12));
    }

    #[test]
    fn selection_shuffling() {
        let mut nodes_config = NodesConfiguration::default();
        let mut nodesets = Vec::with_capacity(24);
        // 12 nodes across 3 regions and 2 zones each = 6 zones in total
        for i in 0..12 {
            let region = i % 3;
            let zone = i % 2;
            nodes_config.upsert_node(generate_node(
                i,
                StorageState::Disabled,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }
        // zone:2. we have 3 * 2 = 6 zones in total.
        let replication: ReplicationProperty = "{zone: 2}".parse().unwrap();

        for partition_id in 0..24 {
            let options = NodeSetSelectorOptions::new(partition_id)
                // try to go as big as you can
                .with_target_size(NodeSetSize::MAX);
            let nodeset = DomainAwareNodeSetSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                |_node_id, _config| true, // all nodes are writeable/alive
                options,
            )
            .unwrap();

            assert_eq!(nodeset.len(), 12);
            nodesets.push(nodeset);
        }
        let mut leaders = NodeSet::new();
        for nodeset in nodesets {
            // First node in every nodeset is a sample of what a natural leader can be for partition
            // processors if it's alive.
            leaders.insert(nodeset.get(0).unwrap());
        }

        // Since this is consistent generation, we know that this run generates 11 unique leaders. A single
        // node in the cluster won't be a leader for any partition and that's a reasonable outcome.
        // albeit, balance of leaders is not guaranteed. This value might change if the generation
        // algorithm changes or if the seed value is updated.
        assert_eq!(leaders.len(), 11);
    }

    #[test]
    fn selection_max_nodeset_size() {
        let mut nodes_config = NodesConfiguration::default();
        for i in 0..1024 {
            let region = i % 3;
            let zone = i % 2;
            nodes_config.upsert_node(generate_node(
                i,
                StorageState::Disabled,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }
        let replication: ReplicationProperty = "{zone: 2}".parse().unwrap();

        let options = NodeSetSelectorOptions::new(12).with_target_size(NodeSetSize::MAX);
        let nodeset = DomainAwareNodeSetSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            |_node_id, _config| true, // all nodes are writeable/alive
            options,
        )
        .unwrap();

        // Nodeset is truncated
        assert_eq!(nodeset.len(), MAX_NODESET_SIZE as usize);
    }
}
