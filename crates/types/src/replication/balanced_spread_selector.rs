// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::Hasher;

use ahash::{HashSet, HashSetExt};
use xxhash_rust::xxh3::Xxh3;

use crate::PlainNodeId;
use crate::locality::topology::{Node, RegionKey, Topology, ZoneKey};
use crate::locality::{LocationScope, NodeLocation};
use crate::nodes_config::{NodeConfig, NodesConfiguration};
use crate::replication::ReplicationProperty;

use super::NodeSet;

const HASH_SALT: u64 = 14712721395741015273;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "not enough candidate nodes to satisfy the replication factor at scope={scope}; available={available} but replication needs {replication_factor}"
    )]
    InsufficientNodes {
        scope: LocationScope,
        available: usize,
        replication_factor: u8,
    },
}

#[derive(Clone)]
pub struct SelectorOptions {
    /// Hash seed that differentiates selections for different partitions / logs.
    pub hashing_id: u64,
    /// Salt/seed for RNG/hash functions used by the selector
    salt: u64,
    /// enabled by default, disable if you don't want repeatable generation
    consistent_hashing: bool,
    preferred_nodes: NodeSet,
}

impl Default for SelectorOptions {
    fn default() -> Self {
        Self {
            hashing_id: 0,
            salt: HASH_SALT,
            consistent_hashing: true,
            preferred_nodes: NodeSet::new(),
        }
    }
}

impl SelectorOptions {
    /// id can be log-id, loglet-id, partition-id, or anything you like. This value seeds the
    /// consistent hashing of domains and nodes during selection.
    pub fn new(id: u64) -> Self {
        Self {
            hashing_id: id,
            ..Default::default()
        }
    }

    pub fn id(&self) -> u64 {
        self.hashing_id
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

    pub fn with_preferred_nodes(mut self, preferred_nodes: NodeSet) -> Self {
        self.preferred_nodes = preferred_nodes;
        self
    }

    pub fn preferred_nodes(&self) -> &NodeSet {
        &self.preferred_nodes
    }

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

    fn hash_domain(&self, domain_hash: u64) -> u64 {
        if self.consistent_hashing {
            let mut hasher = Xxh3::with_seed(self.salt);
            hasher.write_u64(self.hashing_id);
            hasher.write_u64(domain_hash);
            hasher.finish()
        } else {
            rand::random()
        }
    }
}

pub struct BalancedSpreadSelector;

impl BalancedSpreadSelector {
    /// Returns exactly `replication_property.num_copies()` unique node IDs that respect the
    /// location scopes as defined in replication property.
    pub fn select(
        nodes_config: &NodesConfiguration,
        replication_property: &ReplicationProperty,
        is_candidate: impl Fn(PlainNodeId, &NodeConfig) -> bool,
        options: &SelectorOptions,
    ) -> Result<NodeSet, Error> {
        // -------------------------------- 0. Pre‑compute helper masks -----------------------------
        let needs_region = replication_property
            .copies_at_scope(LocationScope::Region)
            .is_some();
        let needs_zone = replication_property
            .copies_at_scope(LocationScope::Zone)
            .is_some();

        // A candidate must have all scopes that appear with a non‑zero factor.
        let required_scopes = move |loc: &NodeLocation| {
            (!needs_region || loc.is_scope_defined(LocationScope::Region))
                && (!needs_zone || loc.is_scope_defined(LocationScope::Zone))
        };

        // Depending on the shape of replication factor, we might need to exclude nodes that are
        // not defined in certain scopes in addition to the candidate filter closure that was
        // supplied by the caller. This might change in the future if topology became shared across
        // invocations.
        let filter_nodes = move |id: PlainNodeId, node_config: &NodeConfig| {
            is_candidate(id, node_config) && required_scopes(&node_config.location)
        };

        // For now, the topology filters out all nodes, regions, and zones that are not candidates for this
        // selection round. This might change in the future.
        let topology = Topology::from_nodes_configuration(nodes_config, filter_nodes);

        select_internal(&topology, replication_property, options)
    }
}

fn select_internal(
    topology: &Topology,
    replication_property: &ReplicationProperty,
    opts: &SelectorOptions,
) -> Result<NodeSet, Error> {
    let replication_factors = replication_property.distinct_replication_factors();
    debug_assert!(!replication_factors.is_empty());

    // Feasibility quick‑check
    for (scope, factor) in replication_factors.iter().copied() {
        let available = topology.count_domains_in_scope(scope);
        if available < factor as usize {
            return Err(Error::InsufficientNodes {
                scope,
                available,
                replication_factor: factor,
            });
        }
    }

    // # Phase 1 – Build domain buckets (respecting mandatory scopes)
    let replicate_across_scope = replication_factors.first().unwrap().0;
    let buckets = build_buckets(topology, replication_property, opts, replicate_across_scope);

    if buckets.is_empty() {
        return Err(Error::InsufficientNodes {
            scope: LocationScope::Node,
            available: 0,
            replication_factor: replication_property.num_copies(),
        });
    }

    let mut heap: BinaryHeap<DomainBucket> = BinaryHeap::from(buckets);
    let mut selected: SelectionState = SelectionState::new(replication_property);

    // Mandatory coverage – region first, then zone ----------------------
    if let Some(required_regions) = replication_property.copies_at_scope(LocationScope::Region) {
        populate_scope(
            LocationScope::Region,
            required_regions,
            &mut heap,
            &mut selected,
        )?;
    }
    if let Some(required_zones) = replication_property.copies_at_scope(LocationScope::Zone) {
        populate_scope(
            LocationScope::Zone,
            required_zones,
            &mut heap,
            &mut selected,
        )?;
    }

    // # Phase 2 – Balanced completion with viability guard
    let total_num_copies = replication_property.num_copies();
    while selected.len() < total_num_copies as usize {
        let mut bucket = heap.pop().ok_or(Error::InsufficientNodes {
            scope: LocationScope::Node,
            available: selected.len(),
            replication_factor: total_num_copies,
        })?;
        if let Some(candidate) = bucket.pop_candidate() {
            if is_candidate_viable(&selected, &candidate.node, replication_property) {
                selected.insert(candidate.node);
            } else {
                // Candidate would dead‑end us – skipping it.
            }
        }
        if !bucket.is_empty() {
            heap.push(bucket);
        }
    }

    Ok(selected.nodes)
}

/// Tracks the domains already represented in the output nodeset.
#[derive(Debug)]
struct SelectionState {
    nodes: NodeSet,
    regions: HashSet<RegionKey>, // indexes regions inside Topology::regions
    zones: HashSet<(RegionKey, ZoneKey)>, // indexes inside Topology
}

impl SelectionState {
    fn new(replication_property: &ReplicationProperty) -> Self {
        Self {
            nodes: NodeSet::with_capacity(replication_property.num_copies() as usize),
            regions: replication_property
                .copies_at_scope(LocationScope::Region)
                .map(|r| HashSet::with_capacity(r as usize))
                .unwrap_or_default(),
            zones: replication_property
                .copies_at_scope(LocationScope::Zone)
                .map(|z| HashSet::with_capacity(z as usize))
                .unwrap_or_default(),
        }
    }

    fn insert(&mut self, node: Node) {
        self.nodes.insert(node.id);
        if let Some(r) = node.region_idx {
            self.regions.insert(r);
        }
        if let (Some(r), Some(z)) = (node.region_idx, node.zone_idx) {
            self.zones.insert((r, z));
        }
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn num_distinct_regions(&self) -> usize {
        self.regions.len()
    }
    fn num_distinct_zones(&self) -> usize {
        self.zones.len()
    }

    fn has_region(&self, r: RegionKey) -> bool {
        self.regions.contains(&r)
    }
    fn has_zone(&self, rz: (RegionKey, ZoneKey)) -> bool {
        self.zones.contains(&rz)
    }
}

#[derive(Debug)]
struct CandidateNode {
    node: Node,
    hash: u64,
    preferred: bool,
}

impl Ord for CandidateNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.preferred, other.preferred) {
            (false, false) | (true, true) => self.hash.cmp(&other.hash),
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
        }
    }
}

impl PartialOrd for CandidateNode {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

impl Eq for CandidateNode {}

impl PartialEq for CandidateNode {
    fn eq(&self, o: &Self) -> bool {
        self.node.id == o.node.id
    }
}

#[derive(Debug)]
struct DomainBucket {
    /// Number of replicas already selected from this domain.
    picked: u8,
    /// Tie‑breaker for heap ordering (lower hash wins once `picked` equal).
    prio: Reverse<u64>,
    /// Candidates ordered by *descending* node‑hash.
    candidates: Vec<CandidateNode>,
}

impl DomainBucket {
    fn pop_candidate(&mut self) -> Option<CandidateNode> {
        let candidate = self.candidates.pop()?;
        self.picked += 1;
        Some(candidate)
    }

    fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    // Places the candidate node back in the set of candidates by without changing the number of
    // picked nodes in this bucket.
    //
    // We don't decrement picked because it'd cause this bucket to remain in its place
    // in the heap during traversal. This will lead to infinite loop when populating the scope.
    //
    // NOTE: this is destined to break if we decided to traverse the buckets with bucket priority
    // in mind.
    fn place_back(&mut self, candidate: CandidateNode) {
        // self.picked = self.picked.saturating_sub(1);
        self.candidates.push(candidate);
    }
}

impl Ord for DomainBucket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.picked, &self.prio)
            .cmp(&(other.picked, &other.prio))
            .reverse()
    }
}
impl PartialOrd for DomainBucket {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Eq for DomainBucket {}
impl PartialEq for DomainBucket {
    fn eq(&self, o: &Self) -> bool {
        self.picked == o.picked && self.prio == o.prio
    }
}

fn build_buckets(
    topology: &Topology,
    replication_property: &ReplicationProperty,
    opts: &SelectorOptions,
    replicate_across_scope: LocationScope,
) -> Vec<DomainBucket> {
    let mut buckets = Vec::new();

    match replicate_across_scope {
        // a single global bucket when doing flat replication
        LocationScope::Root | LocationScope::Node => {
            let mut bucket = DomainBucket {
                picked: 0,
                prio: Reverse(0),
                candidates: Vec::new(),
            };
            for node in topology.iter_all_nodes() {
                push_if_candidate(node, replication_property, &mut bucket, opts);
            }

            if !bucket.candidates.is_empty() {
                bucket.candidates.sort();
                buckets.push(bucket);
            }
        }
        LocationScope::Region => {
            for region in topology.regions() {
                let mut bucket = DomainBucket {
                    picked: 0,
                    prio: Reverse(opts.hash_domain(region.hash())),
                    candidates: Vec::new(),
                };
                for n in region.iter_nodes() {
                    push_if_candidate(n, replication_property, &mut bucket, opts);
                }
                if !bucket.candidates.is_empty() {
                    bucket.candidates.sort();
                    buckets.push(bucket);
                }
            }
        }
        LocationScope::Zone => {
            for region in topology.regions() {
                for zone in region.zones() {
                    // bucket per zone
                    let mut bucket = DomainBucket {
                        picked: 0,
                        prio: Reverse(opts.hash_domain(zone.hash())),
                        candidates: Vec::new(),
                    };
                    for n in zone.iter_nodes() {
                        push_if_candidate(n, replication_property, &mut bucket, opts);
                    }
                    if !bucket.candidates.is_empty() {
                        bucket.candidates.sort();
                        buckets.push(bucket);
                    }
                }
            }
        }
    }

    buckets
}

fn push_if_candidate(
    node: &Node,
    replication_property: &ReplicationProperty,
    bucket: &mut DomainBucket,
    opts: &SelectorOptions,
) {
    // Must carry every scope required by the property.
    if replication_property
        .copies_at_scope(LocationScope::Zone)
        .is_some()
        && node.zone_idx.is_none()
    {
        return;
    }
    if replication_property
        .copies_at_scope(LocationScope::Region)
        .is_some()
        && node.region_idx.is_none()
    {
        return;
    }
    let node_id = node.id;
    let hash = opts.hash_node_id(node_id);

    let candidate_node = CandidateNode {
        node: node.clone(),
        hash,
        preferred: opts.preferred_nodes.contains(node_id),
    };

    bucket.candidates.push(candidate_node);
}

// Mandatory scope coverage
fn populate_scope(
    scope: LocationScope,
    replication_factor: u8,
    heap: &mut BinaryHeap<DomainBucket>,
    selected: &mut SelectionState,
) -> Result<(), Error> {
    while (match scope {
        LocationScope::Region => selected.num_distinct_regions(),
        LocationScope::Zone => selected.num_distinct_zones(),
        _ => 0,
    }) < replication_factor as usize
    {
        // error shouldn't happen as we are guarding externally
        let mut bucket = heap.pop().ok_or(Error::InsufficientNodes {
            scope,
            available: 0,
            replication_factor,
        })?;
        let cand = bucket.pop_candidate().ok_or(Error::InsufficientNodes {
            scope,
            available: 0,
            replication_factor,
        })?;
        let need_candidate = match scope {
            LocationScope::Zone => match (cand.node.region_idx, cand.node.zone_idx) {
                (Some(region_idx), Some(zone_idx)) => !selected.has_zone((region_idx, zone_idx)),
                _ => false,
            },
            LocationScope::Region => cand
                .node
                .region_idx
                .map(|region_idx| !selected.has_region(region_idx))
                .unwrap_or(false),
            _ => false,
        };

        if need_candidate {
            selected.insert(cand.node);
        } else {
            bucket.place_back(cand);
            /* discarded - potentially wrong */
        }
        if !bucket.is_empty() {
            heap.push(bucket);
        }
    }
    Ok(())
}

// Viability guard
fn is_candidate_viable(
    selection: &SelectionState,
    candidate: &Node,
    replication: &ReplicationProperty,
) -> bool {
    debug_assert!(replication.num_copies() as usize > selection.len());
    let remain = replication.num_copies() - selection.len() as u8 - 1; // after taking candidate

    let need_region = replication
        .copies_at_scope(LocationScope::Region)
        .map(|r| {
            let adds = candidate
                .region_idx
                .map(|region_idx| !selection.has_region(region_idx))
                .unwrap_or(false);
            let have = selection.num_distinct_regions() + adds as usize;
            (r as isize - have as isize).max(0) as u8
        })
        .unwrap_or(0);

    let need_zone = replication
        .copies_at_scope(LocationScope::Zone)
        .map(|r| {
            let adds = match (candidate.region_idx, candidate.zone_idx) {
                (Some(region_idx), Some(zone_idx)) => !selection.has_zone((region_idx, zone_idx)),
                _ => false,
            };
            let have = selection.num_distinct_zones() + adds as usize;
            (r as isize - have as isize).max(0) as u8
        })
        .unwrap_or(0);

    need_region + need_zone <= remain
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;

    use super::*;
    use crate::locality::LocationScope;
    use crate::nodes_config::{NodeConfig, NodesConfiguration, Role, WorkerConfig, WorkerState};
    use crate::partitions::worker_candidate_filter;
    use crate::replication::{NodeSet, ReplicationProperty};
    use crate::{GenerationalNodeId, PlainNodeId};

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
        worker_state: WorkerState,
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
            .worker_config(WorkerConfig { worker_state })
            .build()
    }

    #[test]
    fn select_flat_insufficient_capacity() {
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(generate_node(0, WorkerState::Disabled, Role::Worker, ""));
        nodes_config.upsert_node(generate_node(1, WorkerState::Disabled, Role::LogServer, ""));
        nodes_config.upsert_node(generate_node(
            2,
            WorkerState::Provisioning,
            Role::Worker,
            "",
        ));

        let nodeset = BalancedSpreadSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            &Default::default(),
        );

        // provisioning nodes are filtered out
        assert_that!(
            nodeset,
            err(pat!(Error::InsufficientNodes {
                scope: eq(LocationScope::Node),
                available: eq(0),
                replication_factor: eq(2),
            }))
        );
    }

    #[test]
    fn select_workers_single_node_cluster() {
        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(generate_node(1, WorkerState::Active, Role::Worker, ""));

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 1.try_into().unwrap());

        let preferred_nodes = NodeSet::from_single(1);
        let options = SelectorOptions::new(5).with_preferred_nodes(preferred_nodes);
        let selection = BalancedSpreadSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            &options,
        );
        let topology = Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);

        assert_that!(selection, ok(eq(NodeSet::from([1]))));
        assert_that!(
            topology.check_replication_property(&selection.unwrap(), &replication),
            eq(true)
        );
    }

    #[test]
    fn select_workers_no_location() {
        let mut nodes_config = NodesConfiguration::default();
        for i in 1..=24 {
            nodes_config.upsert_node(generate_node(i, WorkerState::Active, Role::Worker, ""));
        }

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 5.try_into().unwrap());

        // generate for 5 partitions, all prefer N5
        let mut leaders = HashSet::new();
        for id in 1..=5 {
            let options = SelectorOptions::new(id).with_preferred_nodes(NodeSet::from_iter([5]));
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            )
            .unwrap();

            assert_that!(nodeset.len(), eq(5));
            let topology =
                Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
            assert_that!(
                topology.check_replication_property(&nodeset, &replication),
                eq(true)
            );
            assert!(nodeset.contains(5));
            leaders.insert(nodeset.first().unwrap());
        }
        // check how many leaders, 5 is always leader!
        assert_that!(leaders.len(), eq(1));

        // try again without preferred nodes
        let mut leaders = HashSet::new();
        for id in 1..=5 {
            let options = SelectorOptions::new(id);
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            )
            .unwrap();

            assert_that!(nodeset.len(), eq(5));
            let topology =
                Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
            assert_that!(
                topology.check_replication_property(&nodeset, &replication),
                eq(true)
            );
            leaders.insert(nodeset.first().unwrap());
        }
        // check how many leaders, we should see diversity
        assert_that!(leaders.len(), eq(5));
    }

    #[test]
    fn select_workers_flat_with_location() {
        let mut nodes_config = NodesConfiguration::default();
        for i in 1..=24 {
            // three regions, 2 zones per region
            let region = i % 3;
            let zone = i % 2;
            // N1 has no location
            if i == 1 {
                nodes_config.upsert_node(generate_node(i, WorkerState::Active, Role::Worker, ""));
            }
            // N6, 8, 10 are not assigned a zone.
            if i == 6 || i == 8 || i == 10 {
                nodes_config.upsert_node(generate_node(
                    i,
                    WorkerState::Active,
                    Role::Worker,
                    &format!("region-{region}"),
                ));
            } else {
                nodes_config.upsert_node(generate_node(
                    i,
                    WorkerState::Active,
                    Role::Worker,
                    &format!("region-{region}.zone-{zone}"),
                ));
            }
        }

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 5.try_into().unwrap());

        let mut combined = HashSet::new();
        // generate for 10 partitions
        let mut leaders = HashSet::new();
        for id in 1..=10 {
            let options = SelectorOptions::new(id);
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            )
            .unwrap();

            assert_that!(nodeset.len(), eq(5));
            let topology =
                Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
            assert_that!(
                topology.check_replication_property(&nodeset, &replication),
                eq(true)
            );
            leaders.insert(nodeset.first().unwrap());
            combined.extend(nodeset.into_iter());
        }
        // check how many leaders, we should see diversity
        assert_that!(leaders.len(), eq(9));
        assert_that!(
            combined,
            superset_of([
                PlainNodeId::new(1),
                PlainNodeId::new(6),
                PlainNodeId::new(10)
            ])
        );
    }

    #[test]
    fn select_nodes_across_regions() {
        let mut nodes_config = NodesConfiguration::default();
        // region-1
        //         .zone-0 [N4, N10]
        //         .zone-1 [N7, N1]
        // region-0
        //         .zone-0 [N6, N12]
        //         .zone-1 [N3, N9]
        // region-2
        //         .zone-0 [N2, N8]
        //         .zone-1 [N5, N11]
        //
        for i in 1..=12 {
            // three regions, 2 zones per region
            let region = i % 3;
            let zone = i % 2;
            nodes_config.upsert_node(generate_node(
                i,
                WorkerState::Active,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }

        // we have only three regions in total
        let replication: ReplicationProperty = "{region: 3}".parse().unwrap();

        for id in 1..=24 {
            let options = SelectorOptions::new(id);
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            )
            .unwrap();

            assert_that!(nodeset.len(), eq(3));
            let topology =
                Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
            assert_that!(
                topology.check_replication_property(&nodeset, &replication),
                eq(true)
            );
        }
    }

    #[test]
    fn do_not_exceed_num_copies() {
        let mut nodes_config = NodesConfiguration::default();
        // region-1
        //   .zone-1  [N1]
        //   .zone-2  [N2]
        //   .zone-4  [N5]
        // region-2
        //   .zone-3 [N3, N4]
        nodes_config.upsert_node(generate_node(
            1,
            WorkerState::Active,
            Role::Worker,
            "region-1.zone-1",
        ));
        nodes_config.upsert_node(generate_node(
            2,
            WorkerState::Active,
            Role::Worker,
            "region-1.zone-2",
        ));
        nodes_config.upsert_node(generate_node(
            3,
            WorkerState::Active,
            Role::Worker,
            "region-2.zone-3",
        ));
        nodes_config.upsert_node(generate_node(
            4,
            WorkerState::Active,
            Role::Worker,
            "region-2.zone-3",
        ));
        nodes_config.upsert_node(generate_node(
            5,
            WorkerState::Active,
            Role::Worker,
            "region-1.zone-4",
        ));

        let replication: ReplicationProperty = "{region: 2, zone: 4, node: 4}".parse().unwrap();

        for id in 1..=1024 {
            let options = SelectorOptions::new(id);
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            )
            .unwrap();

            assert_that!(nodeset.len(), eq(4));
            let topology =
                Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
            assert_that!(
                topology.check_replication_property(&nodeset, &replication),
                eq(true)
            );
        }
    }

    #[test]
    fn maximal_hierarchical_fitment() {
        let mut nodes_config = NodesConfiguration::default();
        // region-1
        //   .zone-1  [N1]
        //   .zone-2  [N2]
        //   .zone-4  [N5]
        // region-2
        //   .zone-3 [N3, N4]
        nodes_config.upsert_node(generate_node(
            1,
            WorkerState::Active,
            Role::Worker,
            "region-1.zone-1",
        ));
        nodes_config.upsert_node(generate_node(
            2,
            WorkerState::Active,
            Role::Worker,
            "region-1.zone-2",
        ));
        nodes_config.upsert_node(generate_node(
            3,
            WorkerState::Active,
            Role::Worker,
            "region-2.zone-3",
        ));
        nodes_config.upsert_node(generate_node(
            4,
            WorkerState::Active,
            Role::Worker,
            "region-2.zone-3",
        ));
        nodes_config.upsert_node(generate_node(
            5,
            WorkerState::Active,
            Role::Worker,
            "region-1.zone-4",
        ));

        // node:5 requires that we pick every node, eventually.
        let replication: ReplicationProperty = "{region: 2, zone: 4, node: 5}".parse().unwrap();

        for id in 1..=1024 {
            let options = SelectorOptions::new(id);
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            )
            .unwrap();

            assert_that!(nodeset.len(), eq(5));
            let topology =
                Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
            assert_that!(
                topology.check_replication_property(&nodeset, &replication),
                eq(true)
            );
        }
    }

    #[test]
    fn select_nodes_insufficient_domains() {
        let mut nodes_config = NodesConfiguration::default();
        for i in 1..=12 {
            // 2 regions, 5 zones per region
            let region = i % 2;
            let zone = i % 5;
            nodes_config.upsert_node(generate_node(
                i,
                WorkerState::Active,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }

        // we have only two regions in total, we can't generate nodesets
        let replication: ReplicationProperty = "{region: 3}".parse().unwrap();

        for id in 1..=24 {
            let options = SelectorOptions::new(id);
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            );

            assert_that!(
                nodeset,
                err(pat!(Error::InsufficientNodes {
                    scope: eq(LocationScope::Region),
                    available: eq(2),
                    replication_factor: eq(3),
                }))
            );
        }
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
                WorkerState::Active,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }
        // zone: 3. we have 3 * 2 = 6 zones in total.
        let replication: ReplicationProperty = "{zone: 3}".parse().unwrap();

        // we like node 1
        let preferred_nodes = NodeSet::from_single(1);
        let partition_id = 5;
        let options = SelectorOptions::new(partition_id).with_preferred_nodes(preferred_nodes);
        let nodeset1 = BalancedSpreadSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            &options,
        )
        .unwrap();

        let topology = Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
        assert_that!(
            topology.check_replication_property(&nodeset1, &replication),
            eq(true)
        );
        assert_that!(nodeset1, eq(NodeSet::from_iter([1, 0, 3])));

        // let's add 4 more nodes to this cluster without touching existing nodes
        for i in 6..=10 {
            // three regions, 2 zones per region
            let region = i % 3;
            let zone = i % 2;
            nodes_config.upsert_node(generate_node(
                i,
                WorkerState::Active,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }

        // the new nodeset have the entirety of the old nodeset as preferred, nothing has changed
        let options = SelectorOptions::new(partition_id).with_preferred_nodes(nodeset1.clone());
        let nodeset2 = BalancedSpreadSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            &options,
        )
        .unwrap();

        // nodeset2 is identical to nodeset1 because we have no reason to skip any of the
        // previously chosen (and preferred) nodes.
        assert_that!(nodeset2, eq(nodeset1.clone()));

        // N0 because draining, we need to fixup the nodeset.
        nodes_config.upsert_node(generate_node(
            0,
            WorkerState::Draining,
            Role::Worker,
            "region-0.zone-0",
        ));

        // Now, regenerating...
        let options = SelectorOptions::new(partition_id).with_preferred_nodes(nodeset1.clone());
        let nodeset2 = BalancedSpreadSelector::select(
            &nodes_config,
            &replication,
            worker_candidate_filter,
            &options,
        )
        .unwrap();

        let topology = Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);

        assert_that!(
            topology.check_replication_property(&nodeset2, &replication),
            eq(true)
        );

        // N0 was replaced naturally by N6 after regeneration
        assert_that!(nodeset2, eq(NodeSet::from_iter([1, 6, 3])));
    }

    #[test]
    fn select_nodes_across_domain_complex() {
        let mut nodes_config = NodesConfiguration::default();
        // 64 nodes across 5 regions, 5 zones each (25 zones)
        // we'll require node=7 across 4 zones. and 3 regions.
        // { region: 3, zone: 4, node: 7}
        for i in 1..=64 {
            // three regions, 2 zones per region
            let region = i % 5;
            let zone = i % 5;
            nodes_config.upsert_node(generate_node(
                i,
                WorkerState::Active,
                Role::Worker,
                &format!("region-{region}.zone-{zone}"),
            ));
        }

        // we have only three regions in total
        let replication: ReplicationProperty = "{region: 3, zone: 4, node: 7}".parse().unwrap();

        for id in 1..=24 {
            let options = SelectorOptions::new(id);
            let nodeset = BalancedSpreadSelector::select(
                &nodes_config,
                &replication,
                worker_candidate_filter,
                &options,
            )
            .unwrap();

            assert_that!(nodeset.len(), eq(7));
            let topology =
                Topology::from_nodes_configuration(&nodes_config, worker_candidate_filter);
            assert_that!(
                topology.check_replication_property(&nodeset, &replication),
                eq(true)
            );
        }
    }
}
