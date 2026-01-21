// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hash::Hasher;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use xxhash_rust::xxh3::Xxh3;

use crate::PlainNodeId;
use crate::nodes_config::{NodeConfig, NodesConfiguration};
use crate::replication::{NodeSet, ReplicationProperty};

use crate::locality::LocationScope;

const HASH_SALT: u64 = 14712721395741015273;
type RegionLabel = smartstring::SmartString<smartstring::LazyCompact>;
type ZoneLabel = smartstring::SmartString<smartstring::LazyCompact>;

pub type RegionKey = u32;
pub type ZoneKey = u32;

#[derive(Debug)]
pub(crate) struct Topology {
    root_nodes: Vec<Node>,
    regions: Vec<Region>,
}

#[derive(Debug)]
pub struct Region {
    hash: u64,
    region_nodes: Vec<Node>,
    zones: Vec<Zone>,
}

#[derive(Debug)]
pub struct Zone {
    hash: u64,
    nodes: Vec<Node>,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub(crate) id: PlainNodeId,
    pub(crate) region_idx: Option<RegionKey>,
    pub(crate) zone_idx: Option<ZoneKey>,
}

impl Topology {
    pub fn from_nodes_configuration(
        cfg: &NodesConfiguration,
        filter: impl Fn(PlainNodeId, &NodeConfig) -> bool,
    ) -> Self {
        // Only used during construction
        #[derive(Default)]
        struct RegionTmp {
            region_nodes: Vec<Node>,
            zones: HashMap<ZoneLabel, Vec<Node>>, // zone name → nodes
        }

        let mut root_nodes = Vec::new();
        let mut region_build = HashMap::<RegionLabel, RegionTmp>::new();

        // 1.  Bucketing every node first
        for (id, node_config) in cfg.iter() {
            if !filter(id, node_config) {
                continue;
            }
            let node_entry = Node {
                id,
                // will be filled in later
                region_idx: None,
                zone_idx: None,
            };
            let location = &node_config.location;
            let region_label: Option<RegionLabel> = location
                .is_scope_defined(LocationScope::Region)
                .then_some(location.label_at(LocationScope::Region).into());
            let zone_label: Option<ZoneLabel> = location
                .is_scope_defined(LocationScope::Zone)
                .then_some(location.label_at(LocationScope::Zone).into());

            match (region_label, zone_label) {
                (None, _) => root_nodes.push(node_entry),
                (Some(r), None) => region_build
                    .entry(r)
                    .or_default()
                    .region_nodes
                    .push(node_entry),
                (Some(r), Some(z)) => region_build
                    .entry(r.clone())
                    .or_default()
                    .zones
                    .entry(z)
                    .or_default()
                    .push(node_entry),
            }
        }

        // 2. Freeze the intermediate maps into Vecs,
        let mut regions: Vec<_> = region_build
            .into_iter()
            .map(|(r_name, tmp_region)| {
                // zones: Region → Vec<Zone>
                let zones: Vec<_> = tmp_region
                    .zones
                    .into_iter()
                    .map(|(z_name, nodes)| Zone {
                        hash: domain_hash(&r_name, Some(&z_name)),
                        nodes,
                    })
                    .collect();

                Region {
                    hash: domain_hash(&r_name, None),
                    region_nodes: tmp_region.region_nodes,
                    zones,
                }
            })
            .collect();

        // 3. Fill cached indices in every Node
        for (region_index, reg) in regions.iter_mut().enumerate() {
            let region_index = region_index as u32;

            // region‑only nodes
            for n in &mut reg.region_nodes {
                n.region_idx = Some(region_index);
                n.zone_idx = None;
            }

            // nodes inside each zone
            for (zone_index, zone) in reg.zones.iter_mut().enumerate() {
                let zone_index = zone_index as u32;
                for n in &mut zone.nodes {
                    n.region_idx = Some(region_index);
                    n.zone_idx = Some(zone_index);
                }
            }
        }

        Self {
            root_nodes,
            regions,
        }
    }

    #[allow(unused)]
    pub fn check_replication_property(
        &self,
        nodeset: &NodeSet,
        replication: &ReplicationProperty,
    ) -> bool {
        // check that the nodeset matches replication property
        let mut distinct_regions = HashSet::new();
        let mut distinct_zones = HashSet::new();
        for node_id in nodeset.iter() {
            if let Some(found) = self.lookup_node(*node_id) {
                if let Some(region_idx) = found.region_idx {
                    distinct_regions.insert(region_idx);
                }
                if let Some(zone_idx) = found.zone_idx {
                    distinct_zones.insert((found.region_idx, zone_idx));
                }
            }
        }

        let mut valid = nodeset.len() == replication.num_copies() as usize;
        for (scope, factor) in replication.distinct_replication_factors() {
            valid &= match scope {
                LocationScope::Zone => factor as usize <= distinct_zones.len(),
                LocationScope::Region => factor as usize <= distinct_regions.len(),
                LocationScope::Node | LocationScope::Root => true,
            };
        }

        valid
    }

    /// Iterate every node exactly once (root, region-only, zone)
    pub fn iter_all_nodes(&self) -> impl Iterator<Item = &Node> {
        self.root_nodes
            .iter()
            .chain(self.regions.iter().flat_map(|reg| {
                reg.region_nodes
                    .iter()
                    .chain(reg.zones.iter().flat_map(|z| &z.nodes))
            }))
    }

    pub fn regions(&self) -> &[Region] {
        &self.regions
    }

    pub fn lookup_node(&self, id: PlainNodeId) -> Option<&Node> {
        self.iter_all_nodes().find(|node| node.id == id)
    }

    pub fn count_domains_in_scope(&self, scope: LocationScope) -> usize {
        match scope {
            LocationScope::Root | LocationScope::Node => self.num_nodes(),
            LocationScope::Region => self.num_distinct_regions(),
            LocationScope::Zone => self.num_distinct_zones(),
        }
    }

    pub fn num_distinct_regions(&self) -> usize {
        self.regions.len()
    }

    pub fn num_nodes(&self) -> usize {
        // todo: can be pre-computed at build time at every level
        self.iter_all_nodes().count()
    }

    pub fn num_distinct_zones(&self) -> usize {
        self.regions.iter().map(|r| r.zones.len()).sum()
    }
}

impl Region {
    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn zones(&self) -> &[Zone] {
        &self.zones
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = &Node> {
        self.region_nodes
            .iter()
            .chain(self.zones.iter().flat_map(|z| z.iter_nodes()))
    }
}

impl Zone {
    pub fn hash(&self) -> u64 {
        self.hash
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }
}

fn domain_hash(region: &RegionLabel, zone: Option<&ZoneLabel>) -> u64 {
    let mut hasher = Xxh3::with_seed(HASH_SALT);
    hasher.write(region.as_bytes());
    if let Some(zone) = zone {
        hasher.write(zone.as_bytes());
    }
    hasher.finish()
}
