// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Formatter};
use std::pin::pin;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use arc_swap::ArcSwap;

use restate_types::NodeId;
use restate_types::identifiers::PartitionId;
use restate_types::net::metadata::MetadataKind;
use restate_types::partition_table::PartitionTable;
use tracing::debug;

use crate::{Metadata, ShutdownError, TaskCenter, TaskId, TaskKind, cancellation_watcher};

/// Discover cluster nodes for a given partition. Compared to the partition table, this view is more
/// dynamic as it changes based on cluster nodes' operational status. Can be cheaply cloned.
#[derive(Clone)]
pub struct PartitionRouting {
    /// A mapping of partition IDs to node IDs that are believed to be authoritative for that serving requests.
    partition_to_node_mappings: Arc<ArcSwap<PartitionToNodesRoutingTable>>,
}

impl PartitionRouting {
    /// Look up a suitable node to process requests for a given partition. Answers are authoritative
    /// though subject to propagation delays through the cluster in distributed deployments.
    /// Generally, as a consumer of routing information, your options are limited to backing off and
    /// retrying the request, or returning an error upstream when information is not available.
    ///
    /// A `None` response indicates that either we have no knowledge about this partition, or that
    /// the routing table has not yet been refreshed for the cluster.
    pub fn get_node_by_partition(&self, partition_id: PartitionId) -> Option<NodeId> {
        self.partition_to_node_mappings
            .load()
            .inner
            .get(&partition_id)
            .cloned()
    }
}

impl Debug for PartitionRouting {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PartitionNodeResolver(")?;
        self.partition_to_node_mappings.load().fmt(f)?;
        f.write_str(")")
    }
}

#[derive(Debug)]
struct PartitionToNodesRoutingTable {
    /// A mapping of partition IDs to node IDs that are believed to be authoritative for that
    /// serving requests for that partition.
    inner: HashMap<PartitionId, NodeId>,
}
impl PartitionToNodesRoutingTable {
    fn new(partition_table: &PartitionTable) -> Self {
        let mut inner = HashMap::<PartitionId, NodeId>::with_capacity(
            partition_table.num_partitions() as usize,
        );
        for (partition_id, partition) in partition_table.iter() {
            if let Some(leader) = partition.placement.leader() {
                inner.insert(*partition_id, leader.into());
            }
        }

        Self { inner }
    }
}

/// Task to refresh the routing information, periodically or on-demand. A single
/// instance of this component per node ensures that we don't get a stampeding
/// herd of queries to the source of truth.
//
// Implementation note: the sole authority for node-level routing is currently the metadata store,
// and in particular, the cluster scheduling plan. This will change in the future so avoid leaking
// implementation details or assumptions about the source of truth.
pub struct PartitionRoutingRefresher {
    inner: Arc<ArcSwap<PartitionToNodesRoutingTable>>,
}

impl Default for PartitionRoutingRefresher {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionRoutingRefresher {
    fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(PartitionToNodesRoutingTable {
                inner: HashMap::default(),
            }))),
        }
    }

    /// Get a handle to the partition-to-node routing table.
    pub fn partition_routing(&self) -> PartitionRouting {
        PartitionRouting {
            partition_to_node_mappings: self.inner.clone(),
        }
    }

    async fn run(self) -> anyhow::Result<()> {
        debug!("Routing information refresher started");

        let mut cancel = pin!(cancellation_watcher());
        let (mut partition_table_watch, mut live_partition_table) =
            Metadata::with_current(|metadata| {
                (
                    metadata.watch(MetadataKind::PartitionTable),
                    metadata.updateable_partition_table(),
                )
            });
        loop {
            tokio::select! {
                _ = &mut cancel => {
                    break;
                }
                _ = partition_table_watch.changed() => {
                    let partition_table = live_partition_table.live_load();
                    debug!("Refreshing routing information based on partition table {}...", partition_table.version());
                    let routing = PartitionToNodesRoutingTable::new(partition_table);
                    self.inner.store(Arc::new(routing));
                }
            }
        }
        Ok(())
    }
}

pub fn spawn_partition_routing_refresher(
    partition_routing_refresher: PartitionRoutingRefresher,
) -> Result<TaskId, ShutdownError> {
    TaskCenter::spawn(
        TaskKind::MetadataBackgroundSync,
        "partition-routing-refresher",
        partition_routing_refresher.run(),
    )
}

#[cfg(any(test, feature = "test-util"))]
pub mod mocks {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arc_swap::ArcSwap;

    use crate::partitions::PartitionRouting;
    use restate_types::identifiers::PartitionId;
    use restate_types::{GenerationalNodeId, NodeId};

    pub fn fixed_single_node(
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
    ) -> PartitionRouting {
        let mut mappings = HashMap::default();
        mappings.insert(partition_id, NodeId::Generational(node_id));

        PartitionRouting {
            partition_to_node_mappings: Arc::new(ArcSwap::new(Arc::new(
                super::PartitionToNodesRoutingTable { inner: mappings },
            ))),
        }
    }
}
