// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use restate_types::net::metadata::MetadataKind;
use tokio::sync::mpsc;
use tracing::{debug, trace};
use xxhash_rust::xxh3::Xxh3Builder;

use restate_types::identifiers::PartitionId;
use restate_types::{NodeId, Version};

use crate::{
    cancellation_watcher, Metadata, ShutdownError, TaskCenter, TaskHandle, TaskId, TaskKind,
};

pub type CommandSender = mpsc::Sender<Command>;
pub type CommandReceiver = mpsc::Receiver<Command>;

pub enum Command {
    /// Request an on-demand refresh of routing information from the authoritative source.
    SyncRoutingInformation,
}

/// Discover cluster nodes for a given partition. Compared to the partition table, this view is more
/// dynamic as it changes based on cluster nodes' operational status. Can be cheaply cloned.
#[derive(Clone)]
pub struct PartitionRouting {
    sender: CommandSender,
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
    /// the routing table has not yet been refreshed for the cluster. The latter condition should be
    /// brief and only on startup, so we can generally treat lack of response as a negative answer.
    /// An automatic refresh is scheduled any time a `None` response is returned.
    pub fn get_node_by_partition(&self, partition_id: PartitionId) -> Option<NodeId> {
        let mappings = self.partition_to_node_mappings.load();

        // This check should ideally be strengthened to make sure we're using reasonably fresh lookup data
        if mappings.version < Version::MIN {
            debug!("Partition routing information not available - requesting refresh");
            self.request_refresh();
            return None;
        }

        let maybe_node = mappings.inner.get(&partition_id).cloned();
        if maybe_node.is_none() {
            debug!(
                ?partition_id,
                "No known node for partition - requesting refresh"
            );
            self.request_refresh();
        }
        maybe_node
    }

    /// Provide a hint that the partition-to-nodes view may be outdated. This is useful when a
    /// caller discovers via some other mechanism that routing information may be invalid - for
    /// example, when a request to a node previously returned by
    /// [`PartitionRouting::get_node_by_partition`] indicates that it is no longer serving that
    /// partition.
    ///
    /// This call returns immediately, while the refresh itself is performed asynchronously on a
    /// best-effort basis. Multiple calls will not result in multiple refresh attempts. You only
    /// need to call this method if you get a node id, and later discover it's incorrect; a `None`
    /// response to a lookup triggers a refresh automatically.
    pub fn request_refresh(&self) {
        // if the channel already contains an unconsumed message, it doesn't matter that we can't send another
        let _ = self.sender.try_send(Command::SyncRoutingInformation).ok();
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
    version: Version,
    /// A mapping of partition IDs to node IDs that are believed to be authoritative for that
    /// serving requests for that partition.
    inner: HashMap<PartitionId, NodeId, Xxh3Builder>,
}

/// Task to refresh the routing information, periodically or on-demand. A single
/// instance of this component per node ensures that we don't get a stampeding
/// herd of queries to the source of truth.
//
// Implementation note: the sole authority for node-level routing is currently the metadata store,
// and in particular, the cluster scheduling plan. This will change in the future so avoid leaking
// implementation details or assumptions about the source of truth.
pub struct PartitionRoutingRefresher {
    sender: CommandSender,
    receiver: CommandReceiver,
    inflight_refresh_task: Option<TaskHandle<()>>,
    inner: Arc<ArcSwap<PartitionToNodesRoutingTable>>,
}

impl Default for PartitionRoutingRefresher {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionRoutingRefresher {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        Self {
            receiver,
            sender,
            inflight_refresh_task: None,
            inner: Arc::new(ArcSwap::new(Arc::new(PartitionToNodesRoutingTable {
                version: Version::INVALID,
                inner: HashMap::default(),
            }))),
        }
    }

    /// Get a handle to the partition-to-node routing table.
    pub fn partition_routing(&self) -> PartitionRouting {
        PartitionRouting {
            sender: self.sender.clone(),
            partition_to_node_mappings: self.inner.clone(),
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        debug!("Routing information refresher started");

        let mut cancel = pin!(cancellation_watcher());
        let mut partition_table_watch =
            Metadata::with_current(|m| m.watch(MetadataKind::PartitionTable));

        loop {
            tokio::select! {
                _ = &mut cancel => {
                    debug!("Routing information refresher stopped");
                    break;
                }
                Some(cmd) = self.receiver.recv() => {
                    match cmd {
                        Command::SyncRoutingInformation => {
                            self.spawn_sync_routing_information_task(*partition_table_watch.borrow());
                        }
                    }
                }
                _ = partition_table_watch.changed() => {
                    trace!("Refreshing routing information...");
                    self.spawn_sync_routing_information_task(*partition_table_watch.borrow());
                }
            }
        }
        Ok(())
    }

    fn spawn_sync_routing_information_task(&mut self, version: Version) {
        if !self
            .inflight_refresh_task
            .as_ref()
            .is_some_and(|t| !t.is_finished())
        {
            let partition_to_node_mappings = self.inner.clone();

            let task = TaskCenter::spawn_unmanaged(
                TaskKind::Disposable,
                "refresh-routing-information",
                sync_routing_information(partition_to_node_mappings, version),
            );
            self.inflight_refresh_task = task.ok();
        } else {
            trace!("Skipping refresh as a refresh task is already in progress");
        }
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

async fn sync_routing_information(
    partition_to_node_mappings: Arc<ArcSwap<PartitionToNodesRoutingTable>>,
    version: Version,
) {
    let Ok(partition_table) = Metadata::current().wait_for_partition_table(version).await else {
        // just return on shutdown error
        return;
    };

    let current_mappings = partition_to_node_mappings.load();
    if partition_table.version() <= current_mappings.version {
        return; // No need for update
    }

    let mut partition_nodes = HashMap::<PartitionId, NodeId, Xxh3Builder>::default();
    for (partition_id, partition) in partition_table.partitions() {
        if let Some(leader) = partition.replica_group.first() {
            partition_nodes.insert(*partition_id, (*leader).into());
        }
    }

    let _ = partition_to_node_mappings.compare_and_swap(
        current_mappings,
        Arc::new(PartitionToNodesRoutingTable {
            version: partition_table.version(),
            inner: partition_nodes,
        }),
    );
}

#[cfg(any(test, feature = "test-util"))]
pub mod mocks {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use tokio::sync::mpsc;

    use crate::partitions::PartitionRouting;
    use restate_types::identifiers::PartitionId;
    use restate_types::{GenerationalNodeId, NodeId, Version};

    pub fn fixed_single_node(
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
    ) -> PartitionRouting {
        let (sender, _) = mpsc::channel(1);

        let mut mappings = HashMap::default();
        mappings.insert(partition_id, NodeId::Generational(node_id));

        PartitionRouting {
            sender,
            partition_to_node_mappings: Arc::new(ArcSwap::new(Arc::new(
                super::PartitionToNodesRoutingTable {
                    version: Version::MIN,
                    inner: mappings,
                },
            ))),
        }
    }
}
