// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tokio::sync::watch;

use restate_core::{Metadata, TaskCenter};
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::cluster_state::ClusterState;
use restate_types::partitions::state::PartitionReplicaSetStates;

use crate::BuildError;
use crate::source_registry::SourceRegistry;
use crate::tables::bifrost_read_stream::BifrostReadStreamsTable;
use crate::tables::migration_records::{IndexesTable, MigrationJobsTable};
use crate::tables::nodes::NodesTable;
use crate::tables::partition_state::PartitionStateTable;
use crate::tables::partitions::PartitionsTable;

use super::{RegisterTable, TableInventoryBuilder};

/// Registers V2's cluster tables, including coordinator-local observations.
pub struct ClusterTables {
    sources: SourceRegistry,
    /// This node's gossip view of the cluster.
    cluster_state: ClusterState,
    /// The partition memberships and leaders this node has observed.
    replica_set_states: PartitionReplicaSetStates,
    cluster_state_watch: Option<watch::Receiver<Arc<LegacyClusterState>>>,
}

impl ClusterTables {
    /// `replica_set_states` are this node's observed partition memberships, the
    /// same the default engine's cluster tables read.
    pub fn new(sources: SourceRegistry, replica_set_states: PartitionReplicaSetStates) -> Self {
        Self {
            sources,
            cluster_state: TaskCenter::with_current(|tc| tc.cluster_state().clone()),
            replica_set_states,
            cluster_state_watch: None,
        }
    }

    /// Supplies the coordinator's local controller view, when available.
    pub fn with_cluster_state_watch(
        mut self,
        watch: Option<watch::Receiver<Arc<LegacyClusterState>>>,
    ) -> Self {
        self.cluster_state_watch = watch;
        self
    }
}

impl RegisterTable for ClusterTables {
    async fn register(&self, inventory: &mut TableInventoryBuilder<'_>) -> Result<(), BuildError> {
        inventory.add::<NodesTable>(
            "cluster",
            "nodes",
            NodesTable::create_provider(Metadata::current(), self.cluster_state.clone()),
        )?;
        inventory.add::<PartitionsTable>(
            "cluster",
            "partitions",
            PartitionsTable::create_provider(Metadata::current(), self.replica_set_states.clone()),
        )?;
        inventory.add::<PartitionStateTable>(
            "cluster",
            "partition_state",
            PartitionStateTable::create_provider(self.cluster_state_watch.clone()),
        )?;
        inventory.add::<BifrostReadStreamsTable>(
            "cluster",
            "bifrost_read_streams",
            BifrostReadStreamsTable::create_provider(
                Metadata::current(),
                self.sources.clone(),
                None, // The node registers its local scanner separately.
            ),
        )?;
        // Worker nodes register their scanners of these separately
        // (`local_scanners::register_cluster_scanners`).
        inventory.add::<IndexesTable>(
            "cluster",
            "indexes",
            IndexesTable::create_provider(Metadata::current(), self.sources.clone()),
        )?;
        inventory.add::<MigrationJobsTable>(
            "cluster",
            "migration_jobs",
            MigrationJobsTable::create_provider(Metadata::current(), self.sources.clone()),
        )
    }
}
