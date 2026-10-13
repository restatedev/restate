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
use restate_types::config::Configuration;
use restate_types::partitions::state::PartitionReplicaSetStates;

use crate::BuildError;
use crate::bifrost_read_stream::BifrostReadStreamsTable;
use crate::config::ConfigTable;
use crate::log::schema::LogTable;
use crate::loglet_worker::LogletWorkersTable;
use crate::node::schema::NodeTable;
use crate::partition::schema::PartitionTable;
use crate::partition_replica_set::schema::PartitionReplicaSetTable;
use crate::partition_state::schema::PartitionStateTable;
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::{RegisterTable, TableInventoryBuilder};

const CLUSTER_LOGS_TAIL_SEGMENTS_VIEW: &str = "CREATE VIEW restate.cluster.logs_tail_segments as SELECT
        l.* FROM restate.cluster.logs AS l JOIN (
            SELECT log_id, max(segment_index) AS segment_index FROM restate.cluster.logs GROUP BY log_id
        ) m
        ON m.log_id=l.log_id AND l.segment_index=m.segment_index";

/// Registers cluster tables and views in `restate.cluster`, independently of session defaults.
pub struct ClusterTables {
    cluster_state: restate_types::cluster_state::ClusterState,
    replica_set_states: PartitionReplicaSetStates,
    cluster_state_watch: watch::Receiver<Arc<LegacyClusterState>>,
    remote_scanner_manager: RemoteScannerManager,
}

impl ClusterTables {
    pub fn new(
        replica_set_states: PartitionReplicaSetStates,
        cluster_state_watch: watch::Receiver<Arc<LegacyClusterState>>,
        remote_scanner_manager: RemoteScannerManager,
    ) -> Self {
        let cluster_state = TaskCenter::with_current(|tc| tc.cluster_state().clone());
        Self {
            cluster_state,
            replica_set_states,
            cluster_state_watch,
            remote_scanner_manager,
        }
    }

    /// Returns a reference to the remote scanner manager. This can be used to
    /// register node-level scanners (e.g., log-server tables) after construction.
    pub fn remote_scanner_manager(&self) -> &RemoteScannerManager {
        &self.remote_scanner_manager
    }
}

impl RegisterTable for ClusterTables {
    async fn register(&self, inventory: &mut TableInventoryBuilder<'_>) -> Result<(), BuildError> {
        let metadata = Metadata::current();
        inventory.add::<NodeTable>(
            "cluster",
            "nodes",
            NodeTable::create_provider(metadata.clone(), self.cluster_state.clone()),
        )?;
        inventory.add::<PartitionTable>(
            "cluster",
            "partitions",
            PartitionTable::create_provider(metadata.clone(), self.replica_set_states.clone()),
        )?;
        inventory.add::<PartitionReplicaSetTable>(
            "cluster",
            "partition_replica_set",
            PartitionReplicaSetTable::create_provider(
                metadata.clone(),
                self.cluster_state.clone(),
                self.replica_set_states.clone(),
            ),
        )?;
        inventory.add::<LogTable>(
            "cluster",
            "logs",
            LogTable::create_provider(metadata.clone()),
        )?;
        inventory.add::<PartitionStateTable>(
            "cluster",
            "partition_state",
            PartitionStateTable::create_provider(self.cluster_state_watch.clone()),
        )?;

        // Node-fan-out tables
        inventory.add::<LogletWorkersTable>(
            "cluster",
            "loglet_workers",
            LogletWorkersTable::create_provider(
                metadata.clone(),
                self.remote_scanner_manager.clone(),
                None, // local scanner is registered separately if this node is also a log-server
            ),
        )?;
        inventory.add::<BifrostReadStreamsTable>(
            "cluster",
            "bifrost_read_streams",
            BifrostReadStreamsTable::create_provider(
                metadata.clone(),
                self.remote_scanner_manager.clone(),
                None, // local scanner is registered separately by the node
            ),
        )?;

        if !Configuration::pinned().common.disable_config_sql_table {
            inventory.add::<ConfigTable>(
                "cluster",
                "config",
                ConfigTable::create_provider(
                    metadata,
                    self.remote_scanner_manager.clone(),
                    None, // local scanner is registered separately by the node
                ),
            )?;
        }

        inventory
            .add_view(
                "logs_tail_segments",
                "cluster",
                CLUSTER_LOGS_TAIL_SEGMENTS_VIEW,
            )
            .await?;

        Ok(())
    }
}
