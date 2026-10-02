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

use datafusion::execution::context::SessionContext;
use tokio::sync::watch;

use restate_core::{Metadata, TaskCenter};
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::config::Configuration;
use restate_types::partitions::state::PartitionReplicaSetStates;

use crate::BuildError;
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::RegisterTable;

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
    async fn register(&self, ctx: &SessionContext) -> Result<(), BuildError> {
        ctx.sql("CREATE SCHEMA IF NOT EXISTS restate.cluster")
            .await?;
        let metadata = Metadata::current();
        crate::node::register_self(ctx, metadata.clone(), self.cluster_state.clone())?;
        crate::partition::register_self(ctx, metadata.clone(), self.replica_set_states.clone())?;
        crate::partition_replica_set::register_self(
            ctx,
            metadata.clone(),
            self.cluster_state.clone(),
            self.replica_set_states.clone(),
        )?;
        crate::log::register_self(ctx, metadata.clone())?;
        crate::partition_state::register_self(ctx, self.cluster_state_watch.clone())?;

        // Node-fan-out tables
        crate::loglet_worker::register_self(
            ctx,
            metadata.clone(),
            self.remote_scanner_manager.clone(),
            None, // local scanner is registered separately if this node is also a log-server
        )?;
        crate::bifrost_read_stream::register_self(
            ctx,
            metadata.clone(),
            self.remote_scanner_manager.clone(),
            None, // local scanner is registered separately by the node
        )?;

        if !Configuration::pinned().common.disable_config_sql_table {
            crate::config::register_self(
                ctx,
                metadata,
                self.remote_scanner_manager.clone(),
                None, // local scanner is registered separately by the node
            )?;
        }

        ctx.sql(CLUSTER_LOGS_TAIL_SEGMENTS_VIEW).await?;

        Ok(())
    }
}
