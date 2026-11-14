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

use restate_core::Metadata;
use restate_types::cluster::cluster_state::LegacyClusterState;

use crate::BuildError;
use crate::source_registry::SourceRegistry;
use crate::tables::bifrost_read_stream::BifrostReadStreamsTable;
use crate::tables::partition_state::PartitionStateTable;

use super::{RegisterTable, TableInventoryBuilder};

/// Registers V2's cluster tables, including coordinator-local observations.
pub struct ClusterTables {
    sources: SourceRegistry,
    cluster_state_watch: Option<watch::Receiver<Arc<LegacyClusterState>>>,
}

impl ClusterTables {
    pub fn new(sources: SourceRegistry) -> Self {
        Self {
            sources,
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
        )
    }
}
