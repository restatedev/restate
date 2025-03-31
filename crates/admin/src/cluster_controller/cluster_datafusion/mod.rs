// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod log;
mod node;
mod node_state;
mod partition;
mod partition_state;

use std::sync::Arc;

use restate_core::Metadata;
use restate_datafusion::{
    BuildError,
    context::{QueryContext, RegisterTable},
};
use restate_types::cluster::cluster_state::ClusterState;
use tokio::sync::watch;

const CLUSTER_LOGS_TAIL_SEGMENTS_VIEW: &str = "CREATE VIEW logs_tail_segments as SELECT
        l.* FROM logs AS l JOIN (
            SELECT log_id, max(segment_index) AS segment_index FROM logs GROUP BY log_id
        ) m
        ON m.log_id=l.log_id AND l.segment_index=m.segment_index";

pub struct ClusterTables {
    cluster_state_watch: watch::Receiver<Arc<ClusterState>>,
}

impl ClusterTables {
    pub fn new(cluster_state_watch: watch::Receiver<Arc<ClusterState>>) -> Self {
        Self {
            cluster_state_watch,
        }
    }
}

impl RegisterTable for ClusterTables {
    async fn register(&self, ctx: &QueryContext) -> Result<(), BuildError> {
        let metadata = Metadata::current();
        node::register_self(ctx, metadata.clone())?;
        partition::register_self(ctx, metadata.clone())?;
        log::register_self(ctx, metadata)?;
        node_state::register_self(ctx, self.cluster_state_watch.clone())?;
        partition_state::register_self(ctx, self.cluster_state_watch.clone())?;

        self.create_view(ctx, CLUSTER_LOGS_TAIL_SEGMENTS_VIEW)
            .await?;

        Ok(())
    }
}
