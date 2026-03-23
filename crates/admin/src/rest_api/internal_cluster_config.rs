// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Json;
use serde::Serialize;

use restate_core::Metadata;

use super::internal_cluster_common::{
    LogsProviderView, PartitionReplicationView, logs_provider_view, partition_replication_view,
};

#[derive(Debug, Clone, Serialize)]
pub struct InternalClusterConfigResponse {
    /// Total number of partitions configured for the cluster.
    pub num_partitions: u32,
    /// Partition replication strategy.
    pub partition_replication: PartitionReplicationView,
    /// Default provider used when creating new logs.
    pub logs_provider: LogsProviderView,
}

pub async fn get_internal_cluster_config() -> Json<InternalClusterConfigResponse> {
    Json(Metadata::with_current(|metadata| {
        let logs = metadata.logs_ref();
        let partition_table = metadata.partition_table_ref();

        InternalClusterConfigResponse {
            num_partitions: u32::from(partition_table.num_partitions()),
            partition_replication: partition_replication_view(partition_table.replication()),
            logs_provider: logs_provider_view(logs.configuration().default_provider.clone()),
        }
    }))
}
