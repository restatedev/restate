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

use datafusion::catalog::TableProvider;

use restate_partition_store::stats::aggregated::DeploymentStatsBinding;
use restate_storage_query_api::tables::stats::deployment_stats::{
    DeploymentStats, DeploymentStatsColumn,
};

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::tables::stats::aggregated_stat_table::gauge_stat_sum_view;

use super::schema::{DeploymentStatsSource, SysDeploymentStatsTable};

impl SysDeploymentStatsTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let statistics = TableStatisticsBuilder::<DeploymentStats>::new()
            .with_num_rows_estimate(RowEstimate::Small)
            .with_foreign_key(DeploymentStatsColumn::DeploymentId, DEPLOYMENT_ROW_ESTIMATE)
            .with_foreign_key(DeploymentStatsColumn::ServiceName, SERVICE_ROW_ESTIMATE);

        let raw_table = PartitionedTableProvider::declared::<DeploymentStatsSource>(
            partition_selector,
            sources,
        )
        .with_statistics(statistics.build());
        let table = gauge_stat_sum_view(&Self::identity(), Arc::new(raw_table))?;

        Ok(Arc::new(table))
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        SchemaScanner::<DeploymentStatsBinding>::new()
    }
}
