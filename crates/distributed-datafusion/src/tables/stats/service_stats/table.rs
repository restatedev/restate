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

use restate_partition_store::stats::aggregated::ServiceStatsBinding;
use restate_storage_query_api::tables::stats::service_stats::{ServiceStats, ServiceStatsColumn};

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::tables::stats::aggregated_stat_table::gauge_stat_sum_view;

use super::schema::{ServiceStatsSource, SysServiceStatsTable};

impl SysServiceStatsTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let statistics = TableStatisticsBuilder::<ServiceStats>::new()
            .with_num_rows_estimate(RowEstimate::Small)
            .with_foreign_key(ServiceStatsColumn::ServiceName, SERVICE_ROW_ESTIMATE);

        let raw_table =
            PartitionedTableProvider::declared::<ServiceStatsSource>(partition_selector, sources)
                .with_statistics(statistics.build());
        let table = gauge_stat_sum_view(&Self::identity(), Arc::new(raw_table))?;

        Ok(Arc::new(table))
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        SchemaScanner::<ServiceStatsBinding>::new()
    }
}
