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

use restate_partition_store::stats::aggregated::VirtualObjectStatsBinding;
use restate_storage_query_api::tables::stats::virtual_object_stats::{
    VirtualObjectStats, VirtualObjectStatsColumn,
};

use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::schema::{SysVirtualObjectStatsTable, VirtualObjectStatsSource};

impl SysVirtualObjectStatsTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<VirtualObjectStats>::new()
            .with_num_rows_estimate(RowEstimate::Large)
            .with_foreign_key(VirtualObjectStatsColumn::ServiceName, SERVICE_ROW_ESTIMATE);

        let table = PartitionedTableProvider::declared::<VirtualObjectStatsSource>(
            partition_selector,
            sources,
        )
        .with_statistics(statistics.build());

        Arc::new(table)
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        SchemaScanner::<VirtualObjectStatsBinding>::new()
    }
}
