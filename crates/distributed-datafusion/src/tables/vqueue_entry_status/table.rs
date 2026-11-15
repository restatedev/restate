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

use restate_storage_query_api::tables::vqueue_entry_status::{
    VqueueEntryStatus, VqueueEntryStatusColumn,
};

use super::read::EntryStatusScanner;
use super::schema::SysVqueueEntryStatusTable;
use crate::context::SelectPartitions;
use crate::source_registry::SourceRegistry;
use crate::statistics::{DEPLOYMENT_ROW_ESTIMATE, RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysVqueueEntryStatusTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<VqueueEntryStatus>::new()
            .with_num_rows_estimate(RowEstimate::Large)
            .with_partition_key(VqueueEntryStatusColumn::PartitionKey)
            .with_primary_key(VqueueEntryStatusColumn::EntryId)
            .with_primary_key(VqueueEntryStatusColumn::CanonicalId)
            .with_foreign_key(VqueueEntryStatusColumn::Deployment, DEPLOYMENT_ROW_ESTIMATE)
            // This can be wrong in some rare cases, but the assumption is that
            // the number of vqueue entries is bigger than the number of vqueues
            .with_foreign_key(VqueueEntryStatusColumn::VqueueId, RowEstimate::Small);

        let table = PartitionedTableProvider::declared::<Self>(partition_selector, sources)
            .with_statistics(statistics.build());

        Arc::new(table)
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        EntryStatusScanner
    }
}
