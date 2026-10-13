// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_partition_store::index::EntryByStageServiceKeyView;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::index::EntryByService;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::index::table::IndexFilter;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::row::append_row;
use super::schema::{IdxEntryByServiceBuilder, IdxEntryByServiceTable};

impl IdxEntryByServiceTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let schema = IdxEntryByServiceBuilder::schema();
        let statistics = TableStatisticsBuilder::new(schema.clone())
            .with_num_rows_estimate(RowEstimate::Large)
            .with_foreign_key("service_name", SERVICE_ROW_ESTIMATE);
        let table = PartitionedTableProvider::new(
            partition_selector,
            schema,
            // A logical scan may concatenate multiple physical partitions. Do not
            // advertise their local secondary-key order as a global SQL ordering.
            Vec::new(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::partition_key(PointReadFanout::PerPartition)
                .with_grouped_vqueue_entry_id("canonical_id")
                .with_grouped_vqueue_entry_id("entry_id"),
        )
        .with_statistics(statistics.build());
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<EntryIndexScanner>::new(partition_store_manager)
    }
}

#[derive(Debug, Clone)]
pub(super) struct EntryIndexScanner;

impl ScanLocalPartition for EntryIndexScanner {
    type Builder = IdxEntryByServiceBuilder;
    type Item<'a> = EntryByStageServiceKeyView<'a>;
    type ConversionError = StorageError;
    type Filter = IndexFilter<EntryByService>;

    fn for_each_row<F>(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>
    where
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    {
        partition_store.scan_entry_by_service(filter.range, &filter.predicate, filter.live, f)
    }

    fn append_row<'a>(
        builder: &mut Self::Builder,
        key: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_row(builder, key)
    }
}
