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

use datafusion::physical_plan::PhysicalExpr;

use restate_partition_store::index::EntryByServiceStageKey;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::filter::Filter;
use restate_storage_api::index::EntryByService;
use restate_types::errors::ConversionError;
use restate_types::sharding::{KeyRange, PartitionId};

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::partition_store_scanner::{
    LocalPartitionsScanner, ScanLocalPartition, ScanLocalPartitionFilter,
};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::row::append_row;
use super::schema::IdxEntryByServiceBuilder;

pub(super) const NAME: &str = "_idx_entry_by_service";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        EntryIndexScanner,
    )) as Arc<dyn ScanPartition>;
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
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::partition_key(PointReadFanout::PerPartition)
            .with_grouped_vqueue_entry_id("canonical_id")
            .with_grouped_vqueue_entry_id("entry_id"),
    )
    .with_statistics(statistics.build());
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
pub(super) struct EntryIndexScanner;

pub(super) struct EntryIndexFilter {
    range: KeyRange,
    predicate: Filter<EntryByService>,
}

impl ScanLocalPartitionFilter for EntryIndexFilter {
    fn new(range: KeyRange, access_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            range,
            predicate: Filter::new(range, access_predicate),
        }
    }
}

impl ScanLocalPartition for EntryIndexScanner {
    type Builder = IdxEntryByServiceBuilder;
    type Item<'a> = (PartitionId, EntryByServiceStageKey);
    type ConversionError = ConversionError;
    type Filter = EntryIndexFilter;

    fn for_each_row<F>(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>
    where
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    {
        let partition_id = partition_store.partition_id();
        partition_store.scan_entry_by_service(filter.range, &filter.predicate, move |key| {
            f((partition_id, key)).map_break(|result| result.map_err(StorageError::from))
        })
    }

    fn append_row<'a>(
        builder: &mut Self::Builder,
        (partition_id, key): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_row(builder, partition_id, key);
        Ok(())
    }
}
