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

use restate_partition_store::index::EntryByStageKey;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::filter::Filter;
use restate_storage_api::index::EntryByStage;
use restate_types::errors::ConversionError;
use restate_types::sharding::{KeyRange, PartitionId};

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::partition_store_scanner::{
    LocalPartitionsScanner, ScanLocalPartition, ScanLocalPartitionFilter,
};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::schema::IdxEntryByStageBuilder;

const NAME: &str = "_idx_entry_by_stage";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        EntryByStageScanner,
    )) as Arc<dyn ScanPartition>;
    let schema = IdxEntryByStageBuilder::schema();
    let statistics =
        TableStatisticsBuilder::new(schema.clone()).with_num_rows_estimate(RowEstimate::Large);
    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        // Local index order is not global order across physical partitions.
        Vec::new(),
        remote_scanner_manager.create_distributed_scanner(NAME, scanner),
        FirstMatchingPartitionKeyExtractor::partition_key(PointReadFanout::PerPartition)
            .with_grouped_vqueue_entry_id("canonical_id")
            .with_grouped_vqueue_entry_id("entry_id"),
    )
    .with_statistics(statistics.build());
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct EntryByStageScanner;

struct EntryByStageFilter {
    range: KeyRange,
    predicate: Filter<EntryByStage>,
}

impl ScanLocalPartitionFilter for EntryByStageFilter {
    fn new(range: KeyRange, access_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            range,
            predicate: Filter::new(range, access_predicate),
        }
    }
}

impl ScanLocalPartition for EntryByStageScanner {
    type Builder = IdxEntryByStageBuilder;
    type Item<'a> = (PartitionId, EntryByStageKey);
    type ConversionError = ConversionError;
    type Filter = EntryByStageFilter;

    fn for_each_row<F>(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        metrics: Option<restate_partition_store::IteratorMetrics>,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>
    where
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    {
        let partition_id = partition_store.partition_id();
        partition_store.scan_entry_by_stage(filter.range, &filter.predicate, metrics, move |key| {
            f((partition_id, key)).map_break(|result| result.map_err(StorageError::from))
        })
    }

    fn append_row<'a>(
        builder: &mut Self::Builder,
        (partition_id, key): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        let mut row = builder.row();
        row.partition_id(partition_id.into());
        row.stage(key.stage.as_str());
        row.transitioned_at(key.transitioned_at.0.to_unix_millis().as_u64() as i64);
        row.status(key.status.as_str());
        if row.is_canonical_id_defined() {
            row.fmt_canonical_id(key.canonical_id);
        }
        if row.is_entry_id_defined() {
            row.fmt_entry_id(key.canonical_id.to_base_entry_id());
        }
        row.partition_key(key.canonical_id.partition_key());
        Ok(())
    }
}
