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

use restate_partition_store::index::EntryNextAtByStageKeyView;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::filter::Filter;
use restate_storage_api::index::EntryNextAtByStage;
use restate_types::sharding::KeyRange;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::partition_store_scanner::{
    LocalPartitionsScanner, ScanLocalPartition, ScanLocalPartitionFilter,
};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::schema::IdxEntryNextAtByStageBuilder;

const NAME: &str = "_idx_entry_next_at_by_stage";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        EntryNextAtByStageScanner,
    )) as Arc<dyn ScanPartition>;
    let schema = IdxEntryNextAtByStageBuilder::schema();
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
struct EntryNextAtByStageScanner;

struct EntryNextAtByStageFilter {
    range: KeyRange,
    predicate: Filter<EntryNextAtByStage>,
}

impl ScanLocalPartitionFilter for EntryNextAtByStageFilter {
    fn new(range: KeyRange, access_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            range,
            predicate: Filter::new(range, access_predicate),
        }
    }
}

impl ScanLocalPartition for EntryNextAtByStageScanner {
    type Builder = IdxEntryNextAtByStageBuilder;
    type Item<'a> = EntryNextAtByStageKeyView<'a>;
    type ConversionError = StorageError;
    type Filter = EntryNextAtByStageFilter;

    fn for_each_row<F>(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        metrics: Option<restate_partition_store::IteratorMetrics>,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>
    where
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    {
        partition_store.scan_entry_next_at_by_stage(filter.range, &filter.predicate, metrics, f)
    }

    fn append_row<'a>(
        builder: &mut Self::Builder,
        key: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        let mut row = builder.row();
        if row.is_stage_defined() {
            row.stage(key.stage.decode()?.as_str());
        }
        if row.is_next_at_defined() {
            row.next_at(key.next_at.decode()?.as_unix_millis().as_u64() as i64);
        }
        if row.is_seq_defined() {
            row.seq(key.seq.decode()?.as_u64());
        }
        if row.is_canonical_id_defined()
            || row.is_entry_id_defined()
            || row.is_partition_key_defined()
        {
            let id = key.canonical_id.decode()?;
            if row.is_canonical_id_defined() {
                row.fmt_canonical_id(id);
            }
            if row.is_entry_id_defined() {
                row.fmt_entry_id(id.to_base_entry_id());
            }
            if row.is_partition_key_defined() {
                row.partition_key(id.partition_key());
            }
        }
        Ok(())
    }
}
