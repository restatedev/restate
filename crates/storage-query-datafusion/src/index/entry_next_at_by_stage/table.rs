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

use restate_partition_store::index::EntryNextAtByStageKeyView;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::index::EntryNextAtByStage;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::index::table::IndexFilter;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::schema::{IdxEntryNextAtByStageBuilder, IdxEntryNextAtByStageTable};

impl IdxEntryNextAtByStageTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let schema = IdxEntryNextAtByStageBuilder::schema();
        let statistics =
            TableStatisticsBuilder::new(schema.clone()).with_num_rows_estimate(RowEstimate::Large);
        let table = PartitionedTableProvider::new(
            partition_selector,
            schema,
            // Local index order is not global order across physical partitions.
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
        LocalPartitionsScanner::<EntryNextAtByStageScanner>::new(partition_store_manager)
    }
}

#[derive(Debug, Clone)]
struct EntryNextAtByStageScanner;

impl ScanLocalPartition for EntryNextAtByStageScanner {
    type Builder = IdxEntryNextAtByStageBuilder;
    type Item<'a> = EntryNextAtByStageKeyView<'a>;
    type ConversionError = StorageError;
    type Filter = IndexFilter<EntryNextAtByStage>;

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
        partition_store.scan_entry_next_at_by_stage(filter.range, &filter.predicate, filter.live, f)
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
