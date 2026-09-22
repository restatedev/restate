// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;

use restate_partition_store::keys::KeyDecoder;
use restate_partition_store::stats::aggregated::{StageCounts, VirtualObjectLoadKey};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::filter::Filter;
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad;
use restate_types::errors::ConversionError;
use restate_types::sharding::PartitionId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::row::append_virtual_object_stats_row;
use super::schema::SysVirtualObjectStatsBuilder;

const NAME: &str = "sys_virtual_object_stats";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        VirtualObjectStatsScanner,
    )) as Arc<dyn ScanPartition>;

    let schema = SysVirtualObjectStatsBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Large)
        .with_foreign_key("service_name", SERVICE_ROW_ESTIMATE);

    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        Vec::new(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        // The typed filter applies the entire predicate rather than each point-read
        // range, so selected keys must share one scan per physical partition.
        FirstMatchingPartitionKeyExtractor::partition_key(PointReadFanout::PerPartition),
    )
    .with_statistics(statistics.build());

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct VirtualObjectStatsScanner;

impl ScanLocalPartition for VirtualObjectStatsScanner {
    type Builder = SysVirtualObjectStatsBuilder;
    type Item<'a> = (
        PartitionId,
        KeyDecoder<'a, VirtualObjectLoadKey>,
        StageCounts,
    );
    type ConversionError = ConversionError;
    type Filter = Filter<VirtualObjectLoad>;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        metrics: Option<restate_partition_store::IteratorMetrics>,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        let partition_id = partition_store.partition_id();
        partition_store.scan_virtual_object_load(&filter, metrics, move |key_decoder, value| {
            f((partition_id, key_decoder, value))
                .map_break(|result| result.map_err(StorageError::from))
        })
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (partition_id, key_decoder, value): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        let key = key_decoder
            .try_full_decode::<VirtualObjectLoad>()
            .map_err(ConversionError::invalid_data)?;
        append_virtual_object_stats_row(row_builder, partition_id, key, value);
        Ok(())
    }
}
