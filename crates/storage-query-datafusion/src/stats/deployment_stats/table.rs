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
use restate_partition_store::stats::aggregated::{DeploymentLoadKey, StageCounts};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::filter::Filter;
use restate_storage_api::stats::deployment_load::DeploymentLoad;
use restate_types::errors::ConversionError;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::stats::aggregated_stat_table::gauge_stat_sum_view;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

use super::row::append_deployment_stats_row;
use super::schema::SysDeploymentStatsBuilder;

const NAME: &str = "sys_deployment_stats";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        DeploymentStatsScanner,
    )) as Arc<dyn ScanPartition>;

    let schema = SysDeploymentStatsBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Small)
        .with_foreign_key("deployment_id", DEPLOYMENT_ROW_ESTIMATE)
        .with_foreign_key("service_name", SERVICE_ROW_ESTIMATE);

    let raw_table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        Vec::new(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default(),
    )
    .with_statistics(statistics.build());
    let table = gauge_stat_sum_view(NAME, Arc::new(raw_table))?;

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct DeploymentStatsScanner;

impl ScanLocalPartition for DeploymentStatsScanner {
    type Builder = SysDeploymentStatsBuilder;
    type Item<'a> = (KeyDecoder<'a, DeploymentLoadKey>, StageCounts);
    type ConversionError = ConversionError;
    type Filter = Filter<DeploymentLoad>;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.scan_deployment_load(&filter, move |key_decoder, value| {
            f((key_decoder, value)).map_break(|result| result.map_err(StorageError::from))
        })
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (key_decoder, value): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        let key = key_decoder
            .try_full_decode::<DeploymentLoad>()
            .map_err(ConversionError::invalid_data)?;
        append_deployment_stats_row(row_builder, key, value);
        Ok(())
    }
}
