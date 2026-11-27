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

use restate_worker_api::PartitionQueryAccess;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::hot_keys::row::append_hot_key_row;
use crate::hot_keys::schema::{SysHotKeysBuilder, SysHotKeysTable, sys_hot_keys_sort_order};
use crate::live_scanners::LivePartitionScanner;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysHotKeysTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let schema = SysHotKeysBuilder::schema();
        let statistics = TableStatisticsBuilder::new(schema.clone())
            .with_num_rows_estimate(RowEstimate::Small)
            .with_partition_key();
        let table = PartitionedTableProvider::new(
            partition_selector,
            schema,
            sys_hot_keys_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default(),
        )
        .with_statistics(statistics.build());
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        access: Arc<dyn PartitionQueryAccess>,
    ) -> impl ScanPartition {
        LivePartitionScanner::new(
            access,
            |access, partition, keys| access.scan_hot_keys(partition, keys),
            |builder, row| append_hot_key_row(builder, &row),
        )
    }
}
