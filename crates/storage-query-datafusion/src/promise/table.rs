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
use std::sync::Arc;

use restate_core::io::IoExecutor;
use restate_partition_store::promise_table::PromiseScan;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::promise_table::OwnedPromiseRow;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_types::sharding::KeyRange;

use super::row::append_promise_row;
use super::schema::{SysPromiseBuilder, sys_promise_sort_order};
use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_promise";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        PromiseScanner,
    )) as Arc<dyn ScanPartition>;

    let table = PartitionedTableProvider::new(
        partition_selector,
        SysPromiseBuilder::schema(),
        sys_promise_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_scope_or_service_key("scope", "service_key"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Clone, Debug)]
struct PromiseScanner;

impl ScanLocalPartition for PromiseScanner {
    type Builder = SysPromiseBuilder;
    type Item<'a> = OwnedPromiseRow;
    type ConversionError = std::convert::Infallible;
    type Filter = KeyRange;

    fn for_each_row<'s, F: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>>(
        partition_store: &'s PartitionStore,
        executor: &'s (impl IoExecutor + Sync),
        range: KeyRange,
        f: F,
    ) -> impl Future<Output = restate_storage_api::Result<ScanOutcome<F>>> + Send {
        partition_store.scan(executor, PromiseScan::new(partition_store, range), f)
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_promise_row(row_builder, value);
        Ok(())
    }
}
