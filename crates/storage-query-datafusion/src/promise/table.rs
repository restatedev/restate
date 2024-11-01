// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::Stream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::promise_table::{OwnedPromiseRow, ReadOnlyPromiseTable};
use restate_types::identifiers::PartitionKey;

use super::row::append_promise_row;
use super::schema::SysPromiseBuilder;
use crate::context::{QueryContext, SelectPartitions};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_promise";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            PromiseScanner,
        )) as Arc<dyn ScanPartition>
    });
    let table = PartitionedTableProvider::new(
        partition_selector,
        SysPromiseBuilder::schema(),
        ctx.create_distributed_scanner(NAME, local_scanner),
    );
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Clone, Debug)]
struct PromiseScanner;

impl ScanLocalPartition for PromiseScanner {
    type Builder = SysPromiseBuilder;
    type Item = OwnedPromiseRow;

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send {
        partition_store.all_promises(range)
    }

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item) {
        append_promise_row(row_builder, string_buffer, value);
    }
}
