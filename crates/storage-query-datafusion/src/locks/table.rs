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

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::{LockState, ScanLocksTable};
use restate_types::identifiers::PartitionKey;
use restate_types::sharding::KeyRange;
use restate_types::{LockName, Scope};

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::locks::row::append_lock_row;
use crate::locks::schema::{SysLocksBuilder, sys_locks_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
const NAME: &str = "sys_locks";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        LocksScanner,
    )) as Arc<dyn ScanPartition>;

    let locks_table = PartitionedTableProvider::new(
        partition_selector,
        SysLocksBuilder::schema(),
        sys_locks_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_scope("scope")
            .with_invocation_id("acquired_by"),
    );

    ctx.register_partitioned_table(NAME, Arc::new(locks_table))
}

#[derive(Debug, Clone)]
struct LocksScanner;

impl ScanLocalPartition for LocksScanner {
    type Builder = SysLocksBuilder;
    type Item<'a> = (PartitionKey, Option<Scope>, LockName, LockState);
    type ConversionError = std::convert::Infallible;
    type Filter = KeyRange;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: KeyRange,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.for_each_lock(range, move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_lock_row(row_builder, value.0, value.1, value.2, value.3);
        Ok(())
    }
}
