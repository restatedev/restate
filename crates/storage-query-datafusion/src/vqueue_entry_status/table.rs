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
use restate_sharding::KeyRange;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{OwnedEntryStatusHeader, ScanVQueueEntryStatusTable};
use restate_types::vqueues::VQueueId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueue_entry_status::row::append_vqueue_entry_status_row;
use crate::vqueue_entry_status::schema::{
    SysVqueueEntryStatusBuilder, sys_vqueue_entry_status_sort_order,
};

const NAME: &str = "sys_vqueue_entry_status";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        VQueueEntryStatusScanner,
    )) as Arc<dyn ScanPartition>;

    let table = PartitionedTableProvider::new(
        partition_selector,
        SysVqueueEntryStatusBuilder::schema(),
        sys_vqueue_entry_status_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_vqueue_entry_id("entry_id")
            .with_partitioned_resource_id::<VQueueId>("vqueue_id"),
    );

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct VQueueEntryStatusScanner;

impl ScanLocalPartition for VQueueEntryStatusScanner {
    type Builder = SysVqueueEntryStatusBuilder;
    type Item<'a> = &'a OwnedEntryStatusHeader;
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
        partition_store
            .for_each_vqueue_entry_status(range, move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        header: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_vqueue_entry_status_row(row_builder, header);
        Ok(())
    }
}
