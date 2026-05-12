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
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, ScanVQueueEntries, Stage};
use restate_types::vqueues::VQueueId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueues::row::append_vqueues_row;
use crate::vqueues::schema::{SysVqueuesBuilder, sys_vqueues_sort_order};

const NAME: &str = "sys_vqueues";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        VQueuesScanner,
    )) as Arc<dyn ScanPartition>;

    let table = PartitionedTableProvider::new(
        partition_selector,
        SysVqueuesBuilder::schema(),
        sys_vqueues_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_partitioned_resource_id::<VQueueId>("id")
            .with_vqueue_entry_id("entry_id"),
    );

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct VQueuesScanner;

impl ScanLocalPartition for VQueuesScanner {
    type Builder = SysVqueuesBuilder;
    type Item<'a> = (&'a VQueueId, Stage, &'a EntryKey, &'a EntryValue);
    type ConversionError = std::convert::Infallible;
    type Filter = VQueueFilter;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: VQueueFilter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.for_each_vqueue_entry(
            filter.partition_keys,
            filter.stages.unwrap_or_default(),
            move |item| f(item).map_break(Result::unwrap),
        )
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        let (qid, stage, entry_key, entry) = value;
        append_vqueues_row(row_builder, qid, stage, entry_key, entry);
        Ok(())
    }
}
