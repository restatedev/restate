// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_datafusion::{
    context::{QueryContext, SelectPartitions},
    partition_filter::FirstMatchingPartitionKeyExtractor,
    partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition},
    remote_query_scanner_manager::RemoteScannerManager,
    table_providers::{PartitionedTableProvider, ScanPartition},
};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::inbox_table::{ReadOnlyInboxTable, SequenceNumberInboxEntry};
use restate_types::identifiers::PartitionKey;

use crate::inbox::row::append_inbox_row;

use super::schema::{SysInboxBuilder, sys_inbox_sort_order};

const NAME: &str = "sys_inbox";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_partition_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            InboxScanner,
        )) as Arc<dyn ScanPartition>
    });

    let table = PartitionedTableProvider::new(
        partition_selector,
        SysInboxBuilder::schema(),
        sys_inbox_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_partition_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("service_key")
            .with_invocation_id("id"),
    );
    ctx.register_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct InboxScanner;

impl ScanLocalPartition for InboxScanner {
    type Builder = SysInboxBuilder;
    type Item = SequenceNumberInboxEntry;

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>
    {
        partition_store.all_inboxes(range)
    }

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item) {
        append_inbox_row(row_builder, string_buffer, value);
    }
}
