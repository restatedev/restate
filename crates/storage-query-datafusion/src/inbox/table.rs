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
use restate_storage_api::inbox_table::{ReadOnlyInboxTable, SequenceNumberInboxEntry};
use restate_types::identifiers::PartitionKey;

use crate::context::{QueryContext, SelectPartitions};
use crate::inbox::row::append_inbox_row;
use crate::inbox::schema::InboxBuilder;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::table_providers::PartitionedTableProvider;

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: PartitionStoreManager,
) -> datafusion::common::Result<()> {
    let table = PartitionedTableProvider::new(
        partition_selector,
        InboxBuilder::schema(),
        LocalPartitionsScanner::new(partition_store_manager, InboxScanner),
    );

    ctx.as_ref()
        .register_table("sys_inbox", Arc::new(table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct InboxScanner;

impl ScanLocalPartition for InboxScanner {
    type Builder = InboxBuilder;
    type Item = SequenceNumberInboxEntry;

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send {
        partition_store.all_inboxes(range)
    }

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item) {
        append_inbox_row(row_builder, string_buffer, value);
    }
}
