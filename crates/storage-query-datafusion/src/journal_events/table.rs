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

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::journal_events::{ScanJournalEventsTable, StoredEvent};
use restate_types::identifiers::{JournalEventId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::journal_events::row::append_journal_event_row;
use crate::journal_events::schema::{SysJournalEventsBuilder, sys_journal_events_sort_order};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_journal_events";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            JournalEventsScanner,
        )) as Arc<dyn ScanPartition>
    });
    let journal_events_table = PartitionedTableProvider::new(
        partition_selector,
        SysJournalEventsBuilder::schema(),
        sys_journal_events_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(journal_events_table))
}

#[derive(Debug, Clone)]
struct JournalEventsScanner;

impl ScanLocalPartition for JournalEventsScanner {
    type Builder = SysJournalEventsBuilder;
    type Item = (JournalEventId, StoredEvent);

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>
    {
        ScanJournalEventsTable::scan_journal_events(partition_store, range.clone())
    }

    fn append_row(row_builder: &mut Self::Builder, value: Self::Item) {
        append_journal_event_row(row_builder, value.0, value.1)
    }
}
