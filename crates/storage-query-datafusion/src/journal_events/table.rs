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
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::journal_events::{EventView, ScanJournalEventsTableRange};
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_types::identifiers::InvocationId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::filter::InvocationIdFilter;
use crate::journal_events::row::append_journal_event_row;
use crate::journal_events::schema::{SysJournalEventsBuilder, sys_journal_events_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_journal_events";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        JournalEventsScanner,
    )) as Arc<dyn ScanPartition>;

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
    type Item<'a> = (InvocationId, EventView);
    type ConversionError = std::convert::Infallible;
    type Filter = InvocationIdFilter;

    fn for_each_row<'s, F: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>>(
        partition_store: &'s PartitionStore,
        executor: &'s (impl IoExecutor + Sync),
        range: InvocationIdFilter,
        f: F,
    ) -> Result<
        impl Future<Output = restate_storage_api::Result<ScanOutcome<F>>> + Send,
        StorageError,
    > {
        partition_store.for_each_journal_event(executor, range.into(), f)
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_journal_event_row(row_builder, value.0, value.1);
        Ok(())
    }
}

impl From<InvocationIdFilter> for ScanJournalEventsTableRange {
    fn from(value: InvocationIdFilter) -> Self {
        if let Some(selection) = value.invocation_ids {
            let (start, last) = selection.bounds();
            ScanJournalEventsTableRange::InvocationId(start..=last)
        } else {
            ScanJournalEventsTableRange::PartitionKey(value.partition_keys)
        }
    }
}
