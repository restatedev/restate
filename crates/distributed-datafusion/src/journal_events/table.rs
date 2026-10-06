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

use datafusion::catalog::TableProvider;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::journal_events::{
    EventView, ScanJournalEventsTable, ScanJournalEventsTableRange,
};
use restate_types::identifiers::InvocationId;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::filter::InvocationIdFilter;
use crate::journal_events::row::append_journal_event_row;
use crate::journal_events::schema::{
    SysJournalEventsBuilder, SysJournalEventsTable, sys_journal_events_sort_order,
};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysJournalEventsTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let journal_events_table = PartitionedTableProvider::new(
            partition_selector,
            SysJournalEventsBuilder::schema(),
            sys_journal_events_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id"),
        );
        Arc::new(journal_events_table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<JournalEventsScanner>::new(partition_store_manager)
    }
}

#[derive(Debug, Clone)]
struct JournalEventsScanner;

impl ScanLocalPartition for JournalEventsScanner {
    type Builder = SysJournalEventsBuilder;
    type Item<'a> = (InvocationId, EventView);
    type ConversionError = std::convert::Infallible;
    type Filter = InvocationIdFilter;

    fn for_each_row<
        F: for<'a> FnMut(
                Self::Item<'a>,
            ) -> std::ops::ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: InvocationIdFilter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store
            .for_each_journal_event(range.into(), move |item| f(item).map_break(Result::unwrap))
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
