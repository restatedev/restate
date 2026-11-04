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
use std::task::{Context, Poll};

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::journal_table::{ScanJournalTable, ScanJournalTableRange};
use restate_storage_api::journal_table_v2::{
    ScanJournalTable as ScanJournalTableV2, ScanJournalTableRange as ScanJournalTableRangeV2,
};
use restate_storage_api::scan_sink::{ScanFlow, ScanSink};
use restate_types::identifiers::JournalEntryId;
use restate_types::storage::StoredRawEntry;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::filter::InvocationIdFilter;
use crate::journal::row::{append_journal_row, append_journal_row_v2};
use crate::journal::schema::{SysJournalBuilder, sys_journal_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::row_sink::RowSink;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_journal";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        JournalScanner,
    )) as Arc<dyn ScanPartition>;

    let journal_table = PartitionedTableProvider::new(
        partition_selector,
        SysJournalBuilder::schema(),
        sys_journal_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(journal_table))
}

// todo: fix this and box the large variant (JournalEntry is 304 bytes)
#[allow(clippy::large_enum_variant)]
pub(crate) enum ScannedEntry {
    V1(JournalEntry),
    V2(StoredRawEntry),
}

#[derive(Debug, Clone)]
struct JournalScanner;

impl ScanLocalPartition for JournalScanner {
    type Builder = SysJournalBuilder;
    type Item<'a> = (JournalEntryId, ScannedEntry);
    type ConversionError = std::convert::Infallible;
    type Filter = InvocationIdFilter;

    fn for_each_row(
        partition_store: &PartitionStore,
        filter: InvocationIdFilter,
        sink: RowSink<Self>,
    ) -> Result<impl Future<Output = restate_storage_api::Result<RowSink<Self>>> + Send, StorageError>
    {
        // Both journal tables feed the same sink, one after the other.
        let v1 = ScanJournalTable::for_each_journal(
            partition_store,
            filter.clone().into(),
            JournalSink(sink),
        )?;

        Ok(async move {
            let sink = v1.await?;
            let sink =
                ScanJournalTableV2::for_each_journal(partition_store, filter.into(), sink)?.await?;
            Ok(sink.0)
        })
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        match value.1 {
            ScannedEntry::V1(v1) => {
                append_journal_row(row_builder, value.0, v1);
            }
            ScannedEntry::V2(v2) => {
                append_journal_row_v2(row_builder, value.0, v2);
            }
        }

        Ok(())
    }
}

/// Feeds the entries of both journal tables into one row sink.
struct JournalSink(RowSink<JournalScanner>);

impl ScanFlow for JournalSink {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        self.0.poll_ready(cx)
    }
}

impl ScanSink<(JournalEntryId, JournalEntry)> for JournalSink {
    fn on_item(&mut self, (id, entry): (JournalEntryId, JournalEntry)) -> ControlFlow<()> {
        self.0.on_item((id, ScannedEntry::V1(entry)))
    }
}

impl ScanSink<(JournalEntryId, StoredRawEntry)> for JournalSink {
    fn on_item(&mut self, (id, entry): (JournalEntryId, StoredRawEntry)) -> ControlFlow<()> {
        self.0.on_item((id, ScannedEntry::V2(entry)))
    }
}

impl From<InvocationIdFilter> for ScanJournalTableRange {
    fn from(value: InvocationIdFilter) -> Self {
        if let Some(selection) = value.invocation_ids {
            let (start, last) = selection.bounds();
            ScanJournalTableRange::InvocationId(start..=last)
        } else {
            ScanJournalTableRange::PartitionKey(value.partition_keys)
        }
    }
}

impl From<InvocationIdFilter> for ScanJournalTableRangeV2 {
    fn from(value: InvocationIdFilter) -> Self {
        if let Some(selection) = value.invocation_ids {
            let (start, last) = selection.bounds();
            ScanJournalTableRangeV2::InvocationId(start..=last)
        } else {
            ScanJournalTableRangeV2::PartitionKey(value.partition_keys)
        }
    }
}
