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
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::journal_table::{ScanJournalTable, ScanJournalTableRange};
use restate_storage_api::journal_table_v2::{
    ScanJournalTable as ScanJournalTableV2, ScanJournalTableRange as ScanJournalTableRangeV2,
};
use restate_storage_api::scan::{RowConsumer, ScanClosePolicy, ScanOutcome};
use restate_types::identifiers::JournalEntryId;
use restate_types::storage::StoredRawEntry;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::filter::InvocationIdFilter;
use crate::journal::row::{append_journal_row, append_journal_row_v2};
use crate::journal::schema::{SysJournalBuilder, sys_journal_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
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

struct V1Consumer<C>(C);

impl<C> RowConsumer for V1Consumer<C>
where
    C: for<'a> RowConsumer<Item<'a> = (JournalEntryId, ScannedEntry)>,
{
    type Item<'a> = (JournalEntryId, JournalEntry);

    const SCAN_CLOSE_POLICY: ScanClosePolicy = C::SCAN_CLOSE_POLICY;

    async fn consume(
        &mut self,
        (id, entry): Self::Item<'_>,
    ) -> restate_storage_api::Result<ControlFlow<()>> {
        self.0.consume((id, ScannedEntry::V1(entry))).await
    }
}

struct V2Consumer<C>(C);

impl<C> RowConsumer for V2Consumer<C>
where
    C: for<'a> RowConsumer<Item<'a> = (JournalEntryId, ScannedEntry)>,
{
    type Item<'a> = (JournalEntryId, StoredRawEntry);

    const SCAN_CLOSE_POLICY: ScanClosePolicy = C::SCAN_CLOSE_POLICY;

    async fn consume(
        &mut self,
        (id, entry): Self::Item<'_>,
    ) -> restate_storage_api::Result<ControlFlow<()>> {
        self.0.consume((id, ScannedEntry::V2(entry))).await
    }
}

impl ScanLocalPartition for JournalScanner {
    type Builder = SysJournalBuilder;
    type Item<'a> = (JournalEntryId, ScannedEntry);
    type ConversionError = std::convert::Infallible;
    type Filter = InvocationIdFilter;

    fn for_each_row<F: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>>(
        partition_store: &PartitionStore,
        filter: InvocationIdFilter,
        f: F,
    ) -> Result<
        impl Future<Output = restate_storage_api::Result<ScanOutcome<F>>> + Send,
        StorageError,
    > {
        // The v1 scan runs first and hands the consumer to the v2 scan. In
        // practice, there are not typically any keys in the v1 table.
        Ok(async move {
            let first = ScanJournalTable::for_each_journal(
                partition_store,
                filter.clone().into(),
                V1Consumer(f),
            )?
            .await?
            .map(|V1Consumer(f)| f);
            let f = match first {
                ScanOutcome::Exhausted(f) => f,
                stopped @ ScanOutcome::Stopped(_) => return Ok(stopped),
            };
            let second = ScanJournalTableV2::for_each_journal(
                partition_store,
                filter.into(),
                V2Consumer(f),
            )?
            .await?;
            Ok(second.map(|V2Consumer(f)| f))
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
