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
use tokio_stream::StreamExt;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::journal_table::ScanJournalTable;
use restate_storage_api::journal_table_v2::ScanJournalTable as ScanJournalTableV2;
use restate_types::identifiers::{JournalEntryId, PartitionKey};
use restate_types::storage::StoredRawEntry;

use crate::context::{QueryContext, SelectPartitions};
use crate::journal::row::{append_journal_row, append_journal_row_v2};
use crate::journal::schema::{SysJournalBuilder, sys_journal_sort_order};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_journal";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            JournalScanner,
        )) as Arc<dyn ScanPartition>
    });
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
    type Item = (JournalEntryId, ScannedEntry);

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>
    {
        let v1 = ScanJournalTable::scan_journals(partition_store, range.clone())?
            .map(|x| x.map(|(id, entry)| (id, ScannedEntry::V1(entry))));

        let v2 = ScanJournalTableV2::scan_journals(partition_store, range)?
            .map(|x| x.map(|(id, entry)| (id, ScannedEntry::V2(entry))));

        Ok(v1.merge(v2))
    }

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item) {
        match value.1 {
            ScannedEntry::V1(v1) => {
                append_journal_row(row_builder, string_buffer, value.0, v1);
            }
            ScannedEntry::V2(v2) => {
                append_journal_row_v2(row_builder, string_buffer, value.0, v2);
            }
        }
    }
}
