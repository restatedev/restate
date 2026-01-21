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
use std::ops::RangeInclusive;
use std::sync::Arc;

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

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        // these two iterators can run concurrently in theory.
        // in practice, there are not typically any keys in the first iterator, but rust rightfully forces us to use a mutex to protect the FnMut
        // the intent here is that iterators race to produce the first row, the winner gets an owned lock guard which it then holds until its done iterating
        // and then the next iterator is able to get a lock guard.

        let (v1, v2) = {
            // once we've started iterating, this arc must be dropped from inside the io threads and not in an async context
            let mut f_v1 = Some(Arc::new(tokio::sync::Mutex::new(f)));
            let mut f_v2 = f_v1.clone();

            let v1 = ScanJournalTable::for_each_journal(partition_store, range.clone(), {
                let mut f_locked = None;
                move |(id, entry)| {
                    let f = f_locked.get_or_insert_with(|| {
                        f_v1.take()
                            .expect("we only take f_v1 once")
                            .blocking_lock_owned()
                    });
                    f((id, ScannedEntry::V1(entry))).map_break(Result::unwrap)
                }
            })?;

            let v2 = ScanJournalTableV2::for_each_journal(partition_store, range, {
                let mut f_locked = None;
                move |(id, entry)| {
                    let f = f_locked.get_or_insert_with(|| {
                        f_v2.take()
                            .expect("we only take f_v2 once")
                            .blocking_lock_owned()
                    });
                    f((id, ScannedEntry::V2(entry))).map_break(Result::unwrap)
                }
            })?;

            (v1, v2)
        };

        Ok(async {
            v1.await?;
            v2.await?;
            Ok(())
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
