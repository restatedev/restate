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
use std::ops::Bound;
use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_partition_store::journal_scan::{JournalTable, JournalV2Table};
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::journal_table_v2::StoredEntry;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::scan::ScanOutcome;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::journal::JournalClause;
use restate_types::identifiers::{InvocationId, JournalEntryId};

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::filter::InvocationIdFilter;
use crate::journal::row::{append_journal_row, append_journal_row_v2};
use crate::journal::schema::{SysJournalBuilder, SysJournalTable, sys_journal_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysJournalTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let journal_table = PartitionedTableProvider::new(
            partition_selector,
            SysJournalBuilder::schema(),
            sys_journal_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id"),
        );
        Arc::new(journal_table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<JournalScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct JournalScanner;

impl ScanLocalPartition for JournalScanner {
    type Builder = SysJournalBuilder;
    type Filter = InvocationIdFilter;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: InvocationIdFilter,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        let selection = match filter.invocation_ids {
            Some(ids) => Selection::All.and(JournalClause::Id(ValuePredicate::In(
                ids.ids.into_iter().collect(),
            ))),
            None => Selection::All,
        };
        let selection = selection.and(JournalClause::PartitionKey(ValuePredicate::Range {
            lower: Bound::Included(filter.partition_keys.start()),
            upper: Bound::Included(filter.partition_keys.end()),
        }));
        let rows = match rows
            .read::<JournalTable>(
                snapshot,
                executor,
                "df-journal",
                Arc::clone(JournalTable::prepare(&selection)?.request()),
                |builder, (key, mut value)| {
                    let (partition_key, uuid, index) = key.decode_all()?.split();
                    let id = JournalEntryId::from_parts(
                        InvocationId::from_parts(partition_key, uuid),
                        index,
                    );
                    append_journal_row(builder, id, JournalEntry::decode(&mut value)?);
                    Ok(())
                },
            )
            .await?
        {
            ScanOutcome::Exhausted(rows) => rows,
            stopped => return Ok(stopped),
        };
        rows.read::<JournalV2Table>(
            snapshot,
            executor,
            "df-journal-v2",
            Arc::clone(JournalV2Table::prepare(&selection)?.request()),
            |builder, (key, mut value)| {
                let (partition_key, uuid, index) = key.decode_all()?.split();
                let id = JournalEntryId::from_parts(
                    InvocationId::from_parts(partition_key, uuid),
                    index,
                );
                append_journal_row_v2(builder, id, StoredEntry::decode(&mut value)?.0);
                Ok(())
            },
        )
        .await
    }
}
