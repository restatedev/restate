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

use restate_partition_store::journal_events::decode_event;
use restate_partition_store::journal_scan::JournalEventsTable;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::scan::ScanOutcome;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::journal_events::JournalEventsClause;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::filter::InvocationIdFilter;
use crate::journal_events::row::append_journal_event_row;
use crate::journal_events::schema::{
    SysJournalEventsBuilder, SysJournalEventsTable, sys_journal_events_sort_order,
};
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
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
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<JournalEventsScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct JournalEventsScanner;

impl ScanLocalPartition for JournalEventsScanner {
    type Builder = SysJournalEventsBuilder;
    type Filter = InvocationIdFilter;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: InvocationIdFilter,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        let selection = match filter.invocation_ids {
            Some(ids) => Selection::All.and(JournalEventsClause::Id(ValuePredicate::In(
                ids.ids.into_iter().collect(),
            ))),
            None => Selection::All,
        };
        let selection = selection.and(JournalEventsClause::PartitionKey(ValuePredicate::Range {
            lower: Bound::Included(filter.partition_keys.start()),
            upper: Bound::Included(filter.partition_keys.end()),
        }));
        rows.read::<JournalEventsTable>(
            snapshot,
            executor,
            "df-journal-events",
            Arc::clone(JournalEventsTable::prepare(&selection)?.request()),
            |builder, (key, mut value)| {
                let (id, event) = decode_event(key.decode_all()?, &mut value)?;
                append_journal_event_row(builder, id, event);
                Ok(())
            },
        )
        .await
    }
}
