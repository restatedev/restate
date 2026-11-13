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

use restate_partition_store::inbox_table::InboxTable;
use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::scan::ScanOutcome;
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::inbox::row::append_inbox_row;
use crate::inbox::schema::{SysInboxBuilder, SysInboxTable, sys_inbox_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysInboxTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let table = PartitionedTableProvider::new(
            partition_selector,
            SysInboxBuilder::schema(),
            sys_inbox_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default()
                .with_service_key("service_key")
                .with_invocation_id("id"),
        );
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<InboxScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct InboxScanner;

impl ScanLocalPartition for InboxScanner {
    type Builder = SysInboxBuilder;
    type Filter = KeyRange;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        range: KeyRange,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        rows.read::<InboxTable>(
            snapshot,
            executor,
            "df-inbox",
            Arc::new(KeySelection::all().restrict_scope(range)),
            |builder, (key, mut value)| {
                let key = key.decode_all()?;
                append_inbox_row(
                    builder,
                    SequenceNumberInboxEntry::new(
                        key.sequence_number,
                        InboxEntry::decode(&mut value)?,
                    ),
                );
                Ok(())
            },
        )
        .await
    }
}
