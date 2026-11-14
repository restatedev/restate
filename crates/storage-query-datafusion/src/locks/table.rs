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

use bilrost::OwnedMessage;
use datafusion::catalog::TableProvider;

use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::locks_table::LocksTable;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::LockState;
use restate_storage_api::scan::ScanOutcome;
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::locks::row::append_lock_row;
use crate::locks::schema::{SysLocksBuilder, SysLocksTable, sys_locks_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
impl SysLocksTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let locks_table = PartitionedTableProvider::new(
            partition_selector,
            SysLocksBuilder::schema(),
            sys_locks_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default()
                .with_scope("scope")
                .with_vqueue_entry_id("acquired_by"),
        );

        Arc::new(locks_table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<LocksScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct LocksScanner;

impl ScanLocalPartition for LocksScanner {
    type Builder = SysLocksBuilder;
    type Filter = KeyRange;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        range: KeyRange,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        rows.read::<LocksTable>(
            snapshot,
            executor,
            "df-locks",
            Arc::new(KeySelection::all().restrict_scope(range)),
            |builder, (key, value)| {
                let (partition_key, scope, name) = key.decode_all()?.split();
                let state = LockState::decode(value).map_err(StorageError::BilrostDecode)?;
                append_lock_row(builder, partition_key, scope, name, state);
                Ok(())
            },
        )
        .await
    }
}
