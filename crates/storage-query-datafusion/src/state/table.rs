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

use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::state_table::{ScopedStateTable, StateTable as StoredStateTable};
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::scan::ScanOutcome;
use restate_types::identifiers::ServiceId;
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::state::row::append_state_row;
use crate::state::schema::{StateBuilder, StateTable, state_sort_order};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl StateTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let table = PartitionedTableProvider::new(
            partition_selector,
            StateBuilder::schema(),
            state_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default()
                .with_scope_or_service_key("scope", "service_key"),
        );
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<StateScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct StateScanner;

impl ScanLocalPartition for StateScanner {
    type Builder = StateBuilder;
    type Filter = KeyRange;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        range: KeyRange,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        let rows = match rows
            .read::<StoredStateTable>(
                snapshot,
                executor,
                "df-user-state",
                Arc::new(KeySelection::all().restrict_scope(range)),
                |builder, (key, value)| {
                    let (partition_key, service_name, service_key, state_key) =
                        key.decode_all()?.split();
                    append_state_row(
                        builder,
                        ServiceId::from_parts(partition_key, service_name, service_key),
                        state_key,
                        value,
                    );
                    Ok(())
                },
            )
            .await?
        {
            ScanOutcome::Exhausted(rows) => rows,
            stopped => return Ok(stopped),
        };
        rows.read::<ScopedStateTable>(
            snapshot,
            executor,
            "df-user-state-scoped",
            Arc::new(KeySelection::all().restrict_scope(range)),
            |builder, (key, value)| {
                let (_, scope, service_name, service_key, state_key) = key.decode_all()?.split();
                append_state_row(
                    builder,
                    ServiceId::new(scope, service_name.as_str(), service_key.as_str()),
                    state_key,
                    value,
                );
                Ok(())
            },
        )
        .await
    }
}
