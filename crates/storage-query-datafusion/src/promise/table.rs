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

use bytestring::ByteString;
use datafusion::catalog::TableProvider;

use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::promise_table::{PromiseTable, ScopedPromiseTable};
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::promise_table::{OwnedPromiseRow, Promise};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::scan::ScanOutcome;
use restate_types::identifiers::ServiceId;
use restate_types::sharding::KeyRange;

use super::row::append_promise_row;
use super::schema::{SysPromiseBuilder, SysPromiseTable, sys_promise_sort_order};
use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysPromiseTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let table = PartitionedTableProvider::new(
            partition_selector,
            SysPromiseBuilder::schema(),
            sys_promise_sort_order(),
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
        LocalPartitionsScanner::<PromiseScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Clone, Debug)]
struct PromiseScanner;

impl ScanLocalPartition for PromiseScanner {
    type Builder = SysPromiseBuilder;
    type Filter = KeyRange;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        range: KeyRange,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        let rows = match rows
            .read::<PromiseTable>(
                snapshot,
                executor,
                "df-promise",
                Arc::new(KeySelection::all().restrict_scope(range)),
                |builder, (key, mut value)| {
                    let (partition_key, service_name, service_key, key) = key.decode_all()?.split();
                    let service_key = ByteString::try_from(service_key)
                        .map_err(|err| StorageError::Generic(err.into()))?;
                    append_promise_row(
                        builder,
                        OwnedPromiseRow {
                            service_id: ServiceId::with_partition_key(
                                partition_key,
                                service_name,
                                service_key,
                            ),
                            key,
                            metadata: Promise::decode(&mut value)?,
                        },
                    );
                    Ok(())
                },
            )
            .await?
        {
            ScanOutcome::Exhausted(rows) => rows,
            stopped => return Ok(stopped),
        };
        rows.read::<ScopedPromiseTable>(
            snapshot,
            executor,
            "df-promise-scoped",
            Arc::new(KeySelection::all().restrict_scope(range)),
            |builder, (key, mut value)| {
                let (_, scope, service_name, service_key, key) = key.decode_all()?.split();
                append_promise_row(
                    builder,
                    OwnedPromiseRow {
                        service_id: ServiceId::new(
                            scope,
                            service_name.as_str(),
                            service_key.as_str(),
                        ),
                        key: ByteString::from(key.as_str()),
                        metadata: Promise::decode(&mut value)?,
                    },
                );
                Ok(())
            },
        )
        .await
    }
}
