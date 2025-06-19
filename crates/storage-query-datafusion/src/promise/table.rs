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

use bytestring::ByteString;
use futures::Stream;

use restate_partition_store::Priority;
use restate_partition_store::keys::TableKey;
use restate_partition_store::promise_table::PromiseKey;
use restate_partition_store::protobuf_types::PartitionStoreProtobufValue;
use restate_partition_store::scan::TableScan;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::promise_table::{OwnedPromiseRow, Promise};
use restate_types::identifiers::{PartitionKey, ServiceId};

use super::row::append_promise_row;
use super::schema::{SysPromiseBuilder, sys_promise_sort_order};
use crate::context::{QueryContext, SelectPartitions};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_promise";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            PromiseScanner,
        )) as Arc<dyn ScanPartition>
    });
    let table = PartitionedTableProvider::new(
        partition_selector,
        SysPromiseBuilder::schema(),
        sys_promise_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Clone, Debug)]
struct PromiseScanner;

impl ScanLocalPartition for PromiseScanner {
    type Builder = SysPromiseBuilder;
    type Item = OwnedPromiseRow;

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>
    {
        partition_store
            .run_iterator(
                "df-promise",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<PromiseKey>(range),
                |(mut k, mut v)| {
                    let key = PromiseKey::deserialize_from(&mut k)?;
                    let metadata = Promise::decode(&mut v)?;

                    let (partition_key, service_name, service_key, promise_key) =
                        key.into_inner_ok_or()?;

                    Ok(OwnedPromiseRow {
                        service_id: ServiceId::with_partition_key(
                            partition_key,
                            service_name,
                            ByteString::try_from(service_key)
                                .map_err(|e| anyhow::anyhow!("Cannot convert to string {e}"))?,
                        ),
                        key: promise_key,
                        metadata,
                    })
                },
            )
            .map_err(|_| StorageError::OperationalError)
    }

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item) {
        append_promise_row(row_builder, string_buffer, value);
    }
}
