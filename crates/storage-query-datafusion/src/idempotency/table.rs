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
use restate_partition_store::idempotency_table::IdempotencyKey;
use restate_partition_store::keys::TableKey;
use restate_partition_store::protobuf_types::PartitionStoreProtobufValue;
use restate_partition_store::scan::TableScan;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_types::identifiers::{IdempotencyId, PartitionKey};

use super::row::append_idempotency_row;
use super::schema::{SysIdempotencyBuilder, sys_idempotency_sort_order};
use crate::context::{QueryContext, SelectPartitions};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_idempotency";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            IdempotencyScanner,
        )) as Arc<dyn ScanPartition>
    });
    let table = PartitionedTableProvider::new(
        partition_selector,
        SysIdempotencyBuilder::schema(),
        sys_idempotency_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("service_key")
            .with_invocation_id("invocation_id"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Clone, Debug)]
struct IdempotencyScanner;

impl ScanLocalPartition for IdempotencyScanner {
    type Builder = SysIdempotencyBuilder;
    type Item = (IdempotencyId, IdempotencyMetadata);

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>
    {
        partition_store
            .run_iterator(
                "df-idempotency",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<IdempotencyKey>(range),
                |(mut key, mut value)| {
                    let key = IdempotencyKey::deserialize_from(&mut key)?;
                    let idempotency_metadata = IdempotencyMetadata::decode(&mut value)?;

                    Ok((
                        IdempotencyId::new(
                            key.service_name_ok_or()?.clone(),
                            key.service_key
                                .clone()
                                .map(|b| {
                                    ByteString::try_from(b)
                                        .map_err(|e| StorageError::Generic(e.into()))
                                })
                                .transpose()?,
                            key.service_handler_ok_or()?.clone(),
                            key.idempotency_key_ok_or()?.clone(),
                        ),
                        idempotency_metadata,
                    ))
                },
            )
            .map_err(|_| StorageError::OperationalError)
    }

    fn append_row(
        row_builder: &mut Self::Builder,
        string_buffer: &mut String,
        (idempotency_id, idempotency_metadata): Self::Item,
    ) {
        append_idempotency_row(
            row_builder,
            string_buffer,
            idempotency_id,
            idempotency_metadata,
        );
    }
}
