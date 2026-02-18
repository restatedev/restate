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
use std::ops::{ControlFlow, RangeInclusive};
use std::sync::Arc;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::idempotency_table::{IdempotencyMetadata, ScanIdempotencyTable};
use restate_types::identifiers::{IdempotencyId, PartitionKey};

use super::row::append_idempotency_row;
use super::schema::{SysIdempotencyBuilder, sys_idempotency_sort_order};
use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_idempotency";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        IdempotencyScanner,
    )) as Arc<dyn ScanPartition>;

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
    type Item<'a> = (IdempotencyId, IdempotencyMetadata);
    type ConversionError = std::convert::Infallible;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<(), StorageError>> + Send, StorageError> {
        partition_store
            .for_each_idempotency_metadata(range, move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (idempotency_id, idempotency_metadata): Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_idempotency_row(row_builder, idempotency_id, idempotency_metadata);
        Ok(())
    }
}
