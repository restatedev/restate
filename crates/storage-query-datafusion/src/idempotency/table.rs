// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use futures::Stream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::idempotency_table::{IdempotencyMetadata, ReadOnlyIdempotencyTable};
use restate_types::identifiers::{IdempotencyId, PartitionKey};

use super::row::append_idempotency_row;
use super::schema::IdempotencyBuilder;
use crate::context::{QueryContext, SelectPartitions};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::table_providers::PartitionedTableProvider;

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: PartitionStoreManager,
) -> datafusion::common::Result<()> {
    let table = PartitionedTableProvider::new(
        partition_selector,
        IdempotencyBuilder::schema(),
        LocalPartitionsScanner::new(partition_store_manager, IdempotencyScanner),
    );

    ctx.as_ref()
        .register_table("sys_idempotency", Arc::new(table))
        .map(|_| ())
}

#[derive(Clone, Debug)]
struct IdempotencyScanner;

impl ScanLocalPartition for IdempotencyScanner {
    type Builder = IdempotencyBuilder;
    type Item = (IdempotencyId, IdempotencyMetadata);

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send {
        partition_store.all_idempotency_metadata(range)
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
