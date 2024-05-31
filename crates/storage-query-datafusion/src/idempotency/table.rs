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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::Sender;

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
    async fn scan_partition_store(
        mut partition_store: PartitionStore,
        tx: Sender<Result<RecordBatch, datafusion::error::DataFusionError>>,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) {
        for_each_state(
            projection,
            tx,
            partition_store.all_idempotency_metadata(range),
        )
        .await;
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: impl Stream<Item = restate_storage_api::Result<(IdempotencyId, IdempotencyMetadata)>>,
) {
    let mut builder = IdempotencyBuilder::new(schema.clone());
    let mut temp = String::new();

    tokio::pin!(rows);
    while let Some(Ok((idempotency_id, idempotency_metadata))) = rows.next().await {
        append_idempotency_row(
            &mut builder,
            &mut temp,
            idempotency_id,
            idempotency_metadata,
        );
        if builder.full() {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = IdempotencyBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
