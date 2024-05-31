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
use restate_storage_api::promise_table::{OwnedPromiseRow, ReadOnlyPromiseTable};
use restate_types::identifiers::PartitionKey;

use super::row::append_promise_row;
use super::schema::PromiseBuilder;
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
        PromiseBuilder::schema(),
        LocalPartitionsScanner::new(partition_store_manager, PromiseScanner),
    );

    ctx.as_ref()
        .register_table("sys_promise", Arc::new(table))
        .map(|_| ())
}

#[derive(Clone, Debug)]
struct PromiseScanner;

impl ScanLocalPartition for PromiseScanner {
    async fn scan_partition_store(
        mut partition_store: PartitionStore,
        tx: Sender<Result<RecordBatch, datafusion::error::DataFusionError>>,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) {
        for_each_state(projection, tx, partition_store.all_promises(range)).await;
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: impl Stream<Item = restate_storage_api::Result<OwnedPromiseRow>>,
) {
    let mut builder = PromiseBuilder::new(schema.clone());
    let mut temp = String::new();

    tokio::pin!(rows);
    while let Some(Ok(owned_promise_row)) = rows.next().await {
        append_promise_row(&mut builder, &mut temp, owned_promise_row);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = PromiseBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
