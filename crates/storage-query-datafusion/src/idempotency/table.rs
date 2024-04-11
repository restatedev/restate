// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::row::append_idempotency_row;
use super::schema::IdempotencyBuilder;

use crate::context::QueryContext;
use crate::generic_table::{GenericTableProvider, RangeScanner};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
pub use datafusion_expr::UserDefinedLogicalNode;
use futures::{Stream, StreamExt};
use restate_storage_api::idempotency_table::{IdempotencyMetadata, ReadOnlyIdempotencyTable};
use restate_types::identifiers::{IdempotencyId, PartitionKey};
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self<I: ReadOnlyIdempotencyTable + Debug + Clone + Sync + Send + 'static>(
    ctx: &QueryContext,
    storage: I,
) -> datafusion::common::Result<()> {
    let table = GenericTableProvider::new(
        IdempotencyBuilder::schema(),
        Arc::new(IdempotencyScanner(storage)),
    );

    ctx.as_ref()
        .register_table("sys_idempotency", Arc::new(table))
        .map(|_| ())
}

#[derive(Clone, Debug)]
struct IdempotencyScanner<I>(I);

impl<I: ReadOnlyIdempotencyTable + Debug + Clone + Sync + Send + 'static> RangeScanner
    for IdempotencyScanner<I>
{
    fn scan(
        &self,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let mut db = self.0.clone();
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let background_task = async move {
            for_each_state(schema, tx, db.all_idempotency_metadata(range)).await;
            Ok(())
        };
        stream_builder.spawn(background_task);
        stream_builder.build()
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
            if tx.send(Ok(batch)).await.is_err() {
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
        let _ = tx.send(Ok(result)).await;
    }
}
