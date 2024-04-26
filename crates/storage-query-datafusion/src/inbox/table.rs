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

use crate::context::QueryContext;
use crate::generic_table::{GenericTableProvider, RangeScanner};
use crate::inbox::row::append_inbox_row;
use crate::inbox::schema::InboxBuilder;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
pub use datafusion_expr::UserDefinedLogicalNode;
use futures::{Stream, StreamExt};
use restate_partition_store::PartitionStore;
use restate_storage_api::inbox_table::{InboxTable, SequenceNumberInboxEntry};
use restate_storage_api::StorageError;
use restate_types::identifiers::PartitionKey;
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self(
    ctx: &QueryContext,
    storage: PartitionStore,
) -> datafusion::common::Result<()> {
    let table = GenericTableProvider::new(InboxBuilder::schema(), Arc::new(InboxScanner(storage)));

    ctx.as_ref()
        .register_table("sys_inbox", Arc::new(table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct InboxScanner(PartitionStore);

impl RangeScanner for InboxScanner {
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
            let mut transaction = db.transaction();
            let rows = transaction.all_inboxes(range);
            for_each_state(schema, tx, rows).await;
            Ok(())
        };
        stream_builder.spawn(background_task);
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: impl Stream<Item = Result<SequenceNumberInboxEntry, StorageError>>,
) {
    let mut builder = InboxBuilder::new(schema.clone());
    let mut temp = String::new();

    tokio::pin!(rows);
    while let Some(Ok(row)) = rows.next().await {
        append_inbox_row(&mut builder, &mut temp, row);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(Ok(batch)).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = InboxBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(Ok(result)).await;
    }
}
