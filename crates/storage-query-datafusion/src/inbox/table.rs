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
use futures::StreamExt;
use restate_schema_api::key::RestateKeyConverter;
use restate_storage_api::inbox_table::{InboxEntry, InboxTable};
use restate_storage_api::GetStream;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::PartitionKey;
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self(
    ctx: &QueryContext,
    storage: RocksDBStorage,
    resolver: impl RestateKeyConverter + Send + Sync + Debug + Clone + 'static,
) -> datafusion::common::Result<()> {
    let table = GenericTableProvider::new(
        InboxBuilder::schema(),
        Arc::new(InboxScanner(storage, resolver)),
    );

    ctx.as_ref()
        .register_table("sys_inbox", Arc::new(table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct InboxScanner<R>(RocksDBStorage, R);

impl<R: RestateKeyConverter + Send + Sync + Debug + Clone + 'static> RangeScanner
    for InboxScanner<R>
{
    fn scan(
        &self,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let db = self.0.clone();
        let schema = projection.clone();
        let resolver = self.1.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let background_task = async move {
            let mut transaction = db.transaction();
            let rows = transaction.all_inboxes(range);
            for_each_state(schema, tx, rows, resolver).await;
        };
        stream_builder.spawn(background_task);
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    mut rows: GetStream<'_, InboxEntry>,
    resolver: impl RestateKeyConverter + Clone,
) {
    let mut builder = InboxBuilder::new(schema.clone());
    let mut temp = String::new();
    while let Some(Ok(row)) = rows.next().await {
        append_inbox_row(&mut builder, &mut temp, row, resolver.clone());
        if builder.full() {
            let batch = builder.finish();
            if tx.blocking_send(Ok(batch)).is_err() {
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
        let _ = tx.blocking_send(Ok(result));
    }
}
