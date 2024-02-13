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
use crate::status::row::append_status_row;
use crate::status::schema::StatusBuilder;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
pub use datafusion_expr::UserDefinedLogicalNode;
use restate_storage_rocksdb::status_table::OwnedStatusRow;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::PartitionKey;
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self(
    ctx: &QueryContext,
    storage: RocksDBStorage,
) -> datafusion::common::Result<()> {
    let status_table =
        GenericTableProvider::new(StatusBuilder::schema(), Arc::new(StatusScanner(storage)));

    ctx.as_ref()
        .register_table("sys_status", Arc::new(status_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct StatusScanner(RocksDBStorage);

impl RangeScanner for StatusScanner {
    fn scan(
        &self,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let db = self.0.clone();
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let background_task = move || {
            let rows = db.all_status(range);
            for_each_status(schema, tx, rows);
            Ok(())
        };
        stream_builder.spawn_blocking(background_task);
        stream_builder.build()
    }
}

fn for_each_status<'a, I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
) where
    I: Iterator<Item = OwnedStatusRow> + 'a,
{
    let mut builder = StatusBuilder::new(schema.clone());
    let mut temp = String::new();
    for row in rows {
        append_status_row(&mut builder, &mut temp, row);
        if builder.full() {
            let batch = builder.finish();
            if tx.blocking_send(Ok(batch)).is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = StatusBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.blocking_send(Ok(result));
    }
}
