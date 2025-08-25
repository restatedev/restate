// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use restate_types::Versioned;
use restate_types::logs::LogletId;
use tokio::sync::mpsc::Sender;

use restate_core::Metadata;
use restate_types::logs::metadata::Logs;

use crate::context::QueryContext;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

use super::row::append_segment_row;
use super::schema::LogBuilder;

pub fn register_self(ctx: &QueryContext, metadata: Metadata) -> datafusion::common::Result<()> {
    let logs_table =
        GenericTableProvider::new(LogBuilder::schema(), Arc::new(LogScanner(metadata)));
    ctx.register_non_partitioned_table("logs", Arc::new(logs_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("LogScanner")]
struct LogScanner(Metadata);

impl Scan for LogScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();

        let logs = self.0.logs_snapshot();
        stream_builder.spawn(async move {
            for_each_log(schema, tx, logs, batch_size).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_log(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    logs: Arc<Logs>,
    batch_size: usize,
) {
    let mut builder = LogBuilder::new(schema.clone());
    for (id, chain) in logs.iter() {
        for segment in chain.iter() {
            let loglet_id = LogletId::new(*id, segment.index());
            let replicated_loglet = logs.get_replicated_loglet(&loglet_id);

            append_segment_row(
                &mut builder,
                logs.version(),
                *id,
                &segment,
                replicated_loglet,
            );

            if builder.num_rows() >= batch_size {
                let batch = builder.finish_and_new();
                if tx.send(batch).await.is_err() {
                    // not sure what to do here?
                    // the other side has hung up on us.
                    // we probably don't want to panic, is it will cause the entire process to exit
                    return;
                }
            }
        }
    }

    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
