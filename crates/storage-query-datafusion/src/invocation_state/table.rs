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
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use tokio::sync::mpsc::Sender;

use restate_invoker_api::{InvocationStatusReport, StatusHandle};
use restate_types::identifiers::{PartitionKey, WithPartitionKey};

use crate::context::QueryContext;
use crate::invocation_state::row::append_invocation_state_row;
use crate::invocation_state::schema::SysInvocationStateBuilder;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

pub(crate) fn register_self(
    ctx: &QueryContext,
    status: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
) -> datafusion::common::Result<()> {
    let status_table = GenericTableProvider::new(
        SysInvocationStateBuilder::schema(),
        Arc::new(StatusScanner(status)),
    );

    ctx.as_ref()
        .register_table("sys_invocation_state", Arc::new(status_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct StatusScanner<S>(S);

impl<S: StatusHandle + Send + Sync + Debug + Clone + 'static> Scan for StatusScanner<S> {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let range = PartitionKey::MIN..=PartitionKey::MAX;
        let status = self.0.clone();
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let background_task = async move {
            let rows = status.read_status(range).await;
            for_each_state(schema, tx, rows).await;
            Ok(())
        };
        stream_builder.spawn(background_task);
        stream_builder.build()
    }
}

async fn for_each_state<'a, I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
) where
    I: Iterator<Item = InvocationStatusReport> + 'a,
{
    let mut builder = SysInvocationStateBuilder::new(schema.clone());
    let mut temp = String::new();
    let mut rows = rows.collect::<Vec<_>>();
    // need to be ordered by partition key for symmetric joins
    rows.sort_unstable_by_key(|row| row.invocation_id().partition_key());
    for row in rows {
        append_invocation_state_row(&mut builder, &mut temp, row);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = SysInvocationStateBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
