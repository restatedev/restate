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
use crate::invocation_state::row::append_state_row;
use crate::invocation_state::schema::StateBuilder;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
pub use datafusion_expr::UserDefinedLogicalNode;
use restate_invoker_api::{InvocationStatusReport, StatusHandle};
use restate_schema_api::key::RestateKeyConverter;
use restate_types::identifiers::{PartitionKey, WithPartitionKey};
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self(
    ctx: &QueryContext,
    status: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
    resolver: impl RestateKeyConverter + Send + Sync + Debug + Clone + 'static,
) -> datafusion::common::Result<()> {
    let status_table = GenericTableProvider::new(
        StateBuilder::schema(),
        Arc::new(StatusScanner(status, resolver)),
    );

    ctx.as_ref()
        .register_table("sys_invocation_state", Arc::new(status_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct StatusScanner<S, R>(S, R);

impl<
        S: StatusHandle + Send + Sync + Debug + Clone + 'static,
        R: RestateKeyConverter + Send + Sync + Debug + Clone + 'static,
    > RangeScanner for StatusScanner<S, R>
{
    fn scan(
        &self,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let status = self.0.clone();
        let schema = projection.clone();
        let resolver = self.1.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let background_task = async move {
            let rows = status.read_status(range).await;
            for_each_state(schema, tx, rows, resolver).await;
        };
        stream_builder.spawn(background_task);
        stream_builder.build()
    }
}

async fn for_each_state<'a, I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
    resolver: impl RestateKeyConverter + Clone,
) where
    I: Iterator<Item = InvocationStatusReport> + 'a,
{
    let mut builder = StateBuilder::new(schema.clone());
    let mut temp = String::new();
    let mut rows = rows.collect::<Vec<_>>();
    // need to be ordered by partition key for symmetric joins
    rows.sort_unstable_by_key(|row| row.full_invocation_id().service_id.partition_key());
    for row in rows {
        append_state_row(&mut builder, &mut temp, row, resolver.clone());
        if builder.full() {
            let batch = builder.finish();
            if tx.send(Ok(batch)).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = StateBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(Ok(result)).await;
    }
}
