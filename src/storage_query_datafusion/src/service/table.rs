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
use crate::service::row::append_service_row;
use crate::service::schema::ServiceBuilder;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
pub use datafusion_expr::UserDefinedLogicalNode;
use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};
use restate_types::identifiers::PartitionKey;
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self(
    ctx: &QueryContext,
    resolver: impl ServiceMetadataResolver + Send + Sync + Debug + Clone + 'static,
) -> datafusion::common::Result<()> {
    let table =
        GenericTableProvider::new(ServiceBuilder::schema(), Arc::new(ServiceScanner(resolver)));

    ctx.as_ref()
        .register_table("services", Arc::new(table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct ServiceScanner<R>(R);

impl<R: ServiceMetadataResolver + Send + Sync + Debug + Clone + 'static> RangeScanner
    for ServiceScanner<R>
{
    fn scan(
        &self,
        _: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let resolver = self.0.clone();
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let background_task = move || {
            let rows = resolver.list_services().into_iter();
            for_each_service(schema, tx, rows);
        };
        stream_builder.spawn_blocking(background_task);
        stream_builder.build()
    }
}

fn for_each_service<'a, I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
) where
    I: Iterator<Item = ServiceMetadata> + 'a,
{
    let mut builder = ServiceBuilder::new(schema.clone());
    for row in rows {
        append_service_row(&mut builder, row);
        if builder.full() {
            let batch = builder.finish();
            if tx.blocking_send(Ok(batch)).is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = ServiceBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.blocking_send(Ok(result));
    }
}
