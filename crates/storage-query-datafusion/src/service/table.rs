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

use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};

use super::schema::ServiceBuilder;
use crate::context::QueryContext;
use crate::service::row::append_service_row;
use crate::table_providers::{GenericTableProvider, Scan};

pub(crate) fn register_self(
    ctx: &QueryContext,
    resolver: impl ServiceMetadataResolver + Send + Sync + Debug + 'static,
) -> datafusion::common::Result<()> {
    let service_table = GenericTableProvider::new(
        ServiceBuilder::schema(),
        Arc::new(ServiceMetadataScanner(resolver)),
    );

    ctx.as_ref()
        .register_table("sys_service", Arc::new(service_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct ServiceMetadataScanner<SMR>(SMR);

impl<SMR: ServiceMetadataResolver + Debug + Sync + Send + 'static> Scan
    for ServiceMetadataScanner<SMR>
{
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();

        let rows = self.0.list_services();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, rows).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: Vec<ServiceMetadata>,
) {
    let mut builder = ServiceBuilder::new(schema.clone());
    let mut temp = String::new();
    for service in rows {
        append_service_row(&mut builder, &mut temp, service);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
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
        let _ = tx.send(result).await;
    }
}
