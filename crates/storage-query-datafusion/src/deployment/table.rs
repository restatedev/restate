// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::DeploymentBuilder;

use crate::context::QueryContext;
use crate::deployment::row::append_deployment_row;
use crate::generic_table::{GenericTableProvider, RangeScanner};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
pub use datafusion_expr::UserDefinedLogicalNode;
use restate_schema_api::deployment::{Deployment, DeploymentMetadataResolver};
use restate_types::identifiers::{PartitionKey, ServiceRevision};
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self(
    ctx: &QueryContext,
    resolver: impl DeploymentMetadataResolver + Send + Sync + Debug + 'static,
) -> datafusion::common::Result<()> {
    let deployment_table = GenericTableProvider::new(
        DeploymentBuilder::schema(),
        Arc::new(DeploymentMetadataScanner(resolver)),
    );

    ctx.as_ref()
        .register_table("sys_deployment", Arc::new(deployment_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct DeploymentMetadataScanner<DMR>(DMR);

/// TODO This trait makes little sense for sys_deployment,
///  but it's fine nevertheless as the caller always uses the full range
impl<DMR: DeploymentMetadataResolver + Debug + Sync + Send + 'static> RangeScanner
    for DeploymentMetadataScanner<DMR>
{
    fn scan(
        &self,
        _range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();

        let rows = self.0.get_deployments();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, rows).await;
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: Vec<(Deployment, Vec<(String, ServiceRevision)>)>,
) {
    let mut builder = DeploymentBuilder::new(schema.clone());
    let mut temp = String::new();
    for (deployment, _) in rows {
        append_deployment_row(&mut builder, &mut temp, deployment);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(Ok(batch)).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = DeploymentBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(Ok(result)).await;
    }
}
