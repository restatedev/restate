// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::live::Live;
use tokio::sync::mpsc::Sender;

use restate_types::identifiers::ServiceRevision;
use restate_types::schema::deployment::{Deployment, DeploymentResolver};

use super::schema::SysDeploymentBuilder;
use crate::context::QueryContext;
use crate::deployment::row::append_deployment_row;
use crate::statistics::{DEPLOYMENT_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

pub(crate) fn register_self(
    ctx: &QueryContext,
    resolver: Live<impl DeploymentResolver + Send + Sync + 'static>,
) -> datafusion::common::Result<()> {
    let schema = SysDeploymentBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(DEPLOYMENT_ROW_ESTIMATE)
        .with_primary_key("id");
    let deployment_table = GenericTableProvider::new(
        SysDeploymentBuilder::schema(),
        Arc::new(DeploymentMetadataScanner(resolver)),
    )
    .with_statistics(statistics.build());
    ctx.register_non_partitioned_table("sys_deployment", Arc::new(deployment_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("DeploymentMetadataScanner")]
struct DeploymentMetadataScanner<DMR>(Live<DMR>);

impl<DMR: DeploymentResolver + Sync + Send + 'static> Scan for DeploymentMetadataScanner<DMR> {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();

        let rows = self.0.pinned().get_deployments();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, rows, batch_size).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: Vec<(Deployment, Vec<(String, ServiceRevision)>)>,
    batch_size: usize,
) {
    let mut builder = SysDeploymentBuilder::new(schema.clone());
    for (deployment, _) in rows {
        append_deployment_row(&mut builder, deployment);
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
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
