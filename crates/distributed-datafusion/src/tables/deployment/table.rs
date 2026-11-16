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
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::mpsc::Sender;

use restate_storage_query_api::tables::deployment::{Deployment, DeploymentColumn};
use restate_types::identifiers::ServiceRevision;
use restate_types::live::Live;
use restate_types::schema::deployment::{self, DeploymentResolver};

use super::row::append_deployment_row;
use super::schema::{SysDeploymentBuilder, SysDeploymentTable};
use crate::source_registry::SourceRegistry;
use crate::statistics::{DEPLOYMENT_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{GenericTableProvider, Scan};

impl SysDeploymentTable {
    pub(crate) fn create_provider(sources: SourceRegistry) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<Deployment>::new()
            .with_num_rows_estimate(DEPLOYMENT_ROW_ESTIMATE)
            .with_primary_key(DeploymentColumn::Id);
        Arc::new(
            GenericTableProvider::declared::<Self>(None, Some(sources))
                .with_statistics(statistics.build()),
        )
    }

    pub(crate) fn create_local_scanner(
        resolver: Live<impl DeploymentResolver + Send + Sync + 'static>,
    ) -> impl Scan {
        DeploymentMetadataScanner(resolver)
    }
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
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();

        let rows = self.0.pinned().get_deployments();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, rows, batch_size, limit).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: Vec<(deployment::Deployment, Vec<(String, ServiceRevision)>)>,
    batch_size: usize,
    limit: Option<usize>,
) {
    let mut builder = SysDeploymentBuilder::new(schema.clone());
    for (deployment, services) in rows.into_iter().take(limit.unwrap_or(usize::MAX)) {
        append_deployment_row(&mut builder, deployment, services);
        if builder.num_rows() >= batch_size {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // The consumer hung up; stop producing.
                return;
            }
        }
    }
    if builder.num_rows() > 0 {
        let _ = tx.send(builder.into_batch()).await;
    }
}
