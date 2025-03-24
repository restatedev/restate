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
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

use restate_types::cluster::cluster_state::ClusterState;

use crate::context::QueryContext;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

use super::row::append_node_row;
use super::schema::NodeStateBuilder;

pub fn register_self(
    ctx: &QueryContext,
    watch: watch::Receiver<Arc<ClusterState>>,
) -> datafusion::common::Result<()> {
    let table = GenericTableProvider::new(
        NodeStateBuilder::schema(),
        Arc::new(NodesStatusScanner { watch }),
    );
    ctx.register_non_partitioned_table("node_state", Arc::new(table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("DeploymentMetadataScanner")]
struct NodesStatusScanner {
    watch: watch::Receiver<Arc<ClusterState>>,
}

impl Scan for NodesStatusScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();

        let current = self.watch.borrow().clone();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, &current).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    cluster_state: &ClusterState,
) {
    let mut builder = NodeStateBuilder::new(schema.clone());
    let mut output = String::new();
    for (id, node_state) in cluster_state.nodes.iter() {
        append_node_row(&mut builder, &mut output, *id, node_state);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = NodeStateBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
