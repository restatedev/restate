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

use restate_core::Metadata;
use restate_datafusion::{
    context::QueryContext,
    table_providers::{GenericTableProvider, Scan},
    table_util::Builder,
};
use restate_types::nodes_config::NodesConfiguration;

use super::row::append_node_row;
use super::schema::NodeBuilder;

pub fn register_self(ctx: &QueryContext, metadata: Metadata) -> datafusion::common::Result<()> {
    let nodes_table =
        GenericTableProvider::new(NodeBuilder::schema(), Arc::new(NodesScanner(metadata)));
    ctx.register_table("nodes", Arc::new(nodes_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("DeploymentMetadataScanner")]
struct NodesScanner(Metadata);

impl Scan for NodesScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();

        let nodes_config = self.0.nodes_config_snapshot();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, nodes_config).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    nodes_config: Arc<NodesConfiguration>,
) {
    let mut builder = NodeBuilder::new(schema.clone());
    let mut output = String::new();
    for (id, node_config) in nodes_config.iter() {
        append_node_row(
            &mut builder,
            &mut output,
            nodes_config.version(),
            id,
            node_config,
        );

        if builder.full() {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = NodeBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
