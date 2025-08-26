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
use restate_types::cluster_state::ClusterState;
use restate_types::nodes_config::NodesConfiguration;

use crate::context::QueryContext;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

use super::row::append_node_row;
use super::schema::NodeBuilder;

pub fn register_self(
    ctx: &QueryContext,
    metadata: Metadata,
    cluster_state: ClusterState,
) -> datafusion::common::Result<()> {
    let nodes_table = GenericTableProvider::new(
        NodeBuilder::schema(),
        Arc::new(NodesScanner {
            metadata,
            cluster_state,
        }),
    );
    ctx.register_non_partitioned_table("nodes", Arc::new(nodes_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("NodesScanner")]
struct NodesScanner {
    metadata: Metadata,
    cluster_state: ClusterState,
}

impl Scan for NodesScanner {
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

        let nodes_config = self.metadata.nodes_config_snapshot();
        let cluster_state = self.cluster_state.clone();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, nodes_config, cluster_state, batch_size).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    nodes_config: Arc<NodesConfiguration>,
    cluster_state: ClusterState,
    batch_size: usize,
) {
    let mut builder = NodeBuilder::new(schema.clone());
    for (id, node_config) in nodes_config.iter() {
        let node_state = cluster_state.get_node_state(node_config.current_generation.into());
        append_node_row(
            &mut builder,
            nodes_config.version(),
            id,
            node_config,
            node_state,
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
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
