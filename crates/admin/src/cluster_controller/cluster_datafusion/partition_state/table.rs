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

use restate_datafusion::{
    context::QueryContext,
    table_providers::{GenericTableProvider, Scan},
    table_util::Builder,
};
use restate_types::cluster::cluster_state::ClusterState;

use super::row::append_partition_row;
use super::schema::PartitionStateBuilder;

pub fn register_self(
    ctx: &QueryContext,
    watch: watch::Receiver<Arc<ClusterState>>,
) -> datafusion::common::Result<()> {
    let table = GenericTableProvider::new(
        PartitionStateBuilder::schema(),
        Arc::new(PartitionStateScanner { watch }),
    );
    ctx.register_table("partition_state", Arc::new(table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("DeploymentMetadataScanner")]
struct PartitionStateScanner {
    watch: watch::Receiver<Arc<ClusterState>>,
}

impl Scan for PartitionStateScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();

        let state = self.watch.borrow().clone();
        stream_builder.spawn(async move {
            for_each_partition(schema, tx, &state).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_partition(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    state: &ClusterState,
) {
    let mut builder = PartitionStateBuilder::new(schema.clone());
    let mut output = String::new();
    for alive in state.alive_nodes() {
        for (partition_id, partition_status) in alive.partitions.iter() {
            append_partition_row(
                &mut builder,
                &mut output,
                alive.generational_node_id,
                *partition_id,
                partition_status,
            );
            if builder.full() {
                let batch = builder.finish();
                if tx.send(batch).await.is_err() {
                    // not sure what to do here?
                    // the other side has hung up on us.
                    // we probably don't want to panic, is it will cause the entire process to exit
                    return;
                }
                builder = PartitionStateBuilder::new(schema.clone());
            }
        }
    }

    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
