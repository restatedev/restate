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
use restate_types::partition_table::PartitionTable;
use restate_types::partitions::state::PartitionReplicaSetStates;

use crate::context::QueryContext;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

use super::row::append_replica_set_row;
use super::schema::PartitionReplicaSetBuilder;

pub fn register_self(
    ctx: &QueryContext,
    metadata: Metadata,
    cluster_state: ClusterState,
    replica_set_states: PartitionReplicaSetStates,
) -> datafusion::common::Result<()> {
    let replica_set_table = GenericTableProvider::new(
        PartitionReplicaSetBuilder::schema(),
        Arc::new(ReplicaSetScanner {
            metadata,
            replica_set_states,
            cluster_state,
        }),
    );
    ctx.register_non_partitioned_table("partition_replica_set", Arc::new(replica_set_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("ReplicaSetScanner")]
struct ReplicaSetScanner {
    metadata: Metadata,
    replica_set_states: PartitionReplicaSetStates,
    cluster_state: ClusterState,
}

impl Scan for ReplicaSetScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let partition_table = self.metadata.partition_table_snapshot();

        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();

        let cluster_state = self.cluster_state.clone();
        let replica_set_states = self.replica_set_states.clone();
        stream_builder.spawn(async move {
            for_each_partition(
                schema,
                tx,
                partition_table,
                cluster_state,
                replica_set_states,
                batch_size,
            )
            .await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_partition(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    partition_table: Arc<PartitionTable>,
    cluster_state: ClusterState,
    replica_set_states: PartitionReplicaSetStates,
    batch_size: usize,
) {
    let mut builder = PartitionReplicaSetBuilder::new(schema.clone());

    for (_, partition) in partition_table.iter() {
        let membership = replica_set_states.membership_state(partition.partition_id);
        append_replica_set_row(&mut builder, membership, &cluster_state, partition);

        if builder.num_rows() >= batch_size {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = PartitionReplicaSetBuilder::new(schema.clone());
        }
    }

    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
