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
use restate_types::partition_table::PartitionTable;
use restate_types::partitions::state::PartitionReplicaSetStates;

use crate::context::QueryContext;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

use super::row::append_partition_row;
use super::schema::PartitionBuilder;

pub fn register_self(
    ctx: &QueryContext,
    metadata: Metadata,
    replica_set_states: PartitionReplicaSetStates,
) -> datafusion::common::Result<()> {
    let partitions_table = GenericTableProvider::new(
        PartitionBuilder::schema(),
        Arc::new(PartitionScanner {
            metadata,
            replica_set_states,
        }),
    );
    ctx.register_non_partitioned_table("partitions", Arc::new(partitions_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("PartitionScanner")]
struct PartitionScanner {
    metadata: Metadata,
    replica_set_states: PartitionReplicaSetStates,
}

impl Scan for PartitionScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let partition_table = self.metadata.partition_table_snapshot();

        let mut stream_builder =
            RecordBatchReceiverStream::builder(projection, partition_table.len());
        let tx = stream_builder.tx();

        let replica_set_states = self.replica_set_states.clone();
        stream_builder.spawn(async move {
            for_each_partition(schema, tx, partition_table, replica_set_states, batch_size).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_partition(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    partition_table: Arc<PartitionTable>,
    replica_set_states: PartitionReplicaSetStates,
    batch_size: usize,
) {
    let mut builder = PartitionBuilder::new(schema.clone());

    for (_, partition) in partition_table.iter() {
        let membership = replica_set_states.membership_state(partition.partition_id);
        append_partition_row(
            &mut builder,
            membership,
            partition_table.version(),
            partition,
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
