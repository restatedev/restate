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

use crate::context::QueryContext;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

use super::row::append_partition_rows;
use super::schema::PartitionBuilder;

pub fn register_self(ctx: &QueryContext, metadata: Metadata) -> datafusion::common::Result<()> {
    let partitions_table = GenericTableProvider::new(
        PartitionBuilder::schema(),
        Arc::new(PartitionScanner(metadata)),
    );
    ctx.register_non_partitioned_table("partitions", Arc::new(partitions_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("DeploymentMetadataScanner")]
struct PartitionScanner(Metadata);

impl Scan for PartitionScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();

        let partition_table = self.0.partition_table_snapshot();
        stream_builder.spawn(async move {
            for_each_partition(schema, tx, partition_table).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_partition(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    partition_table: Arc<PartitionTable>,
) {
    let mut builder = PartitionBuilder::new(schema.clone());

    let mut output = String::new();
    for (_, partition) in partition_table.partitions() {
        append_partition_rows(
            &mut builder,
            &mut output,
            partition_table.version(),
            partition,
        );

        if builder.full() {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = PartitionBuilder::new(schema.clone());
        }
    }

    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
