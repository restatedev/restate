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
use datafusion::common::DataFusionError;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};
use futures::{StreamExt, TryStreamExt};

use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;
use restate_worker_api::{PartitionQueryAccess, PartitionQueryStream};

use crate::table_providers::ScanPartition;
use crate::table_util::Builder;

/// Converts native, partition-scoped live rows into Arrow batches. Partition admission, range
/// restriction, and ordering belong to the query-access backend, not the SQL adapter.
#[derive(derive_more::Debug)]
#[debug("LivePartitionScanner")]
pub(crate) struct LivePartitionScanner<B, R> {
    access: Arc<dyn PartitionQueryAccess>,
    read: fn(&dyn PartitionQueryAccess, PartitionId, KeyRange) -> PartitionQueryStream<R>,
    append: fn(&mut B, R),
}

impl<B, R> LivePartitionScanner<B, R> {
    pub(crate) fn new(
        access: Arc<dyn PartitionQueryAccess>,
        read: fn(&dyn PartitionQueryAccess, PartitionId, KeyRange) -> PartitionQueryStream<R>,
        append: fn(&mut B, R),
    ) -> Self {
        Self {
            access,
            read,
            append,
        }
    }
}

impl<B: Builder + Send + 'static, R: Send + 'static> ScanPartition for LivePartitionScanner<B, R> {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        _predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        _elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&projection), 1);
        let tx = stream.tx();
        let access = Arc::clone(&self.access);
        let read = self.read;
        let append = self.append;
        stream.spawn(async move {
            let mut rows =
                read(access.as_ref(), partition_id, range).take(limit.unwrap_or(usize::MAX));
            let mut builder = B::new(projection);
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            {
                append(&mut builder, row);
                if builder.num_rows() >= batch_size {
                    let batch = builder.finish_and_new()?;
                    if tx.send(Ok(batch)).await.is_err() {
                        return Ok(());
                    }
                }
            }
            if !builder.empty() {
                let batch = builder.finish()?;
                let _ = tx.send(Ok(batch)).await;
            }
            Ok(())
        });
        Ok(stream.build())
    }
}
