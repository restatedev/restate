// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeBounds;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};

use restate_storage_query_api::{LogicalSchema, RowBuilder};
use restate_types::identifiers::PartitionKey;
use restate_types::sharding::KeyRange;
use restate_worker_api::{CapturedPartitionState, PartitionCaptureRequirements};

use crate::access::PrimaryRead;
use crate::selection::Domain;
use crate::table_providers::{
    PartitionReadContext, PreparedPartitionRead, ReadMetrics, ScanPartition,
};

type SelectLiveKeys = fn(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<Domain<PartitionKey>>;

/// Converts admission-captured live rows into Arrow batches. Capture preserves
/// backend ordering; this scanner applies key restrictions. Native DataFusion
/// operators evaluate SQL residuals above this candidate stream. An optional limit
/// is a safe candidate bound supplied by the planner, never a post-filter row budget.
#[derive(derive_more::Debug)]
#[debug("LivePartitionScanner")]
pub(crate) struct LivePartitionScanner<S: LogicalSchema, R> {
    requirements: PartitionCaptureRequirements,
    keys: SelectLiveKeys,
    read: fn(&CapturedPartitionState) -> Option<&Arc<[R]>>,
    partition_key: fn(&R) -> PartitionKey,
    append: fn(&mut S::RowBuilder, &R),
}

impl<S: LogicalSchema, R> LivePartitionScanner<S, R> {
    pub(crate) fn new(
        requirements: PartitionCaptureRequirements,
        keys: SelectLiveKeys,
        read: fn(&CapturedPartitionState) -> Option<&Arc<[R]>>,
        partition_key: fn(&R) -> PartitionKey,
        append: fn(&mut S::RowBuilder, &R),
    ) -> Self {
        Self {
            requirements,
            keys,
            read,
            partition_key,
            append,
        }
    }
}

impl<S: LogicalSchema, R: Send + Sync + 'static> ScanPartition for LivePartitionScanner<S, R> {
    fn requires_leader(&self) -> bool {
        true
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        anyhow::ensure!(
            matches!(access, PrimaryRead::Scan),
            "live source requires a scan"
        );
        let range = crate::selection::key_range(&(self.keys)(predicate.as_slice())?);
        Ok(Arc::new(PreparedLiveRead {
            scanner: self,
            range,
            projection,
            limit,
        }))
    }
}

#[derive(derive_more::Debug)]
#[debug("PreparedLiveRead")]
struct PreparedLiveRead<S: LogicalSchema, R> {
    scanner: Arc<LivePartitionScanner<S, R>>,
    range: Option<KeyRange>,
    projection: SchemaRef,
    limit: Option<usize>,
}

impl<S: LogicalSchema, R: Send + Sync + 'static> PreparedPartitionRead for PreparedLiveRead<S, R> {
    fn capture_requirements(&self) -> PartitionCaptureRequirements {
        self.scanner.requirements
    }

    fn execute(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        _metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let projection = Arc::clone(&self.projection);
        let range = self.range;
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&projection), 1);
        let tx = stream.tx();
        let rows = Arc::clone((self.scanner.read)(context.captured()).ok_or(
            restate_worker_api::PartitionReadError::LiveCaptureUnavailable(context.partition_id()),
        )?);
        let partition_key = self.scanner.partition_key;
        let append = self.scanner.append;
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        stream.spawn(async move {
            if remaining == 0 {
                return Ok(());
            }
            let Some(range) = range else {
                return Ok(());
            };
            let mut builder = S::RowBuilder::new(projection);
            for row in rows
                .iter()
                .filter(|row| range.contains(&partition_key(row)))
            {
                if remaining == 0 || tx.is_closed() {
                    break;
                }
                append(&mut builder, row);
                remaining -= 1;
                if builder.num_rows() >= batch_size {
                    let batch = builder.finish()?;
                    if tx.send(Ok(batch)).await.is_err() {
                        return Ok(());
                    }
                }
            }
            if builder.num_rows() > 0 {
                let batch = builder.into_batch()?;
                let _ = tx.send(Ok(batch)).await;
            }
            Ok(())
        });
        Ok(stream.build())
    }
}
