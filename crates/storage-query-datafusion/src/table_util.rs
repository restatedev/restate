// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use tokio::sync::mpsc::Sender;

use restate_storage_query_api::{LogicalSchema, RowBuilder};

#[macro_export]
macro_rules! log_data_corruption_error {
    ($table_name:expr, $key:expr, $field_name:expr, $err:expr) => {
        tracing::error!(
            error = %$err,
            "Cannot convert field '{}' for '{}' table with row key {:?}. This might indicate a data corruption problem.",
            $field_name,
            $table_name,
            $key
        )
    };
}

pub(crate) fn find_sort_columns(
    ordering: &[String],
    schema: &Schema,
) -> Vec<Arc<dyn PhysicalExpr>> {
    // find the maximal ordered prefix from @ordering, that exists in @schema.
    ordering
        .iter()
        .map_while(|column_name| col(column_name, schema).ok())
        .collect()
}

pub(crate) fn make_ordering(columns: Vec<Arc<dyn PhysicalExpr>>) -> Vec<PhysicalSortExpr> {
    columns
        .into_iter()
        .map(|expr| PhysicalSortExpr {
            expr,
            options: Default::default(),
        })
        .collect()
}

pub trait Builder {
    fn new(projected_schema: SchemaRef) -> Self;

    fn num_rows(&self) -> usize;

    fn empty(&self) -> bool;

    fn finish(self) -> datafusion::common::Result<RecordBatch>;

    fn finish_and_new(&mut self) -> datafusion::common::Result<RecordBatch>;
}

/// Builds V1 batches with the row builder of a table schema that V1 and V2
/// share (a [`LogicalSchema`]), so both engines return the same columns. Like
/// V1's other builders, it builds only the columns the query selects.
pub(crate) struct SchemaBuilder<S: LogicalSchema>(pub(crate) S::RowBuilder);

impl<S: LogicalSchema> Builder for SchemaBuilder<S> {
    fn new(schema: SchemaRef) -> Self {
        Self(S::RowBuilder::new(schema))
    }

    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    fn empty(&self) -> bool {
        self.num_rows() == 0
    }

    fn finish(self) -> datafusion::common::Result<RecordBatch> {
        self.0.into_batch()
    }

    fn finish_and_new(&mut self) -> datafusion::common::Result<RecordBatch> {
        self.0.finish()
    }
}

/// Builds Arrow batches from rows, applies the SQL filter, and sends each full
/// batch to the query, waiting while the query isn't ready for more. Call
/// `finish` only after a successful scan. Dropping it instead discards the
/// buffered rows, which is what an error or a cancelled query needs.
pub(crate) struct BatchSender<B: Builder> {
    builder: B,
    tx: Sender<datafusion::error::Result<RecordBatch>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    coalescer: LimitedBatchCoalescer,
    batch_size: usize,
    limit_reached: bool,
}

impl<B: Builder> BatchSender<B> {
    pub(crate) fn new(
        projection: SchemaRef,
        tx: Sender<datafusion::error::Result<RecordBatch>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Self {
        let builder = B::new(projection.clone());
        let coalescer = LimitedBatchCoalescer::new(projection, batch_size, limit);
        Self {
            builder,
            tx,
            predicate,
            coalescer,
            batch_size,
            limit_reached: false,
        }
    }

    pub(crate) fn builder_mut(&mut self) -> &mut B {
        &mut self.builder
    }

    pub(crate) async fn send_if_needed(&mut self) -> datafusion::common::Result<ControlFlow<()>> {
        if self.limit_reached {
            Ok(ControlFlow::Break(()))
        } else if self.builder.num_rows() >= self.batch_size {
            let batch = self.builder.finish_and_new()?;
            self.filter_coalesce_send(batch, false).await
        } else {
            Ok(ControlFlow::Continue(()))
        }
    }

    /// Sends the last, partly filled batch after a scan read everything. A scan
    /// that reached its LIMIT has already sent its rows.
    pub(crate) async fn finish(mut self) -> datafusion::common::Result<()> {
        if !self.limit_reached {
            let batch = self.builder.finish_and_new()?;
            let _ = self.filter_coalesce_send(batch, true).await?;
        }
        Ok(())
    }

    async fn filter_coalesce_send(
        &mut self,
        batch: RecordBatch,
        last: bool,
    ) -> datafusion::common::Result<ControlFlow<()>> {
        let filtered = self.apply_filter(batch)?;
        match self.coalescer.push_batch(filtered)? {
            PushBatchStatus::Continue => {}
            PushBatchStatus::LimitReached => {
                self.limit_reached = true;
            }
        }

        if last || self.limit_reached {
            self.coalescer.finish()?;
            let _ = self.send_completed_batches().await;
            Ok(ControlFlow::Break(()))
        } else {
            Ok(self.send_completed_batches().await)
        }
    }

    fn apply_filter(&self, batch: RecordBatch) -> datafusion::common::Result<RecordBatch> {
        let Some(predicate) = &self.predicate else {
            return Ok(batch);
        };

        let filter_result = predicate.evaluate(&batch)?;
        let filter_array = filter_result.into_array(batch.num_rows())?;
        let filter_array = as_boolean_array(&filter_array).map_err(|_| {
            datafusion::error::DataFusionError::Internal(
                "Cannot create filter_array from non-boolean predicates".into(),
            )
        })?;

        Ok(filter_record_batch(&batch, filter_array)?)
    }

    async fn send_completed_batches(&mut self) -> ControlFlow<()> {
        while let Some(batch) = self.coalescer.next_completed_batch() {
            if self.tx.send(Ok(batch)).await.is_err() {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}
