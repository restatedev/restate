// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem::ManuallyDrop;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::compute::{SortOptions, filter_record_batch};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use tokio::sync::mpsc::Sender;

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

pub(crate) fn make_decending_ordering(
    columns: Vec<Arc<dyn PhysicalExpr>>,
) -> Vec<PhysicalSortExpr> {
    columns
        .into_iter()
        .map(|expr| PhysicalSortExpr {
            expr,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        })
        .collect()
}

pub(crate) trait Builder {
    fn new(projected_schema: SchemaRef) -> Self;

    fn num_rows(&self) -> usize;

    fn empty(&self) -> bool;

    fn finish(self) -> datafusion::common::Result<RecordBatch>;

    fn finish_and_new(&mut self) -> datafusion::common::Result<RecordBatch>;
}

pub(crate) struct BatchSender<B: Builder> {
    builder: ManuallyDrop<B>,
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
            builder: ManuallyDrop::new(builder),
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

    pub(crate) fn send_if_needed(&mut self) -> ControlFlow<()> {
        if self.limit_reached {
            ControlFlow::Break(())
            // TOOD; this check could be based on selectivity metrics
            // ie, we could finish the batch when we predict that post-filter it will be big enough to send
        } else if self.builder.num_rows() >= self.batch_size {
            let batch = match self.builder.finish_and_new() {
                Ok(batch) => batch,
                Err(e) => {
                    let _ = self.tx.blocking_send(Err(e));
                    return ControlFlow::Break(());
                }
            };

            self.filter_coalesce_send(batch, false)
        } else {
            ControlFlow::Continue(())
        }
    }

    fn filter_coalesce_send(&mut self, batch: RecordBatch, last: bool) -> ControlFlow<()> {
        // Apply predicate filter
        let filtered = match self.apply_filter(batch) {
            Ok(batch) => batch,
            Err(e) => {
                let _ = self.tx.blocking_send(Err(e));
                return ControlFlow::Break(());
            }
        };

        match self.coalescer.push_batch(filtered) {
            Ok(PushBatchStatus::Continue) => {}
            Ok(PushBatchStatus::LimitReached) => {
                self.limit_reached = true;
            }
            Err(e) => {
                let _ = self.tx.blocking_send(Err(e));
                return ControlFlow::Break(());
            }
        }

        if last || self.limit_reached {
            if let Err(e) = self.coalescer.finish() {
                let _ = self.tx.blocking_send(Err(e));
                return ControlFlow::Break(());
            }
            let _ = self.send_completed_batches();
            ControlFlow::Break(())
        } else {
            self.send_completed_batches()
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

    fn send_completed_batches(&mut self) -> ControlFlow<()> {
        while let Some(batch) = self.coalescer.next_completed_batch() {
            if self.tx.blocking_send(Ok(batch)).is_err() {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

impl<B: Builder> Drop for BatchSender<B> {
    fn drop(&mut self) {
        // Safety: self.builder is exclusively taken here and never accessed again.
        let builder = unsafe { ManuallyDrop::take(&mut self.builder) };

        if self.limit_reached {
            // Already finished and sent when limit was reached
            return;
        }

        let batch = match builder.finish() {
            Ok(batch) => batch,
            Err(e) => {
                let _ = self.tx.blocking_send(Err(e));
                return;
            }
        };

        let _ = self.filter_coalesce_send(batch, true);
    }
}
