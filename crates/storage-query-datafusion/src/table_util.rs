// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use std::mem::ManuallyDrop;
use std::sync::Arc;
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

pub(crate) trait Builder {
    fn new(projected_schema: SchemaRef) -> Self;

    fn num_rows(&self) -> usize;

    fn empty(&self) -> bool;

    fn finish(self) -> datafusion::common::Result<RecordBatch>;

    fn finish_and_new(&mut self) -> datafusion::common::Result<RecordBatch>;
}

pub(crate) struct BatchSender<B: Builder> {
    builder: ManuallyDrop<B>,
    coalescer: datafusion::arrow::compute::BatchCoalescer,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    tx: Sender<datafusion::error::Result<RecordBatch>>,
}

impl<B: Builder> BatchSender<B> {
    pub(crate) fn new(
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        target_batch_size: usize,
        tx: Sender<datafusion::error::Result<RecordBatch>>,
    ) -> Self {
        let builder = B::new(projection.clone());
        let coalescer =
            datafusion::arrow::compute::BatchCoalescer::new(projection, target_batch_size);
        Self {
            builder: ManuallyDrop::new(builder),
            coalescer,
            predicate,
            tx,
        }
    }

    fn push_batch(
        &mut self,
        batch: RecordBatch,
    ) -> Result<(), datafusion::arrow::error::ArrowError> {
        match &self.predicate {
            Some(predicate) => {
                let filter_array = predicate.evaluate(&batch)?.into_array(batch.num_rows())?;

                let filter_bool_array = filter_array.as_boolean_opt().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "Cannot create filter_array from non-boolean predicates".into(),
                    )
                })?;
                self.coalescer
                    .push_batch_with_filter(batch, filter_bool_array)
            }
            None => self.coalescer.push_batch(batch),
        }
    }

    pub(crate) fn send(
        &mut self,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<datafusion::error::Result<RecordBatch>>>
    {
        match self.builder.finish_and_new() {
            Ok(batch) => match self.push_batch(batch) {
                Ok(()) => match self.coalescer.next_completed_batch() {
                    Some(batch) => self.tx.blocking_send(Ok(batch)),
                    None => Ok(()),
                },
                Err(err) => self.tx.blocking_send(Err(err.into())),
            },
            Err(err) => self.tx.blocking_send(Err(err)),
        }
    }

    pub(crate) fn builder_mut(&mut self) -> &mut B {
        &mut self.builder
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.builder.num_rows()
    }
}

impl<B: Builder> Drop for BatchSender<B> {
    fn drop(&mut self) {
        if !self.builder.empty() {
            // Safety: self.builder is exclusively taken at drop time, and never accessed again.
            let builder = unsafe { ManuallyDrop::take(&mut self.builder) };

            match builder.finish() {
                Ok(batch) => match self.push_batch(batch) {
                    Ok(()) => {}
                    Err(err) => {
                        let _ = self.tx.blocking_send(Err(err.into()));
                        return;
                    }
                },
                Err(err) => {
                    let _ = self.tx.blocking_send(Err(err));
                    return;
                }
            }
        }

        if let Err(err) = self.coalescer.finish_buffered_batch() {
            let _ = self.tx.blocking_send(Err(err.into()));
            return;
        }

        if let Some(final_batch) = self.coalescer.next_completed_batch()
            && final_batch.num_rows() > 0
        {
            let _ = self.tx.blocking_send(Ok(final_batch));
        }
    }
}
