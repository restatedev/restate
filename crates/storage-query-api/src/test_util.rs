// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL oracle support for small, persisted-key correctness fixtures.
//! Production engines use their bounded partition scanners instead.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::sync::Arc;

pub use datafusion;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::split_conjunction;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::channel::oneshot;

use crate::scan::Scan;
use crate::selection::Selection;
use crate::{LogicalSchema, PreparedRead, RowBuilder, SchemaBinding, SelectionSchema};

/// SQL oracle adapter for a single-partition backend. Filters are inexact
/// at the SQL boundary so unsupported expressions stay above the scan, before LIMIT.
pub struct SchemaProvider<B: SchemaBinding, S> {
    backend: Arc<S>,
    schema: SchemaRef,
    _binding: PhantomData<fn() -> B>,
}

impl<B: SchemaBinding, S> SchemaProvider<B, S> {
    pub fn new(backend: Arc<S>) -> Self {
        Self {
            backend,
            schema: B::Schema::arrow_schema(),
            _binding: PhantomData,
        }
    }
}

impl<B: SchemaBinding, S: Debug> Debug for SchemaProvider<B, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaProvider")
            .field("backend", &self.backend)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait::async_trait]
impl<B: SchemaBinding, S: Scan<B::Table>> TableProvider for SchemaProvider<B, S> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(self.schema())?;
        let physical = filters
            .iter()
            .cloned()
            .reduce(Expr::and)
            .map(|expr| state.create_physical_expr(expr, &df_schema))
            .transpose()?;
        let static_filters: Vec<_> = physical
            .iter()
            .flat_map(split_conjunction)
            .filter(|expr| !crate::selection::contains_dynamic_filter(expr))
            .cloned()
            .collect();
        let candidates = B::Schema::analyze(&static_filters, NonZeroUsize::new(16).unwrap())
            .map_err(|err| DataFusionError::External(err.into()))?;
        // Candidate analysis can prove impossibility. Surviving domains are not
        // strict selections: unsupported conditions still need the SQL residual.
        let selection = if candidates.is_empty() {
            Selection::Empty
        } else {
            crate::selection::compile::<B::Schema>(physical.as_ref())
        };
        let prepared =
            B::prepare(&selection).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema(),
            vec![Arc::new(SchemaStream::<B, S> {
                backend: Arc::clone(&self.backend),
                schema: self.schema(),
                prepared,
            })],
            projection,
            [],
            false,
            None,
        )?))
    }
}

struct SchemaStream<B: SchemaBinding, S> {
    backend: Arc<S>,
    schema: SchemaRef,
    prepared: PreparedRead<B::Table>,
}

impl<B: SchemaBinding, S> Debug for SchemaStream<B, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaStream").finish_non_exhaustive()
    }
}

impl<B: SchemaBinding, S: Scan<B::Table>> PartitionStream for SchemaStream<B, S> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let backend = Arc::clone(&self.backend);
        let prepared = self.prepared.clone();
        // Defer the scan to execution, then materialize the small test fixture in
        // one batch. The oracle intentionally collects the complete fixture;
        // production adapters provide bounded batching and backpressure.
        let schema = Arc::clone(self.schema());
        let stream = futures::stream::once(async move {
            let (completed, output) = oneshot::channel();
            let mut collector = BatchCollector::<B::Schema> {
                state: Some((
                    <B::Schema as LogicalSchema>::RowBuilder::new(schema),
                    completed,
                )),
            };
            B::read(backend.as_ref(), &prepared, move |row| {
                collector.append(row);
                ControlFlow::Continue(())
            })
            .map_err(|err| DataFusionError::External(Box::new(err)))?
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
            output
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?
                .into_batch()
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

/// The scan callback exclusively owns the builder, avoiding synchronization on
/// every row. Dropping the callback returns ownership once. The receiver only
/// finishes the batch after the scan succeeds, so errors never emit partial data.
struct BatchCollector<T: LogicalSchema> {
    state: Option<(T::RowBuilder, oneshot::Sender<T::RowBuilder>)>,
}

impl<T: LogicalSchema> BatchCollector<T> {
    fn append(&mut self, row: T::Row<'_>) {
        self.state
            .as_mut()
            .expect("collector is alive")
            .0
            .append(row);
    }
}

impl<T: LogicalSchema> Drop for BatchCollector<T> {
    fn drop(&mut self) {
        if let Some((builder, completed)) = self.state.take() {
            // A cancelled or failed scan may have already dropped the receiver.
            let _ = completed.send(builder);
        }
    }
}
