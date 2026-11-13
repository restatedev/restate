// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runs plain SQL over a schema binding and one admitted backend, for tests.
//!
//! [`BindingTableProvider`] adapts a [`SchemaBinding`] into a DataFusion `TableProvider`
//! so tests can compare prepared reads with SQL over an independent reference
//! table. It collects small fixtures in one batch; production engines provide
//! bounded batching and backpressure through their own sinks.

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
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};

use restate_storage_api::Result;

use crate::scan::{CursorScan, Scan, Table};
use crate::selection::{Selection, static_conjuncts};
use crate::{
    AsyncRowSink, LogicalSchema, PreparedRead, RowBuilder, RowSink, SchemaBinding, SelectionSchema,
};

/// SQL oracle adapter for a single-partition backend. Filters are inexact at the
/// SQL boundary so unsupported expressions stay above the scan, before LIMIT.
pub struct BindingTableProvider<B: SchemaBinding, S: Scan> {
    backend: Arc<S>,
    executor: Arc<S::Executor>,
    schema: SchemaRef,
    _binding: PhantomData<fn() -> B>,
}

impl<B: SchemaBinding, S: Scan> BindingTableProvider<B, S> {
    /// Creates a SQL table for binding `B`, using the backend's existing read
    /// snapshot and the supplied query IO pool for every scan.
    pub fn new(backend: Arc<S>, executor: Arc<S::Executor>) -> Self {
        Self {
            backend,
            executor,
            schema: B::Schema::arrow_schema(),
            _binding: PhantomData,
        }
    }
}

impl<B: SchemaBinding, S: Scan> Debug for BindingTableProvider<B, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BindingTableProvider")
            .field("backend", &self.backend)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<B, S> TableProvider for BindingTableProvider<B, S>
where
    B: SchemaBinding,
    S: Scan + CursorScan<B::Table>,
    for<'a> <B::Table as Table>::Record<'a>: Copy + Send,
{
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
        let static_filters = static_conjuncts(physical.as_slice());
        // Bound Boolean expansion to 16 alternatives for these small fixtures.
        // Larger expressions broaden the candidates; SQL still checks the filter.
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
            B::prepare(&selection).map_err(|err| DataFusionError::External(err.into()))?;
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema(),
            vec![Arc::new(SchemaStream::<B, S> {
                backend: Arc::clone(&self.backend),
                executor: Arc::clone(&self.executor),
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

struct SchemaStream<B: SchemaBinding, S: Scan> {
    backend: Arc<S>,
    executor: Arc<S::Executor>,
    schema: SchemaRef,
    prepared: PreparedRead<B::Table>,
}

impl<B: SchemaBinding, S: Scan> Debug for SchemaStream<B, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaStream").finish_non_exhaustive()
    }
}

impl<B, S> PartitionStream for SchemaStream<B, S>
where
    B: SchemaBinding,
    S: Scan + CursorScan<B::Table>,
    for<'a> <B::Table as Table>::Record<'a>: Copy + Send,
{
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let backend = Arc::clone(&self.backend);
        let executor = Arc::clone(&self.executor);
        let prepared = self.prepared.clone();
        let schema = Arc::clone(self.schema());
        let stream = futures::stream::once(async move {
            let collector = BatchCollector::<B::Schema> {
                builder: <B::Schema as LogicalSchema>::RowBuilder::new(schema),
            };
            let outcome = B::read(backend.as_ref(), executor.as_ref(), &prepared, collector)
                .map_err(|err| DataFusionError::External(err.into()))?
                .await
                .map_err(|err| DataFusionError::External(err.into()))?;
            outcome.into_inner().builder.into_batch()
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

struct BatchCollector<S: LogicalSchema> {
    builder: S::RowBuilder,
}

impl<S: LogicalSchema> AsyncRowSink<S> for BatchCollector<S> {
    async fn reserve(&mut self) -> Result<ControlFlow<()>> {
        Ok(ControlFlow::Continue(()))
    }
}

impl<S: LogicalSchema> RowSink<S> for BatchCollector<S> {
    fn append_with(
        &mut self,
        write: impl FnOnce(&mut S::RowBuilder) -> Result<()>,
    ) -> ControlFlow<Result<()>> {
        match write(&mut self.builder) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(Err(error)),
        }
    }
}
