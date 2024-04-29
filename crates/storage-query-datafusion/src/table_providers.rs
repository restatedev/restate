// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};

use restate_types::identifiers::{PartitionId, PartitionKey};

use crate::context::SelectPartitions;
use crate::table_util::compute_ordering;

pub(crate) trait ScanPartition: Send + Sync + Debug + 'static {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream;
}

pub(crate) struct PartitionedTableProvider<T, S> {
    partition_selector: S,
    schema: SchemaRef,
    partition_scanner: T,
}

impl<T, S> PartitionedTableProvider<T, S> {
    pub(crate) fn new(processors_manager: S, schema: SchemaRef, partition_scanner: T) -> Self {
        Self {
            partition_selector: processors_manager,
            schema,
            partition_scanner,
        }
    }
}

#[async_trait]
impl<T, S> TableProvider for PartitionedTableProvider<T, S>
where
    T: ScanPartition + Clone,
    S: SelectPartitions,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => SchemaRef::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        let live_partitions = self
            .partition_selector
            .get_live_partitions()
            .await
            .map_err(DataFusionError::External)?;

        Ok(Arc::new(PartitionedExecutionPlan {
            live_partitions,
            output_ordering: compute_ordering(projected_schema.clone()),
            projected_schema,
            scanner: self.partition_scanner.clone(),
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        let res = filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect();

        Ok(res)
    }
}

#[derive(Debug, Clone)]
struct PartitionedExecutionPlan<T> {
    live_partitions: Vec<PartitionId>,
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    projected_schema: SchemaRef,
    scanner: T,
}

impl<T> ExecutionPlan for PartitionedExecutionPlan<T>
where
    T: ScanPartition,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.live_partitions.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        new_children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !new_children.is_empty() {
            return Err(DataFusionError::Internal(
                "PartitionedExecutionPlan does not support children".to_owned(),
            ));
        }

        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        // fake range until we can handle filters
        let range = 0..=PartitionKey::MAX;
        // map df partitions to our partition ids by index.
        let partition_id = self
            .live_partitions
            .get(partition)
            .expect("num_partitions within bounds");
        let stream =
            self.scanner
                .scan_partition(*partition_id, range, self.projected_schema.clone());
        Ok(stream)
    }
}

impl<T> DisplayAs for PartitionedExecutionPlan<T>
where
    T: std::fmt::Debug,
{
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PartitionedExecutionPlan({:?})", self.scanner)
            }
        }
    }
}

// Generic-based table provider that provides node-level or global data rather than
// partition-keyed data.
pub(crate) trait Scan: Send + Sync + Debug + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> SendableRecordBatchStream;
}

pub(crate) type ScannerRef = Arc<dyn Scan>;
pub(crate) struct GenericTableProvider {
    schema: SchemaRef,
    scanner: ScannerRef,
}

impl GenericTableProvider {
    pub(crate) fn new(schema: SchemaRef, scanner: ScannerRef) -> Self {
        Self { schema, scanner }
    }
}

#[async_trait]
impl TableProvider for GenericTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => SchemaRef::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        Ok(Arc::new(GenericExecutionPlan::new(
            projected_schema,
            filters,
            limit,
            Arc::clone(&self.scanner),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        let res = filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect();

        Ok(res)
    }
}

#[derive(Debug, Clone)]
struct GenericExecutionPlan {
    projected_schema: SchemaRef,
    scanner: ScannerRef,
    limit: Option<usize>,
    filters: Vec<Expr>,
}

impl GenericExecutionPlan {
    fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        scanner: ScannerRef,
    ) -> Self {
        Self {
            projected_schema,
            scanner,
            limit,
            filters: filters.to_vec(),
        }
    }
}

impl ExecutionPlan for GenericExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        new_children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !new_children.is_empty() {
            return Err(DataFusionError::Internal(
                "GenericExecutionPlan does not support children".to_owned(),
            ));
        }

        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let stream = self
            .scanner
            .scan(self.projected_schema.clone(), &self.filters, self.limit);
        Ok(stream)
    }
}

impl DisplayAs for GenericExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "GenericExecutionPlan()",)
            }
        }
    }
}
