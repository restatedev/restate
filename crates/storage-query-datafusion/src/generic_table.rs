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

use crate::table_util::compute_ordering;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
pub use datafusion_expr::UserDefinedLogicalNode;
use restate_types::identifiers::PartitionKey;

pub(crate) trait RangeScanner: Send + Sync + Debug + 'static {
    fn scan(
        &self,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream;
}

pub(crate) type RangeScannerRef = Arc<dyn RangeScanner>;

pub(crate) struct GenericTableProvider {
    schema: SchemaRef,
    scanner: RangeScannerRef,
}

impl GenericTableProvider {
    pub(crate) fn new(schema: SchemaRef, scanner: RangeScannerRef) -> Self {
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => SchemaRef::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        Ok(Arc::new(GenericTableExecutionPlan {
            output_ordering: compute_ordering(projected_schema.clone()),
            projected_schema,
            scanner: Arc::clone(&self.scanner),
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
struct GenericTableExecutionPlan {
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    projected_schema: SchemaRef,
    scanner: RangeScannerRef,
}

impl ExecutionPlan for GenericTableExecutionPlan {
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
        self.output_ordering.as_deref()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let range = 0..=PartitionKey::MAX;
        let stream = self.scanner.scan(range, self.projected_schema.clone());
        Ok(stream)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for GenericTableExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "GenericTableExecutionPlan()",)
            }
        }
    }
}
