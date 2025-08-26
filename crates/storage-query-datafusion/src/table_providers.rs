// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{self, StreamExt, TryStreamExt};

use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::partition_table::Partition;

use crate::context::SelectPartitions;
use crate::partition_filter::{FirstMatchingPartitionKeyExtractor, PartitionKeyExtractor};
use crate::table_util::{find_sort_columns, make_ordering};

pub trait ScanPartition: Send + Sync + Debug + 'static {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        batch_size: usize,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}

#[derive(Debug)]
pub(crate) struct PartitionedTableProvider<T, S> {
    partition_selector: S,
    schema: SchemaRef,
    ordering: Vec<String>,
    partition_scanner: T,
    partition_key_extractor: FirstMatchingPartitionKeyExtractor,
}

impl<T, S> PartitionedTableProvider<T, S> {
    pub(crate) fn new(
        partition_selector: S,
        schema: SchemaRef,
        ordering: Vec<String>,
        partition_scanner: T,
        partition_key_extractor: FirstMatchingPartitionKeyExtractor,
    ) -> Self {
        Self {
            partition_selector,
            schema,
            ordering,
            partition_scanner,
            partition_key_extractor,
        }
    }
}

#[derive(Debug, Clone)]
struct LogicalPartition {
    physical_partitions: Vec<(PartitionId, Partition)>,
}

impl LogicalPartition {
    fn new(physical_partitions: Vec<(PartitionId, Partition)>) -> Self {
        Self {
            physical_partitions,
        }
    }
}

fn physical_partitions_to_logical(
    physical_partitions: Vec<(PartitionId, Partition)>,
    target_partitions: usize,
) -> Vec<LogicalPartition> {
    if physical_partitions.len() <= target_partitions {
        // don't bother to coalesce physical partitions together, just
        // use them as-is.
        return physical_partitions
            .into_iter()
            .map(|p| LogicalPartition::new(vec![p]))
            .collect();
    }

    let mut logical_partitions = vec![LogicalPartition::new(Default::default()); target_partitions];
    let mut logical_index = 0;

    for partition in physical_partitions {
        logical_partitions[logical_index]
            .physical_partitions
            .push(partition);
        logical_index = (logical_index + 1) % target_partitions;
    }

    logical_partitions
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
        state: &(dyn datafusion::catalog::Session),
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => SchemaRef::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        let partition_key = self
            .partition_key_extractor
            .try_extract(filters)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let physical_partitions: Vec<(PartitionId, Partition)> = self
            .partition_selector
            .get_live_partitions()
            .await
            .map_err(DataFusionError::External)?
            .into_iter()
            .filter_map(|(partition_id, partition)| {
                let Some(partition_key) = partition_key else {
                    return Some((partition_id, partition));
                };
                if partition.key_range.contains(&partition_key) {
                    let new_range = partition_key..=partition_key;
                    let new_partition = Partition::new(partition_id, new_range);
                    Some((partition_id, new_partition))
                } else {
                    None
                }
            })
            .collect();

        let target_partitions = state.config().target_partitions();
        let logical_partitions =
            physical_partitions_to_logical(physical_partitions, target_partitions);

        let sort_columns = find_sort_columns(&self.ordering, &projected_schema);

        let eq_properties = if sort_columns.is_empty() {
            EquivalenceProperties::new(projected_schema.clone())
        } else {
            let ordering = make_ordering(sort_columns.clone());
            EquivalenceProperties::new_with_orderings(projected_schema.clone(), [ordering])
        };

        let plan = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(logical_partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(
            // all our scan functions use RecordBatchReceiverStream to build the result, which is cooperative
            datafusion::physical_plan::execution_plan::SchedulingType::Cooperative,
        );

        Ok(Arc::new(PartitionedExecutionPlan {
            logical_partitions,
            projected_schema,
            limit,
            scanner: self.partition_scanner.clone(),
            plan,
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
    logical_partitions: Vec<LogicalPartition>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    scanner: T,
    plan: PlanProperties,
}

impl<T> ExecutionPlan for PartitionedExecutionPlan<T>
where
    T: ScanPartition + Clone + Send,
{
    fn name(&self) -> &str {
        "PartitionedExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let physical_partitions = self
            .logical_partitions
            .get(partition)
            .expect("partition exists")
            .physical_partitions
            .to_vec();

        let sequential_scanners_stream = stream::iter(physical_partitions)
            .map({
                let scanner = self.scanner.clone();
                let schema = self.projected_schema.clone();
                let limit = self.limit;
                let batch_size = context.session_config().batch_size();
                move |(partition_id, partition)| {
                    scanner
                        .scan_partition(
                            partition_id,
                            partition.key_range,
                            schema.clone(),
                            batch_size,
                            limit,
                        )
                        .map_err(|e| DataFusionError::External(e.into()))
                }
            })
            .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            sequential_scanners_stream,
        )))
    }
}

impl<T> DisplayAs for PartitionedExecutionPlan<T>
where
    T: Debug,
{
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PartitionedExecutionPlan({:?})", self.scanner)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "PartitionedExecutionPlan\nscanner={:?}", self.scanner)
            }
        }
    }
}

// Generic-based table provider that provides node-level or global data rather than
// partition-keyed data.
pub(crate) trait Scan: Debug + Send + Sync + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream;
}

pub(crate) type ScannerRef = Arc<dyn Scan>;

#[derive(Debug)]
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
        _state: &(dyn datafusion::catalog::Session),
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
    plan_properties: PlanProperties,
}

impl GenericExecutionPlan {
    fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        scanner: ScannerRef,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());

        let plan_properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            projected_schema,
            scanner,
            limit,
            filters: filters.to_vec(),
            plan_properties,
        }
    }
}

impl ExecutionPlan for GenericExecutionPlan {
    fn name(&self) -> &str {
        "GenericExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let stream = self.scanner.scan(
            self.projected_schema.clone(),
            &self.filters,
            context.session_config().batch_size(),
            self.limit,
        );
        Ok(stream)
    }
}

impl DisplayAs for GenericExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "GenericExecutionPlan()",)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "GenericExecutionPlan()",)
            }
        }
    }
}
