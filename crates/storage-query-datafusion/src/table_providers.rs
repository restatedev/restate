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

use crate::context::SelectPartitions;
use crate::partition_filter::{FirstMatchingPartitionKeyExtractor, PartitionKeyExtractor};
use crate::table_util::{find_sort_columns, make_ordering};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::partition_table::Partition;

pub trait ScanPartition: Send + Sync + Debug + 'static {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
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

fn filter_partitions(
    partition_key: PartitionKey,
    mut partitions: impl Iterator<Item = (PartitionId, Partition)>,
) -> Vec<(PartitionId, Partition)> {
    partitions
        .find_map(|(partition_id, partition)| {
            if partition.key_range.contains(&partition_key) {
                let new_range = partition_key..=partition_key;
                let new_partition = Partition::new(partition_id, new_range);
                Some((partition_id, new_partition))
            } else {
                None
            }
        })
        .into_iter()
        .collect()
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

        let live_partitions = self
            .partition_selector
            .get_live_partitions()
            .await
            .map_err(DataFusionError::External)?;

        let partitions_to_scan = if let Some(partition_key) = partition_key {
            filter_partitions(partition_key, live_partitions.into_iter())
        } else {
            live_partitions
        };

        let sort_columns = find_sort_columns(&self.ordering, &projected_schema);

        let eq_properties = if sort_columns.is_empty() {
            EquivalenceProperties::new(projected_schema.clone())
        } else {
            let ordering = make_ordering(sort_columns.clone());
            EquivalenceProperties::new_with_orderings(projected_schema.clone(), &[ordering])
        };

        let partition_plan = if sort_columns.is_empty() {
            Partitioning::UnknownPartitioning(partitions_to_scan.len())
        } else if partitions_to_scan.len() <= state.config_options().execution.target_partitions {
            Partitioning::Hash(sort_columns, partitions_to_scan.len())
        } else {
            // although we have a valid sort order, we must ask datafusion to repartition the rows
            // to fit into `target_partitions`.
            // this is a hidden requirement/bug in the scan interface.
            Partitioning::UnknownPartitioning(partitions_to_scan.len())
        };

        let plan = PlanProperties::new(
            eq_properties,
            partition_plan,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Arc::new(PartitionedExecutionPlan {
            live_partitions: partitions_to_scan,
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
    live_partitions: Vec<(PartitionId, Partition)>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    scanner: T,
    plan: PlanProperties,
}

impl<T> ExecutionPlan for PartitionedExecutionPlan<T>
where
    T: ScanPartition,
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
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let (partition_id, partition) = self
            .live_partitions
            .get(partition)
            .expect("num_partitions within bounds");
        let range = partition.key_range.clone();
        let stream = self
            .scanner
            .scan_partition(
                *partition_id,
                range,
                self.projected_schema.clone(),
                self.limit,
            )
            .map_err(|e| DataFusionError::External(e.into()))?;
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
pub(crate) trait Scan: Debug + Send + Sync + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
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
