// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The statistics tables of the V1 engine: `sys_service_stats`,
//! `sys_deployment_stats` and `sys_virtual_object_stats`, with the same columns
//! and sums as V2.
//!
//! Each partition stores its own counts. For example, if partitions 1 and 2
//! count 2 and 3 running entries of `counter/inc`, the service table must show
//! one row with 5. So the service and deployment tables are views that sum the
//! partitions' rows first: a filter such as `num_entries > 4`, an `ORDER BY` or
//! a `LIMIT` then sees 5, never 2 or 3. Queries can't see the per-partition
//! rows behind the views. Virtual-object rows aren't summed: each keeps its
//! entries' routing `partition_key`. Entries of one object can be routed by
//! different keys, for example in custom queues, and their rows stay separate.
//!
//! A scan reads only statistics that are Ready in its read snapshot. If a
//! partition can't provide them, the query fails rather than return a partial
//! total.

use std::marker::PhantomData;
use std::ops::Bound::Included;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::{ViewTable, provider_as_source};
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::{LogicalPlanBuilder, col};
use datafusion::physical_plan::PhysicalExpr;

use restate_partition_store::stats::aggregated::{
    DeploymentStatsBinding, ServiceStatsBinding, VirtualObjectStatsBinding,
};
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::scan::ScanOutcome;
use restate_storage_query_api::scan::{CursorScan, Table};
use restate_storage_query_api::selection::{Selection, ValuePredicate, compile};
use restate_storage_query_api::tables::stats::deployment_stats::DeploymentStats;
use restate_storage_query_api::tables::stats::service_stats::ServiceStats;
use restate_storage_query_api::tables::stats::virtual_object_stats::{
    VirtualObjectStats, VirtualObjectStatsClause,
};
use restate_storage_query_api::{LogicalSchema, QueryEngineTable, SchemaBinding};
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{
    LocalPartitionsScanner, Rows, ScanLocalPartition, ScanLocalPartitionFilter,
};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::table_util::SchemaBuilder;

#[cfg(test)]
mod tests;

macro_rules! table_identity {
    ($table:ident, $schema:ty) => {
        #[derive(Debug, Clone)]
        pub(crate) struct $table;

        impl QueryEngineTable for $table {
            fn identity() -> ReString {
                <$schema as LogicalSchema>::NAME.into()
            }
        }
    };
}

table_identity!(SysServiceStatsTable, ServiceStats);
table_identity!(SysDeploymentStatsTable, DeploymentStats);
table_identity!(SysVirtualObjectStatsTable, VirtualObjectStats);

type StatSchema<T> = <<T as StatTable>::Binding as SchemaBinding>::Schema;
type StatStorage<T> = <<T as StatTable>::Binding as SchemaBinding>::Table;

/// A statistics table: how to read it from storage, and the table queries see.
pub(crate) trait StatTable: QueryEngineTable {
    /// Reads the stored statistic and builds the table's rows from it.
    type Binding: SchemaBinding<Schema: LogicalSchema<RowBuilder: Sync>>;

    /// Whether queries see the partitions' rows summed; see the module
    /// documentation.
    const SUM_PARTITIONS: bool;

    /// What a scan of one partition reads: `range` is the part of the
    /// partition's keys the scan covers, and `initial` the query's filter as it
    /// was when the scan started. Later updates of the filter only drop rows.
    fn selection(
        range: KeyRange,
        initial: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Selection<StatSchema<Self>>;

    /// The table queries see: the rows of every partition, summed if
    /// [`Self::SUM_PARTITIONS`].
    fn create_provider(
        partition_selector: impl SelectPartitions,
        manager: &RemoteScannerManager,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let raw = Arc::new(PartitionedTableProvider::new(
            partition_selector,
            StatSchema::<Self>::arrow_schema(),
            Vec::new(),
            manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default(),
        ));
        if Self::SUM_PARTITIONS {
            Ok(Arc::new(sum_view(&Self::identity(), raw)?))
        } else {
            Ok(raw)
        }
    }

    /// Scans the table on the partitions this node holds.
    fn create_local_scanner(
        manager: Arc<PartitionStoreManager>,
        executor: QueryIoExecutor,
    ) -> impl ScanPartition
    where
        ReadSnapshot: CursorScan<StatStorage<Self>>,
        for<'a> <StatStorage<Self> as Table>::Record<'a>: Copy + Send,
    {
        LocalPartitionsScanner::<StatScanner<Self>>::new(manager, executor)
    }
}

impl StatTable for SysServiceStatsTable {
    type Binding = ServiceStatsBinding;
    const SUM_PARTITIONS: bool = true;

    fn selection(
        _range: KeyRange,
        initial: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Selection<ServiceStats> {
        compile(initial)
    }
}

impl StatTable for SysDeploymentStatsTable {
    type Binding = DeploymentStatsBinding;
    const SUM_PARTITIONS: bool = true;

    fn selection(
        _range: KeyRange,
        initial: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Selection<DeploymentStats> {
        compile(initial)
    }
}

impl StatTable for SysVirtualObjectStatsTable {
    type Binding = VirtualObjectStatsBinding;
    const SUM_PARTITIONS: bool = false;

    fn selection(
        range: KeyRange,
        initial: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Selection<VirtualObjectStats> {
        // V1 can scan one partition several times in a query, each time for
        // another key range, for example once per value of
        // `WHERE partition_key IN (...)`. Each scan reads only its own range,
        // also when the query doesn't select `partition_key`; otherwise each
        // scan would return the others' rows too, as duplicates.
        compile(initial).and(VirtualObjectStatsClause::PartitionKey(
            ValuePredicate::Range {
                lower: Included(range.start()),
                upper: Included(range.end()),
            },
        ))
    }
}

/// The view that sums `raw`'s per-partition rows, as V2 does: one row for each
/// combination of the other columns, with their `num_entries` added up.
fn sum_view(name: &str, raw: Arc<dyn TableProvider>) -> datafusion::common::Result<ViewTable> {
    let schema = raw.schema();
    let groups = schema
        .fields()
        .iter()
        .filter(|field| field.name() != "num_entries")
        .map(|field| col(field.name()));
    let plan = LogicalPlanBuilder::scan(
        format!("__{name}_partitioned"),
        provider_as_source(raw),
        None,
    )?
    .aggregate(groups, [sum(col("num_entries")).alias("num_entries")])?
    .build()?;
    Ok(ViewTable::new(plan, None))
}

/// Scans statistics table `T` on one partition of this node.
#[derive(derive_more::Debug)]
struct StatScanner<T>(PhantomData<fn() -> T>);

/// What a [`StatScanner`] reads; see [`StatTable::selection`].
struct StatFilter {
    range: KeyRange,
    initial: Option<Arc<dyn PhysicalExpr>>,
}

impl ScanLocalPartitionFilter for StatFilter {
    fn new(range: KeyRange, initial: Option<Arc<dyn PhysicalExpr>>) -> Self {
        Self { range, initial }
    }
}

impl<T: StatTable> ScanLocalPartition for StatScanner<T>
where
    ReadSnapshot: CursorScan<StatStorage<T>>,
    for<'a> <StatStorage<T> as Table>::Record<'a>: Copy + Send,
{
    type Builder = SchemaBuilder<StatSchema<T>>;
    type Filter = StatFilter;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: StatFilter,
        rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        let prepared = T::Binding::prepare(&T::selection(filter.range, filter.initial.as_ref()))?;
        rows.read_binding::<T::Binding>(snapshot, executor, &prepared)
            .await
    }
}
