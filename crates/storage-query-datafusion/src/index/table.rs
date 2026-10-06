// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::PhysicalExpr;

use restate_partition_store::PartitionStoreManager;
use restate_storage_api::filter::{Filter, FilterTarget, LiveFilter};
use restate_types::sharding::KeyRange;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, LivePredicate};
use crate::partition_store_scanner::{
    LocalPartitionsScanner, ScanLocalPartition, ScanLocalPartitionFilter,
};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

pub(super) struct IndexFilter<T: FilterTarget> {
    pub range: KeyRange,
    pub predicate: Filter<T>,
    /// Refines `predicate` with the query's dynamic filters during the scan.
    pub live: Option<Box<dyn LiveFilter<T>>>,
}

impl<T: FilterTarget + 'static> ScanLocalPartitionFilter for IndexFilter<T> {
    fn new(range: KeyRange, access_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        Self::new_live(range, access_predicate, None)
    }

    fn new_live(
        range: KeyRange,
        access_predicate: Option<Arc<dyn PhysicalExpr>>,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let live = LivePredicate::new(range, access_predicate.as_ref(), predicate)
            .map(|live| Box::new(live) as Box<dyn LiveFilter<T>>);
        Self {
            range,
            predicate: Filter::new(range, access_predicate),
            live,
        }
    }
}

pub(super) fn register<S>(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    manager: Arc<PartitionStoreManager>,
    remote: &RemoteScannerManager,
    name: &'static str,
    schema: SchemaRef,
    extractor: FirstMatchingPartitionKeyExtractor,
) -> datafusion::common::Result<()>
where
    S: ScanLocalPartition + Default,
    S::Builder: Send + Sync + 'static,
{
    let scanner =
        Arc::new(LocalPartitionsScanner::new(manager, S::default())) as Arc<dyn ScanPartition>;
    let statistics =
        TableStatisticsBuilder::new(schema.clone()).with_num_rows_estimate(RowEstimate::Large);
    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        Vec::new(),
        // Local physical index ordering is not a global SQL ordering.
        remote.create_distributed_scanner(name, scanner),
        extractor,
    )
    .with_statistics(statistics.build());
    ctx.register_partitioned_table(name, Arc::new(table))
}
