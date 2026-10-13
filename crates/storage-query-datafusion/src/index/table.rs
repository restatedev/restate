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
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::PhysicalExpr;

use restate_storage_api::filter::{Filter, FilterTarget, LiveFilter};
use restate_storage_query_api::QueryEngineTable;
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, LivePredicate};
use crate::partition_store_scanner::ScanLocalPartitionFilter;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::PartitionedTableProvider;

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

pub(super) fn create_provider<T: QueryEngineTable>(
    partition_selector: impl SelectPartitions,
    remote: &RemoteScannerManager,
    schema: SchemaRef,
    extractor: FirstMatchingPartitionKeyExtractor,
) -> Arc<dyn TableProvider> {
    let statistics =
        TableStatisticsBuilder::new(schema.clone()).with_num_rows_estimate(RowEstimate::Large);
    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        Vec::new(),
        // Local physical index ordering is not a global SQL ordering.
        remote.create_distributed_scanner::<T>(),
        extractor,
    )
    .with_statistics(statistics.build());
    Arc::new(table)
}
