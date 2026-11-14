// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Owner-local inspection of persisted secondary indexes.

use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_partition_store::vqueue_table::{
    FinishedIndexBinding, InboxIndexBinding, PausedIndexBinding, RunningIndexBinding,
    SuspendedIndexBinding,
};
use restate_storage_query_api::tables::index::{
    EntryByQueue, EntryByQueueColumn, EntryByService, EntryByServiceColumn,
};
use restate_util_string::ReString;

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

super::table_adapter!(pub(crate) EntryByServiceTable, EntryByService);

/// The registered index's instances, which a replica admits only once Ready.
/// Distinct from the table name that sources had before the registry, so a
/// worker without Ready admission fails the work instead of reading its own
/// keys, and the other way around.
#[derive(Debug, Clone)]
pub(crate) struct EntryByServiceSource;

impl QuerySource for EntryByServiceSource {
    type Schema = EntryByService;

    fn identity() -> ReString {
        ReString::from_static("_idx_entry_by_service.v2")
    }
}

impl crate::sharding::PartitionSource for EntryByServiceSource {}

impl EntryByServiceTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<EntryByService>::new()
            .with_num_rows_estimate(RowEstimate::Large)
            .with_foreign_key(EntryByServiceColumn::ServiceName, SERVICE_ROW_ESTIMATE)
            .build();
        // The declaration describes physical key order, not a global SQL order.
        // Inspection does not make this index eligible to replace a primary read.
        Arc::new(
            PartitionedTableProvider::declared::<EntryByServiceSource>(partitions, sources)
                .with_statistics(statistics),
        )
    }

    /// The service index's definition isn't in this build yet (#5856), so its
    /// table stays hidden (see `context.rs`) and a read fails clearly.
    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        UnavailableIndexScanner("entry_by_service")
    }
}

super::table_adapter!(pub(crate) EntryByQueueTable, EntryByQueue);

macro_rules! stage_source {
    ($source:ident, $identity:literal, $binding:ty) => {
        /// One stage table, a disjoint slice of the index.
        #[derive(Debug, Clone)]
        pub(crate) struct $source;

        impl QuerySource for $source {
            type Schema = EntryByQueue;

            fn identity() -> ReString {
                ReString::from_static($identity)
            }
        }

        impl crate::sharding::PartitionSource for $source {}

        impl $source {
            fn register(sources: &SourceRegistry) {
                sources
                    .register_declared_scanner::<Self>(Arc::new(SchemaScanner::<$binding>::new()));
            }
        }
    };
}

stage_source!(
    InboxStageSource,
    "_idx_entry_by_queue.inbox",
    InboxIndexBinding
);
stage_source!(
    RunningStageSource,
    "_idx_entry_by_queue.running",
    RunningIndexBinding
);
stage_source!(
    SuspendedStageSource,
    "_idx_entry_by_queue.suspended",
    SuspendedIndexBinding
);
stage_source!(
    PausedStageSource,
    "_idx_entry_by_queue.paused",
    PausedIndexBinding
);
stage_source!(
    FinishedStageSource,
    "_idx_entry_by_queue.finished",
    FinishedIndexBinding
);

impl EntryByQueueTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<EntryByQueue>::new()
            .with_num_rows_estimate(RowEstimate::Large)
            .with_foreign_key(EntryByQueueColumn::VqueueId, RowEstimate::Large)
            .build();
        // Each stage is a separate physical table; together they form the index.
        // Inspection does not make this index eligible to replace a primary read.
        Arc::new(
            PartitionedTableProvider::declared::<InboxStageSource>(partitions, sources)
                .with_source::<RunningStageSource>(sources)
                .with_source::<SuspendedStageSource>(sources)
                .with_source::<PausedStageSource>(sources)
                .with_source::<FinishedStageSource>(sources)
                .with_statistics(statistics),
        )
    }

    pub(crate) fn register_scanners(sources: &SourceRegistry) {
        InboxStageSource::register(sources);
        RunningStageSource::register(sources);
        SuspendedStageSource::register(sources);
        PausedStageSource::register(sources);
        FinishedStageSource::register(sources);
    }
}

/// A scanner for an index whose definition isn't in this build: every read fails
/// with an error naming the index.
#[derive(Debug)]
pub(crate) struct UnavailableIndexScanner(&'static str);

impl ScanPartition for UnavailableIndexScanner {
    fn prepare_read(
        self: Arc<Self>,
        _access: crate::access::PrimaryRead,
        _projection: datafusion::arrow::datatypes::SchemaRef,
        _predicate: Option<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        _limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn crate::table_providers::PreparedPartitionRead>> {
        anyhow::bail!("index {} isn't available in this build", self.0)
    }
}
