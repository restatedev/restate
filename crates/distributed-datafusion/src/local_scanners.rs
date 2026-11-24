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

use restate_core::Metadata;
use restate_limiter::rule_book::RuleBookObserver;
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor};
use restate_types::live::Live;
use restate_types::schema::deployment::DeploymentResolver;
use restate_types::schema::service::ServiceMetadataResolver;
use restate_worker_api::ProcessorsManagerHandle;

use crate::source_registry::SourceRegistry;
use crate::tables::deployment::schema::SysDeploymentTable;
use crate::tables::invocation_state::SysInvocationStateTable;
use crate::tables::invocation_status::SysInvocationStatusTable;
use crate::tables::journal::SysJournalTable;
use crate::tables::journal_events::SysJournalEventsTable;
use crate::tables::locks::SysLocksTable;
use crate::tables::migration_records::{MigrationJobsTable, RegistryTable};
use crate::tables::partition_state::{PartitionStateReports, PartitionStateTable};
use crate::tables::promise::SysPromiseTable;
use crate::tables::rules::schema::SysRulesTable;
use crate::tables::scheduler_status::schema::SysSchedulerTable;
use crate::tables::service::schema::SysServiceTable;
use crate::tables::state::StateTable;
use crate::tables::stats::deployment_stats::schema::{
    DeploymentStatsSource, SysDeploymentStatsTable,
};
use crate::tables::stats::service_stats::schema::{ServiceStatsSource, SysServiceStatsTable};
use crate::tables::stats::virtual_object_stats::schema::{
    SysVirtualObjectStatsTable, VirtualObjectStatsSource,
};
use crate::tables::user_limits::SysUserLimitsTable;
use crate::tables::vqueue_entry_status::schema::SysVqueueEntryStatusTable;
use crate::tables::vqueue_meta::SysVqueueMetaTable;

/// Registers live sources for a worker, an offline backend, or a test backend.
pub fn register_live_scanners(sources: &SourceRegistry) {
    sources.register_declared_scanner::<SysUserLimitsTable>(Arc::new(
        SysUserLimitsTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<SysInvocationStateTable>(Arc::new(
        SysInvocationStateTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<SysSchedulerTable>(Arc::new(
        SysSchedulerTable::create_local_scanner(),
    ));
}

/// Registers complete metadata sources on every node, independently of its roles.
/// Metadata tables route each query to one node serving them, preferring this one.
///
/// `rule_book_observer` is the co-located worker's rule-book cache, if any;
/// `sys_rules` reads it before falling back to the metadata store.
pub fn register_metadata_scanners(
    schemas: Live<
        impl DeploymentResolver + ServiceMetadataResolver + Clone + Send + Sync + 'static,
    >,
    metadata_store_client: MetadataStoreClient,
    rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    sources: &SourceRegistry,
) {
    sources.register_node_scanner::<SysServiceTable>(Arc::new(
        SysServiceTable::create_local_scanner(schemas.clone()),
    ));
    sources.register_node_scanner::<SysDeploymentTable>(Arc::new(
        SysDeploymentTable::create_local_scanner(schemas),
    ));
    sources.register_node_scanner::<SysRulesTable>(Arc::new(SysRulesTable::create_local_scanner(
        metadata_store_client,
        rule_book_observer,
    )));
}

/// Restored/offline storage. Running workers use [`register_worker_partition_scanners`].
/// Storage reads run on `query_io`, the query IO pool the existing engine uses too.
pub fn register_partition_scanners(
    partition_store_manager: Arc<PartitionStoreManager>,
    query_io: QueryIoExecutor,
    sources: &SourceRegistry,
) {
    sources.register_offline_reads(partition_store_manager, query_io);
    register_storage_sources(sources);
}

/// Production storage access is admitted by the local partition processors.
/// Storage reads run on `query_io`, the query IO pool the existing engine uses too.
pub fn register_worker_partition_scanners(
    processors: ProcessorsManagerHandle,
    query_io: QueryIoExecutor,
    sources: &SourceRegistry,
) {
    sources.register_processor_reads(processors, query_io);
    register_storage_sources(sources);
}

/// The worker's report for `cluster.partition_state`, which coordinators ask
/// every worker for.
pub fn register_partition_state_reports(
    processors: ProcessorsManagerHandle,
    metadata: Metadata,
    sources: &SourceRegistry,
) {
    sources.register_node_scanner::<PartitionStateReports>(
        PartitionStateTable::create_reports_scanner(processors, metadata),
    );
}

fn register_storage_sources(sources: &SourceRegistry) {
    sources.register_declared_scanner::<SysInvocationStatusTable>(Arc::new(
        SysInvocationStatusTable::create_local_scanner(),
    ));
    StateTable::register_scanners(sources);
    SysJournalTable::register_scanners(sources);
    SysPromiseTable::register_scanners(sources);
    sources.register_declared_scanner::<SysJournalEventsTable>(Arc::new(
        SysJournalEventsTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<SysLocksTable>(Arc::new(
        SysLocksTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<SysVqueueMetaTable>(Arc::new(
        SysVqueueMetaTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<SysVqueueEntryStatusTable>(Arc::new(
        SysVqueueEntryStatusTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<ServiceStatsSource>(Arc::new(
        SysServiceStatsTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<DeploymentStatsSource>(Arc::new(
        SysDeploymentStatsTable::create_local_scanner(),
    ));
    sources.register_declared_scanner::<VirtualObjectStatsSource>(Arc::new(
        SysVirtualObjectStatsTable::create_local_scanner(),
    ));
}

/// Registers this worker node's scanners of the cluster tables `registry` and
/// `migration_jobs`: the default engine's, which read the partition replicas
/// `partition_store_manager` has open on `query_io`. Register them before the
/// cluster tables.
pub fn register_cluster_scanners(
    partition_store_manager: Arc<PartitionStoreManager>,
    metadata: Metadata,
    query_io: QueryIoExecutor,
    sources: &SourceRegistry,
) {
    let (registry, jobs) =
        restate_storage_query_datafusion::migration_records::create_local_scanners(
            partition_store_manager,
            metadata,
            query_io,
        );
    sources.register_node_scanner::<RegistryTable>(registry);
    sources.register_node_scanner::<MigrationJobsTable>(jobs);
}
