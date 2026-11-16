// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_query_api::LogicalSchema;
use restate_storage_query_api::tables::stats::{
    deployment_stats::DeploymentStats, service_stats::ServiceStats,
    virtual_object_stats::VirtualObjectStats,
};
use restate_storage_query_api::tables::{
    bifrost_read_streams::BifrostReadStreams,
    config::Config,
    deployment::Deployment,
    index::{EntryByQueue, EntryByService},
    indexes::Indexes,
    invocation_state::InvocationState,
    journal::Journal,
    journal_events::JournalEvents,
    locks::Locks,
    loglet_workers::LogletWorkers,
    logs::Logs,
    migration_jobs::MigrationJobs,
    nodes::Nodes,
    partition_replica_set::PartitionReplicaSet,
    partition_state::PartitionState,
    partitions::Partitions,
    promise::Promise,
    rules::Rules,
    scheduler::Scheduler,
    service::Service,
    state::State,
    vqueue_entry_status::VqueueEntryStatus,
};

pub use restate_storage_query_api::schema::rows::TableColumn;

/// Documentation emitted by the shared logical declarations.
#[derive(Debug, Copy, Clone)]
pub struct StaticTableDocs {
    pub name: &'static str,
    pub description: &'static str,
    pub columns: &'static [TableColumn],
}

macro_rules! table_docs {
    ($schema:ty) => {
        StaticTableDocs {
            name: <$schema>::NAME,
            description: <$schema>::DESCRIPTION,
            columns: <$schema>::COLUMN_DOCS,
        }
    };
}

pub const ALL_TABLE_DOCS: &[StaticTableDocs] = &[
    table_docs!(restate_storage_query_api::tables::user_limits::UserLimits),
    table_docs!(restate_storage_query_api::tables::invocation_status::InvocationStatus),
    table_docs!(BifrostReadStreams),
    table_docs!(Config),
    table_docs!(Deployment),
    table_docs!(EntryByService),
    table_docs!(EntryByQueue),
    table_docs!(Indexes),
    table_docs!(InvocationState),
    table_docs!(Journal),
    table_docs!(JournalEvents),
    table_docs!(Locks),
    table_docs!(LogletWorkers),
    table_docs!(Logs),
    table_docs!(MigrationJobs),
    table_docs!(Promise),
    table_docs!(restate_storage_query_api::tables::vqueue_meta::VqueueMeta),
    table_docs!(Nodes),
    table_docs!(Partitions),
    table_docs!(PartitionReplicaSet),
    table_docs!(PartitionState),
    table_docs!(Rules),
    table_docs!(Scheduler),
    table_docs!(Service),
    table_docs!(State),
    table_docs!(VqueueEntryStatus),
    table_docs!(DeploymentStats),
    table_docs!(ServiceStats),
    table_docs!(VirtualObjectStats),
];
