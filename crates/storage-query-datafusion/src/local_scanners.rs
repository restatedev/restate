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
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor};
use restate_worker_api::PartitionQueryAccess;

use crate::inbox::schema::SysInboxTable;
use crate::invocation_state::schema::SysInvocationStateTable;
use crate::invocation_status::schema::SysInvocationStatusTable;
use crate::journal::schema::SysJournalTable;
use crate::journal_events::schema::SysJournalEventsTable;
use crate::locks::schema::SysLocksTable;
use crate::migration_records::{IndexesTable, MigrationJobsTable, create_local_scanners};
use crate::promise::schema::SysPromiseTable;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::scheduler_status::schema::SysSchedulerTable;
use crate::state::schema::StateTable;
use crate::stats::{
    StatTable, SysDeploymentStatsTable, SysServiceStatsTable, SysVirtualObjectStatsTable,
};
use crate::user_limits::schema::SysUserLimitsTable;
use crate::vqueue_entry_status::schema::SysVqueueEntryStatusTable;
use crate::vqueue_meta::schema::SysVqueueMetaTable;
use crate::vqueues::schema::SysVqueuesTable;

/// Registers live sources for a worker, an offline backend, or a test backend.
pub fn register_live_scanners(
    access: Arc<dyn PartitionQueryAccess>,
    remote_scanner_manager: &RemoteScannerManager,
) {
    remote_scanner_manager.register_partition_scanner::<SysInvocationStateTable>(Arc::new(
        SysInvocationStateTable::create_local_scanner(Arc::clone(&access)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysSchedulerTable>(Arc::new(
        SysSchedulerTable::create_local_scanner(Arc::clone(&access)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysUserLimitsTable>(Arc::new(
        SysUserLimitsTable::create_local_scanner(access),
    ));
}

/// Registers the scanners of the stored tables. Their reads run on
/// `io_executor`, the query IO pool the rest of the query engine also uses.
pub fn register_partition_scanners(
    partition_store_manager: Arc<PartitionStoreManager>,
    io_executor: QueryIoExecutor,
    remote_scanner_manager: &RemoteScannerManager,
) {
    macro_rules! register_statistics {
        ($($table:ty),+ $(,)?) => { $(
            remote_scanner_manager.register_partition_scanner::<$table>(Arc::new(
                <$table>::create_local_scanner(
                    Arc::clone(&partition_store_manager),
                    io_executor.clone(),
                ),
            ));
        )+ };
    }
    register_statistics!(
        SysServiceStatsTable,
        SysDeploymentStatsTable,
        SysVirtualObjectStatsTable
    );
    remote_scanner_manager.register_partition_scanner::<StateTable>(Arc::new(
        StateTable::create_local_scanner(Arc::clone(&partition_store_manager), io_executor.clone()),
    ));
    remote_scanner_manager.register_partition_scanner::<SysInvocationStatusTable>(Arc::new(
        SysInvocationStatusTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysLocksTable>(Arc::new(
        SysLocksTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysJournalTable>(Arc::new(
        SysJournalTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysJournalEventsTable>(Arc::new(
        SysJournalEventsTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysInboxTable>(Arc::new(
        SysInboxTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysPromiseTable>(Arc::new(
        SysPromiseTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysVqueueMetaTable>(Arc::new(
        SysVqueueMetaTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysVqueueEntryStatusTable>(Arc::new(
        SysVqueueEntryStatusTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
            io_executor.clone(),
        ),
    ));
    remote_scanner_manager.register_partition_scanner::<SysVqueuesTable>(Arc::new(
        SysVqueuesTable::create_local_scanner(partition_store_manager, io_executor),
    ));
}

/// Registers this worker node's scanners of the cluster tables `indexes` and
/// `migration_jobs`. They read the partition replicas `partition_store_manager`
/// has open on `io_executor`, the shared query IO pool.
pub fn register_cluster_scanners(
    partition_store_manager: Arc<PartitionStoreManager>,
    metadata: Metadata,
    io_executor: QueryIoExecutor,
    remote_scanner_manager: &RemoteScannerManager,
) {
    let (indexes, jobs) = create_local_scanners(partition_store_manager, metadata, io_executor);
    remote_scanner_manager.register_node_scanner::<IndexesTable>(indexes);
    remote_scanner_manager.register_node_scanner::<MigrationJobsTable>(jobs);
}
