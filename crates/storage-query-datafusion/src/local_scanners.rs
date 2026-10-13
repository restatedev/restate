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

use restate_partition_store::PartitionStoreManager;
use restate_worker_api::PartitionQueryAccess;

use crate::inbox::schema::SysInboxTable;
use crate::index::busy_vqueue::schema::IdxBusyVqueueTable;
use crate::index::by_service::schema::IdxEntryByServiceTable;
use crate::index::by_virtual_object::schema::IdxEntryByVirtualObjectTable;
use crate::index::entry_by_stage::schema::IdxEntryByStageTable;
use crate::index::entry_next_at_by_service::schema::IdxEntryNextAtByServiceTable;
use crate::index::entry_next_at_by_stage::schema::IdxEntryNextAtByStageTable;
use crate::index::entry_next_at_by_virtual_object::schema::IdxEntryNextAtByVirtualObjectTable;
use crate::invocation_state::schema::SysInvocationStateTable;
use crate::invocation_status::schema::SysInvocationStatusTable;
use crate::journal::schema::SysJournalTable;
use crate::journal_events::schema::SysJournalEventsTable;
use crate::locks::schema::SysLocksTable;
use crate::promise::schema::SysPromiseTable;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::scheduler_status::schema::SysSchedulerTable;
use crate::state::schema::StateTable;
use crate::stats::deployment_stats::schema::SysDeploymentStatsTable;
use crate::stats::service_stats::schema::SysServiceStatsTable;
use crate::stats::virtual_object_stats::schema::SysVirtualObjectStatsTable;
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

pub fn register_partition_scanners(
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) {
    remote_scanner_manager.register_partition_scanner::<StateTable>(Arc::new(
        StateTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysInvocationStatusTable>(Arc::new(
        SysInvocationStatusTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysLocksTable>(Arc::new(
        SysLocksTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysJournalTable>(Arc::new(
        SysJournalTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysJournalEventsTable>(Arc::new(
        SysJournalEventsTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysInboxTable>(Arc::new(
        SysInboxTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysPromiseTable>(Arc::new(
        SysPromiseTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysVqueueMetaTable>(Arc::new(
        SysVqueueMetaTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysVqueueEntryStatusTable>(Arc::new(
        SysVqueueEntryStatusTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysVqueuesTable>(Arc::new(
        SysVqueuesTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<IdxEntryByServiceTable>(Arc::new(
        IdxEntryByServiceTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<IdxEntryByStageTable>(Arc::new(
        IdxEntryByStageTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<IdxEntryNextAtByServiceTable>(Arc::new(
        IdxEntryNextAtByServiceTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<IdxEntryByVirtualObjectTable>(Arc::new(
        IdxEntryByVirtualObjectTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<IdxEntryNextAtByVirtualObjectTable>(
        Arc::new(IdxEntryNextAtByVirtualObjectTable::create_local_scanner(
            Arc::clone(&partition_store_manager),
        )),
    );
    remote_scanner_manager.register_partition_scanner::<IdxBusyVqueueTable>(Arc::new(
        IdxBusyVqueueTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<IdxEntryNextAtByStageTable>(Arc::new(
        IdxEntryNextAtByStageTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysServiceStatsTable>(Arc::new(
        SysServiceStatsTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysDeploymentStatsTable>(Arc::new(
        SysDeploymentStatsTable::create_local_scanner(Arc::clone(&partition_store_manager)),
    ));
    remote_scanner_manager.register_partition_scanner::<SysVirtualObjectStatsTable>(Arc::new(
        SysVirtualObjectStatsTable::create_local_scanner(partition_store_manager),
    ));
}
