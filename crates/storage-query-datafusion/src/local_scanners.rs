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

use crate::remote_query_scanner_manager::RemoteScannerManager;

/// Registers live sources for a worker, an offline backend, or a test backend.
pub fn register_live_scanners(
    access: Arc<dyn PartitionQueryAccess>,
    remote_scanner_manager: &RemoteScannerManager,
) {
    crate::invocation_state::register_local_scanner(Arc::clone(&access), remote_scanner_manager);
    crate::scheduler_status::register_local_scanner(Arc::clone(&access), remote_scanner_manager);
    crate::user_limits::register_local_scanner(access, remote_scanner_manager);
}

pub fn register_partition_scanners(
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) {
    crate::state::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::invocation_status::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::locks::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::journal::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::journal_events::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::inbox::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::promise::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::vqueue_meta::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::vqueue_entry_status::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::vqueues::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::index::by_service::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::index::entry_by_stage::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::index::entry_next_at_by_service::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::index::by_virtual_object::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::index::entry_next_at_by_virtual_object::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::index::busy_vqueue::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::index::entry_next_at_by_stage::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::stats::service_stats::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::stats::deployment_stats::register_local_scanner(
        Arc::clone(&partition_store_manager),
        remote_scanner_manager,
    );
    crate::stats::virtual_object_stats::register_local_scanner(
        partition_store_manager,
        remote_scanner_manager,
    );
}
