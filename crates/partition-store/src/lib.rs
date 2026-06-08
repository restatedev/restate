// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod deduplication_table;
mod durable_lsn_tracking;
pub mod error;
pub mod fsm_table;
pub mod inbox_table;
pub mod invocation_status_table;
pub mod journal_events;
pub mod journal_table;
pub mod journal_table_v2;
pub mod keys;
pub mod locks_table;
mod memory;
mod metric_definitions;
mod migrations;
pub mod outbox_table;
mod owned_iter;
mod partition_db;
mod partition_store;
mod partition_store_manager;
pub mod promise_table;
pub mod scan;
pub mod service_status_table;
pub mod snapshots;
pub mod state_table;
pub mod timer_table;
pub mod vqueue_table;

#[cfg(test)]
mod tests;

pub use error::*;
pub use journal_table_v2::{OrphanCleanupResult, cleanup_orphaned_completion_id_index_entries};
pub use partition_db::PartitionDb;
pub use partition_store::*;
pub use partition_store_manager::*;
// re-export
pub use restate_rocksdb::Priority;

use crate::scan::TableScan;

// Optimized for modern CPU branch predictors
#[inline]
fn convert_to_upper_bound(bytes: &mut [u8]) -> bool {
    for b in bytes.iter_mut().rev() {
        let x = *b;
        if x != 0xFF {
            *b = x.wrapping_add(1); // safe: we just checked != 0xFF
            return true;
        }
        *b = 0;
    }
    false
}
