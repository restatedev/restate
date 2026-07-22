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
pub mod migrations;
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
pub use migrations::{MigrationError, migrate_to_locks_table};
pub use partition_db::PartitionDb;
pub use partition_store::*;
pub use partition_store_manager::*;
// re-export
pub use restate_rocksdb::Priority;

use crate::scan::TableScan;

// FixedPrefixTransform requires seek keys to be at least as long as its configured prefix.
// Shorter prefixes must bypass prefix seeking and its bloom filters.
fn configure_prefix_iterator_opts<B: Into<Vec<u8>>>(opts: &mut rocksdb::ReadOptions, prefix: B) {
    let prefix = prefix.into();
    if prefix.len() >= DB_PREFIX_LENGTH {
        opts.set_prefix_same_as_start(true);
        opts.set_total_order_seek(false);
    } else {
        opts.set_prefix_same_as_start(false);
        opts.set_total_order_seek(true);
    }
    opts.set_iterate_range(rocksdb::PrefixRange(prefix));
}

fn configure_range_iterator_opts<S, E>(
    opts: &mut rocksdb::ReadOptions,
    scan_mode: ScanMode,
    start: S,
    end: E,
) where
    S: Into<Vec<u8>>,
    E: Into<Vec<u8>>,
{
    opts.set_total_order_seek(scan_mode == ScanMode::TotalOrder);
    opts.set_iterate_range(start.into()..end.into());
}

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
