// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tokio::time::Instant;
use tracing::warn;

use restate_partition_store::PartitionDb;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::net::partition_processor::PartitionProcessorRpcError;

const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const FULL_PERCENT: u64 = 90;

/// Rejects new invocations once the partition uses 90% of `worker.storage.partition-capacity`.
#[derive(Default)]
pub(super) struct StorageGuard {
    used_bytes: u64,
    refreshed_at: Option<Instant>,
}

impl StorageGuard {
    pub fn check(
        &mut self,
        partition_id: PartitionId,
        partition_db: &PartitionDb,
    ) -> Result<(), PartitionProcessorRpcError> {
        let Some(capacity) = Configuration::pinned().worker.storage.partition_capacity else {
            return Ok(());
        };

        if self
            .refreshed_at
            .is_none_or(|at| at.elapsed() >= REFRESH_INTERVAL)
        {
            match partition_db.live_sst_files_size() {
                Ok(used_bytes) => self.used_bytes = used_bytes,
                Err(err) => warn!(%partition_id, "Failed to read the partition store size: {err}"),
            }
            self.refreshed_at = Some(Instant::now());
        }

        if is_full(self.used_bytes, capacity.as_u64()) {
            return Err(PartitionProcessorRpcError::PartitionFull {
                partition_id,
                used_bytes: self.used_bytes,
                capacity_bytes: capacity.as_u64(),
            });
        }
        Ok(())
    }
}

fn is_full(used_bytes: u64, capacity_bytes: u64) -> bool {
    u128::from(used_bytes) * 100 >= u128::from(capacity_bytes) * u128::from(FULL_PERCENT)
}

#[cfg(test)]
mod tests {
    use super::is_full;

    #[test]
    fn full_at_ninety_percent_of_capacity() {
        assert!(!is_full(899, 1000));
        assert!(is_full(900, 1000));
        assert!(is_full(2000, 1000));
        assert!(!is_full(u64::MAX / 2, u64::MAX));
        assert!(is_full(u64::MAX, u64::MAX));
    }
}
