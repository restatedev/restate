// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod migrate_to_locks_table;
pub mod migrate_to_scoped_promise_table;
pub mod migrate_to_scoped_state_table;

use std::num::NonZeroU16;
use std::sync::Arc;

use bytes::BytesMut;
use tokio_util::sync::CancellationToken;

use restate_clock::{AtomicStorage, HlcClock, WallClock};
use restate_storage_api::StorageError;
use restate_types::SemanticRestateVersion;
use restate_types::config::Configuration;
use restate_types::sharding::KeyRange;
use restate_types::sharding::subsharding::ShardPlan;
use restate_util_string::ReString;

use crate::{PartitionDb, Result};

/// Migration error
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("migration barrier: {0}")]
    MigrationBarrier(String),
    #[error("partition requires Restate version {required_min_version} or newer")]
    VersionBarrier {
        required_min_version: SemanticRestateVersion,
    },
    #[error(
        "partition storage requires Restate version {required_min_version} or newer due to enabled storage features {storage_features:?}"
    )]
    StorageFeatureVersionBarrier {
        required_min_version: SemanticRestateVersion,
        storage_features: Vec<ReString>,
    },
    #[error("migration was cancelled")]
    MigrationCancelled,
    #[error("error during migration: {0}")]
    StorageError(#[from] StorageError),
}

#[allow(dead_code)]
pub struct MigrationContext<'a> {
    pub clock: HlcClock<WallClock, Arc<AtomicStorage>>,
    pub arena: BytesMut,
    pub partition_db: &'a PartitionDb,
    pub key_range: KeyRange,
    pub cancel: CancellationToken,
}

#[allow(dead_code)]
impl<'a> MigrationContext<'a> {
    pub fn new(
        config: &Configuration,
        partition_db: &'a PartitionDb,
        key_range: KeyRange,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            clock: HlcClock::new(
                config.common.hlc_max_drift(),
                WallClock,
                Arc::new(AtomicStorage::default()),
            )
            .expect("failed to create HLC clock"),
            arena: BytesMut::default(),
            partition_db,
            key_range,
            cancel,
        }
    }

    pub fn split(&self, max_shards: NonZeroU16) -> std::vec::IntoIter<Self> {
        ShardPlan::new(self.key_range, max_shards)
            .shards()
            .map(|shard| {
                let key_range = *shard.key_range();
                assert!(
                    key_range.num_keys() > 0,
                    "each split range must contain at least one key"
                );
                self.clone_with_key_range(key_range)
            })
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn clone_with_key_range(&self, key_range: KeyRange) -> Self {
        Self {
            clock: self.clock.clone(),
            arena: BytesMut::default(),
            partition_db: self.partition_db,
            key_range,
            cancel: self.cancel.clone(),
        }
    }

    fn fail_if_cancelled(&self) -> Result<(), MigrationError> {
        if self.cancel.is_cancelled() {
            Err(MigrationError::MigrationCancelled)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use restate_clock::ClockUpkeep;
    use restate_rocksdb::RocksDbManager;
    use restate_types::identifiers::{PartitionId, PartitionKey, ServiceId, WithPartitionKey};
    use restate_types::partitions::Partition;
    use std::collections::HashSet;

    use crate::PartitionStoreManager;

    use super::*;

    pub fn distinct_service_ids(count: usize) -> Vec<ServiceId> {
        let mut service_ids = Vec::with_capacity(count);
        let mut partition_keys = HashSet::with_capacity(count);

        for idx in 0..10_000 {
            let service_id = ServiceId::new(None, "migration-svc", format!("migration-key-{idx}"));
            if partition_keys.insert(service_id.partition_key()) {
                service_ids.push(service_id);
                if service_ids.len() == count {
                    return service_ids;
                }
            }
        }

        panic!("failed to find distinct service partition keys");
    }

    #[restate_core::test]
    async fn split_returns_independent_sub_contexts() {
        let _clock_guard = ClockUpkeep::start().expect("clock upkeep should start");
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true)
            .await
            .expect("DB storage creation succeeds");
        let store = manager
            .open(
                &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
                None,
            )
            .await
            .expect("DB storage creation succeeds");

        let config = Configuration::default();
        let cancel = CancellationToken::new();
        let mut ctx = MigrationContext::new(
            &config,
            store.partition_db(),
            KeyRange::new(1, 100),
            cancel.clone(),
        );
        ctx.arena.extend_from_slice(b"dirty parent arena");

        let mut split = ctx.split(NonZeroU16::new(2).expect("two is non-zero"));
        assert_eq!(split.len(), 2);
        let left_ctx = split.next().expect("left context should exist");
        let right_ctx = split.next().expect("right context should exist");
        assert!(split.next().is_none());
        assert_eq!(left_ctx.key_range, KeyRange::new(1, 50));
        assert_eq!(right_ctx.key_range, KeyRange::new(51, 100));
        assert!(left_ctx.arena.is_empty());
        assert!(right_ctx.arena.is_empty());

        let four_shard_ranges = ctx
            .split(NonZeroU16::new(4).expect("four is non-zero"))
            .map(|ctx| ctx.key_range)
            .collect::<Vec<_>>();
        assert_eq!(
            four_shard_ranges,
            vec![
                KeyRange::new(1, 25),
                KeyRange::new(26, 50),
                KeyRange::new(51, 75),
                KeyRange::new(76, 100),
            ]
        );

        let left_timestamp = left_ctx.clock.next();
        let right_timestamp = right_ctx.clock.next();
        assert!(left_timestamp < right_timestamp);

        let single_key_ctx =
            MigrationContext::new(&config, store.partition_db(), KeyRange::new(42, 42), cancel);
        let single_key_ranges = single_key_ctx
            .split(NonZeroU16::new(2).expect("two is non-zero"))
            .map(|ctx| ctx.key_range)
            .collect::<Vec<_>>();
        assert_eq!(single_key_ranges, vec![KeyRange::new(42, 42)]);

        drop((ctx, left_ctx, right_ctx, single_key_ctx));
        RocksDbManager::get().shutdown().await;
    }

    #[restate_core::test]
    async fn verify_and_run_migrations_rejects_unknown_persisted_version() {
        use crate::fsm_table::put_storage_version;

        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true)
            .await
            .expect("DB storage creation succeeds");
        let mut store = manager
            .open(
                &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
                None,
            )
            .await
            .expect("DB storage creation succeeds");

        // Seed the FSM with an applied LSN so the empty-partition fast path is skipped,
        // and a StorageVersion discriminant that this binary doesn't know.
        {
            use restate_storage_api::Transaction;
            use restate_storage_api::fsm_table::WriteFsmTable;
            let mut txn = store.transaction();
            txn.put_applied_lsn(restate_types::logs::Lsn::new(1))
                .expect("applied lsn write should succeed");
            txn.commit().await.expect("commit should succeed");
        }
        let partition_id = store.partition_id();
        put_storage_version(&mut store, partition_id, 9999_u16)
            .await
            .expect("seeding unknown storage version should succeed");

        let err = store
            .verify_and_run_migrations(CancellationToken::new(), &Configuration::pinned())
            .await
            .expect_err("unknown storage version should fail verify_and_run_migrations");
        let msg = err.to_string();
        assert!(
            msg.contains("9999") && msg.contains("storage version"),
            "unexpected error message: {msg}"
        );

        RocksDbManager::get().shutdown().await;
    }
}

#[cfg(test)]
#[path = "tests/migrations_test/verify_and_run_migrations.rs"]
mod verify_and_run_migrations_test;
