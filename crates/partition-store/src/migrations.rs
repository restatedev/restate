// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod migrate_to_locks_table;
mod migrate_to_scoped_promise_table;
mod migrate_to_scoped_state_table;

use std::num::NonZeroU16;
use std::sync::Arc;

use bytes::BytesMut;
use restate_types::sharding::KeyRange;
use restate_types::sharding::subsharding::ShardPlan;
use strum::EnumCount;

use restate_clock::{AtomicStorage, HlcClock, WallClock};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;

use crate::{PartitionDb, PartitionStore, Result};

// NOTE: The representation numbers here must be strictly monotonically increasing.
#[derive(Debug, Eq, PartialEq, strum::FromRepr, strum::EnumCount)]
#[repr(u16)]
pub(crate) enum SchemaVersion {
    /// Before 1.5
    None = 0,
    /// Migrations:
    /// * Invocation status V1 -> V2
    V1_5 = 1,
}

pub(crate) const LATEST_VERSION: SchemaVersion =
    SchemaVersion::from_repr((SchemaVersion::COUNT as u16) - 1).unwrap();

impl From<u16> for SchemaVersion {
    fn from(value: u16) -> Self {
        SchemaVersion::from_repr(value).unwrap_or(SchemaVersion::V1_5)
    }
}

impl SchemaVersion {
    fn next(self) -> Self {
        ((self as u16) + 1).into()
    }

    pub(crate) async fn run_all_migrations(mut self, storage: &mut PartitionStore) -> Result<Self> {
        while self != LATEST_VERSION {
            self.do_migration(storage).await?;
            self = self.next();
        }
        Ok(self)
    }

    // Add migrations here!
    async fn do_migration(&self, _storage: &mut PartitionStore) -> Result<()> {
        match self {
            SchemaVersion::None => {
                // Version 1.6+ does not support upgrading from pre-1.5
                // The InvocationStatusV1 migration was removed in 1.6
                return Err(StorageError::Generic(anyhow::anyhow!(
                    "Cannot upgrade from version <1.5 directly to 1.6 or later. \
                     Please upgrade to version 1.5 first, which will migrate your data, \
                     and then upgrade to 1.6+"
                )));
            }
            SchemaVersion::V1_5 => {
                // todo(tillrohrmann) add migrations for:
                //  * service_status_table -> locks_table
                //  * promise_table -> scoped promise_table
                //  * state_table -> scoped state_table
                //  * and more
            }
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub struct MigrationContext<'a> {
    clock: HlcClock<WallClock, Arc<AtomicStorage>>,
    arena: BytesMut,
    partition_db: &'a PartitionDb,
    key_range: KeyRange,
}

#[allow(dead_code)]
impl<'a> MigrationContext<'a> {
    pub fn new(config: &Configuration, partition_db: &'a PartitionDb, key_range: KeyRange) -> Self {
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
        let manager = PartitionStoreManager::create()
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
        let mut ctx = MigrationContext::new(&config, store.partition_db(), KeyRange::new(1, 100));
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
            MigrationContext::new(&config, store.partition_db(), KeyRange::new(42, 42));
        let single_key_ranges = single_key_ctx
            .split(NonZeroU16::new(2).expect("two is non-zero"))
            .map(|ctx| ctx.key_range)
            .collect::<Vec<_>>();
        assert_eq!(single_key_ranges, vec![KeyRange::new(42, 42)]);

        drop((ctx, left_ctx, right_ctx, single_key_ctx));
        RocksDbManager::get().shutdown().await;
    }
}
