// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The status gauges kept before the registry, under static statistic IDs.
//!
//! Registered instances replace them (`crate::vqueue_table::STATUS_GAUGES`):
//! nothing maintains or reads the legacy keys any more, and this feature is never
//! enabled again. Opening a partition deletes the legacy keys and the feature's
//! marker once the store passed every compatibility check, before any other
//! change, so they never coexist with an active family.

use rocksdb::WriteBatch;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use restate_rocksdb::{IoMode, Priority};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::{RESTATE_VERSION_1_8_2, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::keys::KeyKind;
use crate::{MigrationError, PartitionStore};

use super::{LoadedStorageFeatures, StorageFeature, StorageFeatures};

impl StorageFeature for super::TrackStatusGaugesFeature {
    fn persisted_name() -> &'static ReString {
        const NAME: &ReString = &ReString::from_static("status-gauges");
        NAME
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_8_2
    }

    fn should_enable(
        _config: &Configuration,
        _current_version: &SemanticRestateVersion,
        _is_store_empty: bool,
    ) -> bool {
        false
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_track_status_gauges
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_track_status_gauges = true;
    }

    async fn enable(
        _storage: &mut PartitionStore,
        _cancel: &CancellationToken,
        _config: &Configuration,
        _finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        Err(StorageError::Generic(anyhow::anyhow!(
            "the legacy status gauges are retired; registered instances replace them"
        ))
        .into())
    }
}

impl PartitionStore {
    /// Deletes the legacy status gauges and their marker.
    ///
    /// One range delete covers every key of the retired [`KeyKind::LegacyStats`]
    /// kind that belongs to this partition, whatever its statistic ID,
    /// aggregation kind or payload; nothing else ever used that kind. The delete
    /// is written at every open without looking for keys first: it is cheap and
    /// idempotent, so an open that comes before an earlier deletion was flushed
    /// simply repeats it. Nothing is flushed here; the deletion becomes durable
    /// with the next regular memtable flush. The write is admitted through the
    /// partition's session like any other write, so a sealed partition rejects it.
    pub(crate) async fn remove_legacy_status_gauges(
        &mut self,
        features: &mut LoadedStorageFeatures,
    ) -> Result<(), MigrationError> {
        let db = self.partition_db().clone();
        let partition_id = db.partition().id();
        let (start, end) = legacy_status_gauge_range(partition_id);
        let mut batch = WriteBatch::default();
        let marker = features.append_legacy_status_gauges_removal(
            db.cf_handle(),
            &mut batch,
            partition_id,
        )?;
        batch.delete_range_cf(db.cf_handle(), &start, &end);
        let mut options = rocksdb::WriteOptions::default();
        options.disable_wal(true);
        let lease = db
            .gate()
            .try_acquire()
            .ok_or(StorageError::OperationalError)?;
        db.rocksdb()
            .write_batch_leased(
                "legacy-status-gauges-removal",
                Priority::High,
                IoMode::Default,
                options,
                batch,
                lease,
            )
            .await
            .map_err(|err| StorageError::Generic(err.into()))?;
        if marker {
            features.mark_persisted();
            self.set_storage_features(*features.enabled());
            warn!(
                partition_id = %partition_id,
                "Deleted the status gauges kept before the index and statistics registry"
            );
        }
        Ok(())
    }
}

/// The key range holding every legacy status gauge of `partition_id`: from the
/// partition's prefix up to, excluding, the next prefix in byte order.
fn legacy_status_gauge_range(partition_id: PartitionId) -> (Vec<u8>, Vec<u8>) {
    let start = legacy_status_gauge_prefix(partition_id);
    // The prefix plus one as a big-endian integer. The padding bytes are zero,
    // so a carry never reaches the key kind.
    let mut end = start.clone();
    for byte in end.iter_mut().rev() {
        if *byte == u8::MAX {
            *byte = 0;
        } else {
            *byte += 1;
            break;
        }
    }
    (start, end)
}

/// `key kind (2) | padding (2) | partition id (2)` of the retired layout.
#[allow(deprecated)]
fn legacy_status_gauge_prefix(partition_id: PartitionId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(6);
    prefix.extend_from_slice(KeyKind::LegacyStats.as_bytes());
    prefix.extend_from_slice(&[0, 0]);
    prefix.extend_from_slice(&(*partition_id).to_be_bytes());
    prefix
}

/// A legacy status gauge key of the retired layout, `key kind (2) | padding (2) |
/// partition id (2) | statistic id (2) | aggregation kind (1) | reserved (1)`,
/// for tests that seed such keys.
#[cfg(test)]
pub(crate) fn legacy_status_gauge_key(partition_id: PartitionId, id: u16) -> Vec<u8> {
    let mut key = legacy_status_gauge_prefix(partition_id);
    key.extend_from_slice(&id.to_be_bytes());
    key.push(crate::stats::StatKind::StageStatusBucketedGauge as u8);
    key.push(0);
    key
}

#[cfg(test)]
mod tests {
    use rocksdb::ReadOptions;
    use tokio_util::sync::CancellationToken;
    use zerocopy::IntoBytes;

    use restate_rocksdb::RocksDbManager;
    use restate_types::partition_table::Partition;
    use restate_types::sharding::KeyRange;

    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db, put_storage_features_json,
    };
    use crate::stats::{AggregateKeyPrefix, StatKind};
    use crate::{PartitionStore, PartitionStoreManager};

    use super::*;

    /// The keys of `kind` in the store, as RocksDB holds them.
    fn keys(store: &PartitionStore, kind: KeyKind) -> Vec<(Vec<u8>, Vec<u8>)> {
        let db = store.partition_db();
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true);
        options.set_iterate_range(rocksdb::PrefixRange(kind.as_bytes()));
        let mut it = db
            .rocksdb()
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(db.cf_handle(), options);
        it.seek_to_first();
        let mut out = Vec::new();
        while it.valid() {
            out.push((it.key().unwrap().to_vec(), it.value().unwrap().to_vec()));
            it.next();
        }
        out
    }

    /// Writes the storage-feature ledger with `future-feature` and, optionally,
    /// the retired marker; returns the ledger expected after the cleanup.
    async fn seed_ledger(store: &mut PartitionStore, marker: bool) -> serde_json::Value {
        let mut ledger = serde_json::to_value(
            get_storage_features_from_partition_db(store.partition_db())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        ledger["features"]["future-feature"] = serde_json::json!({"min_required_version": "1.8.0"});
        let kept = ledger.clone();
        if marker {
            ledger["features"]["status-gauges"] = serde_json::json!({
                "min_required_version": *restate_types::RESTATE_VERSION_1_8_2,
            });
        }
        put_storage_features_json(
            store,
            store.partition_id(),
            &serde_json::to_vec(&ledger).unwrap(),
        )
        .await
        .unwrap();
        kept
    }

    async fn open(store: &mut PartitionStore) {
        store
            .verify_and_run_migrations(CancellationToken::new(), &Configuration::default())
            .await
            .unwrap();
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn opening_deletes_the_legacy_gauges_of_the_partition_and_their_marker() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        // A marker only (a reset by the retired build), keys only (an interrupted
        // legacy backfill), both, and neither.
        for (case, (marker, legacy_keys)) in
            [(true, false), (false, true), (true, true), (false, false)]
                .into_iter()
                .enumerate()
        {
            let partition = Partition::new(PartitionId::from(10 + case as u16), KeyRange::FULL);
            let other = PartitionId::from(20 + case as u16);
            let mut store = manager.open(&partition, None).await.unwrap();
            open(&mut store).await;
            let kept = seed_ledger(&mut store, marker).await;
            let raw = store.partition_db().rocksdb().inner().as_raw_db();
            let cf = store.partition_db().cf_handle();
            if legacy_keys {
                for id in [1, 2, 3, 1023] {
                    let mut key = legacy_status_gauge_key(partition.id(), id);
                    key.extend_from_slice(b"payload");
                    raw.put_cf(cf, key, b"value").unwrap();
                }
            }
            // Another partition's legacy gauge and a current statistic survive.
            let foreign = legacy_status_gauge_key(other, 1);
            raw.put_cf(cf, &foreign, b"value").unwrap();
            let mut current = AggregateKeyPrefix::new(partition.id(), 70)
                .as_bytes()
                .to_vec();
            current.push(StatKind::StageStatusBucketedGauge as u8);
            current.extend_from_slice(b"payload");
            raw.put_cf(cf, &current, b"value").unwrap();
            // `future-feature` raises the version floor at the next open; the
            // cleanup must neither lose nor move it afterwards.
            let mut floor = None;

            for attempt in 0..2 {
                open(&mut store).await;
                let ledger = serde_json::to_value(
                    get_storage_features_from_partition_db(store.partition_db())
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(ledger, kept, "only_the_marker_is_removed: case {case}");
                assert!(!store.storage_features().is_track_status_gauges);
                #[allow(deprecated)]
                let remaining = keys(&store, KeyKind::LegacyStats);
                assert_eq!(
                    remaining,
                    vec![(foreign.clone(), b"value".to_vec())],
                    "only_this_partitions_legacy_keys_are_deleted: case {case}, attempt {attempt}"
                );
                assert_eq!(
                    keys(&store, KeyKind::Stats),
                    vec![(current.clone(), b"value".to_vec())],
                    "current_statistics_are_kept: case {case}"
                );
                let current_floor =
                    get_min_restate_version_from_partition_db(store.partition_db()).unwrap();
                assert!(
                    current_floor.is_equal_or_newer_than(&SemanticRestateVersion::new(1, 8, 0))
                );
                assert_eq!(
                    *floor.get_or_insert_with(|| current_floor.clone()),
                    current_floor
                );
            }
        }
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_through_a_sealed_partition_changes_nothing() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::from(30), KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        open(&mut store).await;
        seed_ledger(&mut store, true).await;
        let legacy = legacy_status_gauge_key(partition.id(), 1);
        let db = store.partition_db().clone();
        db.rocksdb()
            .inner()
            .as_raw_db()
            .put_cf(db.cf_handle(), &legacy, b"value")
            .unwrap();
        let mut features = LoadedStorageFeatures::load(
            get_storage_features_from_partition_db(&db).unwrap(),
            get_storage_version_from_partition_db(&db).unwrap(),
            &SemanticRestateVersion::new(1, 8, 3),
        )
        .unwrap();
        assert!(features.enabled().is_track_status_gauges);

        // Closing seals the partition's session; the old handle is rejected and
        // the failed open discards its in-memory state.
        manager.close(partition.id()).await;
        assert!(matches!(
            store.remove_legacy_status_gauges(&mut features).await,
            Err(MigrationError::StorageError(StorageError::OperationalError))
        ));

        // Nothing changed on disk: the next open finds the marker and the key, and deletes them.
        let mut store = manager.open(&partition, None).await.unwrap();
        let db = store.partition_db().clone();
        assert!(
            db.rocksdb()
                .inner()
                .as_raw_db()
                .get_cf(db.cf_handle(), &legacy)
                .unwrap()
                .is_some()
        );
        assert!(
            get_storage_features_from_partition_db(&db)
                .unwrap()
                .unwrap()
                .features
                .contains_key(&ReString::from_static("status-gauges"))
        );
        open(&mut store).await;
        assert!(
            db.rocksdb()
                .inner()
                .as_raw_db()
                .get_cf(db.cf_handle(), &legacy)
                .unwrap()
                .is_none()
        );
        assert!(!store.storage_features().is_track_status_gauges);
    }

    #[test]
    fn legacy_range_covers_exactly_one_partition() {
        let (start, end) = legacy_status_gauge_range(PartitionId::from(7));
        assert_eq!(start, b"zS\x00\x00\x00\x07");
        assert_eq!(end, b"zS\x00\x00\x00\x08");
        let key = legacy_status_gauge_key(PartitionId::from(7), 3);
        assert!(start.as_slice() <= key.as_slice() && key.as_slice() < end.as_slice());
        assert!(legacy_status_gauge_key(PartitionId::from(8), 1).as_slice() >= end.as_slice());

        // The last partition's range ends in the padding, never in the key kind.
        let (start, end) = legacy_status_gauge_range(PartitionId::from(u16::MAX));
        assert_eq!(start, b"zS\x00\x00\xff\xff");
        assert_eq!(end, b"zS\x00\x01\x00\x00");
    }
}
