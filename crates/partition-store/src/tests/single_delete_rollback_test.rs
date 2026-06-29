// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rollback-safety tests for the `Delete` -> `SingleDelete` migration.
//!
//! `SingleDelete` is only correct when a key carries exactly one live `Put`; a `SingleDelete`
//! sitting above more than one un-deleted `Put` can let a stale value resurface after compaction.
//! Across a version rollback an older binary still issues a regular `Delete`, so the migration
//! relies on a forced bottommost compaction over the vqueue/lock ranges (run on roll-forward, see
//! `PartitionStoreManager::compact_vqueue_and_lock_ranges`) to physically drop any legacy `Delete`
//! tombstones before any `SingleDelete` is issued.
//!
//! These tests reproduce the exact `Put`/`Delete`/`SingleDelete` interleavings such a
//! rollback -> roll-forward(+compaction) cycle produces on the lock range and assert that no stale
//! value ever resurfaces. The resurfacing bug only manifests *after* compaction merges the SSTs,
//! so every step is flushed to its own SST (modelling writes by different binary versions over
//! time) and reads are taken after the same forced compaction the production roll-forward uses.

use bytes::Bytes;
use rocksdb::BottommostLevelCompaction;

use restate_rocksdb::{CfName, RocksDbManager};
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::keys::KeyKind;
use crate::{PartitionStore, PartitionStoreManager, TableKind};

const PARTITION: Partition = Partition::new(
    PartitionId::MIN,
    KeyRange::new(PartitionKey::MIN, PartitionKey::MAX),
);

/// A key inside the lock range (`[b"lo", b"lp")`). The exact structure is irrelevant to the
/// marker-safety property; the key only needs to be byte-stable across ops and live in range.
fn lock_key(suffix: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 8 + suffix.len());
    key.extend_from_slice(KeyKind::Lock.as_bytes()); // b"lo"
    key.extend_from_slice(&[0u8; 8]); // partition-key placeholder
    key.extend_from_slice(suffix.as_bytes());
    key
}

fn raw_put(store: &PartitionStore, key: &[u8], value: &[u8]) {
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = store.partition_db().table_cf_handle(TableKind::Locks);
    db.put_cf(cf, key, value).expect("put_cf should succeed");
}

fn raw_delete(store: &PartitionStore, key: &[u8]) {
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = store.partition_db().table_cf_handle(TableKind::Locks);
    db.delete_cf(cf, key).expect("delete_cf should succeed");
}

fn raw_single_delete(store: &PartitionStore, key: &[u8]) {
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = store.partition_db().table_cf_handle(TableKind::Locks);
    db.single_delete_cf(cf, key)
        .expect("single_delete_cf should succeed");
}

fn raw_get(store: &PartitionStore, key: &[u8]) -> Option<Vec<u8>> {
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = store.partition_db().table_cf_handle(TableKind::Locks);
    db.get_cf(cf, key).expect("get_cf should succeed")
}

fn assert_value(store: &PartitionStore, key: &[u8], expected: Option<&[u8]>) {
    assert_eq!(raw_get(store, key).as_deref(), expected);
}

async fn flush(store: &PartitionStore) {
    store
        .partition_db()
        .flush_memtables(true)
        .await
        .expect("flush should succeed");
}

/// Runs the same forced-bottommost compaction over the lock range that the production roll-forward
/// uses (`PartitionStoreManager::compact_vqueue_and_lock_ranges`), guaranteeing legacy `Delete`
/// tombstones are physically dropped.
async fn compact_locks(store: &PartitionStore) {
    let cf = CfName::from(store.partition_db().partition().cf_name());
    store
        .partition_db()
        .rocksdb()
        .clone()
        .compact_range(
            cf,
            Some(Bytes::copy_from_slice(KeyKind::Lock.as_bytes())),
            Some(Bytes::copy_from_slice(
                &KeyKind::Lock.exclusive_upper_bound(),
            )),
            BottommostLevelCompaction::Force,
        )
        .await
        .expect("compaction should succeed");
}

/// `Put -> SingleDelete` (new), rollback `Put -> Delete -> Put` (old), roll-forward (compact) ->
/// `SingleDelete` (new).
async fn put_single_delete_rollback_then_forward(store: &PartitionStore) {
    let key = lock_key("scenario-a");
    let (v1, v2, v3) = (b"v1".as_slice(), b"v2".as_slice(), b"v3".as_slice());

    // new code: acquire (Put) -> release (SingleDelete)
    raw_put(store, &key, v1);
    flush(store).await;
    raw_single_delete(store, &key);
    flush(store).await;
    assert_value(store, &key, None);

    // rolled back to old code: acquire -> release (regular Delete) -> acquire
    raw_put(store, &key, v2);
    flush(store).await;
    raw_delete(store, &key);
    flush(store).await;
    raw_put(store, &key, v3);
    flush(store).await;
    assert_value(store, &key, Some(v3));

    // roll-forward compaction over the lock range: v3 must survive, v1/v2 must not resurface.
    compact_locks(store).await;
    assert_value(store, &key, Some(v3));

    // new code resumes: release (SingleDelete)
    raw_single_delete(store, &key);
    flush(store).await;
    assert_value(store, &key, None);

    // final compaction must leave nothing resurfaced.
    compact_locks(store).await;
    assert_value(store, &key, None);
}

/// `... -> SingleDelete -> Put -> Delete`: a `SingleDelete` followed by a later `Put` and a
/// regular `Delete` (old binary) must leave the key absent after compaction.
async fn single_delete_then_put_delete(store: &PartitionStore) {
    let key = lock_key("scenario-b");
    let (w1, w2) = (b"w1".as_slice(), b"w2".as_slice());

    // the single matching Put, then SingleDelete (new code release)
    raw_put(store, &key, w1);
    flush(store).await;
    raw_single_delete(store, &key);
    flush(store).await;
    assert_value(store, &key, None);

    // rolled back to old code: acquire (Put) -> release (regular Delete)
    raw_put(store, &key, w2);
    flush(store).await;
    assert_value(store, &key, Some(w2));
    raw_delete(store, &key);
    flush(store).await;
    assert_value(store, &key, None);

    // neither w1 nor w2 may resurface after compaction.
    compact_locks(store).await;
    assert_value(store, &key, None);
}

/// Forward-path invariants the migration relies on in steady state.
async fn forward_path_invariants(store: &PartitionStore) {
    // A single Put then SingleDelete is fully removed, even when the Put was already pushed to the
    // bottom level by an earlier compaction.
    let key = lock_key("scenario-c-1");
    raw_put(store, &key, b"a");
    flush(store).await;
    compact_locks(store).await;
    raw_single_delete(store, &key);
    flush(store).await;
    compact_locks(store).await;
    assert_value(store, &key, None);

    // A SingleDelete must not remove a later Put.
    let key = lock_key("scenario-c-2");
    raw_put(store, &key, b"a");
    flush(store).await;
    raw_single_delete(store, &key);
    flush(store).await;
    raw_put(store, &key, b"b");
    flush(store).await;
    compact_locks(store).await;
    assert_value(store, &key, Some(b"b".as_slice()));
}

#[restate_core::test]
async fn single_delete_rollback_safety() -> googletest::Result<()> {
    let rocksdb = RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await?;
    // In test builds the cluster-marker gating is stubbed to always-safe, so `open` does not
    // auto-compact; the scenarios drive `compact_locks` explicitly to model the roll-forward.
    let store = manager.open(&PARTITION, None).await?;

    put_single_delete_rollback_then_forward(&store).await;
    single_delete_then_put_delete(&store).await;
    forward_path_invariants(&store).await;

    rocksdb.shutdown().await;
    Ok(())
}
