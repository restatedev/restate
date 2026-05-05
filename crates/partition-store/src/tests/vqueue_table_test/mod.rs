// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for `VQueueStore` implementation on `PartitionDb`.
//!
//! These tests verify that the RocksDB-backed readers (`VQueueRunningReader` and
//! `VQueueWaitingReader`) follow the key-ordering contract:
//! - Items are returned in lexicographic `EntryKey` order
//! - `EntryKey` ordering is `has_lock` (true first), then `run_at`, then `seq`
//! - Running stage items are separate from Inbox stage items
//!
//!
//! Invariants:
//! - The waiting cursor must not cross the boundary to adjacent vqueue ids.
//! - The waiting cursor must not cross partition-key boundaries either.
//! - The waiting cursor uses snapshot semantics: it captures a consistent
//!   view of storage at creation time. Writes/deletes that happen after the
//!   cursor is created are NOT visible to that cursor — callers must create
//!   a fresh cursor to observe them.

use restate_clock::time::MillisSinceEpoch;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::ScanVQueueInboxStages;
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, EntryValue, Options, Stage, Status, VQueueCursor,
    VQueueRunningCursor, VQueueStore, WriteVQueueTable, stats::EntryStatistics,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, VQueueId};

use crate::PartitionStore;

fn test_qid() -> VQueueId {
    VQueueId::custom(PartitionKey::from(1000u64), "1")
}

fn entry_id(id: u8) -> EntryId {
    EntryId::new(EntryKind::Invocation, [id; 16])
}

const TEST_BASE_RUN_AT: u64 = 1_744_000_000_000;
const TEST_RUN_AT_STEP_MS: u64 = 1_000;

fn test_run_at(run_at: u64) -> MillisSinceEpoch {
    MillisSinceEpoch::new(TEST_BASE_RUN_AT + run_at * TEST_RUN_AT_STEP_MS)
}

fn entry_key(id: u8, has_lock: bool, run_at: u64, seq: u64) -> EntryKey {
    EntryKey::new(has_lock, test_run_at(run_at), seq, entry_id(id))
}

fn entry_value(id: u8, original_run_at: u64, num_attempts: u32) -> EntryValue {
    let created_at = UniqueTimestamp::try_from(1000u64 + id as u64).unwrap();
    let original_run_at =
        restate_clock::RoughTimestamp::from_unix_millis_clamped(test_run_at(original_run_at));
    let mut stats = EntryStatistics::new(created_at, original_run_at);
    if num_attempts > 0 {
        stats.num_attempts = num_attempts;
        stats.first_attempt_at = Some(created_at);
        stats.latest_attempt_at = Some(created_at);
    }
    let status = if num_attempts > 0 {
        Status::Started
    } else if stats.first_runnable_at > created_at.to_unix_millis() {
        Status::Scheduled
    } else {
        Status::New
    };

    EntryValue {
        status,
        metadata: EntryMetadata::default(),
        stats,
    }
}

fn entry(id: u8, has_lock: bool, run_at: u64, seq: u64) -> (EntryKey, EntryValue) {
    (
        entry_key(id, has_lock, run_at, seq),
        entry_value(id, run_at, seq as u32),
    )
}

fn default_entry(id: u8) -> (EntryKey, EntryValue) {
    entry(id, false, 0, id as u64)
}

fn inbox_reader(db: &crate::PartitionDb, qid: &VQueueId) -> impl VQueueCursor + use<> {
    db.new_inbox_reader(
        qid,
        Options {
            allow_blocking_io: true,
        },
    )
}

/// Collects all items from an inbox cursor.
fn collect_cursor<C: VQueueCursor>(cursor: &mut C) -> Vec<(EntryKey, EntryValue)> {
    let mut items = Vec::new();
    cursor.seek_to_first();
    while let Ok(Some(item)) = cursor.peek() {
        items.push(item);
        cursor.advance();
    }
    items
}

/// Collects all items from a running cursor (different error type than inbox).
fn collect_running<C: VQueueRunningCursor>(cursor: &mut C) -> Vec<(EntryKey, EntryValue)> {
    let mut items = Vec::new();
    cursor.seek_to_first();
    while let Ok(Some(item)) = cursor.peek() {
        items.push(item);
        cursor.advance();
    }
    items
}

fn collect_ids(items: &[(EntryKey, EntryValue)]) -> Vec<EntryId> {
    items.iter().map(|(key, _)| *key.entry_id()).collect()
}

/// Test: Inbox items are returned in `EntryKey` ordering.
async fn key_ordering_by_has_lock_run_at_seq<W: WriteVQueueTable>(txn: &mut W) {
    let qid = test_qid();

    // Insert entries in non-sorted order
    let entries = [
        entry(1, false, 15, 10),
        entry(2, true, 100, 1),
        entry(3, true, 150, 2),
        entry(4, false, 5, 2),
        entry(5, false, 5, 1),
    ];

    for (key, value) in &entries {
        txn.put_vqueue_inbox(&qid, Stage::Inbox, key, value);
    }
}

fn verify_key_ordering_by_has_lock_run_at_seq(db: &crate::PartitionDb) {
    let qid = test_qid();
    let mut reader = inbox_reader(db, &qid);
    let items = collect_cursor(&mut reader);

    assert_eq!(items.len(), 5, "Expected 5 items in inbox");

    assert_eq!(items[0].0, entry_key(2, true, 100, 1));
    assert_eq!(items[1].0, entry_key(3, true, 150, 2));
    assert_eq!(items[2].0, entry_key(5, false, 5, 1));
    assert_eq!(items[3].0, entry_key(4, false, 5, 2));
    assert_eq!(items[4].0, entry_key(1, false, 15, 10));
    assert_eq!(
        collect_ids(&items),
        vec![
            entry_id(2),
            entry_id(3),
            entry_id(5),
            entry_id(4),
            entry_id(1)
        ]
    );
}

/// Test: With equal `has_lock`, keys sort by `run_at` then `seq`.
async fn ordering_within_same_lock_domain<W: WriteVQueueTable>(txn: &mut W) {
    let qid = VQueueId::custom(2000, "1");

    let entries = [
        entry(3, false, 3_000, 5),
        entry(1, false, 0, 9),
        entry(2, false, 2_000, 10),
        entry(4, false, 2_000, 2),
    ];

    for (key, value) in &entries {
        txn.put_vqueue_inbox(&qid, Stage::Inbox, key, value);
    }
}

fn verify_ordering_within_same_lock_domain(db: &crate::PartitionDb) {
    let qid = VQueueId::custom(2000, "1");
    let mut reader = inbox_reader(db, &qid);
    let items = collect_cursor(&mut reader);

    assert_eq!(items.len(), 4, "Expected 4 items");
    assert_eq!(items[0].0, entry_key(1, false, 0, 9));
    assert_eq!(items[1].0, entry_key(4, false, 2_000, 2));
    assert_eq!(items[2].0, entry_key(2, false, 2_000, 10));
    assert_eq!(items[3].0, entry_key(3, false, 3_000, 5));
    assert_eq!(
        collect_ids(&items),
        vec![entry_id(1), entry_id(4), entry_id(2), entry_id(3)]
    );
}

/// Test: Running and inbox stages are separate namespaces.
async fn running_and_inbox_are_separate<W: WriteVQueueTable>(txn: &mut W) {
    let qid = VQueueId::custom(3000, "1");

    // Put one entry in Run stage
    let run_entry = default_entry(10);
    txn.put_vqueue_inbox(&qid, Stage::Running, &run_entry.0, &run_entry.1);

    // Put two entries in Inbox stage
    let inbox_entry1 = default_entry(20);
    let inbox_entry2 = default_entry(21);
    txn.put_vqueue_inbox(&qid, Stage::Inbox, &inbox_entry1.0, &inbox_entry1.1);
    txn.put_vqueue_inbox(&qid, Stage::Inbox, &inbox_entry2.0, &inbox_entry2.1);
}

fn verify_running_and_inbox_are_separate(db: &crate::PartitionDb) {
    let qid = VQueueId::custom(3000, "1");

    // Running reader should only see the Run stage entry
    let mut run_reader = db.new_run_reader(&qid);
    let run_items = collect_running(&mut run_reader);
    assert_eq!(run_items.len(), 1, "Running reader should see 1 item");
    assert_eq!(*run_items[0].0.entry_id(), entry_id(10));

    // Inbox reader should only see the Inbox stage entries
    let mut inbox_reader = inbox_reader(db, &qid);
    let inbox_items = collect_cursor(&mut inbox_reader);
    assert_eq!(inbox_items.len(), 2, "Inbox reader should see 2 items");
    assert_eq!(collect_ids(&inbox_items), vec![entry_id(20), entry_id(21)]);
}

/// Test: seek_after positions cursor strictly after the given item.
async fn seek_after_works<W: WriteVQueueTable>(txn: &mut W) {
    let qid = VQueueId::custom(4000, "1");

    // Insert entries in order
    let entries: Vec<_> = (1..=5).map(default_entry).collect();
    for (key, value) in &entries {
        txn.put_vqueue_inbox(&qid, Stage::Inbox, key, value);
    }
}

fn verify_seek_after_works(db: &crate::PartitionDb) {
    let qid = VQueueId::custom(4000, "1");

    let entries: Vec<_> = (1..=5).map(default_entry).collect();

    let mut reader = inbox_reader(db, &qid);

    // Seek after the 3rd entry (id=3)
    reader.seek_after(&entries[2].0);
    let item = reader.peek().unwrap();
    assert!(item.is_some(), "Should have items after seek_after");
    // Next item should be entry 4 (strictly after entry 3)
    assert_eq!(
        *item.unwrap().0.entry_id(),
        entry_id(4),
        "After seek_after(entry3), next should be entry4"
    );
}

/// Test: Empty queue returns None from peek.
fn verify_empty_queue_returns_none(db: &crate::PartitionDb) {
    let qid = VQueueId::custom(9999, "99");

    let mut run_reader = db.new_run_reader(&qid);
    run_reader.seek_to_first();
    assert!(
        run_reader.peek().unwrap().is_none(),
        "Empty running queue should return None"
    );

    let mut inbox_reader = inbox_reader(db, &qid);
    inbox_reader.seek_to_first();
    assert!(
        inbox_reader.peek().unwrap().is_none(),
        "Empty inbox queue should return None"
    );
}

/// Test: Different vqueues (different parent/instance) are isolated.
async fn vqueue_isolation<W: WriteVQueueTable>(txn: &mut W) {
    let pkey = PartitionKey::from(5000u64);
    let qid1 = VQueueId::custom(pkey, "1");
    let qid2 = VQueueId::custom(pkey, "2");
    let qid3 = VQueueId::custom(pkey, "3");

    let entry1 = default_entry(1);
    let entry2 = default_entry(2);
    let entry3 = default_entry(3);

    txn.put_vqueue_inbox(&qid1, Stage::Inbox, &entry1.0, &entry1.1);
    txn.put_vqueue_inbox(&qid2, Stage::Inbox, &entry2.0, &entry2.1);
    txn.put_vqueue_inbox(&qid3, Stage::Inbox, &entry3.0, &entry3.1);
}

fn verify_vqueue_isolation(db: &crate::PartitionDb) {
    let pkey = PartitionKey::from(5000u64);

    let qid1 = VQueueId::custom(pkey, "1");
    let qid2 = VQueueId::custom(pkey, "2");
    let qid3 = VQueueId::custom(pkey, "3");

    // Each queue should only see its own entry
    let mut reader1 = inbox_reader(db, &qid1);
    let items1 = collect_cursor(&mut reader1);
    assert_eq!(items1.len(), 1);
    assert_eq!(*items1[0].0.entry_id(), entry_id(1));

    let mut reader2 = inbox_reader(db, &qid2);
    let items2 = collect_cursor(&mut reader2);
    assert_eq!(items2.len(), 1);
    assert_eq!(*items2[0].0.entry_id(), entry_id(2));

    let mut reader3 = inbox_reader(db, &qid3);
    let items3 = collect_cursor(&mut reader3);
    assert_eq!(items3.len(), 1);
    assert_eq!(*items3[0].0.entry_id(), entry_id(3));
}

/// Test: Waiting cursor must not cross adjacent vqueue boundaries.
async fn waiting_cursor_boundary_is_respected<W: WriteVQueueTable>(txn: &mut W) {
    let pkey = PartitionKey::from(5_100u64);
    let qid_a = VQueueId::custom(pkey, "a");
    let qid_b = VQueueId::custom(pkey, "b");

    let a1 = entry(11, false, 10, 1);
    let a2 = entry(12, false, 10, 2);
    let b1 = entry(21, false, 0, 1);

    txn.put_vqueue_inbox(&qid_a, Stage::Inbox, &a1.0, &a1.1);
    txn.put_vqueue_inbox(&qid_a, Stage::Inbox, &a2.0, &a2.1);
    txn.put_vqueue_inbox(&qid_b, Stage::Inbox, &b1.0, &b1.1);
}

fn verify_waiting_cursor_boundary_is_respected(db: &crate::PartitionDb) {
    let pkey = PartitionKey::from(5_100u64);
    let qid_a = VQueueId::custom(pkey, "a");
    let qid_b = VQueueId::custom(pkey, "b");
    let a2 = entry(12, false, 10, 2);

    let mut reader_a = inbox_reader(db, &qid_a);
    reader_a.seek_to_first();

    assert_eq!(
        reader_a.peek().unwrap().as_ref().map(|e| *e.0.entry_id()),
        Some(entry_id(11))
    );
    reader_a.advance();
    assert_eq!(
        reader_a.peek().unwrap().as_ref().map(|e| *e.0.entry_id()),
        Some(entry_id(12))
    );
    reader_a.advance();

    assert!(
        reader_a.peek().unwrap().is_none(),
        "Reader for qid_a must stop before qid_b entries"
    );

    reader_a.seek_after(&a2.0);
    assert!(
        reader_a.peek().unwrap().is_none(),
        "seek_after(last_item) must not cross into the next vqueue"
    );

    let mut reader_b = inbox_reader(db, &qid_b);
    let items_b = collect_cursor(&mut reader_b);
    assert_eq!(collect_ids(&items_b), vec![entry_id(21)]);
}

/// Test: Waiting cursor must not cross partition-key boundaries.
///
/// This specifically stresses the RocksDB fixed-prefix extractor boundary (key kind +
/// partition key), while the reader itself narrows iteration to a full qid prefix.
async fn waiting_cursor_partition_prefix_boundary_is_respected<W: WriteVQueueTable>(txn: &mut W) {
    let qid_prev_partition = VQueueId::custom(PartitionKey::from(5_199u64), "shared-boundary");
    let qid_target_partition = VQueueId::custom(PartitionKey::from(5_200u64), "shared-boundary");
    let qid_next_partition = VQueueId::custom(PartitionKey::from(5_201u64), "shared-boundary");

    let prev = entry(31, false, 0, 1);
    let target1 = entry(41, false, 10, 1);
    let target2 = entry(42, false, 10, 2);
    let next = entry(51, false, 0, 1);

    txn.put_vqueue_inbox(&qid_prev_partition, Stage::Inbox, &prev.0, &prev.1);
    txn.put_vqueue_inbox(&qid_target_partition, Stage::Inbox, &target1.0, &target1.1);
    txn.put_vqueue_inbox(&qid_target_partition, Stage::Inbox, &target2.0, &target2.1);
    txn.put_vqueue_inbox(&qid_next_partition, Stage::Inbox, &next.0, &next.1);
}

fn verify_waiting_cursor_partition_prefix_boundary_is_respected(db: &crate::PartitionDb) {
    let qid_prev_partition = VQueueId::custom(PartitionKey::from(5_199u64), "shared-boundary");
    let qid_target_partition = VQueueId::custom(PartitionKey::from(5_200u64), "shared-boundary");
    let qid_next_partition = VQueueId::custom(PartitionKey::from(5_201u64), "shared-boundary");
    let target2 = entry(42, false, 10, 2);

    let mut reader_target = inbox_reader(db, &qid_target_partition);
    reader_target.seek_to_first();

    assert_eq!(
        reader_target
            .peek()
            .unwrap()
            .as_ref()
            .map(|e| *e.0.entry_id()),
        Some(entry_id(41))
    );
    reader_target.advance();
    assert_eq!(
        reader_target
            .peek()
            .unwrap()
            .as_ref()
            .map(|e| *e.0.entry_id()),
        Some(entry_id(42))
    );
    reader_target.advance();

    assert!(
        reader_target.peek().unwrap().is_none(),
        "Reader for target qid must stop before adjacent partition keys"
    );

    reader_target.seek_after(&target2.0);
    assert!(
        reader_target.peek().unwrap().is_none(),
        "seek_after(last_item) must not cross into adjacent partition-key prefixes"
    );

    let mut reader_prev = inbox_reader(db, &qid_prev_partition);
    assert_eq!(
        collect_ids(&collect_cursor(&mut reader_prev)),
        vec![entry_id(31)]
    );

    let mut reader_next = inbox_reader(db, &qid_next_partition);
    assert_eq!(
        collect_ids(&collect_cursor(&mut reader_next)),
        vec![entry_id(51)]
    );
}

/// Test: a freshly created reader sees the current state of storage,
/// including writes that landed after a previous reader was created.
///
/// This is the snapshot-semantics counterpart to the previous tailing-iterator
/// tests: callers must construct a new reader to observe post-creation writes.
async fn fresh_reader_sees_current_state(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(6000, "1");

    let entry1 = default_entry(1);
    let entry2 = default_entry(2);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry2.0, &entry2.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Create the original reader and drain it.
    {
        let db = rocksdb.partition_db();
        let mut reader = inbox_reader(db, &qid);
        let items = collect_cursor(&mut reader);
        assert_eq!(collect_ids(&items), vec![entry_id(1), entry_id(2)]);
    }

    // Append a new item.
    let entry3 = default_entry(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.commit().await.expect("commit should succeed");
    }

    // A fresh reader sees all three items.
    let db = rocksdb.partition_db();
    let mut reader = inbox_reader(db, &qid);
    let items = collect_cursor(&mut reader);
    assert_eq!(
        collect_ids(&items),
        vec![entry_id(1), entry_id(2), entry_id(3)]
    );
}

/// Test: an existing reader holds a snapshot — writes after creation are not
/// observable, even after `seek_to_first` or `seek_after`.
async fn existing_reader_does_not_see_post_snapshot_writes(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(6_100, "1");

    let entry1 = default_entry(1);
    let entry2 = default_entry(2);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry2.0, &entry2.1);
        txn.commit().await.expect("commit should succeed");
    }

    let db = rocksdb.partition_db();
    let mut reader = inbox_reader(db, &qid);
    reader.seek_to_first();
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(1));

    // Insert a new item after the reader has taken its snapshot.
    let entry3 = default_entry(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Re-seeking the same reader still only shows the snapshot's two items.
    reader.seek_to_first();
    let items = {
        let mut items = Vec::new();
        while let Ok(Some(item)) = reader.peek() {
            items.push(item);
            reader.advance();
        }
        items
    };
    assert_eq!(collect_ids(&items), vec![entry_id(1), entry_id(2)]);
}

/// Test: Stage scan reads only the requested stage key kind.
///
/// This validates the datafusion-oriented scan API and ensures stage-specific
/// scans do not leak rows from adjacent stage key kinds or partition keys.
async fn stage_scan_is_filtered_by_stage(rocksdb: &mut PartitionStore) {
    let target_partition_key = PartitionKey::from(9_300u64);
    let other_partition_key = PartitionKey::from(9_301u64);
    let target_qid = VQueueId::custom(target_partition_key, "scan-target");
    let other_qid = VQueueId::custom(other_partition_key, "scan-target");

    let stages = [
        Stage::Inbox,
        Stage::Running,
        Stage::Suspended,
        Stage::Paused,
        Stage::Finished,
    ];

    {
        let mut txn = rocksdb.transaction();
        for (index, stage) in stages.into_iter().enumerate() {
            let entry_id = 100 + index as u8;
            let target_entry = default_entry(entry_id);
            let other_entry = default_entry(entry_id + 10);

            txn.put_vqueue_inbox(&target_qid, stage, &target_entry.0, &target_entry.1);
            txn.put_vqueue_inbox(&other_qid, stage, &other_entry.0, &other_entry.1);
        }
        txn.commit().await.expect("commit should succeed");
    }

    let range = KeyRange::from(target_partition_key..=target_partition_key);

    for (index, stage) in stages.into_iter().enumerate() {
        let expected_key = default_entry(100 + index as u8).0;
        let rows = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let rows_for_scan = rows.clone();

        rocksdb
            .for_each_vqueue_inbox_entry(range, stage, move |(qid, got_stage, key, _)| {
                rows_for_scan
                    .lock()
                    .expect("stage scan lock should not be poisoned")
                    .push((qid.clone(), got_stage, *key));
                std::ops::ControlFlow::Continue(())
            })
            .expect("stage scan setup should succeed")
            .await
            .expect("stage scan should succeed");

        let rows = rows
            .lock()
            .expect("stage scan lock should not be poisoned")
            .clone();
        assert_eq!(rows.len(), 1, "stage {stage} should return one row");
        assert_eq!(rows[0].0, target_qid, "stage {stage} returned wrong qid");
        assert_eq!(rows[0].1, stage, "stage {stage} returned wrong stage");
        assert_eq!(rows[0].2, expected_key, "stage {stage} returned wrong key");
    }
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();

    // Populate test data
    key_ordering_by_has_lock_run_at_seq(&mut txn).await;
    ordering_within_same_lock_domain(&mut txn).await;
    running_and_inbox_are_separate(&mut txn).await;
    seek_after_works(&mut txn).await;
    vqueue_isolation(&mut txn).await;
    waiting_cursor_boundary_is_respected(&mut txn).await;
    waiting_cursor_partition_prefix_boundary_is_respected(&mut txn).await;

    txn.commit().await.expect("commit should succeed");

    // Verify using the PartitionDb (not transaction) to test the VQueueStore impl
    let db = rocksdb.partition_db();

    verify_key_ordering_by_has_lock_run_at_seq(db);
    verify_ordering_within_same_lock_domain(db);
    verify_running_and_inbox_are_separate(db);
    verify_seek_after_works(db);
    verify_empty_queue_returns_none(db);
    verify_vqueue_isolation(db);
    verify_waiting_cursor_boundary_is_respected(db);
    verify_waiting_cursor_partition_prefix_boundary_is_respected(db);

    stage_scan_is_filtered_by_stage(&mut rocksdb).await;
    // Snapshot-iterator tests — exercise the contract that a fresh reader
    // sees current storage and that an existing reader holds a fixed view.
    fresh_reader_sees_current_state(&mut rocksdb).await;
    existing_reader_does_not_see_post_snapshot_writes(&mut rocksdb).await;
}
