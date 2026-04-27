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
//! - When the waiting cursor is "seeked" to to first after a higher-order item has been inserted
//!   (i.e. item with has_lock=true, or with older run_at), the cursor must show this added item.

use restate_clock::time::MillisSinceEpoch;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::ScanVQueueInboxStages;
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, EntryValue, Stage, Status, VQueueCursor, VQueueStore,
    WriteVQueueTable, stats::EntryStatistics,
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

/// Collects all items from a cursor into a Vec
fn collect_cursor<C: VQueueCursor>(cursor: &mut C) -> Vec<(EntryKey, EntryValue)> {
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
    let mut reader = db.new_inbox_reader(&qid);
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
    let mut reader = db.new_inbox_reader(&qid);
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
    let run_items = collect_cursor(&mut run_reader);
    assert_eq!(run_items.len(), 1, "Running reader should see 1 item");
    assert_eq!(*run_items[0].0.entry_id(), entry_id(10));

    // Inbox reader should only see the Inbox stage entries
    let mut inbox_reader = db.new_inbox_reader(&qid);
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

    let mut reader = db.new_inbox_reader(&qid);

    // Seek after the 3rd entry (id=3)
    reader.seek_after(&qid, &entries[2].0);
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

    let mut inbox_reader = db.new_inbox_reader(&qid);
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
    let mut reader1 = db.new_inbox_reader(&qid1);
    let items1 = collect_cursor(&mut reader1);
    assert_eq!(items1.len(), 1);
    assert_eq!(*items1[0].0.entry_id(), entry_id(1));

    let mut reader2 = db.new_inbox_reader(&qid2);
    let items2 = collect_cursor(&mut reader2);
    assert_eq!(items2.len(), 1);
    assert_eq!(*items2[0].0.entry_id(), entry_id(2));

    let mut reader3 = db.new_inbox_reader(&qid3);
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

    let mut reader_a = db.new_inbox_reader(&qid_a);
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

    reader_a.seek_after(&qid_a, &a2.0);
    assert!(
        reader_a.peek().unwrap().is_none(),
        "seek_after(last_item) must not cross into the next vqueue"
    );

    let mut reader_b = db.new_inbox_reader(&qid_b);
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

    let mut reader_target = db.new_inbox_reader(&qid_target_partition);
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

    reader_target.seek_after(&qid_target_partition, &target2.0);
    assert!(
        reader_target.peek().unwrap().is_none(),
        "seek_after(last_item) must not cross into adjacent partition-key prefixes"
    );

    let mut reader_prev = db.new_inbox_reader(&qid_prev_partition);
    assert_eq!(
        collect_ids(&collect_cursor(&mut reader_prev)),
        vec![entry_id(31)]
    );

    let mut reader_next = db.new_inbox_reader(&qid_next_partition);
    assert_eq!(
        collect_ids(&collect_cursor(&mut reader_next)),
        vec![entry_id(51)]
    );
}

/// Test: Tailing iterator sees newly enqueued items after seek_to_first.
///
/// This verifies that the inbox reader (which uses a tailing iterator) can see
/// items that were added after the reader was created, when re-seeking.
async fn tailing_iterator_sees_new_items_on_reseek(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(6000, "1");

    // Insert initial entries
    let entry1 = default_entry(1);
    let entry2 = default_entry(2);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry2.0, &entry2.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader and verify initial state
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    let item = reader.peek().unwrap();
    assert_eq!(item.as_ref().map(|e| *e.0.entry_id()), Some(entry_id(1)));
    reader.advance();

    let item = reader.peek().unwrap();
    assert_eq!(item.as_ref().map(|e| *e.0.entry_id()), Some(entry_id(2)));
    reader.advance();

    // Should be empty now
    assert!(reader.peek().unwrap().is_none());

    // Now add a new entry while the reader is still open
    let entry3 = default_entry(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Re-seek to first - should now see all 3 entries
    reader.seek_to_first();
    let items = {
        let mut items = Vec::new();
        while let Ok(Some(item)) = reader.peek() {
            items.push(item);
            reader.advance();
        }
        items
    };

    assert_eq!(items.len(), 3, "Should see all 3 items after reseek");
    assert_eq!(
        collect_ids(&items),
        vec![entry_id(1), entry_id(2), entry_id(3)]
    );
}

/// Test: Re-seek to first sees newly inserted higher-order items.
///
/// This covers both invariants mentioned in the module docs:
/// - new item with older `run_at` appears first after re-seek
/// - new item with `has_lock=true` appears first after re-seek
async fn reseek_shows_new_higher_order_items(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(6_500, "1");

    let base1 = entry(1, false, 100, 1);
    let base2 = entry(2, false, 200, 2);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &base1.0, &base1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &base2.0, &base2.1);
        txn.commit().await.expect("commit should succeed");
    }

    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(1));

    // Add an item with older run_at (higher order among unlocked entries)
    let older_run_at = entry(3, false, 50, 3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &older_run_at.0, &older_run_at.1);
        txn.commit().await.expect("commit should succeed");
    }

    reader.seek_to_first();
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(3));

    // Add an item that has a lock (always higher order than unlocked entries)
    let locked = entry(4, true, 300, 4);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &locked.0, &locked.1);
        txn.commit().await.expect("commit should succeed");
    }

    reader.seek_to_first();
    let items = {
        let mut items = Vec::new();
        while let Ok(Some(item)) = reader.peek() {
            items.push(item);
            reader.advance();
        }
        items
    };

    assert_eq!(
        collect_ids(&items),
        vec![entry_id(4), entry_id(3), entry_id(1), entry_id(2)]
    );
}

/// Test: Tailing iterator sees newly enqueued items via seek_after.
///
/// This verifies that when using seek_after to resume iteration, newly added
/// items that sort after the seek position are visible.
async fn tailing_iterator_sees_new_items_on_seek_after(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(7000, "1");

    // Insert initial entries with different key prefixes
    let entry_first = entry(1, true, 10, 1);
    let entry_second = entry(2, false, 10, 2);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry_first.0, &entry_first.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry_second.0, &entry_second.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader and read the first item
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    let first = reader.peek().unwrap().unwrap();
    assert_eq!(*first.0.entry_id(), entry_id(1));

    // Now add a new entry that sorts after the seek position
    let entry_third = entry(3, false, 10, 3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry_third.0, &entry_third.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Seek after the first item - should see entry_second and entry_third
    reader.seek_after(&qid, &first.0);

    let mut remaining = Vec::new();
    while let Ok(Some(item)) = reader.peek() {
        remaining.push(item);
        reader.advance();
    }

    assert_eq!(remaining.len(), 2, "Should see 2 items after seek_after");
    assert_eq!(collect_ids(&remaining), vec![entry_id(2), entry_id(3)]);
}

/// Test: Tailing iterator sees items inserted ahead of current position without re-seek.
///
/// Scenario (1): after the cursor has advanced at least once, if a new item is inserted
/// at a key greater than the current position, it should become visible by continuing to
/// call `advance()`/`peek()` without any seek.
async fn tailing_iterator_sees_inserted_ahead_without_reseek(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(7_200, "1");

    let entry1 = entry(1, false, 10, 1);
    let entry3 = entry(3, false, 30, 3);
    let entry5 = entry(5, false, 50, 5);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry5.0, &entry5.1);
        txn.commit().await.expect("commit should succeed");
    }

    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(1));
    reader.advance();
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(3));

    let entry4 = entry(4, false, 40, 4);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry4.0, &entry4.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Continue from current position without seek.
    reader.advance();
    let maybe_inserted = reader.peek().unwrap().as_ref().map(|e| *e.0.entry_id());
    assert_eq!(
        maybe_inserted,
        Some(entry_id(4)),
        "Inserted item ahead of current position should be visible without re-seek"
    );
    reader.advance();
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(5));
}

/// Test: Tailing iterator sees flushed insertions while mid-iteration without re-seek.
///
/// Scenario: the cursor advances a couple of times and still has items to read.
/// New entries are inserted ahead of the current position, but before the next
/// existing item (splicing), and memtables are flushed.
/// Continuing with `advance()`/`peek()` (without seek) should surface the flushed items.
async fn tailing_iterator_sees_flushed_insertions_mid_iteration(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(7_250, "1");

    let entry1 = entry(1, false, 10, 1);
    let entry3 = entry(3, false, 30, 3);
    let entry6 = entry(6, false, 60, 6);
    let entry9 = entry(9, false, 90, 9);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry6.0, &entry6.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry9.0, &entry9.1);
        txn.commit().await.expect("commit should succeed");
    }

    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(1));
    reader.advance();
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(3));

    // Splice entries between current position (3) and next existing item (6).
    let entry4 = entry(4, false, 40, 4);
    let entry5 = entry(5, false, 50, 5);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry4.0, &entry4.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry5.0, &entry5.1);
        txn.commit().await.expect("commit should succeed");
    }

    rocksdb
        .partition_db()
        .flush_memtables(true)
        .await
        .expect("flush memtables should succeed");

    // Continue from current position without seek.
    reader.advance();
    let mut remaining_ids = Vec::new();
    while let Ok(Some(item)) = reader.peek() {
        remaining_ids.push(*item.0.entry_id());
        reader.advance();
    }

    assert_eq!(
        remaining_ids,
        vec![entry_id(4), entry_id(5), entry_id(6), entry_id(9)],
        "Spliced flushed items should be visible and not skipped without re-seek"
    );
}

/// Test: Seeked tailing iterator sees spliced insertions after a pre-insert flush.
///
/// Scenario: perform one seek to position the cursor on the item immediately
/// before the splice point, flush memtables, then insert a new item in the middle.
/// Advancing from that seeked position should return the new spliced item first.
async fn seeked_tailing_iterator_sees_spliced_insertions_after_preflush(
    rocksdb: &mut PartitionStore,
) {
    let qid = VQueueId::custom(7_255, "1");

    let entry1 = entry(1, false, 10, 1);
    let entry3 = entry(3, false, 30, 3);
    let entry6 = entry(6, false, 60, 6);
    let entry9 = entry(9, false, 90, 9);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry6.0, &entry6.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry9.0, &entry9.1);
        txn.commit().await.expect("commit should succeed");
    }

    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);

    // Single seek before inserting new items: land on entry3.
    reader.seek_after(&qid, &entry1.0);
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(3));

    rocksdb
        .partition_db()
        .flush_memtables(true)
        .await
        .expect("flush memtables should succeed");

    // Splice one entry between current position (3) and next existing item (6).
    let entry4 = entry(4, false, 40, 4);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry4.0, &entry4.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Continue from the seeked position without another seek.
    reader.advance();

    assert_eq!(
        *reader.peek().unwrap().unwrap().0.entry_id(),
        entry_id(4),
        "advance from seeked predecessor should surface the spliced item first"
    );

    reader.advance();
    let mut tail_ids = Vec::new();
    while let Ok(Some(item)) = reader.peek() {
        tail_ids.push(*item.0.entry_id());
        reader.advance();
    }

    assert_eq!(
        tail_ids,
        vec![entry_id(6), entry_id(9)],
        "Remaining tail after the spliced item should keep original order"
    );
}

/// Test: Seeked tailing iterator sees appended insertions after a pre-insert flush.
///
/// Scenario: perform one seek, flush memtables, then insert new items that are strictly
/// after the existing tail. Continuing with `advance()`/`peek()` from that seeked
/// position should eventually surface the appended items.
async fn seeked_tailing_iterator_sees_appended_insertions_after_preflush(
    rocksdb: &mut PartitionStore,
) {
    let qid = VQueueId::custom(7_256, "1");

    let entry1 = entry(1, false, 10, 1);
    let entry3 = entry(3, false, 30, 3);
    let entry6 = entry(6, false, 60, 6);
    let entry9 = entry(9, false, 90, 9);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry6.0, &entry6.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry9.0, &entry9.1);
        txn.commit().await.expect("commit should succeed");
    }

    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);

    // Single seek before appending new tail items: land on entry3.
    reader.seek_after(&qid, &entry1.0);
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(3));

    rocksdb
        .partition_db()
        .flush_memtables(true)
        .await
        .expect("flush memtables should succeed");

    let entry10 = entry(10, false, 100, 10);
    let entry11 = entry(11, false, 110, 11);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry10.0, &entry10.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry11.0, &entry11.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Continue from the seeked position without another seek.
    reader.advance();
    let mut remaining_ids = Vec::new();
    while let Ok(Some(item)) = reader.peek() {
        remaining_ids.push(*item.0.entry_id());
        reader.advance();
    }

    assert_eq!(
        remaining_ids,
        vec![entry_id(6), entry_id(9), entry_id(10), entry_id(11)],
        "Appended flushed items should be visible after traversing existing tail"
    );
}

/// Test: Tailing iterator sees items inserted after reaching the end without re-seek.
///
/// Scenario (2): after the cursor reaches end-of-iteration, if a new item is inserted,
/// continuing with `advance()`/`peek()` (without seek) should surface the new item.
async fn tailing_iterator_sees_item_added_after_end_without_reseek(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(7_300, "1");

    let entry1 = entry(1, false, 10, 1);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.commit().await.expect("commit should succeed");
    }

    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(1));
    reader.advance();
    assert!(reader.peek().unwrap().is_none(), "Reader should be at end");

    let entry2 = entry(2, false, 20, 2);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry2.0, &entry2.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Stay in the same iterator path, no re-seek.
    reader.advance();
    assert!(
        reader.peek().unwrap().is_none(),
        "Inserted item after end is not visible without re-seek"
    );

    // Re-seeking makes the newly inserted item visible.
    reader.seek_after(&qid, &entry1.0);
    assert_eq!(*reader.peek().unwrap().unwrap().0.entry_id(), entry_id(2));
}

/// Test: Deleted items don't appear after reseek.
///
/// This verifies that when an item is deleted while the reader is open,
/// re-seeking will not return the deleted item.
async fn deleted_items_not_visible_after_reseek(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(8000, "1");

    // Insert initial entries
    let entry1 = default_entry(1);
    let entry2 = default_entry(2);
    let entry3 = default_entry(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry2.0, &entry2.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader and verify we see all 3
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    let items = collect_cursor(&mut reader);
    assert_eq!(items.len(), 3);

    // Delete the middle entry while the reader is still open
    {
        let mut txn = rocksdb.transaction();
        assert!(
            txn.get_vqueue_inbox(&qid, Stage::Inbox, &entry2.0)
                .unwrap()
                .is_some()
        );
        txn.delete_vqueue_inbox(&qid, Stage::Inbox, &entry2.0);
        txn.commit().await.expect("commit should succeed");
    }

    // Re-seek and verify we only see entries 1 and 3
    reader.seek_to_first();
    let items = {
        let mut items = Vec::new();
        while let Ok(Some(item)) = reader.peek() {
            items.push(item);
            reader.advance();
        }
        items
    };

    assert_eq!(items.len(), 2, "Should only see 2 items after deletion");
    assert_eq!(collect_ids(&items), vec![entry_id(1), entry_id(3)]);
}

/// Test: Deleted items don't appear after seek_after.
///
/// This verifies that deleted items are not returned when using seek_after
/// to resume iteration past a certain point.
async fn deleted_items_not_visible_after_seek_after(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(8500, "1");

    // Insert entries
    let entry1 = default_entry(1);
    let entry2 = default_entry(2);
    let entry3 = default_entry(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry1.0, &entry1.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry2.0, &entry2.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry3.0, &entry3.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    // Read first item
    let first = reader.peek().unwrap().unwrap();
    assert_eq!(*first.0.entry_id(), entry_id(1));

    // Delete entries 2 and 3 while reader is open
    {
        let mut txn = rocksdb.transaction();
        assert!(
            txn.get_vqueue_inbox(&qid, Stage::Inbox, &entry2.0)
                .unwrap()
                .is_some()
        );
        txn.delete_vqueue_inbox(&qid, Stage::Inbox, &entry2.0);
        assert!(
            txn.get_vqueue_inbox(&qid, Stage::Inbox, &entry3.0)
                .unwrap()
                .is_some()
        );
        txn.delete_vqueue_inbox(&qid, Stage::Inbox, &entry3.0);
        txn.commit().await.expect("commit should succeed");
    }

    // Seek after first - should see nothing since 2 and 3 are deleted
    reader.seek_after(&qid, &first.0);
    assert!(
        reader.peek().unwrap().is_none(),
        "Should see no items after seek_after when remaining items are deleted"
    );
}

/// Test: Concurrent enqueue and delete operations are handled correctly.
///
/// This tests a more complex scenario where items are both added and removed
/// while the reader is open.
async fn concurrent_enqueue_and_delete(rocksdb: &mut PartitionStore) {
    let qid = VQueueId::custom(9000, "1");

    // Insert initial entries in deterministic key order
    let entry_high = entry(10, true, 10, 10);
    let entry_mid = entry(20, false, 20, 20);
    let entry_low = entry(30, false, 30, 30);
    {
        let mut txn = rocksdb.transaction();
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry_high.0, &entry_high.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry_mid.0, &entry_mid.1);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry_low.0, &entry_low.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader and read first item
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    let first = reader.peek().unwrap().unwrap();
    assert_eq!(*first.0.entry_id(), entry_id(10));

    // Simultaneously: delete entry_mid, add a new entry that sorts first
    let entry_new_first = entry(5, true, 5, 5);
    {
        let mut txn = rocksdb.transaction();
        assert!(
            txn.get_vqueue_inbox(&qid, Stage::Inbox, &entry_mid.0)
                .unwrap()
                .is_some()
        );
        txn.delete_vqueue_inbox(&qid, Stage::Inbox, &entry_mid.0);
        txn.put_vqueue_inbox(&qid, Stage::Inbox, &entry_new_first.0, &entry_new_first.1);
        txn.commit().await.expect("commit should succeed");
    }

    // Re-seek from start - should see: id=5, id=10, id=30
    // (id=20 is deleted, id=5 is added)
    reader.seek_to_first();
    let items = {
        let mut items = Vec::new();
        while let Ok(Some(item)) = reader.peek() {
            items.push(item);
            reader.advance();
        }
        items
    };

    assert_eq!(items.len(), 3, "Should see 3 items");
    assert_eq!(
        collect_ids(&items),
        vec![entry_id(5), entry_id(10), entry_id(30)]
    );
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

    // Tailing iterator tests - these need mutable access to rocksdb for writes
    tailing_iterator_sees_new_items_on_reseek(&mut rocksdb).await;
    reseek_shows_new_higher_order_items(&mut rocksdb).await;
    tailing_iterator_sees_new_items_on_seek_after(&mut rocksdb).await;
    tailing_iterator_sees_inserted_ahead_without_reseek(&mut rocksdb).await;
    tailing_iterator_sees_flushed_insertions_mid_iteration(&mut rocksdb).await;
    seeked_tailing_iterator_sees_spliced_insertions_after_preflush(&mut rocksdb).await;
    seeked_tailing_iterator_sees_appended_insertions_after_preflush(&mut rocksdb).await;
    tailing_iterator_sees_item_added_after_end_without_reseek(&mut rocksdb).await;
    deleted_items_not_visible_after_reseek(&mut rocksdb).await;
    deleted_items_not_visible_after_seek_after(&mut rocksdb).await;
    concurrent_enqueue_and_delete(&mut rocksdb).await;
}
