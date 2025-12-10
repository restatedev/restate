// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
//! `VQueueWaitingReader`) follow the priority queue contract:
//! - Items are returned in priority order (lower priority value = higher precedence)
//! - Within the same priority, items are ordered by `visible_at`, then `created_at`
//! - Running stage items are separate from Inbox stage items

use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{
    EntryCard, EntryId, EntryKind, Stage, VQueueCursor, VQueueStore, VisibleAt, WriteVQueueTable,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueue::{EffectivePriority, VQueueId, VQueueInstance, VQueueParent};

use crate::PartitionStore;

fn test_qid() -> VQueueId {
    VQueueId {
        partition_key: PartitionKey::from(1000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    }
}

fn entry_card(id: u8) -> EntryCard {
    entry_card_with_priority(id, EffectivePriority::UserDefault)
}

fn entry_card_with_priority(id: u8, priority: EffectivePriority) -> EntryCard {
    EntryCard {
        priority,
        visible_at: VisibleAt::Now,
        created_at: UniqueTimestamp::try_from(1000u64 + id as u64).unwrap(),
        kind: EntryKind::Invocation,
        id: EntryId::new([id; 16]),
    }
}

fn entry_card_with_visible_at(
    id: u8,
    priority: EffectivePriority,
    visible_at: u64,
    created_at: UniqueTimestamp,
) -> EntryCard {
    EntryCard {
        priority,
        visible_at: VisibleAt::from_raw(visible_at),
        created_at,
        kind: EntryKind::Invocation,
        id: EntryId::new([id; 16]),
    }
}

/// Collects all items from a cursor into a Vec
fn collect_cursor<C: VQueueCursor>(cursor: &mut C) -> Vec<C::Item> {
    let mut items = Vec::new();
    cursor.seek_to_first();
    while let Ok(Some(item)) = cursor.peek() {
        items.push(item);
        cursor.advance();
    }
    items
}

/// Test: Items in the inbox are returned in priority order.
///
/// Priority ordering (lower value = higher precedence):
/// TokenHeld (0) > Started (1) > System (2) > UserHigh (3) > UserDefault (4)
async fn priority_ordering<W: WriteVQueueTable>(txn: &mut W) {
    let qid = test_qid();

    // Insert entries with different priorities in non-sorted order
    let entries = [
        entry_card_with_priority(1, EffectivePriority::UserDefault), // lowest priority
        entry_card_with_priority(2, EffectivePriority::TokenHeld),   // highest priority
        entry_card_with_priority(3, EffectivePriority::System),
        entry_card_with_priority(4, EffectivePriority::UserHigh),
        entry_card_with_priority(5, EffectivePriority::Started),
    ];

    for entry in &entries {
        txn.put_inbox_entry(&qid, Stage::Inbox, entry);
    }
}

fn verify_priority_ordering(db: &crate::PartitionDb) {
    let qid = test_qid();
    let mut reader = db.new_inbox_reader(&qid);
    let items = collect_cursor(&mut reader);

    assert_eq!(items.len(), 5, "Expected 5 items in inbox");

    // Verify priority order: TokenHeld < Started < System < UserHigh < UserDefault
    assert_eq!(
        items[0].priority,
        EffectivePriority::TokenHeld,
        "First item should be TokenHeld (highest priority)"
    );
    assert_eq!(items[1].priority, EffectivePriority::Started);
    assert_eq!(items[2].priority, EffectivePriority::System);
    assert_eq!(items[3].priority, EffectivePriority::UserHigh);
    assert_eq!(
        items[4].priority,
        EffectivePriority::UserDefault,
        "Last item should be UserDefault (lowest priority)"
    );
}

/// Test: Within the same priority, items are ordered by visible_at then created_at.
async fn ordering_within_same_priority<W: WriteVQueueTable>(txn: &mut W) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(2000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // All entries have UserDefault priority, but different visible_at and created_at
    // visible_at ordering: Now (0) < At(ts)
    let entries = [
        entry_card_with_visible_at(
            3,
            EffectivePriority::UserDefault,
            3000,
            UniqueTimestamp::try_from(1000u64).unwrap(),
        ), // visible later
        entry_card_with_visible_at(
            1,
            EffectivePriority::UserDefault,
            0,
            UniqueTimestamp::try_from(4000u64).unwrap(),
        ), // Now (visible_at=0)
        entry_card_with_visible_at(
            2,
            EffectivePriority::UserDefault,
            2000,
            UniqueTimestamp::try_from(2000u64).unwrap(),
        ), // visible sooner
    ];

    for entry in &entries {
        txn.put_inbox_entry(&qid, Stage::Inbox, entry);
    }
}

fn verify_ordering_within_same_priority(db: &crate::PartitionDb) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(2000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };
    let mut reader = db.new_inbox_reader(&qid);
    let items = collect_cursor(&mut reader);

    assert_eq!(items.len(), 3, "Expected 3 items");

    // Order should be: visible_at=0 (Now) < visible_at=2000 < visible_at=3000
    assert_eq!(items[0].visible_at, VisibleAt::Now);
    assert_eq!(items[1].visible_at, VisibleAt::from_raw(2000));
    assert_eq!(items[2].visible_at, VisibleAt::from_raw(3000));
}

/// Test: Running and inbox stages are separate namespaces.
async fn running_and_inbox_are_separate<W: WriteVQueueTable>(txn: &mut W) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(3000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Put one entry in Run stage
    let run_entry = entry_card(10);
    txn.put_inbox_entry(&qid, Stage::Run, &run_entry);

    // Put two entries in Inbox stage
    let inbox_entry1 = entry_card(20);
    let inbox_entry2 = entry_card(21);
    txn.put_inbox_entry(&qid, Stage::Inbox, &inbox_entry1);
    txn.put_inbox_entry(&qid, Stage::Inbox, &inbox_entry2);
}

fn verify_running_and_inbox_are_separate(db: &crate::PartitionDb) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(3000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Running reader should only see the Run stage entry
    let mut run_reader = db.new_run_reader(&qid);
    let run_items = collect_cursor(&mut run_reader);
    assert_eq!(run_items.len(), 1, "Running reader should see 1 item");
    assert_eq!(run_items[0].id, EntryId::new([10; 16]));

    // Inbox reader should only see the Inbox stage entries
    let mut inbox_reader = db.new_inbox_reader(&qid);
    let inbox_items = collect_cursor(&mut inbox_reader);
    assert_eq!(inbox_items.len(), 2, "Inbox reader should see 2 items");
    // Both should have created_at based on their id
    assert!(
        inbox_items
            .iter()
            .all(|e| e.id == EntryId::new([20; 16]) || e.id == EntryId::new([21; 16]))
    );
}

/// Test: seek_after positions cursor strictly after the given item.
async fn seek_after_works<W: WriteVQueueTable>(txn: &mut W) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(4000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Insert entries in order
    let entries: Vec<_> = (1..=5).map(entry_card).collect();
    for entry in &entries {
        txn.put_inbox_entry(&qid, Stage::Inbox, entry);
    }
}

fn verify_seek_after_works(db: &crate::PartitionDb) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(4000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    let entries: Vec<_> = (1..=5).map(entry_card).collect();

    let mut reader = db.new_inbox_reader(&qid);

    // Seek after the 3rd entry (id=3)
    reader.seek_after(&qid, &entries[2]);
    let item = reader.peek().unwrap();
    assert!(item.is_some(), "Should have items after seek_after");
    // Next item should be entry 4 (strictly after entry 3)
    assert_eq!(
        item.unwrap().id,
        EntryId::new([4; 16]),
        "After seek_after(entry3), next should be entry4"
    );
}

/// Test: Empty queue returns None from peek.
fn verify_empty_queue_returns_none(db: &crate::PartitionDb) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(9999u64), // unused partition key
        parent: VQueueParent::from_raw(99),
        instance: VQueueInstance::from_raw(99),
    };

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

    let qid1 = VQueueId {
        partition_key: pkey,
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };
    let qid2 = VQueueId {
        partition_key: pkey,
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(2), // different instance
    };
    let qid3 = VQueueId {
        partition_key: pkey,
        parent: VQueueParent::from_raw(2), // different parent
        instance: VQueueInstance::from_raw(1),
    };

    txn.put_inbox_entry(&qid1, Stage::Inbox, &entry_card(1));
    txn.put_inbox_entry(&qid2, Stage::Inbox, &entry_card(2));
    txn.put_inbox_entry(&qid3, Stage::Inbox, &entry_card(3));
}

fn verify_vqueue_isolation(db: &crate::PartitionDb) {
    let pkey = PartitionKey::from(5000u64);

    let qid1 = VQueueId {
        partition_key: pkey,
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };
    let qid2 = VQueueId {
        partition_key: pkey,
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(2),
    };
    let qid3 = VQueueId {
        partition_key: pkey,
        parent: VQueueParent::from_raw(2),
        instance: VQueueInstance::from_raw(1),
    };

    // Each queue should only see its own entry
    let mut reader1 = db.new_inbox_reader(&qid1);
    let items1 = collect_cursor(&mut reader1);
    assert_eq!(items1.len(), 1);
    assert_eq!(items1[0].id, EntryId::new([1; 16]));

    let mut reader2 = db.new_inbox_reader(&qid2);
    let items2 = collect_cursor(&mut reader2);
    assert_eq!(items2.len(), 1);
    assert_eq!(items2[0].id, EntryId::new([2; 16]));

    let mut reader3 = db.new_inbox_reader(&qid3);
    let items3 = collect_cursor(&mut reader3);
    assert_eq!(items3.len(), 1);
    assert_eq!(items3[0].id, EntryId::new([3; 16]));
}

/// Test: Tailing iterator sees newly enqueued items after seek_to_first.
///
/// This verifies that the inbox reader (which uses a tailing iterator) can see
/// items that were added after the reader was created, when re-seeking.
async fn tailing_iterator_sees_new_items_on_reseek(rocksdb: &mut PartitionStore) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(6000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Insert initial entries
    let entry1 = entry_card(1);
    let entry2 = entry_card(2);
    {
        let mut txn = rocksdb.transaction();
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry1);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry2);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader and verify initial state
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    let item = reader.peek().unwrap();
    assert_eq!(item.as_ref().map(|e| e.id), Some(EntryId::new([1; 16])));
    reader.advance();

    let item = reader.peek().unwrap();
    assert_eq!(item.as_ref().map(|e| e.id), Some(EntryId::new([2; 16])));
    reader.advance();

    // Should be empty now
    assert!(reader.peek().unwrap().is_none());

    // Now add a new entry while the reader is still open
    let entry3 = entry_card(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry3);
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
    assert_eq!(items[0].id, EntryId::new([1; 16]));
    assert_eq!(items[1].id, EntryId::new([2; 16]));
    assert_eq!(items[2].id, EntryId::new([3; 16]));
}

/// Test: Tailing iterator sees newly enqueued items via seek_after.
///
/// This verifies that when using seek_after to resume iteration, newly added
/// items that sort after the seek position are visible.
async fn tailing_iterator_sees_new_items_on_seek_after(rocksdb: &mut PartitionStore) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(7000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Insert initial entries with different priorities
    let entry_high = entry_card_with_priority(1, EffectivePriority::System);
    let entry_low = entry_card_with_priority(2, EffectivePriority::UserDefault);
    {
        let mut txn = rocksdb.transaction();
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry_high);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry_low);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader and read the first item
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    let first = reader.peek().unwrap().unwrap();
    assert_eq!(first.priority, EffectivePriority::System);

    // Now add a new entry with lower priority (sorts after entry_low)
    let entry_lowest = entry_card_with_priority(3, EffectivePriority::UserDefault);
    {
        let mut txn = rocksdb.transaction();
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry_lowest);
        txn.commit().await.expect("commit should succeed");
    }

    // Seek after the first item - should see entry_low and the newly added entry_lowest
    reader.seek_after(&qid, &first);

    let mut remaining = Vec::new();
    while let Ok(Some(item)) = reader.peek() {
        remaining.push(item);
        reader.advance();
    }

    assert_eq!(remaining.len(), 2, "Should see 2 items after seek_after");
    // entry_low (id=2) comes before entry_lowest (id=3) due to created_at ordering
    assert_eq!(remaining[0].id, EntryId::new([2; 16]));
    assert_eq!(remaining[1].id, EntryId::new([3; 16]));
}

/// Test: Deleted items don't appear after reseek.
///
/// This verifies that when an item is deleted while the reader is open,
/// re-seeking will not return the deleted item.
async fn deleted_items_not_visible_after_reseek(rocksdb: &mut PartitionStore) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(8000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Insert initial entries
    let entry1 = entry_card(1);
    let entry2 = entry_card(2);
    let entry3 = entry_card(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry1);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry2);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry3);
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
        txn.delete_inbox_entry(&qid, Stage::Inbox, &entry2);
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
    assert_eq!(items[0].id, EntryId::new([1; 16]));
    assert_eq!(items[1].id, EntryId::new([3; 16]));
}

/// Test: Deleted items don't appear after seek_after.
///
/// This verifies that deleted items are not returned when using seek_after
/// to resume iteration past a certain point.
async fn deleted_items_not_visible_after_seek_after(rocksdb: &mut PartitionStore) {
    let qid = VQueueId {
        partition_key: PartitionKey::from(8500u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Insert entries
    let entry1 = entry_card(1);
    let entry2 = entry_card(2);
    let entry3 = entry_card(3);
    {
        let mut txn = rocksdb.transaction();
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry1);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry2);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry3);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    // Read first item
    let first = reader.peek().unwrap().unwrap();
    assert_eq!(first.id, EntryId::new([1; 16]));

    // Delete entries 2 and 3 while reader is open
    {
        let mut txn = rocksdb.transaction();
        txn.delete_inbox_entry(&qid, Stage::Inbox, &entry2);
        txn.delete_inbox_entry(&qid, Stage::Inbox, &entry3);
        txn.commit().await.expect("commit should succeed");
    }

    // Seek after first - should see nothing since 2 and 3 are deleted
    reader.seek_after(&qid, &first);
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
    let qid = VQueueId {
        partition_key: PartitionKey::from(9000u64),
        parent: VQueueParent::from_raw(1),
        instance: VQueueInstance::from_raw(1),
    };

    // Insert initial entries with different priorities
    let entry_high = entry_card_with_priority(10, EffectivePriority::System);
    let entry_mid = entry_card_with_priority(20, EffectivePriority::UserHigh);
    let entry_low = entry_card_with_priority(30, EffectivePriority::UserDefault);
    {
        let mut txn = rocksdb.transaction();
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry_high);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry_mid);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry_low);
        txn.commit().await.expect("commit should succeed");
    }

    // Create reader and read first item
    let db = rocksdb.partition_db();
    let mut reader = db.new_inbox_reader(&qid);
    reader.seek_to_first();

    let first = reader.peek().unwrap().unwrap();
    assert_eq!(first.priority, EffectivePriority::System);
    assert_eq!(first, entry_high);

    // Simultaneously: delete entry_mid, add a new high-priority entry
    let entry_new_high = entry_card_with_priority(5, EffectivePriority::Started);
    {
        let mut txn = rocksdb.transaction();
        txn.delete_inbox_entry(&qid, Stage::Inbox, &entry_mid);
        txn.put_inbox_entry(&qid, Stage::Inbox, &entry_new_high);
        txn.commit().await.expect("commit should succeed");
    }

    // Re-seek from start - should see: Started(5), System(10), UserDefault(30)
    // (entry_mid with UserHigh is deleted, entry_new_high with Started is added)
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
        items[0].priority,
        EffectivePriority::Started,
        "First should be Started (new high priority)"
    );
    assert_eq!(items[0].id, EntryId::new([5; 16]));
    assert_eq!(
        items[1].priority,
        EffectivePriority::System,
        "Second should be System"
    );
    assert_eq!(items[1].id, EntryId::new([10; 16]));
    assert_eq!(
        items[2].priority,
        EffectivePriority::UserDefault,
        "Third should be UserDefault"
    );
    assert_eq!(items[2].id, EntryId::new([30; 16]));
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();

    // Populate test data
    priority_ordering(&mut txn).await;
    ordering_within_same_priority(&mut txn).await;
    running_and_inbox_are_separate(&mut txn).await;
    seek_after_works(&mut txn).await;
    vqueue_isolation(&mut txn).await;

    txn.commit().await.expect("commit should succeed");

    // Verify using the PartitionDb (not transaction) to test the VQueueStore impl
    let db = rocksdb.partition_db();

    verify_priority_ordering(db);
    verify_ordering_within_same_priority(db);
    verify_running_and_inbox_are_separate(db);
    verify_seek_after_works(db);
    verify_empty_queue_returns_none(db);
    verify_vqueue_isolation(db);

    // Tailing iterator tests - these need mutable access to rocksdb for writes
    tailing_iterator_sees_new_items_on_reseek(&mut rocksdb).await;
    tailing_iterator_sees_new_items_on_seek_after(&mut rocksdb).await;
    deleted_items_not_visible_after_reseek(&mut rocksdb).await;
    deleted_items_not_visible_after_seek_after(&mut rocksdb).await;
    concurrent_enqueue_and_delete(&mut rocksdb).await;
}
