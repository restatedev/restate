use bytes::Bytes;
use common::types::{EnrichedEntryHeader, EnrichedRawEntry, JournalEntry, ServiceId};
use futures_util::StreamExt;
use storage_api::journal_table::JournalTable;
use storage_api::{Storage, Transaction};
use storage_rocksdb::RocksDBStorage;

// false positive because of Bytes
#[allow(clippy::declare_interior_mutable_const)]
const MOCK_JOURNAL_ENTRY: JournalEntry = JournalEntry::Entry(EnrichedRawEntry::new(
    EnrichedEntryHeader::ClearState,
    Bytes::new(),
));

async fn populate_data<T: JournalTable>(txn: &mut T) {
    txn.put_journal_entry(
        1337,
        &ServiceId::new("svc-1", "key-1"),
        0,
        MOCK_JOURNAL_ENTRY,
    )
    .await;
    txn.put_journal_entry(
        1337,
        &ServiceId::new("svc-1", "key-1"),
        1,
        MOCK_JOURNAL_ENTRY,
    )
    .await;
    txn.put_journal_entry(
        1337,
        &ServiceId::new("svc-1", "key-1"),
        2,
        MOCK_JOURNAL_ENTRY,
    )
    .await;
    txn.put_journal_entry(
        1337,
        &ServiceId::new("svc-1", "key-1"),
        3,
        MOCK_JOURNAL_ENTRY,
    )
    .await;
}

async fn get_entire_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = txn.get_journal(1337, &ServiceId::new("svc-1", "key-1"), 4);
    let mut count = 0;
    while (journal.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 4);
}

async fn get_subset_of_a_journal<T: JournalTable>(txn: &mut T) {
    let mut journal = txn.get_journal(1337, &ServiceId::new("svc-1", "key-1"), 2);
    let mut count = 0;
    while (journal.next().await).is_some() {
        count += 1;
    }

    assert_eq!(count, 2);
}

async fn point_lookups<T: JournalTable>(txn: &mut T) {
    let result = txn
        .get_journal_entry(1337, &ServiceId::new("svc-1", "key-1"), 2)
        .await
        .expect("should not fail");

    assert!(result.is_some());

    let result = txn
        .get_journal_entry(1337, &ServiceId::new("svc-1", "key-1"), 10000)
        .await
        .expect("should not fail");

    assert!(result.is_none());
}

async fn delete_journal<T: JournalTable>(txn: &mut T) {
    txn.delete_journal(1337, &ServiceId::new("svc-1", "key-1"), 4)
        .await;
}

async fn verify_journal_deleted<T: JournalTable>(txn: &mut T) {
    for i in 0..4 {
        let result = txn
            .get_journal_entry(1337, &ServiceId::new("svc-1", "key-1"), i)
            .await
            .expect("should not fail");

        assert!(result.is_none());
    }
}

pub(crate) async fn run_tests(rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    get_entire_journal(&mut txn).await;
    get_subset_of_a_journal(&mut txn).await;
    point_lookups(&mut txn).await;
    delete_journal(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_journal_deleted(&mut txn).await;
}
