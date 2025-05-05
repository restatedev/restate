use rand::{RngCore, rng};
use tokio::sync::watch;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::{Transaction, fsm_table::FsmTable};
use restate_types::{
    config::{CommonOptions, RocksDbOptions, StorageOptions},
    identifiers::{PartitionId, PartitionKey},
    live::Constant,
    logs::{Lsn, SequenceNumber},
};

use crate::{OpenMode, PartitionStoreManager};

#[restate_core::test]
async fn track_latest_applied_lsn() -> googletest::Result<()> {
    let rocksdb = RocksDbManager::init(Constant::new(CommonOptions::default()));

    let (persisted_lsn_tx, mut persisted_lsn_rx) = watch::channel((PartitionId::MIN, Lsn::INVALID));
    let partition_store_manager = PartitionStoreManager::create(
        Constant::new(StorageOptions::default()),
        &[],
        Some(persisted_lsn_tx.clone()),
    )
    .await?;

    let partition_id = PartitionId::MIN;
    let mut partition_store = partition_store_manager
        .open_partition_store(
            partition_id,
            PartitionKey::MIN..=PartitionKey::MAX,
            OpenMode::CreateIfMissing,
            &RocksDbOptions::default(),
        )
        .await?;

    let mut txn = partition_store.transaction();
    txn.put_applied_lsn(Lsn::new(100)).await.unwrap();
    txn.commit().await.expect("commit succeeds");

    assert!(!persisted_lsn_rx.has_changed().unwrap());

    partition_store.flush_memtables(true).await?;
    {
        let persisted_lsn = persisted_lsn_rx.borrow_and_update();
        assert!(persisted_lsn.has_changed());
        assert_eq!((partition_id, Lsn::new(100)), *persisted_lsn);
    }

    drop(partition_store);
    partition_store_manager
        .close_partition_store(partition_id)
        .await?;
    persisted_lsn_tx.send_replace((PartitionId::MIN, Lsn::INVALID));

    let mut partition_store = partition_store_manager
        .open_partition_store(
            PartitionId::MIN,
            PartitionKey::MIN..=PartitionKey::MAX,
            OpenMode::CreateIfMissing,
            &RocksDbOptions::default(),
        )
        .await?;
    {
        let persisted_lsn = persisted_lsn_rx.borrow_and_update();
        assert!(persisted_lsn.has_changed());
        assert_eq!(
            (partition_id, Lsn::new(100)),
            *persisted_lsn,
            "partition store manager should announce the persisted LSN on open"
        );
    }

    let mut rng = rng();
    for lsn in 101..=200 {
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(Lsn::new(lsn)).await.unwrap();
        txn.put_inbox_seq_number(rng.next_u64()).await.unwrap();
        txn.commit().await.expect("commit succeeds");

        assert!(!persisted_lsn_rx.has_changed().unwrap());
    }

    partition_store.flush_memtables(true).await?;
    let persisted_lsn = persisted_lsn_rx.borrow_and_update();
    assert!(persisted_lsn.has_changed());
    assert_eq!((partition_id, Lsn::new(200)), *persisted_lsn);

    rocksdb.shutdown().await;
    Ok(())
}
