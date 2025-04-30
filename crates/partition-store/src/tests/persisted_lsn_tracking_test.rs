use rand::{RngCore, rng};
use tokio::sync::mpsc;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::{Transaction, fsm_table::FsmTable};
use restate_types::{
    config::{CommonOptions, RocksDbOptions, StorageOptions},
    identifiers::{PartitionId, PartitionKey},
    live::Constant,
    logs::Lsn,
};

use crate::{OpenMode, PartitionStoreManager};

#[restate_core::test]
async fn track_latest_applied_lsn() -> googletest::Result<()> {
    let rocksdb = RocksDbManager::init(Constant::new(CommonOptions::default()));

    let (persisted_lsn_tx, mut persisted_lsn_rx) = mpsc::channel(10);
    let partition_store_manager = PartitionStoreManager::create(
        Constant::new(StorageOptions::default()),
        &[],
        Some(persisted_lsn_tx),
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

    let persisted_lsn_update = persisted_lsn_rx.try_recv();
    assert_eq!(Err(mpsc::error::TryRecvError::Empty), persisted_lsn_update);

    partition_store.flush_memtables(true).await?;
    let persisted_lsn_update = persisted_lsn_rx.try_recv();
    assert_eq!(Ok((partition_id, Lsn::new(100))), persisted_lsn_update);

    drop(partition_store);
    partition_store_manager
        .close_partition_store(partition_id)
        .await;

    let mut partition_store = partition_store_manager
        .open_partition_store(
            PartitionId::MIN,
            PartitionKey::MIN..=PartitionKey::MAX,
            OpenMode::CreateIfMissing,
            &RocksDbOptions::default(),
        )
        .await?;
    let persisted_lsn_update = persisted_lsn_rx.try_recv();
    assert_eq!(
        Ok((partition_id, Lsn::new(100))),
        persisted_lsn_update,
        "partition store manager should announce the persisted LSN on open"
    );

    let mut rng = rng();
    for lsn in 101..=200 {
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(Lsn::new(lsn)).await.unwrap();
        txn.put_inbox_seq_number(rng.next_u64()).await.unwrap();
        txn.commit().await.expect("commit succeeds");

        let persisted_lsn_update = persisted_lsn_rx.try_recv();
        assert_eq!(Err(mpsc::error::TryRecvError::Empty), persisted_lsn_update);
    }

    partition_store.flush_memtables(true).await?;
    let persisted_lsn_update = persisted_lsn_rx.try_recv();
    assert_eq!(Ok((partition_id, Lsn::new(200))), persisted_lsn_update);

    rocksdb.shutdown().await;
    Ok(())
}
