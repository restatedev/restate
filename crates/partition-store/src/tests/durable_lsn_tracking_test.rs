use rand::{RngCore, rng};

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

    let partition_store_manager =
        PartitionStoreManager::create(Constant::new(StorageOptions::default()), &[]).await?;

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

    assert_eq!(
        Some(Lsn::INVALID),
        partition_store_manager.get_durable_lsn(partition_id)
    );

    partition_store.flush_memtables(true).await?;
    assert_eq!(
        Some(Lsn::new(100)),
        partition_store_manager.get_durable_lsn(partition_id)
    );

    drop(partition_store);
    partition_store_manager
        .close_partition_store(partition_id)
        .await?;
    assert_eq!(None, partition_store_manager.get_durable_lsn(partition_id));

    let mut partition_store = partition_store_manager
        .open_partition_store(
            PartitionId::MIN,
            PartitionKey::MIN..=PartitionKey::MAX,
            OpenMode::CreateIfMissing,
            &RocksDbOptions::default(),
        )
        .await?;
    assert_eq!(
        Some(Lsn::new(100)),
        partition_store_manager.get_durable_lsn(partition_id),
        "partition store manager should announce the durable LSN on open"
    );

    partition_store.flush_memtables(true).await?;

    let mut rng = rng();
    for lsn in 101..=200 {
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(Lsn::new(lsn)).await.unwrap();
        txn.put_inbox_seq_number(rng.next_u64()).await.unwrap();
        txn.commit().await.expect("commit succeeds");

        assert_eq!(
            Some(Lsn::new(100)),
            partition_store_manager.get_durable_lsn(partition_id),
            "durable LSN remains unchanged"
        );
    }

    partition_store.flush_memtables(true).await?;
    assert_eq!(
        Some(Lsn::new(200)),
        partition_store_manager.get_durable_lsn(partition_id),
    );

    rocksdb.shutdown().await;
    Ok(())
}
