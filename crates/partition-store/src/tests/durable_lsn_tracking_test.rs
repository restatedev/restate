use googletest::prelude::*;
use rand::{RngCore, rng};

use restate_rocksdb::RocksDbManager;
use restate_storage_api::{
    Transaction,
    fsm_table::{FsmTable, PartitionDurability, ReadOnlyFsmTable},
};
use restate_types::{
    config::{CommonOptions, RocksDbOptions, StorageOptions},
    identifiers::{PartitionId, PartitionKey},
    live::Constant,
    logs::{Lsn, SequenceNumber},
    time::MillisSinceEpoch,
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

#[restate_core::test]
async fn partition_durability_fsm() -> googletest::Result<()> {
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
    // by default, we don't have partition durability.
    assert_that!(txn.get_partition_durability().await?, none());

    // lets update it to current.
    let now = MillisSinceEpoch::now();
    txn.put_partition_durability(&PartitionDurability {
        durable_point: Lsn::new(100),
        modification_time: now,
    })
    .await?;

    let inflight_dur = txn.get_partition_durability().await?;
    assert_that!(
        inflight_dur,
        some(eq(PartitionDurability {
            durable_point: Lsn::new(100),
            modification_time: now,
        }))
    );

    // commit.
    txn.commit().await?;

    // did it persist?
    let mut txn = partition_store.transaction();
    let current_dur = txn.get_partition_durability().await?;
    assert_that!(
        current_dur,
        some(eq(PartitionDurability {
            durable_point: Lsn::new(100),
            modification_time: now,
        }))
    );

    // Validate that durability ordering is correct
    assert_that!(
        PartitionDurability {
            durable_point: Lsn::new(100),
            modification_time: now,
        },
        gt(PartitionDurability {
            durable_point: Lsn::new(99),
            modification_time: now,
        })
    );

    rocksdb.shutdown().await;
    Ok(())
}
