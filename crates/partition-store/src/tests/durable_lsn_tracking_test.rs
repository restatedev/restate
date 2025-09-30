use googletest::prelude::*;
use rand::{RngCore, rng};

use restate_rocksdb::RocksDbManager;
use restate_storage_api::{
    Transaction,
    fsm_table::{PartitionDurability, ReadFsmTable, WriteFsmTable},
};
use restate_types::{
    identifiers::{PartitionId, PartitionKey},
    logs::Lsn,
    partitions::Partition,
    time::MillisSinceEpoch,
};

use crate::PartitionStoreManager;

const PARTITION: Partition =
    Partition::new(PartitionId::MIN, PartitionKey::MIN..=PartitionKey::MAX);

#[restate_core::test]
async fn track_latest_applied_lsn() -> googletest::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .pretty()
        .with_line_number(true)
        .with_file(true)
        .with_thread_names(true)
        .with_thread_ids(true)
        .init();

    let rocksdb = RocksDbManager::init();

    let partition_store_manager = PartitionStoreManager::create().await?;

    let mut partition_store = partition_store_manager.open(&PARTITION, None).await?;

    let watch_durable_lsn = partition_store.get_durable_lsn().await?;

    let mut txn = partition_store.transaction();
    txn.put_applied_lsn(Lsn::new(100)).unwrap();
    txn.commit().await.expect("commit succeeds");

    assert_eq!(None, *partition_store.get_durable_lsn().await?.borrow());
    assert_eq!(None, *watch_durable_lsn.borrow());

    partition_store.partition_db().flush_memtables(true).await?;

    assert_eq!(Some(Lsn::new(100)), *watch_durable_lsn.borrow());
    assert_eq!(
        Some(Lsn::new(100)),
        *partition_store.get_durable_lsn().await?.borrow()
    );

    drop(partition_store);
    partition_store_manager
        .close_partition_store(PARTITION.partition_id)
        .await;
    assert_eq!(None, *watch_durable_lsn.borrow());

    let mut partition_store = partition_store_manager.open(&PARTITION, None).await?;
    assert_eq!(
        Some(Lsn::new(100)),
        *partition_store.get_durable_lsn().await?.borrow(),
        "partition store should announce the durable LSN on open"
    );
    // old watch should be _still_ None
    assert_eq!(None, *watch_durable_lsn.borrow());
    // new watch should have Some(100)
    let watch_durable_lsn = partition_store.get_durable_lsn().await?;
    assert_eq!(Some(Lsn::new(100)), *watch_durable_lsn.borrow());

    partition_store.partition_db().flush_memtables(true).await?;

    let mut rng = rng();
    for lsn in 101..=200 {
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(Lsn::new(lsn)).unwrap();
        txn.put_inbox_seq_number(rng.next_u64()).unwrap();
        txn.commit().await.expect("commit succeeds");

        assert_eq!(
            Some(Lsn::new(100)),
            *partition_store.get_durable_lsn().await?.borrow(),
            "durable LSN remains unchanged"
        );
        assert_eq!(Some(Lsn::new(100)), *watch_durable_lsn.borrow());
    }

    partition_store.partition_db().flush_memtables(true).await?;
    assert_eq!(Some(Lsn::new(200)), *watch_durable_lsn.borrow());

    rocksdb.shutdown().await;
    Ok(())
}

#[restate_core::test]
async fn partition_durability_fsm() -> googletest::Result<()> {
    let rocksdb = RocksDbManager::init();

    let partition_store_manager = PartitionStoreManager::create().await?;

    let mut partition_store = partition_store_manager.open(&PARTITION, None).await?;

    // by default, we don't have partition durability.
    assert_that!(partition_store.get_partition_durability().await?, none());
    let mut txn = partition_store.transaction();

    // lets update it to current.
    let now = MillisSinceEpoch::now();
    txn.put_partition_durability(&PartitionDurability {
        durable_point: Lsn::new(100),
        modification_time: now,
    })?;

    // commit.
    txn.commit().await?;

    // did it persist?
    let current_dur = partition_store.get_partition_durability().await?;
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
