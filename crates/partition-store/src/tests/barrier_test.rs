use googletest::prelude::*;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::{
    Transaction,
    fsm_table::{FsmTable, ReadOnlyFsmTable},
};
use restate_types::{
    SemanticRestateVersion,
    config::{CommonOptions, RocksDbOptions, StorageOptions},
    identifiers::{PartitionId, PartitionKey},
    live::Constant,
};

use crate::{OpenMode, PartitionStoreManager};

#[restate_core::test]
async fn barrier_fsm() -> googletest::Result<()> {
    // sanity check
    assert_that!(
        SemanticRestateVersion::current(),
        not(eq(&SemanticRestateVersion::unknown()))
    );

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
    // we default to unknown if FSM doesn't have a min version, in that case, any "real" version
    // should be greater.
    let current_min = txn.get_min_restate_version().await?;
    // current_min should be equal to unknown
    assert_that!(current_min, eq(SemanticRestateVersion::unknown()));
    assert_that!(
        SemanticRestateVersion::current().is_equal_or_newer_than(&current_min),
        eq(true)
    );

    // lets update it to current.
    txn.put_min_restate_version(SemanticRestateVersion::current())
        .await?;

    let current_min = txn.get_min_restate_version().await?;
    assert_that!(&current_min, eq(SemanticRestateVersion::current()));

    // commit.
    txn.commit().await?;

    // did it persist?
    let mut txn = partition_store.transaction();
    let current_min = txn.get_min_restate_version().await?;
    // it's actually persisted
    assert_that!(&current_min, eq(SemanticRestateVersion::current()));

    rocksdb.shutdown().await;
    Ok(())
}
