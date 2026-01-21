// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use googletest::prelude::*;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::{
    Transaction,
    fsm_table::{ReadFsmTable, WriteFsmTable},
};
use restate_types::{
    SemanticRestateVersion,
    identifiers::{PartitionId, PartitionKey},
    partitions::Partition,
};

use crate::PartitionStoreManager;

#[restate_core::test]
async fn barrier_fsm() -> googletest::Result<()> {
    // sanity check
    assert_that!(
        SemanticRestateVersion::current(),
        not(eq(&SemanticRestateVersion::unknown()))
    );

    let rocksdb = RocksDbManager::init();

    let partition_store_manager = PartitionStoreManager::create().await?;

    let partition = Partition::new(PartitionId::MIN, PartitionKey::MIN..=PartitionKey::MAX);
    let mut partition_store = partition_store_manager.open(&partition, None).await?;

    // we default to unknown if FSM doesn't have a min version, in that case, any "real" version
    // should be greater.
    let current_min = partition_store.get_min_restate_version().await?;
    // current_min should be equal to unknown
    assert_that!(current_min, eq(SemanticRestateVersion::unknown()));
    assert_that!(
        SemanticRestateVersion::current().is_equal_or_newer_than(&current_min),
        eq(true)
    );

    let mut txn = partition_store.transaction();
    // lets update it to current.
    txn.put_min_restate_version(SemanticRestateVersion::current())?;

    // commit.
    txn.commit().await?;

    // did it persist?
    let current_min = partition_store.get_min_restate_version().await?;
    // it's actually persisted
    assert_that!(&current_min, eq(SemanticRestateVersion::current()));

    rocksdb.shutdown().await;
    Ok(())
}
