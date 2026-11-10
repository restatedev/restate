// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{FsmPosition, ReadFsmTable, WriteFsmTable};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::PartitionStoreManager;
use crate::fsm_table::{get_applied_lsn_on_its_own, get_locally_durable_lsn};

/// A transaction persists the partition's position with every commit that writes something or
/// applies a record while byte offsets are on, with the applied LSN of that commit. The commit
/// that turns them on deletes the applied LSN persisted on its own, and later ones don't write it.
/// Without a position, the applied LSN persisted on its own applies; with one, the position's
/// applied LSN takes precedence, also as the durable applied LSN.
#[restate_core::test]
async fn fsm_position_is_persisted_with_every_commit() -> anyhow::Result<()> {
    RocksDbManager::init();
    let mut store = PartitionStoreManager::create(true)
        .await?
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await?;
    let mut reader = store.clone();
    let mut txn = store.transaction();
    txn.set_fsm_position(FsmPosition::new(Lsn::INVALID));

    // Off: only the applied LSN is persisted.
    txn.put_applied_lsn(Lsn::new(1))?;
    txn.advance_fsm_position(Lsn::new(1), 10);
    txn.commit().await?;
    assert_eq!(reader.get_fsm_position().await?, None);
    assert_eq!(reader.get_applied_lsn().await?, Some(Lsn::new(1)));
    reader.partition_db().flush_memtables(true).await?;
    assert_eq!(get_locally_durable_lsn(&mut reader)?, Some(Lsn::new(1)));

    // On from the record at LSN 2, which doesn't count, as the processor turns them on.
    txn.start_byte_offsets(Lsn::new(2))?;
    for (lsn, bytes) in [(2, 20), (3, 30)] {
        txn.put_applied_lsn(Lsn::new(lsn))?;
        txn.advance_fsm_position(Lsn::new(lsn), bytes);
    }
    txn.commit().await?;
    let position = FsmPosition {
        applied_lsn: Lsn::new(3),
        byte_offsets_base_lsn: Lsn::new(2),
        log_byte_offset: 30,
    };
    assert_eq!(reader.get_fsm_position().await?, Some(position));
    assert_eq!(txn.fsm_position(), Some(position));
    assert_eq!(get_applied_lsn_on_its_own(reader.partition_db())?, None);
    // Applying a record persists the position, also when the commit has nothing else to write.
    txn.put_applied_lsn(Lsn::new(4))?;
    txn.advance_fsm_position(Lsn::new(4), 40);
    txn.commit().await?;
    let position = FsmPosition {
        applied_lsn: Lsn::new(4),
        log_byte_offset: 70,
        ..position
    };
    assert_eq!(reader.get_fsm_position().await?, Some(position));
    assert_eq!(get_applied_lsn_on_its_own(reader.partition_db())?, None);
    // A commit with nothing to write persists nothing, not even the position.
    txn.advance_fsm_position(Lsn::new(5), 50);
    txn.commit().await?;
    assert_eq!(reader.get_fsm_position().await?, Some(position));
    drop(txn);

    // The position's applied LSN takes precedence over one written without it.
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(6))?;
    txn.commit().await?;
    assert_eq!(reader.get_applied_lsn().await?, Some(Lsn::new(4)));
    let mut db = reader.partition_db().clone();
    assert_eq!(db.get_applied_lsn().await?, Some(Lsn::new(4)));
    reader.partition_db().flush_memtables(true).await?;
    assert_eq!(get_locally_durable_lsn(&mut reader)?, Some(Lsn::new(4)));
    Ok(())
}
