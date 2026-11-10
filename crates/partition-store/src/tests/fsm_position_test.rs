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
use crate::fsm_table::{get_applied_lsn_on_its_own, get_locally_durable_lsn, put_fsm_position};

/// A transaction persists the partition's position with every commit that writes something or
/// applies a record while byte offsets are on: with the applied LSN of that commit, and with the
/// partition store byte offset advanced by the size of the batch it commits, the position's own
/// put included. The commit that turns them on deletes the applied LSN persisted on its own, and
/// later ones don't write it. Without a position, the applied LSN persisted on its own applies;
/// with one, the position's applied LSN takes precedence, also as the durable applied LSN.
#[restate_core::test]
async fn fsm_position_is_persisted_with_every_commit() -> anyhow::Result<()> {
    RocksDbManager::init();
    let mut store = PartitionStoreManager::create(true)
        .await?
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await?;
    let mut reader = store.clone();
    // What putting a position without zero fields adds to a batch of this partition.
    let position_put_size = {
        let mut probe = reader.transaction();
        let empty = probe.estimated_size_in_bytes();
        put_fsm_position(
            &mut probe,
            &FsmPosition {
                applied_lsn: Lsn::OLDEST,
                byte_offsets_base_lsn: Lsn::OLDEST,
                log_byte_offset: 1,
                partition_store_byte_offset: 1,
            },
        )?;
        (probe.estimated_size_in_bytes() - empty) as u64
    };
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

    // On from the record at LSN 2, which doesn't count, as the processor turns them on. The second
    // commit only applies a record, and persists the position with nothing else.
    txn.start_byte_offsets(Lsn::new(2))?;
    let mut partition_store_byte_offset = 0;
    for records in [&[(2, 20), (3, 30)][..], &[(4, 40)]] {
        for &(lsn, bytes) in records {
            txn.put_applied_lsn(Lsn::new(lsn))?;
            txn.advance_fsm_position(Lsn::new(lsn), bytes);
        }
        partition_store_byte_offset += txn.estimated_size_in_bytes() as u64 + position_put_size;
        txn.commit().await?;
    }
    let position = FsmPosition {
        applied_lsn: Lsn::new(4),
        byte_offsets_base_lsn: Lsn::new(2),
        log_byte_offset: 70,
        partition_store_byte_offset,
    };
    assert_eq!(reader.get_fsm_position().await?, Some(position));
    assert_eq!(txn.fsm_position(), Some(position));
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
    drop(txn);
    assert_eq!(reader.get_applied_lsn().await?, Some(Lsn::new(4)));
    let mut db = reader.partition_db().clone();
    assert_eq!(db.get_applied_lsn().await?, Some(Lsn::new(4)));
    reader.partition_db().flush_memtables(true).await?;
    assert_eq!(get_locally_durable_lsn(&mut reader)?, Some(Lsn::new(4)));

    // Both byte offsets wrap at 2^64.
    let near_max = u64::MAX - 5;
    let mut txn = store.transaction();
    txn.set_fsm_position(FsmPosition {
        log_byte_offset: near_max,
        partition_store_byte_offset: near_max,
        ..position
    });
    txn.put_applied_lsn(Lsn::new(5))?;
    txn.advance_fsm_position(Lsn::new(5), 10);
    let batch_size = txn.estimated_size_in_bytes() as u64;
    txn.commit().await?;
    let position = reader.get_fsm_position().await?.unwrap();
    assert_eq!(position.log_byte_offset, 4);
    assert_eq!(
        position.partition_store_byte_offset.wrapping_sub(near_max),
        batch_size + position_put_size
    );
    Ok(())
}
