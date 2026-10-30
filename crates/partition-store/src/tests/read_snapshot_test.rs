// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use rocksdb::ReadOptions;
use tokio::time::timeout;

use restate_rocksdb::RocksDbManager;
use restate_rocksdb::reads::ReadIterator;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
use restate_types::identifiers::PartitionId;
use restate_types::logs::Lsn;
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::keys::KeyKind;
use crate::scan::PhysicalScan;
use crate::{
    PartitionReadSnapshot, PartitionStore, PartitionStoreManager, ScanMode, convert_to_upper_bound,
};

const DEADLINE: Duration = Duration::from_secs(10);
// How long an operation that must wait for a snapshot is given to (wrongly)
// finish. A slow machine can't make the check fail; it can only miss a bug.
const STILL_WAITING: Duration = Duration::from_millis(100);

type Entry = (Vec<u8>, Vec<u8>);

async fn write_applied_lsn(store: &mut PartitionStore, lsn: u64) -> anyhow::Result<()> {
    let mut tx = store.transaction();
    tx.put_applied_lsn(Lsn::new(lsn))?;
    tx.commit().await?;
    Ok(())
}

/// Collects every entry from the iterator's current position on. Fails if there
/// are none, so that comparing two results can't pass by accident.
fn collect<R>(mut iterator: ReadIterator<'_, R>) -> anyhow::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    while let Some((key, value)) = iterator.item()? {
        entries.push((key.to_vec(), value.to_vec()));
        iterator.next();
    }
    anyhow::ensure!(!entries.is_empty(), "the scan found nothing");
    Ok(entries)
}

/// Every key and value of the partition's FSM table, as seen by `snapshot`.
fn fsm_entries(snapshot: &PartitionReadSnapshot) -> anyhow::Result<Vec<Entry>> {
    let prefix: &[u8] = KeyKind::Fsm.as_bytes();
    collect(snapshot.scan(
        PhysicalScan::Prefix(KeyKind::Fsm, prefix),
        ReadOptions::default(),
    ))
}

#[restate_core::test]
async fn read_snapshot_sees_the_partition_as_it_was_when_taken() -> anyhow::Result<()> {
    let rocksdb = RocksDbManager::init();
    let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
    for layout in [false, true] {
        {
            let manager = PartitionStoreManager::create(layout).await?;
            let mut store = manager.open(&partition, None).await?;
            write_applied_lsn(&mut store, 10).await?;

            let before = store.partition_db().read_snapshot()?;
            let captured = fsm_entries(&before)?;
            write_applied_lsn(&mut store, 20).await?;
            assert_eq!(store.get_applied_lsn().await?, Some(Lsn::new(20)));

            // The snapshot doesn't see the later write, but a new snapshot does.
            assert_eq!(fsm_entries(&before)?, captured);
            let current = fsm_entries(&store.partition_db().read_snapshot()?)?;
            let (key, old_value) = captured
                .iter()
                .find(|(key, value)| {
                    current.iter().any(|(current_key, current_value)| {
                        current_key == key && current_value != value
                    })
                })
                .expect("the applied LSN changed");

            // A point read through the snapshot also returns the old value.
            let reader = before.reader(KeyKind::Fsm, ReadOptions::default());
            assert_eq!(reader.get(key)?.as_ref(), Some(old_value));

            // A total-order range scan over the same keys finds the same entries.
            let prefix = KeyKind::Fsm.as_bytes();
            let mut end = prefix.to_vec();
            assert!(convert_to_upper_bound(&mut end));
            let range = PhysicalScan::RangeExclusive(
                KeyKind::Fsm,
                ScanMode::TotalOrder,
                prefix.as_slice(),
                end.as_slice(),
            );
            assert_eq!(
                collect(before.scan(range, ReadOptions::default()))?,
                captured
            );

            // So does an owned scan, which doesn't borrow the bytes it was given:
            // they can be dropped before it is used.
            let key_buf = prefix.to_vec();
            let owned = before.owned_scan(
                PhysicalScan::Prefix(KeyKind::Fsm, key_buf.as_slice()),
                ReadOptions::default(),
            );
            drop(key_buf);
            assert_eq!(collect(owned)?, captured);
        }
        rocksdb.reset().await?;
    }
    Ok(())
}

#[restate_core::test]
async fn closing_or_dropping_a_partition_waits_for_its_read_snapshots() -> anyhow::Result<()> {
    let rocksdb = RocksDbManager::init();
    let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
    for layout in [false, true] {
        {
            let manager = PartitionStoreManager::create(layout).await?;
            let mut store = manager.open(&partition, None).await?;
            write_applied_lsn(&mut store, 10).await?;

            // Closing the partition waits for the snapshot. Meanwhile no new snapshot
            // can be taken, but reads through the existing one keep working.
            let snapshot = store.partition_db().read_snapshot()?;
            let captured = fsm_entries(&snapshot)?;
            let mut closing = Box::pin(manager.close(partition.partition_id));
            assert!(timeout(STILL_WAITING, closing.as_mut()).await.is_err());
            assert!(store.partition_db().read_snapshot().is_err());
            assert_eq!(fsm_entries(&snapshot)?, captured);
            drop(snapshot);
            timeout(DEADLINE, closing).await?;

            // A handle kept from before the partition was reopened stays closed. The
            // reopened partition takes snapshots again.
            let reopened = manager.open(&partition, None).await?;
            assert!(store.partition_db().read_snapshot().is_err());
            let snapshot = reopened.partition_db().read_snapshot()?;
            assert_eq!(fsm_entries(&snapshot)?, captured);

            // Deleting the partition's column family waits for the snapshot as well,
            // even once every store handle is gone, because the snapshot keeps the
            // column family alive.
            let mut released = Box::pin(reopened.partition_db().released());
            drop(store);
            drop(reopened);
            let mut dropping = Box::pin(manager.drop_partition_store(&partition));
            assert!(timeout(STILL_WAITING, dropping.as_mut()).await.is_err());
            assert!(timeout(STILL_WAITING, released.as_mut()).await.is_err());
            assert_eq!(fsm_entries(&snapshot)?, captured);
            drop(snapshot);
            timeout(DEADLINE, released).await?;
            assert!(timeout(DEADLINE, dropping).await??);

            // The partition's data is gone.
            let mut fresh = manager.open(&partition, None).await?;
            assert_eq!(fresh.get_applied_lsn().await?, None);
        }
        rocksdb.reset().await?;
    }
    Ok(())
}
