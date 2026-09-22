// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{IndexKeyPrefix, KeyKind};
use crate::{PartitionStoreTransaction, StorageAccess};

use super::SecondaryIndexKey;

impl PartitionStoreTransaction<'_> {
    /// Applies a change in secondary-index membership to this transaction.
    ///
    /// Inserts use an empty value; removals use SingleDelete. Identical encoded
    /// keys produce no writes, including when the source record's other fields
    /// changed. All keys are scoped to this transaction's physical partition.
    ///
    /// `old` must describe the actual previous membership, including earlier
    /// changes in this transaction. `None` means absent, not unknown: repeatedly
    /// inserting with `old = None` would overwrite an entry and violate the
    /// SingleDelete contract. This operation performs no storage reads.
    pub fn update_secondary_index<K: SecondaryIndexKey>(
        &mut self,
        old: Option<&K>,
        new: Option<&K>,
    ) {
        if old.is_none() && new.is_none() {
            return;
        }

        let partition_id = self.partition_id();
        let encoded_len = |key: &K| IndexKeyPrefix::SERIALIZED_LENGTH + key.encoded_len();
        let capacity = old.map_or(0, encoded_len) + new.map_or(0, encoded_len);
        let (keys, old_len) = {
            let buffer = self.cleared_key_buffer_mut(capacity);
            if let Some(old) = old {
                old.encode_key(partition_id, buffer);
            }
            let old_len = buffer.len();
            if let Some(new) = new {
                new.encode_key(partition_id, buffer);
            }

            if old.is_some() && new.is_some() && buffer[..old_len] == buffer[old_len..] {
                return;
            }
            (buffer.split(), old_len)
        };

        if old.is_some() {
            self.raw_single_delete_cf(KeyKind::SecondaryIndex, &keys[..old_len]);
        }
        if new.is_some() {
            self.raw_put_cf(KeyKind::SecondaryIndex, &keys[old_len..], []);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;
    use std::ops::ControlFlow;

    use rocksdb::ReadOptions;

    use restate_clock::UniqueTimestamp;
    use restate_rocksdb::{Priority, RocksDbManager};
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::Stage;
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};

    use crate::index::InvocationByServiceStageKey;
    use crate::scan::PhysicalScan;
    use crate::{PartitionStore, PartitionStoreManager, TableKind};

    use super::*;

    async fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
        let mut prefix = Vec::new();
        InvocationByServiceStageKey::prefix(store.partition_id(), &mut prefix);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .iterator_for_each_physical(
                "test-secondary-index-maintenance",
                Priority::Low,
                ReadOptions::default(),
                PhysicalScan::Prefix(TableKind::SecondaryIndex, prefix.into()),
                move |(key, value)| {
                    assert!(value.is_empty());
                    sender.send(key.to_vec()).unwrap();
                    ControlFlow::Continue(())
                },
            )
            .unwrap()
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some(key) = receiver.recv().await {
            keys.push(key);
        }
        keys
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn membership_changes_are_transactional_and_unchanged_keys_do_not_write() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = PartitionId::MIN;
        let mut store = manager
            .open(&Partition::new(partition, KeyRange::FULL), None)
            .await
            .unwrap();
        let observer = store.clone();
        let at = UniqueTimestamp::try_from_parts(100, 1).unwrap();
        let later = UniqueTimestamp::try_from_parts(100, 2).unwrap();
        let id = InvocationId::from_parts(3337, InvocationUuid::from_u128(1));
        let other_id = InvocationId::from_parts(3337, InvocationUuid::from_u128(2));
        let inbox = InvocationByServiceStageKey::borrowed("svc", Stage::Inbox, Reverse(at), id);
        let other =
            InvocationByServiceStageKey::borrowed("svc", Stage::Inbox, Reverse(at), other_id);
        let running = InvocationByServiceStageKey::borrowed("svc", Stage::Running, Reverse(at), id);
        let newer =
            InvocationByServiceStageKey::borrowed("svc", Stage::Running, Reverse(later), id);
        let encoded = |key: &_| {
            let mut bytes = Vec::new();
            SecondaryIndexKey::encode_key(key, partition, &mut bytes);
            bytes
        };

        let mut tx = store.transaction();
        let empty_size = tx.estimated_size_in_bytes();
        tx.update_secondary_index::<InvocationByServiceStageKey>(None, None);
        assert_eq!(tx.estimated_size_in_bytes(), empty_size);
        tx.update_secondary_index(None, Some(&inbox));
        tx.update_secondary_index(None, Some(&other));
        let inserted_size = tx.estimated_size_in_bytes();
        // Distinct objects encoding the same key must not issue a second Put.
        let unchanged = InvocationByServiceStageKey::borrowed("svc", Stage::Inbox, Reverse(at), id);
        tx.update_secondary_index(Some(&inbox), Some(&unchanged));
        assert_eq!(tx.estimated_size_in_bytes(), inserted_size);
        assert!(
            tx.get(TableKind::SecondaryIndex, encoded(&inbox))
                .unwrap()
                .is_some()
        );
        assert!(index_keys(&observer).await.is_empty());
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            index_keys(&observer).await,
            vec![encoded(&inbox), encoded(&other)]
        );

        // Two transitions in one batch, including a timestamp-only change.
        let mut tx = store.transaction();
        tx.update_secondary_index(Some(&inbox), Some(&running));
        tx.update_secondary_index(Some(&running), Some(&newer));
        assert!(
            tx.get(TableKind::SecondaryIndex, encoded(&inbox))
                .unwrap()
                .is_none()
        );
        assert!(
            tx.get(TableKind::SecondaryIndex, encoded(&running))
                .unwrap()
                .is_none()
        );
        assert!(
            tx.get(TableKind::SecondaryIndex, encoded(&newer))
                .unwrap()
                .is_some()
        );
        let updated_size = tx.estimated_size_in_bytes();
        tx.update_secondary_index(Some(&newer), Some(&newer));
        assert_eq!(tx.estimated_size_in_bytes(), updated_size);
        assert_eq!(
            index_keys(&observer).await,
            vec![encoded(&inbox), encoded(&other)]
        );
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            index_keys(&observer).await,
            vec![encoded(&other), encoded(&newer)]
        );

        // Uncommitted removal is rolled back by dropping the transaction.
        let mut tx = store.transaction();
        tx.update_secondary_index(Some(&newer), None);
        drop(tx);
        assert_eq!(
            index_keys(&observer).await,
            vec![encoded(&other), encoded(&newer)]
        );

        let mut tx = store.transaction();
        tx.update_secondary_index(Some(&newer), None);
        tx.update_secondary_index(Some(&other), None);
        tx.commit().await.unwrap();
        drop(tx);
        assert!(index_keys(&observer).await.is_empty());

        // Reusing an old key starts a new Put/SingleDelete lifetime, even when both
        // operations are in the same transaction.
        let mut tx = store.transaction();
        tx.update_secondary_index(None, Some(&inbox));
        tx.update_secondary_index(Some(&inbox), None);
        tx.commit().await.unwrap();
        drop(tx);
        assert!(index_keys(&observer).await.is_empty());
    }
}
