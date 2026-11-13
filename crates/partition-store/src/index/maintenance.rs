// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Index membership changes on writes: the keys a transaction adds and removes.
//!
//! An entry write hands each index the entry's old and new key, `None` when the
//! entry is outside the index: the old key is removed, the new one written,
//! nothing when they are equal. Removal uses RocksDB's SingleDelete, cheaper than
//! a regular delete but only correct for a key written once since it was last
//! deleted. The rule that keeps that true: `old` must be what the index holds
//! for the entry right now, including changes earlier in the same transaction,
//! and `None` means "absent", never "unknown". Inserting with `old = None` for a
//! key that exists would write it a second time.
//!
//! For example, an entry moving from `inbox` to `running` in the stage index:
//! `update_secondary_index(Some(&inbox_key), Some(&running_key))` single-deletes
//! the first and puts the second. An entry the index does not reflect yet (see
//! `registry::active`) passes `None` as `old` and only gets its new key.

use restate_types::sharding::PartitionId;

use crate::keys::header::IndexScope;
use crate::keys::{IndexKeyPrefix, KeyKind};
use crate::{PartitionStoreTransaction, StorageRead};

use super::{SecondaryIndex, SecondaryIndexKey};

impl PartitionStoreTransaction<'_> {
    /// Replaces the entry's key in a static index of this partition: removes
    /// `old`, writes `new` with an empty value, nothing when both encode to the
    /// same key. `old` must be the key the index holds now (see the module doc);
    /// `None` means absent. Reads nothing.
    pub fn update_secondary_index<K>(&mut self, old: Option<&K>, new: Option<&K>)
    where
        K: SecondaryIndexKey,
        K::Index: SecondaryIndex<Scope = PartitionId>,
    {
        self.update_covering_secondary_index(
            old.map(|key| (key, &[][..])),
            new.map(|key| (key, &[][..])),
        );
    }

    /// Like [`Self::update_secondary_index`] for a covering index, whose keys
    /// carry a value: a changed value at the same key is a SingleDelete followed
    /// by a put, so every key is still written once per lifetime. `old` must be
    /// the key and value the index holds now.
    pub fn update_covering_secondary_index<K>(
        &mut self,
        old: Option<(&K, &[u8])>,
        new: Option<(&K, &[u8])>,
    ) where
        K: SecondaryIndexKey,
        K::Index: SecondaryIndex<Scope = PartitionId>,
    {
        self.update_index_entries(self.partition_id(), old, new);
    }

    /// [`Self::update_secondary_index`] for the registered physical index `id`.
    /// `old` is `None` when the index does not reflect the entry yet.
    pub fn update_secondary_index_with_id<K>(&mut self, id: u32, old: Option<&K>, new: Option<&K>)
    where
        K: SecondaryIndexKey,
        K::Index: SecondaryIndex<Scope = IndexScope>,
    {
        let scope = IndexScope {
            partition: self.partition_id(),
            index: id,
        };
        self.update_index_entries(
            scope,
            old.map(|key| (key, &[][..])),
            new.map(|key| (key, &[][..])),
        );
    }

    /// Writes the keys of `scope`.
    fn update_index_entries<K: SecondaryIndexKey>(
        &mut self,
        scope: <K::Index as SecondaryIndex>::Scope,
        old: Option<(&K, &[u8])>,
        new: Option<(&K, &[u8])>,
    ) {
        if old.is_none() && new.is_none() {
            return;
        }

        let encode = |key: &K, buffer: &mut bytes::BytesMut| key.encode_key(scope, buffer);
        let encoded_len =
            |(key, _): (&K, &[u8])| IndexKeyPrefix::SERIALIZED_LENGTH + key.serialized_length();
        let capacity = old.map_or(0, encoded_len) + new.map_or(0, encoded_len);
        let (keys, old_len) = {
            let buffer = self.cleared_key_buffer_mut(capacity);
            if let Some((old, _)) = old {
                encode(old, buffer);
            }
            let old_len = buffer.len();
            if let Some((new, _)) = new {
                encode(new, buffer);
            }

            let same_key = old.is_some() && new.is_some() && buffer[..old_len] == buffer[old_len..];
            if same_key && old.map(|(_, value)| value) == new.map(|(_, value)| value) {
                // Same key and value. Nothing to be done here.
                return;
            }
            (buffer.split(), old_len)
        };

        if old.is_some() {
            self.raw_single_delete_cf(KeyKind::SecondaryIndex, &keys[..old_len]);
        }
        if let Some((_, value)) = new {
            self.raw_put_cf(KeyKind::SecondaryIndex, &keys[old_len..], value);
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
    use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::vqueues::{EntryId, EntryKind, Seq};

    use crate::index::EntryByStageServiceKey;
    use crate::scan::PhysicalScan;
    use crate::{PartitionStore, PartitionStoreManager};

    use super::*;

    /// A registered index's index ID; maintenance needs no registry.
    const ID: u32 = 64;

    async fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
        let mut prefix = Vec::new();
        EntryByStageServiceKey::prefix(
            IndexScope {
                partition: store.partition_id(),
                index: ID,
            },
            &mut prefix,
        );
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .iterator_for_each_physical(
                "test-secondary-index-maintenance",
                Priority::Low,
                ReadOptions::default(),
                PhysicalScan::Prefix(KeyKind::SecondaryIndex, prefix.into()),
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
        let id = BaseEntryId::from(id).canonicalize(Seq::new(1));
        // Kind alone distinguishes entries with the same partition, remainder, and sequence.
        let other_id = BaseEntryId::new(
            id.partition_key(),
            EntryId::new(
                EntryKind::StateMutation,
                *id.as_entry_id().remainder_bytes(),
            ),
        )
        .canonicalize(id.seq());
        let inbox = EntryByStageServiceKey::borrowed(Stage::Inbox, "svc", Reverse(at), id);
        let other = EntryByStageServiceKey::borrowed(Stage::Inbox, "svc", Reverse(at), other_id);
        let running = EntryByStageServiceKey::borrowed(Stage::Running, "svc", Reverse(at), id);
        let newer = EntryByStageServiceKey::borrowed(Stage::Running, "svc", Reverse(later), id);
        let encoded = |key: &_| {
            let mut bytes = Vec::new();
            SecondaryIndexKey::encode_key(
                key,
                IndexScope {
                    partition,
                    index: ID,
                },
                &mut bytes,
            );
            bytes
        };

        let mut tx = store.transaction();
        let empty_size = tx.estimated_size_in_bytes();
        tx.update_secondary_index_with_id::<EntryByStageServiceKey>(ID, None, None);
        assert_eq!(tx.estimated_size_in_bytes(), empty_size);
        tx.update_secondary_index_with_id(ID, None, Some(&inbox));
        tx.update_secondary_index_with_id(ID, None, Some(&other));
        let inserted_size = tx.estimated_size_in_bytes();
        // Distinct objects encoding the same key must not issue a second Put.
        let unchanged = EntryByStageServiceKey::borrowed(Stage::Inbox, "svc", Reverse(at), id);
        tx.update_secondary_index_with_id(ID, Some(&inbox), Some(&unchanged));
        assert_eq!(tx.estimated_size_in_bytes(), inserted_size);
        assert!(
            tx.get(KeyKind::SecondaryIndex, encoded(&inbox))
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
        tx.update_secondary_index_with_id(ID, Some(&inbox), Some(&running));
        tx.update_secondary_index_with_id(ID, Some(&running), Some(&newer));
        assert!(
            tx.get(KeyKind::SecondaryIndex, encoded(&inbox))
                .unwrap()
                .is_none()
        );
        assert!(
            tx.get(KeyKind::SecondaryIndex, encoded(&running))
                .unwrap()
                .is_none()
        );
        assert!(
            tx.get(KeyKind::SecondaryIndex, encoded(&newer))
                .unwrap()
                .is_some()
        );
        let updated_size = tx.estimated_size_in_bytes();
        tx.update_secondary_index_with_id(ID, Some(&newer), Some(&newer));
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
        tx.update_secondary_index_with_id(ID, Some(&newer), None);
        drop(tx);
        assert_eq!(
            index_keys(&observer).await,
            vec![encoded(&other), encoded(&newer)]
        );

        let mut tx = store.transaction();
        tx.update_secondary_index_with_id(ID, Some(&newer), None);
        tx.update_secondary_index_with_id(ID, Some(&other), None);
        tx.commit().await.unwrap();
        drop(tx);
        assert!(index_keys(&observer).await.is_empty());

        // Reusing an old key starts a new Put/SingleDelete lifetime, even when both
        // operations are in the same transaction.
        let mut tx = store.transaction();
        tx.update_secondary_index_with_id(ID, None, Some(&inbox));
        tx.update_secondary_index_with_id(ID, Some(&inbox), None);
        tx.commit().await.unwrap();
        drop(tx);
        assert!(index_keys(&observer).await.is_empty());
    }
}
