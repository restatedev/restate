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
//! For example, an entry moving from `inbox` to `running` in a stage index:
//! `update_secondary_index_with_id(id, Some(&inbox_key), Some(&running_key))`
//! single-deletes the first and puts the second. An entry the index does not reflect yet (see
//! `registry::active`) passes `None` as `old` and only gets its new key.
//!
//! An index with a value ([`IndexValue`](super::IndexValue)) passes each key with
//! its value. Unchanged means the same key and the same encoded value; a new value
//! under the same key single-deletes the key and puts it again, never a second Put
//! over the first, which a later SingleDelete would leave behind.

use crate::keys::header::IndexScope;
use crate::keys::{IndexKeyPrefix, KeyKind};
use crate::{PartitionStoreTransaction, StorageRead};

use super::{IndexValueOf, IndexValueRef, SecondaryIndex, SecondaryIndexKey};

impl PartitionStoreTransaction<'_> {
    /// Replaces the entry's key in the registered index `id` of this partition:
    /// removes `old`, writes `new` with an empty value, nothing when both encode
    /// to the same key. `old` must be the key the index holds now (see the module
    /// doc); `None` means absent, as for an entry the index does not reflect yet.
    /// Reads nothing.
    pub fn update_secondary_index_with_id<K>(&mut self, id: u32, old: Option<&K>, new: Option<&K>)
    where
        K: SecondaryIndexKey,
        K::Index: SecondaryIndex<Scope = IndexScope, Value = ()>,
    {
        self.update_secondary_index_entry_with_id(
            id,
            old.map(|key| (key, ())),
            new.map(|key| (key, ())),
        );
    }

    /// Replaces the entry's key and value in the registered index `id` of this
    /// partition: removes `old`, writes `new`, nothing when both encode to the
    /// same key and value. `old` must be what the index holds now (see the module
    /// doc). Reads nothing.
    pub fn update_secondary_index_entry_with_id<'v, K>(
        &mut self,
        id: u32,
        old: Option<(&K, IndexValueOf<'v, K::Index>)>,
        new: Option<(&K, IndexValueOf<'v, K::Index>)>,
    ) where
        K: SecondaryIndexKey,
        K::Index: SecondaryIndex<Scope = IndexScope>,
    {
        if old.is_none() && new.is_none() {
            return;
        }
        let scope = IndexScope {
            partition: self.partition_id(),
            index: id,
        };
        let encoded_len = |(key, value): &(&K, IndexValueOf<'v, K::Index>)| {
            IndexKeyPrefix::SERIALIZED_LENGTH + key.serialized_length() + value.encoded_len()
        };
        let capacity = old.as_ref().map_or(0, encoded_len) + new.as_ref().map_or(0, encoded_len);
        // One buffer, laid out as old key | new key | old value | new value.
        let (bytes, old_key, new_key, old_value) = {
            let buffer = self.cleared_key_buffer_mut(capacity);
            if let Some((key, _)) = &old {
                key.encode_key(scope, buffer);
            }
            let old_key = buffer.len();
            if let Some((key, _)) = &new {
                key.encode_key(scope, buffer);
            }
            let new_key = buffer.len();
            if let Some((_, value)) = &old {
                value.encode(buffer);
            }
            let old_value = buffer.len();
            if let Some((_, value)) = &new {
                value.encode(buffer);
            }
            if old.is_some()
                && new.is_some()
                && buffer[..old_key] == buffer[old_key..new_key]
                && buffer[new_key..old_value] == buffer[old_value..]
            {
                // Same key and value. Nothing to be done here.
                return;
            }
            (buffer.split(), old_key, new_key, old_value)
        };

        if old.is_some() {
            self.raw_single_delete_cf(KeyKind::SecondaryIndex, &bytes[..old_key]);
        }
        if new.is_some() {
            self.raw_put_cf(
                KeyKind::SecondaryIndex,
                &bytes[old_key..new_key],
                &bytes[old_value..],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use bytes::BufMut;
    use rocksdb::ReadOptions;

    use restate_clock::UniqueTimestamp;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::vqueue_table::Stage;
    use restate_storage_api::{StorageError, Transaction};
    use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::vqueues::{EntryId, EntryKind, Seq};

    use crate::index::IndexValue;
    use crate::keys::codecs::{
        CanonicalEntryIdCodec, ReverseUniqueTimestampCodec, ServiceNameCodec, StageRankCodec,
    };
    use crate::scan::PhysicalScan;
    use crate::{PartitionStore, PartitionStoreManager};

    use super::*;

    crate::index::macros::define_secondary_index!(
        /// A test index of entries by stage and service, newest transition first.
        TestIndex,
        definition: "test_index/1",
        introduced: (1, 8, 2),
        key: TestIndexKey {
            stage: StageRankCodec,
            service_name: ServiceNameCodec => str,
            transitioned_at: ReverseUniqueTimestampCodec,
            canonical_id: CanonicalEntryIdCodec (primary_key),
        }
    );

    /// A test value: an entry's handler, stored as its UTF-8 bytes.
    pub enum Handler {}

    impl IndexValue for Handler {
        type Ref<'a> = &'a str;
    }

    impl<'a> IndexValueRef<'a> for &'a str {
        fn encoded_len(&self) -> usize {
            self.len()
        }

        fn encode<B: BufMut>(&self, buffer: &mut B) {
            buffer.put_slice(self.as_bytes());
        }

        fn decode(bytes: &'a [u8]) -> crate::Result<Self> {
            std::str::from_utf8(bytes).map_err(|_| StorageError::DataIntegrityError)
        }
    }

    crate::index::macros::define_secondary_index!(
        /// A test index of entries by service, with each entry's handler as its value.
        TestValuedIndex,
        definition: "test_valued_index/1",
        introduced: (1, 8, 2),
        key: TestValuedIndexKey {
            service_name: ServiceNameCodec => str,
            canonical_id: CanonicalEntryIdCodec (primary_key),
        },
        value: Handler,
    );

    /// A registered index's index ID; maintenance needs no registry.
    const ID: u32 = 64;
    const VALUED_ID: u32 = 65;

    /// The keys and values of index `index`, in key order.
    async fn index_entries(store: &PartitionStore, index: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut prefix = Vec::new();
        TestIndexKey::prefix(
            IndexScope {
                partition: store.partition_id(),
                index,
            },
            &mut prefix,
        );
        let snapshot = store.partition_db().read_snapshot().unwrap();
        let mut rows = snapshot.scan(
            PhysicalScan::Prefix(KeyKind::SecondaryIndex, prefix),
            ReadOptions::default(),
        );
        let mut entries = Vec::new();
        while let Some((key, value)) = rows.item().unwrap() {
            entries.push((key.to_vec(), value.to_vec()));
            rows.next();
        }
        entries
    }

    /// The keys of the key-only test index, whose values must be empty.
    async fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
        index_entries(store, ID)
            .await
            .into_iter()
            .map(|(key, value)| {
                <() as IndexValueRef>::decode(&value).unwrap();
                key
            })
            .collect()
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
        let inbox = TestIndexKey::borrowed(Stage::Inbox, "svc", Reverse(at), id);
        let other = TestIndexKey::borrowed(Stage::Inbox, "svc", Reverse(at), other_id);
        let running = TestIndexKey::borrowed(Stage::Running, "svc", Reverse(at), id);
        let newer = TestIndexKey::borrowed(Stage::Running, "svc", Reverse(later), id);
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
        tx.update_secondary_index_with_id::<TestIndexKey>(ID, None, None);
        assert_eq!(tx.estimated_size_in_bytes(), empty_size);
        tx.update_secondary_index_with_id(ID, None, Some(&inbox));
        tx.update_secondary_index_with_id(ID, None, Some(&other));
        let inserted_size = tx.estimated_size_in_bytes();
        // Distinct objects encoding the same key must not issue a second Put.
        let unchanged = TestIndexKey::borrowed(Stage::Inbox, "svc", Reverse(at), id);
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
        // A key-only index's value is empty; anything else is not its encoding.
        assert!(<() as IndexValueRef>::decode(b"x").is_err());
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn values_are_written_with_their_keys_and_a_new_value_replaces_the_key() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = PartitionId::MIN;
        let mut store = manager
            .open(&Partition::new(partition, KeyRange::FULL), None)
            .await
            .unwrap();
        let observer = store.clone();
        let entry = |uuid| {
            let id = InvocationId::from_parts(3337, InvocationUuid::from_u128(uuid));
            BaseEntryId::from(id).canonicalize(Seq::new(1))
        };
        let first = TestValuedIndexKey::borrowed("svc", entry(1));
        let second = TestValuedIndexKey::borrowed("svc", entry(2));
        let encoded = |key: &TestValuedIndexKeyRef<'_>| {
            let mut bytes = Vec::new();
            key.encode_key(
                IndexScope {
                    partition,
                    index: VALUED_ID,
                },
                &mut bytes,
            );
            bytes
        };
        let stored = async || {
            index_entries(&observer, VALUED_ID)
                .await
                .into_iter()
                .map(|(key, value)| (key, <&str>::decode(&value).unwrap().to_owned()))
                .collect::<Vec<_>>()
        };

        let mut tx = store.transaction();
        tx.update_secondary_index_entry_with_id(VALUED_ID, None, Some((&first, "add")));
        tx.update_secondary_index_entry_with_id(VALUED_ID, None, Some((&second, "get")));
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            stored().await,
            vec![
                (encoded(&first), "add".to_owned()),
                (encoded(&second), "get".to_owned())
            ]
        );

        // The same key and value write nothing; a new value under the same key
        // removes and rewrites it.
        let mut tx = store.transaction();
        let empty_size = tx.estimated_size_in_bytes();
        tx.update_secondary_index_entry_with_id(
            VALUED_ID,
            Some((&first, "add")),
            Some((&first, "add")),
        );
        assert_eq!(tx.estimated_size_in_bytes(), empty_size);
        tx.update_secondary_index_entry_with_id(
            VALUED_ID,
            Some((&first, "add")),
            Some((&first, "set")),
        );
        assert!(tx.estimated_size_in_bytes() > empty_size);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            stored().await,
            vec![
                (encoded(&first), "set".to_owned()),
                (encoded(&second), "get".to_owned())
            ]
        );

        // Removal takes the key and its value.
        let mut tx = store.transaction();
        tx.update_secondary_index_entry_with_id(VALUED_ID, Some((&first, "set")), None);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(stored().await, vec![(encoded(&second), "get".to_owned())]);
    }
}
