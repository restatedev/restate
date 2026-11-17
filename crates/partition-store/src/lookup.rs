// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Exact-key lookups: the records of a finite set of complete keys, fetched by
//! multi-get instead of found by navigating key ranges.
//!
//! A query naming, say, 251 queue IDs spread over many partition keys would
//! otherwise walk the key ranges between them. A [`LookupBinding`] says which
//! columns of its logical schema form the table's complete key; when a query
//! pins finitely many of them, the engine sends their keys ([`LookupKeys`]) to
//! each partition, which fetches exactly those, a batch of
//! [`MultiGetTable::BATCH_SIZE`] keys per multi-get. The binding turns the
//! fetched records into rows exactly as it does for a scan, and the query's
//! predicate still applies to them.
//!
//! ```text
//! WHERE id IN ('q1', 'q2')  ──lookup_ids──▶ {q1, q2}  ──encode──▶ keys
//!                                                         │ per partition
//! LookupKeys::decode ◀──────────────────────────────────────┘
//!   └─ scan_cursor(snapshot) ─▶ LookupCursor: multi-get q1, q2 ─▶ (key decoder, value)
//! ```
//!
//! A lookup applies the same gates as a scan of the table: nothing on an
//! inactive layout ([`is_active`](crate::table::PhysicalTable::is_active)), only keys inside the read's
//! scope (the header's bounds), and only records the partition owns
//! ([`owns_record`](crate::table::PhysicalTable::owns_record)). Keys are built by the engine, so this suits
//! tables whose header needs no store-local value. Tables whose header holds
//! one, such as a registered index ID, need the snapshot to build their keys.

use std::collections::BTreeSet;
use std::ops::RangeBounds;
use std::sync::Arc;

use anyhow::{Context, ensure};
use bilrost::{Message, OwnedMessage};

use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::{RecordsOf, SchemaBinding, SelectionSchema, Table};
use restate_types::sharding::KeyRange;

use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKey};
use crate::table::{MultiGetCursor, MultiGetTable};
use crate::{ReadSnapshot, Result};

/// A table whose records can be fetched by complete key: every key has the same
/// length, and decodes back to the ID it was built from.
///
/// The engine builds the keys from IDs alone, so the key's header must not hold
/// a store-local value, such as a registered index ID. A key built with the
/// wrong one would fall outside the read's bounds, and the lookup would find no
/// records instead of failing.
pub trait LookupTable:
    MultiGetTable<Id: Send + Sync + 'static, Key: DecodeTableKey + KeySchema>
{
    /// The length of every key of the table, in bytes.
    const KEY_LENGTH: usize;

    /// The ID a key was built from: the inverse of [`MultiGetTable::key`].
    fn id(key: Self::Key) -> Self::Id;
}

/// A binding whose table can be read by complete key instead of scanned.
pub trait LookupBinding: SchemaBinding<Table: LookupTable> {
    /// The IDs one Boolean alternative of a selection can match, as a domain of
    /// the table's IDs. The engine looks the IDs up only when the union over all
    /// alternatives is a finite set ([`Domain::into_values`]); otherwise it scans.
    fn lookup_ids(
        domains: &<Self::Schema as SelectionSchema>::Domains,
    ) -> Domain<<Self::Table as MultiGetTable>::Id>;
}

/// A nonempty set of complete keys of `T`'s records: the request of a lookup.
///
/// Sent as each key's stored bytes, in ascending order. [`Self::decode`] checks
/// exactly what the key codec can check: the key kind, the length, that each key
/// decodes back to the same bytes, and that keys are strictly increasing. It
/// doesn't check what the bytes mean beyond that.
#[derive(derive_more::Debug)]
pub struct LookupKeys<T: LookupTable> {
    #[debug(skip)]
    ids: BTreeSet<T::Id>,
}

#[derive(bilrost::Message)]
struct EncodedKeys {
    #[bilrost(tag(1), encoding(unpacked<plainbytes>))]
    keys: Vec<Vec<u8>>,
}

impl<T: LookupTable> LookupKeys<T> {
    /// Encodes the keys of `ids`, which must be in ascending order without
    /// duplicates.
    pub fn encode<'a>(ids: impl IntoIterator<Item = &'a T::Id>) -> Vec<u8> {
        EncodedKeys {
            keys: ids.into_iter().map(stored_key::<T>).collect(),
        }
        .encode_to_vec()
    }

    /// Decodes `count` keys encoded by [`Self::encode`].
    pub fn decode(count: usize, encoded: &[u8]) -> anyhow::Result<Self> {
        let keys = EncodedKeys::decode(encoded)
            .context("invalid lookup keys")?
            .keys;
        ensure!(
            count > 0 && keys.len() == count,
            "lookup key count mismatch: expected {count}, got {}",
            keys.len()
        );
        ensure!(
            keys.windows(2).all(|pair| pair[0] < pair[1]),
            "lookup keys must be strictly increasing"
        );
        let ids = keys
            .iter()
            .map(|bytes| {
                // Checked before decoding: some ID codecs copy a fixed length and
                // panic on shorter input.
                ensure!(
                    bytes.len() == T::KEY_LENGTH,
                    "lookup key has {} bytes, expected {}",
                    bytes.len(),
                    T::KEY_LENGTH
                );
                let mut remaining = bytes.as_slice();
                let id = T::id(T::Key::deserialize_from(&mut remaining)?);
                ensure!(remaining.is_empty(), "trailing bytes after a lookup key");
                ensure!(
                    stored_key::<T>(&id) == *bytes,
                    "lookup key doesn't encode its own ID"
                );
                Ok(id)
            })
            .collect::<anyhow::Result<BTreeSet<_>>>()?;
        Ok(Self { ids })
    }

    /// How many keys the set has; never zero.
    pub fn count(&self) -> usize {
        self.ids.len()
    }

    /// Whether some key belongs to a partition key outside `range`.
    pub fn outside(&self, range: KeyRange) -> bool {
        self.ids
            .iter()
            .any(|id| !range.contains(&T::partition_key(id)))
    }
}

fn stored_key<T: MultiGetTable>(id: &T::Id) -> Vec<u8> {
    let key = T::key(id);
    let mut bytes = Vec::with_capacity(key.serialized_length());
    key.serialize_to(&mut bytes);
    bytes
}

/// The records of a [`LookupKeys`] request, in key order. Keys without a record
/// yield none. It keeps the read snapshot alive while it reads; reaching the end
/// or dropping the cursor releases its storage.
pub struct LookupCursor<T: LookupTable> {
    inner: Option<MultiGetCursor<T>>,
    /// Whether the last step produced a record.
    positioned: bool,
}

impl<T: LookupTable> CursorScan<RecordsOf<T, LookupKeys<T>>> for ReadSnapshot {
    type Cursor = LookupCursor<T>;

    fn scan_cursor(&self, request: Arc<LookupKeys<T>>) -> Result<Self::Cursor> {
        self.check_active()?;
        let metadata = self.metadata();
        let bounds = if T::is_active(&metadata) {
            let scope = <<T::Key as KeySchema>::Header as KeyHeader>::scope(&metadata)?;
            <<T::Key as KeySchema>::Header as KeyHeader>::bounds(scope)
        } else {
            None
        };
        let inner = bounds.map(|bounds| {
            let ids = request
                .ids
                .iter()
                .filter(|id| bounds.0.contains_key(stored_key::<T>(id)))
                .cloned()
                .collect();
            MultiGetCursor::new(self.clone(), ids)
        });
        Ok(LookupCursor {
            inner,
            positioned: false,
        })
    }
}

impl<T: LookupTable> ReadCursor<RecordsOf<T, LookupKeys<T>>> for LookupCursor<T> {
    fn advance(&mut self) -> Result<CursorStep> {
        self.positioned = false;
        let Some(cursor) = &mut self.inner else {
            return Ok(CursorStep::Done);
        };
        match cursor.advance()? {
            CursorStep::Record => {
                let (key, _) = cursor.current_key()?;
                self.positioned = T::owns_record(
                    <T::Key as KeySchema>::decoder(key)?,
                    &cursor.snapshot().metadata(),
                )?;
                Ok(if self.positioned {
                    CursorStep::Record
                } else {
                    CursorStep::Skipped
                })
            }
            CursorStep::Skipped => Ok(CursorStep::Skipped),
            CursorStep::Done => {
                // Release the snapshot as soon as the keys are exhausted.
                self.inner = None;
                Ok(CursorStep::Done)
            }
        }
    }

    fn current(&self) -> Result<<RecordsOf<T, LookupKeys<T>> as Table>::Record<'_>> {
        let cursor = self
            .inner
            .as_ref()
            .filter(|_| self.positioned)
            .ok_or(restate_storage_api::StorageError::DataIntegrityError)?;
        let (key, value) = cursor.current_key()?;
        Ok((<T::Key as KeySchema>::decoder(key)?, value))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bilrost::Message;
    use tokio_util::sync::CancellationToken;

    use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
    use restate_limiter::LimitKey;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::vqueues::VQueueId;

    use crate::keys::selection::KeySelection;
    use crate::keys::{KeyDecoder, KeyKind};
    use crate::table::{PhysicalTable, ReadMetadata};
    use crate::vqueue_table::{ActiveKey, MetaKey, VQueueMetaTable};
    use crate::{PartitionStore, PartitionStoreManager};

    use super::*;

    /// Lookups of `ids` on `T`: the IDs and values found, and how many steps
    /// the cursor took before it was done.
    fn look_up<T: LookupTable<Key = MetaKey, Id = VQueueId>>(
        snapshot: &ReadSnapshot,
        request: &Arc<LookupKeys<T>>,
    ) -> (Vec<(VQueueId, Vec<u8>)>, usize) {
        let mut cursor = <ReadSnapshot as CursorScan<RecordsOf<T, LookupKeys<T>>>>::scan_cursor(
            snapshot,
            Arc::clone(request),
        )
        .unwrap();
        let (mut found, mut steps) = (Vec::new(), 0);
        loop {
            match cursor.advance().unwrap() {
                CursorStep::Record => {
                    let (key, value) = cursor.current().unwrap();
                    let key: MetaKey = key.decode_all().unwrap();
                    found.push((VQueueId::from(key), value.to_vec()));
                }
                CursorStep::Skipped => {}
                CursorStep::Done => return (found, steps),
            }
            steps += 1;
        }
    }

    fn keys<T: LookupTable<Id = VQueueId>>(ids: &BTreeSet<VQueueId>) -> Arc<LookupKeys<T>> {
        Arc::new(LookupKeys::decode(ids.len(), &LookupKeys::<T>::encode(ids)).unwrap())
    }

    async fn store_with(owner: KeyRange, records: &[(&VQueueId, &[u8])]) -> PartitionStore {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, owner), None)
            .await
            .unwrap();
        let mut tx = store.transaction();
        for (id, value) in records {
            tx.raw_put_cf(
                KeyKind::VQueueMeta,
                stored_key::<VQueueMetaTable>(id),
                *value,
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        store
    }

    fn meta() -> Vec<u8> {
        let created =
            UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000))
                .unwrap();
        VQueueMeta::new(created, None, LimitKey::None, VQueueLink::None).encode_to_vec()
    }

    /// A lookup takes one step per requested key the partition owns, however
    /// many records lie between them, finds the same records a scan finds, and
    /// can be read again from the same request.
    #[restate_core::test]
    async fn lookups_take_one_step_per_owned_key() {
        let owner = KeyRange::new(20, 30);
        let meta = meta();
        let wanted: Vec<_> = [20, 25, 30]
            .map(|key: PartitionKey| VQueueId::custom(key, "wanted"))
            .into();
        let missing = VQueueId::custom(27, "missing");
        // Thousands of records between the wanted ones, and records outside the
        // partition whose invalid values must never be read.
        let filler: Vec<_> = (21..30)
            .flat_map(|key| (0..300).map(move |n| VQueueId::custom(key, format!("q{n}"))))
            .collect();
        let foreign = [
            VQueueId::custom(19, "wanted"),
            VQueueId::custom(31, "wanted"),
        ];
        let mut records: Vec<(&VQueueId, &[u8])> = wanted
            .iter()
            .chain(&filler)
            .map(|id| (id, meta.as_slice()))
            .collect();
        records.extend(foreign.iter().map(|id| (id, &b"\xff"[..])));
        let store = store_with(owner, &records).await;
        let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();

        let ids: BTreeSet<_> = wanted
            .iter()
            .chain([&missing])
            .chain(&foreign)
            .cloned()
            .collect();
        let request = keys::<VQueueMetaTable>(&ids);
        let mut expected: Vec<_> = wanted.iter().map(|id| (id.clone(), meta.clone())).collect();
        expected.sort();
        // Dropping one read halfway doesn't affect the next reads of the request.
        let mut first = <ReadSnapshot as CursorScan<
            RecordsOf<VQueueMetaTable, LookupKeys<VQueueMetaTable>>,
        >>::scan_cursor(&snapshot, Arc::clone(&request))
        .unwrap();
        first.advance().unwrap();
        drop(first);
        for _ in 0..2 {
            // Three records and one missing key; the foreign keys aren't fetched.
            assert_eq!(look_up(&snapshot, &request), (expected.clone(), 4));
        }
        assert!(request.outside(owner));
        let owned: BTreeSet<_> = wanted.iter().cloned().collect();
        assert!(!keys::<VQueueMetaTable>(&owned).outside(owner));

        drop(snapshot);
        drop(store);
        tokio::time::timeout(std::time::Duration::from_secs(5), closed)
            .await
            .expect("finished lookups release the snapshot");
    }

    /// A layout over the metadata keys whose activity and record ownership tests
    /// choose, fetched two keys per multi-get.
    enum Gated<const ACTIVE: bool> {}

    impl<const ACTIVE: bool> Table for Gated<ACTIVE> {
        type Record<'a> = (KeyDecoder<'a, MetaKey>, &'a [u8]);
        type Scan = KeySelection<MetaKey>;
    }

    impl<const ACTIVE: bool> PhysicalTable for Gated<ACTIVE> {
        type Key = MetaKey;

        fn is_active(_: &ReadMetadata<'_>) -> bool {
            ACTIVE
        }

        fn owns_record(key: KeyDecoder<'_, MetaKey>, _: &ReadMetadata<'_>) -> Result<bool> {
            let key: MetaKey = key.decode_all()?;
            Ok(VQueueId::from(key) != VQueueId::custom(25, "disowned"))
        }
    }

    impl<const ACTIVE: bool> MultiGetTable for Gated<ACTIVE> {
        type Id = VQueueId;
        const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(2).unwrap();

        fn partition_key(id: &VQueueId) -> PartitionKey {
            id.partition_key()
        }

        fn key(id: &VQueueId) -> MetaKey {
            MetaKey::from(id)
        }
    }

    impl<const ACTIVE: bool> LookupTable for Gated<ACTIVE> {
        const KEY_LENGTH: usize = MetaKey::serialized_length_fixed();

        fn id(key: MetaKey) -> VQueueId {
            VQueueId::from(key)
        }
    }

    /// Lookups apply a scan's gates: an inactive layout has no records, and a
    /// record the partition doesn't own is skipped. Missing keys and skipped
    /// records across batch boundaries don't end the lookup.
    #[restate_core::test]
    async fn lookups_apply_the_layout_gates_across_batches() {
        let meta = meta();
        let ids: Vec<_> = ["a", "disowned", "b", "c", "d"]
            .map(|name| VQueueId::custom(25, name))
            .into();
        let missing = [VQueueId::custom(25, "m1"), VQueueId::custom(25, "m2")];
        let records: Vec<(&VQueueId, &[u8])> = ids.iter().map(|id| (id, meta.as_slice())).collect();
        let store = store_with(KeyRange::FULL, &records).await;
        let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
        let all: BTreeSet<_> = ids.iter().chain(&missing).cloned().collect();

        assert_eq!(look_up(&snapshot, &keys::<Gated<false>>(&all)), (vec![], 0));
        let (found, steps) = look_up(&snapshot, &keys::<Gated<true>>(&all));
        let mut expected: Vec<_> = ids
            .iter()
            .filter(|id| **id != VQueueId::custom(25, "disowned"))
            .map(|id| (id.clone(), meta.clone()))
            .collect();
        expected.sort();
        assert_eq!((found, steps), (expected, all.len()));
    }

    /// A worker accepts only what an encoder produces: the stated count of
    /// keys of the table's kind and length, strictly increasing. Malformed input
    /// is an error, never a panic.
    #[test]
    fn decoding_rejects_malformed_key_sets() {
        let ids: BTreeSet<_> = (0..3).map(|n| VQueueId::custom(n, "q")).collect();
        let decode = |count, keys: Vec<Vec<u8>>| {
            LookupKeys::<VQueueMetaTable>::decode(count, &EncodedKeys { keys }.encode_to_vec())
                .map(|keys| keys.count())
        };
        let keys: Vec<_> = ids.iter().map(stored_key::<VQueueMetaTable>).collect();
        assert_eq!(decode(3, keys.clone()).unwrap(), 3);

        let mut active = Vec::new();
        ActiveKey {
            qid: ids.first().unwrap().clone(),
        }
        .serialize_to(&mut active);
        let short = keys[0][..keys[0].len() - 1].to_vec();
        let mut long = keys[0].clone();
        long.push(0);
        for (case, count, keys) in [
            ("no keys", 0, vec![]),
            ("count mismatch", 2, keys.clone()),
            (
                "unsorted",
                3,
                vec![keys[1].clone(), keys[0].clone(), keys[2].clone()],
            ),
            ("duplicate", 2, vec![keys[0].clone(), keys[0].clone()]),
            ("another key kind", 1, vec![active]),
            ("short key", 1, vec![short]),
            ("trailing bytes", 1, vec![long]),
        ] {
            assert!(decode(count, keys).is_err(), "{case}");
        }
        assert!(LookupKeys::<VQueueMetaTable>::decode(1, b"\xff\xff\xff").is_err());
    }
}
