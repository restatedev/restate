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
//! pins finitely many of them, the engine prepares their stored keys once
//! ([`LookupCandidates`]) and sends each partition its share ([`LookupKeys`]),
//! which it fetches exactly, a batch of [`LookupTable::BATCH_SIZE`] keys per
//! multi-get. The binding turns the fetched records into rows exactly as it does
//! for a scan, and the query's predicate still applies to them.
//!
//! ```text
//! WHERE id IN ('q1', 'q2')  ──lookup_ids──▶ {q1, q2}  ──prepare──▶ stored keys, byte order
//!                                                         │ within(partition)
//! LookupKeys::decode (check_key each) ◀─────────────────────┘
//!   └─ scan_cursor(snapshot) ─▶ LookupCursor: multi-get q1, q2 ─▶ (key decoder, value)
//! ```
//!
//! Keys travel as stored bytes, and a partition checks each one it receives
//! ([`LookupTable::check_key`]) before anything decodes it: stored-key decoders
//! may assume valid UTF-8 or lengths because storage wrote the bytes itself.
//!
//! A lookup applies the same gates as a scan of the table: nothing on an
//! inactive layout ([`is_active`](crate::table::PhysicalTable::is_active)), only keys inside the read's
//! scope (the header's bounds), and only records the partition owns
//! ([`owns_record`](crate::table::PhysicalTable::owns_record)). Keys are built by the engine, so this suits
//! tables whose header needs no store-local value. Tables whose header holds
//! one, such as a registered index ID, need the snapshot to build their keys.

use std::ops::RangeBounds;
use std::sync::Arc;

use anyhow::{Context, ensure};
use bilrost::{Message, OwnedMessage};
use bytes::{Bytes, BytesMut};

use restate_storage_api::StorageError;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::{RecordsOf, SchemaBinding, SelectionSchema, Table};
use restate_types::Scope;
use restate_types::identifiers::{PartitionKey, ServiceId};
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use crate::keys::codecs::DelimitedBytesCodec;
use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::{EncodeTableKey, FieldCodec, KeyKind};
use crate::table::{BatchKey, KeyBatchCursor, PhysicalTable, WithSnapshot};
use crate::{ReadSnapshot, Result};

/// A table whose records can be fetched by complete key.
///
/// Keys travel between nodes as their stored bytes, so a table states how to
/// check bytes it receives ([`Self::check_key`]) without trusting them: the
/// decoders of stored keys may assume the bytes are a key this store wrote.
///
/// The engine builds the keys from IDs alone, so the key's header must not hold
/// a store-local value, such as a registered index ID. A key built with the
/// wrong one would fall outside the read's bounds, and the lookup would find no
/// records instead of failing.
pub trait LookupTable: PhysicalTable<Key: EncodeTableKey> {
    /// How a query names a record, such as a queue's ID.
    type Id: Clone + Ord + Send + Sync + 'static;

    /// Keys per multi-get; smaller for tables with large values.
    const BATCH_SIZE: std::num::NonZeroUsize;

    /// The stored key of the record `id` names, or `None` if `id` can't name one,
    /// such as a scope that isn't valid. A lookup with such an ID scans instead.
    fn key(id: &Self::Id) -> Option<Self::Key>;

    /// Checks bytes received as a stored key, without trusting them: every field
    /// is validated before anything decodes it. Returns the key's partition key
    /// only if the bytes are exactly the canonical stored key of a valid record
    /// identity, stored under that partition key.
    fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey>;
}

/// A binding's table whose records a lookup fetches by the keys of [`Self::Keys`]:
/// a [`LookupTable`] itself, or its records lent with their snapshot.
pub trait LookupSource: Table {
    type Keys: LookupTable;
}

impl<T: LookupTable> LookupSource for T {
    type Keys = T;
}

impl<T: LookupTable> LookupSource for WithSnapshot<T> {
    type Keys = T;
}

/// The keys a lookup of binding `B` fetches.
pub type LookupKeysOf<B> = LookupKeys<<<B as SchemaBinding>::Table as LookupSource>::Keys>;

/// A binding whose table can be read by complete key instead of scanned.
pub trait LookupBinding: SchemaBinding<Table: LookupSource> {
    /// The IDs one Boolean alternative of a selection can match, as a domain of
    /// the table's IDs. The engine looks the IDs up only when the union over all
    /// alternatives is a finite set ([`Domain::into_values`]) and every ID names a
    /// stored key; otherwise it scans. For a key of several columns this is a
    /// candidate cover: the query's predicate still applies to every record.
    fn lookup_ids(
        domains: &<Self::Schema as SelectionSchema>::Domains,
    ) -> Domain<<<Self::Table as LookupSource>::Keys as LookupTable>::Id>;
}

/// A stored key, with the partition key it's stored under.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StoredKey {
    bytes: Bytes,
    partition_key: PartitionKey,
}

impl BatchKey for StoredKey {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }

    fn encode(&self, target: &mut BytesMut) {
        target.extend_from_slice(&self.bytes);
    }
}

#[derive(bilrost::Message)]
struct EncodedKeys {
    #[bilrost(tag(1), encoding(unpacked))]
    keys: Vec<Bytes>,
}

/// The stored keys a query's IDs name, prepared once when the query is planned:
/// in stored-byte order, without duplicates. Each partition is sent its share
/// ([`Self::within`]).
#[derive(derive_more::Debug)]
#[debug("LookupCandidates({})", keys.len())]
pub struct LookupCandidates<T: LookupTable> {
    keys: Vec<StoredKey>,
    _table: std::marker::PhantomData<fn() -> T>,
}

impl<T: LookupTable> LookupCandidates<T> {
    /// The stored keys of `ids`, or `None` if some ID names none, so that the
    /// query scans instead. No IDs give no keys: nothing is read.
    pub fn prepare<'a>(ids: impl IntoIterator<Item = &'a T::Id>) -> Option<Self> {
        let mut keys = ids
            .into_iter()
            .map(|id| {
                let key = T::key(id)?;
                let mut bytes = Vec::with_capacity(key.serialized_length());
                key.serialize_to(&mut bytes);
                // The partition comes from the same check the workers apply.
                let partition_key = T::check_key(&bytes).ok()?;
                Some(StoredKey {
                    bytes: Bytes::from(bytes),
                    partition_key,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        keys.sort_unstable_by(|left, right| left.bytes.cmp(&right.bytes));
        keys.dedup_by(|left, right| left.bytes == right.bytes);
        Some(Self {
            keys,
            _table: std::marker::PhantomData,
        })
    }

    /// The number and encoding of the keys stored under a partition key within
    /// `range`, if any.
    pub fn within(&self, range: KeyRange) -> Option<(usize, Vec<u8>)> {
        let keys: Vec<_> = self
            .keys
            .iter()
            .filter(|key| range.contains(&key.partition_key))
            .map(|key| key.bytes.clone())
            .collect();
        (!keys.is_empty()).then(|| (keys.len(), EncodedKeys { keys }.encode_to_vec()))
    }
}

/// Encodes `keys` as a lookup request as given, without checking or sorting
/// them: for tests that send malformed requests.
#[cfg(any(test, feature = "test-util"))]
pub fn encode_unchecked(keys: Vec<Vec<u8>>) -> Vec<u8> {
    EncodedKeys {
        keys: keys.into_iter().map(Bytes::from).collect(),
    }
    .encode_to_vec()
}

/// A nonempty set of stored keys of `T`, received by a partition: the request of
/// a lookup. [`Self::decode`] accepts exactly what [`LookupCandidates::within`]
/// produces, checking each key with [`LookupTable::check_key`].
#[derive(derive_more::Debug)]
#[debug("LookupKeys({})", keys.len())]
pub struct LookupKeys<T: LookupTable> {
    keys: Vec<StoredKey>,
    _table: std::marker::PhantomData<fn() -> T>,
}

impl<T: LookupTable> LookupKeys<T> {
    /// Decodes `count` keys in strictly increasing stored-byte order.
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
        let keys = keys
            .into_iter()
            .map(|bytes| {
                let partition_key = T::check_key(&bytes)?;
                Ok(StoredKey {
                    bytes,
                    partition_key,
                })
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Self {
            keys,
            _table: std::marker::PhantomData,
        })
    }

    /// How many keys the set has; never zero.
    pub fn count(&self) -> usize {
        self.keys.len()
    }

    /// Whether some key is stored under a partition key outside `range`.
    pub fn outside(&self, range: KeyRange) -> bool {
        self.keys
            .iter()
            .any(|key| !range.contains(&key.partition_key))
    }
}

/// The records of a [`LookupKeys`] request, in stored-key order, a batch of
/// [`LookupTable::BATCH_SIZE`] keys per multi-get. Keys without a record yield
/// none. It keeps the read snapshot alive while it reads; reaching the end or
/// dropping the cursor releases its storage.
pub struct LookupCursor<T: LookupTable> {
    inner: Option<KeyBatchCursor<StoredKey, std::vec::IntoIter<StoredKey>>>,
    /// Whether the last step produced a record.
    positioned: bool,
    _table: std::marker::PhantomData<fn() -> T>,
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
            let keys: Vec<_> = request
                .keys
                .iter()
                .filter(|key| bounds.0.contains_key(&key.bytes))
                .cloned()
                .collect();
            KeyBatchCursor::new(
                self.clone(),
                keys.into_iter(),
                T::BATCH_SIZE,
                T::Key::KEY_KIND,
            )
        });
        Ok(LookupCursor {
            inner,
            positioned: false,
            _table: std::marker::PhantomData,
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
                let (_, key, _) = cursor.current()?;
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
            .ok_or(StorageError::DataIntegrityError)?;
        let (_, key, value) = cursor.current()?;
        Ok((<T::Key as KeySchema>::decoder(key)?, value))
    }
}

/// The object a lookup ID names, such as the owner of a state key, if `scope` is
/// valid. Its partition key follows the same rule as writes.
pub(crate) fn object_id(
    scope: &Option<ReString>,
    service_name: &str,
    service_key: &str,
) -> Option<ServiceId> {
    let scope = scope
        .as_deref()
        .map(Scope::try_non_interned)
        .transpose()
        .ok()?;
    Some(ServiceId::new(scope, service_name, service_key))
}

/// Reads a delimited field of a received key as UTF-8, checking its bounds.
pub(crate) fn utf8_field(rest: &mut &[u8], field: &str) -> anyhow::Result<ReString> {
    let encoded = DelimitedBytesCodec::take_encoded(rest).with_context(|| format!("no {field}"))?;
    let bytes = DelimitedBytesCodec::decode_encoded(encoded)?;
    Ok(ReString::from(
        std::str::from_utf8(&bytes).with_context(|| format!("{field} isn't UTF-8"))?,
    ))
}

/// Requires `bytes` to be exactly the stored bytes of `key`, which a received
/// identity rebuilt, stored under the partition key `stored`.
pub(crate) fn ensure_canonical(
    bytes: &[u8],
    stored: [u8; 8],
    key: &impl EncodeTableKey,
) -> anyhow::Result<()> {
    let mut canonical = Vec::with_capacity(bytes.len());
    key.serialize_to(&mut canonical);
    ensure!(
        canonical[KeyKind::SERIALIZED_LENGTH..][..8] == stored,
        "key stored under another partition key"
    );
    ensure!(canonical == bytes, "key isn't canonical");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;

    use bilrost::Message;
    use bytes::Bytes;
    use futures::FutureExt;
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
    use crate::keys::{FixedLengthTableKey, KeyDecoder, KeyKind};
    use crate::table::{MultiGetCursor, MultiGetTable, PhysicalTable, ReadMetadata};
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

    /// The request a partition owning every key receives for `ids`.
    fn keys<T: LookupTable>(ids: &BTreeSet<T::Id>) -> Arc<LookupKeys<T>> {
        let (count, encoded) = LookupCandidates::<T>::prepare(ids)
            .unwrap()
            .within(KeyRange::FULL)
            .unwrap();
        Arc::new(LookupKeys::decode(count, &encoded).unwrap())
    }

    fn meta_key(id: &VQueueId) -> Vec<u8> {
        MetaKey::from(id).to_bytes().to_vec()
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
            tx.raw_put_cf(KeyKind::VQueueMeta, meta_key(id), *value);
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
    /// choose, fetched two keys per multi-get, by lookup or by ID set.
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
        type Id = VQueueId;
        const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(2).unwrap();

        fn key(id: &VQueueId) -> Option<MetaKey> {
            Some(MetaKey::from(id))
        }

        fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
            VQueueMetaTable::check_key(bytes)
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
            LookupKeys::<VQueueMetaTable>::decode(count, &encode_unchecked(keys))
                .map(|keys| keys.count())
        };
        let keys: Vec<_> = ids.iter().map(meta_key).collect();
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

    // A key of variable length: a partition key and a length-prefixed name, so
    // names of different lengths sort differently as IDs and as stored bytes.
    crate::keys::define_table_key!(
        crate::TableKind::State,
        KeyKind::State,
        VarKey(partition_key: PartitionKey, name: Bytes)
    );

    impl KeySchema for VarKey {
        type Header = crate::keys::header::PartitionKeyHeader<Self>;

        fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
            Ok(KeyDecoder::from_payload(key))
        }
    }

    impl crate::keys::KeyDecode for VarKey {
        fn decode(source: &mut &[u8]) -> Result<Self> {
            <Self as crate::keys::DecodeTableKey>::deserialize_from(source)
        }
    }

    /// A table of [`VarKey`]s. For the fallback test, the name `none` names no
    /// key and the name `bad` fails [`LookupTable::check_key`].
    enum VarTable {}

    impl Table for VarTable {
        type Record<'a> = (KeyDecoder<'a, VarKey>, &'a [u8]);
        type Scan = KeySelection<VarKey>;
    }

    impl PhysicalTable for VarTable {
        type Key = VarKey;
    }

    impl LookupTable for VarTable {
        type Id = (PartitionKey, Vec<u8>);
        const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(2).unwrap();

        fn key((partition_key, name): &Self::Id) -> Option<VarKey> {
            if name == b"none" {
                return None;
            }
            Some(VarKey {
                partition_key: *partition_key,
                name: Bytes::copy_from_slice(name),
            })
        }

        /// Checked parse: kind, partition key, varint length, exact remainder,
        /// then canonical re-encoding.
        fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
            let header = KeyKind::SERIALIZED_LENGTH + size_of::<PartitionKey>();
            ensure!(bytes.len() >= header, "short key");
            ensure!(
                bytes[..KeyKind::SERIALIZED_LENGTH] == *KeyKind::State.as_bytes(),
                "another key kind"
            );
            let partition_key =
                PartitionKey::from_be_bytes(bytes[KeyKind::SERIALIZED_LENGTH..header].try_into()?);
            let mut rest = &bytes[header..];
            let len = prost::encoding::decode_varint(&mut rest)?;
            ensure!(u64::try_from(rest.len())? == len, "name length mismatch");
            ensure!(rest != b"bad", "a name the check rejects");
            let mut canonical = Vec::with_capacity(bytes.len());
            Self::key(&(partition_key, rest.to_vec()))
                .unwrap()
                .serialize_to(&mut canonical);
            ensure!(canonical == bytes, "not a canonical key");
            Ok(partition_key)
        }
    }

    fn var_key(name: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        VarTable::key(&(7, name.as_bytes().to_vec()))
            .unwrap()
            .serialize_to(&mut bytes);
        bytes
    }

    /// Keys of different lengths are fetched in stored-byte order, two per
    /// multi-get: each record stays paired with its own key across batches, and
    /// missing keys yield none. The snapshot is released when the lookup
    /// finishes, or when an unfinished or cancelled lookup is dropped. A request
    /// in ID order instead of byte order, and malformed keys, are rejected
    /// without panicking.
    #[restate_core::test]
    async fn variable_length_lookups_keep_byte_order_across_batches() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let names = ["aa", "b", "ccc", "d", "eeee"];
        let mut tx = store.transaction();
        for name in names {
            tx.raw_put_cf(KeyKind::State, var_key(name), name.as_bytes());
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();

        let ids: BTreeSet<_> = names
            .iter()
            .chain(&["zz"])
            .map(|name| (7, name.as_bytes().to_vec()))
            .collect();
        let request = keys::<VarTable>(&ids);
        let mut cursor =
            <ReadSnapshot as CursorScan<RecordsOf<VarTable, LookupKeys<VarTable>>>>::scan_cursor(
                &snapshot,
                Arc::clone(&request),
            )
            .unwrap();
        let (mut found, mut steps) = (Vec::new(), 0);
        loop {
            match cursor.advance().unwrap() {
                CursorStep::Record => {
                    let (key, value) = cursor.current().unwrap();
                    let key: VarKey = key.decode_all().unwrap();
                    assert_eq!(key.name, value, "a record stays with its key");
                    found.push(String::from_utf8(value.to_vec()).unwrap());
                }
                CursorStep::Skipped => {}
                CursorStep::Done => break,
            }
            steps += 1;
        }
        // Shorter names first: the length prefix leads the stored bytes.
        assert_eq!(found, ["b", "d", "aa", "ccc", "eeee"]);
        assert_eq!(steps, 6, "five records and one missing key");
        // The finished cursor, still alive, no longer holds the snapshot.
        drop(snapshot);
        assert!(
            closed.now_or_never().is_some(),
            "a finished lookup releases its snapshot"
        );
        drop(cursor);

        // Unfinished lookups, past the first batch: one dropped, one cancelled.
        // Each holds its snapshot until the cursor is dropped.
        for cancelled in [false, true] {
            let cancel = CancellationToken::new();
            let (snapshot, closed) = store.query_read(cancel.clone()).unwrap();
            let mut closed = std::pin::pin!(closed);
            let mut cursor = <ReadSnapshot as CursorScan<
                RecordsOf<VarTable, LookupKeys<VarTable>>,
            >>::scan_cursor(&snapshot, Arc::clone(&request))
            .unwrap();
            drop(snapshot);
            for name in ["b", "d", "aa"] {
                assert_eq!(cursor.advance().unwrap(), CursorStep::Record);
                assert_eq!(cursor.current().unwrap().1, name.as_bytes());
            }
            if cancelled {
                cancel.cancel();
                assert!(cursor.advance().is_err(), "a cancelled lookup stops");
            }
            assert!(
                closed.as_mut().now_or_never().is_none(),
                "an unfinished lookup holds its snapshot (cancelled: {cancelled})"
            );
            drop(cursor);
            assert!(
                closed.as_mut().now_or_never().is_some(),
                "dropping it releases the snapshot (cancelled: {cancelled})"
            );
        }

        let decode = |keys: Vec<Vec<u8>>| {
            LookupKeys::<VarTable>::decode(keys.len(), &encode_unchecked(keys))
        };
        assert!(decode(vec![var_key("b"), var_key("aa")]).is_ok());
        let mut long = var_key("b");
        long.push(b'x');
        let mut wrong_kind = var_key("b");
        wrong_kind[..KeyKind::SERIALIZED_LENGTH].copy_from_slice(KeyKind::VQueueMeta.as_bytes());
        let mut overflow = var_key("")[..10].to_vec();
        overflow.extend_from_slice(&[0xff; 11]);
        for (case, keys) in [
            (
                "ID order is not byte order",
                vec![var_key("aa"), var_key("b")],
            ),
            ("duplicate", vec![var_key("b"), var_key("b")]),
            ("short", vec![var_key("b")[..5].to_vec()]),
            ("truncated name", vec![var_key("ccc")[..12].to_vec()]),
            ("trailing byte", vec![long]),
            ("another key kind", vec![wrong_kind]),
            ("invalid length", vec![overflow]),
        ] {
            assert!(decode(keys).is_err(), "{case}");
        }
    }

    /// One ID that names no key, or whose key fails the workers' check, fails
    /// the whole preparation, so the query scans instead of dropping that ID. No
    /// IDs prepare a lookup that sends nothing.
    #[test]
    fn preparation_fails_as_a_whole() {
        let id = |name: &str| (7, name.as_bytes().to_vec());
        let prepare = |ids: &[(PartitionKey, Vec<u8>)]| LookupCandidates::<VarTable>::prepare(ids);
        let valid = prepare(&[id("a"), id("bb")]).unwrap();
        assert_eq!(valid.within(KeyRange::FULL).unwrap().0, 2);
        assert!(
            prepare(&[id("a"), id("none")]).is_none(),
            "an ID without a key"
        );
        assert!(
            prepare(&[id("a"), id("bad")]).is_none(),
            "a key the check rejects"
        );
        assert!(prepare(&[]).unwrap().within(KeyRange::FULL).is_none());
    }

    /// The ID-set cursor reads batches of keys: a batch whose keys all lie
    /// outside the partition is one skipped step, not the end, and the owned
    /// record after two such batches comes with its own ID.
    #[restate_core::test]
    async fn id_batches_outside_the_partition_are_skipped_in_bounded_steps() {
        let meta = meta();
        let owned = VQueueId::custom(25, "owned");
        let store = store_with(KeyRange::new(20, 30), &[(&owned, meta.as_slice())]).await;
        let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
        let ids: BTreeSet<_> = (0..4)
            .map(|n| VQueueId::custom(5, format!("f{n}")))
            .chain([owned.clone()])
            .collect();
        let mut cursor = MultiGetCursor::<Gated<true>>::new(snapshot, ids);
        let steps: Vec<_> = std::iter::from_fn(|| {
            let step = cursor.advance().unwrap();
            if step == CursorStep::Record {
                assert_eq!(cursor.current().unwrap(), (owned.clone(), meta.as_slice()));
            }
            (step != CursorStep::Done).then_some(step)
        })
        .collect();
        assert_eq!(
            steps,
            [CursorStep::Skipped, CursorStep::Skipped, CursorStep::Record]
        );
    }
}
