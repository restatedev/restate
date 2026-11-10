// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use bytes::{Bytes, BytesMut};
use rocksdb::ReadOptions;

use restate_core::io::IoExecutor;
use restate_rocksdb::reads::IteratorVisitor;
use restate_rocksdb::{IterAction, LeasePolicy};
use restate_storage_api::scan::{RowConsumer, ScanClosePolicy, ScanOutcome};
use restate_storage_api::{Result, StorageError};
use restate_types::sharding::KeyRange;

use crate::keys::EncodeTableKey;
use crate::keys::{EncodeTableKeyPrefix, KeyEncode, KeyKind};
use crate::scan::TableScan::{Prefix, RangeInclusive, ScanPartitionKeyRange};
use crate::{
    PartitionStore, ScanMode, configure_prefix_iterator_opts, configure_range_iterator_opts,
    convert_to_upper_bound,
};

/// One read of a scan, as a value: a key range or a set of keys, and how to
/// decode its entries. Tables define these; run them, alone or chained, with
/// [`PartitionStore::scan`].
pub trait ScanOp: Send + Sync + 'static {
    /// What the consumer gets for each row.
    type Item<'a>: Send;
    /// Labels the read's metrics.
    const NAME: &'static str;

    /// What to read.
    fn plan(&self) -> ScanPlan;

    /// Decodes the entry at `key` and hands it to `consumer`. Rows may borrow
    /// the key, the value, or what's decoded from them.
    fn visit<C>(
        &self,
        key: &[u8],
        value: &[u8],
        consumer: &mut C,
    ) -> impl Future<Output = Result<ControlFlow<()>>>
    where
        C: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>;
}

/// What a [`ScanOp`] reads.
pub enum ScanPlan {
    /// A key range.
    Range(PhysicalScan<Bytes>),
    /// A set of keys, looked up with batched multi-gets instead of a scan.
    Keys(KeySet),
}

/// Fixed-length keys of one kind, in key order, packed back to back, and how
/// many to look up per multi-get.
pub struct KeySet {
    pub(crate) key_kind: KeyKind,
    pub(crate) key_len: usize,
    pub(crate) batch_size: usize,
    pub(crate) keys: Bytes,
}

impl ScanPlan {
    /// A plan to read `scan`.
    #[allow(dead_code)]
    pub(crate) fn range<K: EncodeTableKeyPrefix>(scan: TableScan<K>) -> Self {
        Self::Range(scan.into())
    }

    /// A plan to look up `keys`, `batch_size` at a time. They must be in key
    /// order and all have the fixed length `key_len`.
    // The table scans in the next commit use this.
    #[allow(dead_code)]
    pub(crate) fn keys<K: EncodeTableKey>(
        key_len: usize,
        batch_size: usize,
        keys: impl ExactSizeIterator<Item = K>,
    ) -> Self {
        let mut buf = BytesMut::with_capacity(keys.len() * key_len);
        for key in keys {
            key.serialize_to(&mut buf);
        }
        debug_assert_eq!(
            buf.len() % key_len.max(1),
            0,
            "keys must have length {key_len}"
        );
        debug_assert!(
            buf.chunks_exact(key_len.max(1)).is_sorted(),
            "keys must be in key order"
        );
        Self::Keys(KeySet {
            key_kind: K::KEY_KIND,
            key_len,
            batch_size,
            keys: buf.freeze(),
        })
    }
}

/// Something [`PartitionStore::scan`] can run: a single [`ScanOp`], or several
/// chained one after the other ([`Chain`], or a `Vec` of ops).
pub trait ScanSequence: Send + Sync + 'static {
    /// What the consumer gets for each row.
    type Item<'a>: Send;

    /// Runs the scan on `executor`, handing its rows to `consumer`, with
    /// `close_policy` deciding what closing the partition does to it.
    fn run<'s, C>(
        self,
        store: &'s PartitionStore,
        executor: &'s (impl IoExecutor + Sync),
        close_policy: ScanClosePolicy,
        consumer: C,
    ) -> impl Future<Output = Result<ScanOutcome<C>>> + Send + 's
    where
        C: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>;

    /// Runs this scan, then hands the consumer to `next`, unless this scan
    /// stopped it.
    fn chain<N>(self, next: N) -> Chain<Self, N>
    where
        Self: Sized,
        N: for<'a> ScanSequence<Item<'a> = Self::Item<'a>>,
    {
        Chain(self, next)
    }
}

impl<O: ScanOp> ScanSequence for O {
    type Item<'a> = O::Item<'a>;

    fn run<'s, C>(
        self,
        store: &'s PartitionStore,
        executor: &'s (impl IoExecutor + Sync),
        close_policy: ScanClosePolicy,
        consumer: C,
    ) -> impl Future<Output = Result<ScanOutcome<C>>> + Send + 's
    where
        C: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>,
    {
        store.run_scan_op(executor, self, close_policy, consumer)
    }
}

/// Runs `A`, then hands the consumer to `B`, unless `A` stopped it.
pub struct Chain<A, B>(pub A, pub B);

impl<A, B> ScanSequence for Chain<A, B>
where
    A: ScanSequence,
    B: for<'a> ScanSequence<Item<'a> = A::Item<'a>>,
{
    type Item<'a> = A::Item<'a>;

    fn run<'s, C>(
        self,
        store: &'s PartitionStore,
        executor: &'s (impl IoExecutor + Sync),
        close_policy: ScanClosePolicy,
        consumer: C,
    ) -> impl Future<Output = Result<ScanOutcome<C>>> + Send + 's
    where
        C: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>,
    {
        let Chain(first, second) = self;
        async move {
            match first.run(store, executor, close_policy, consumer).await? {
                ScanOutcome::Exhausted(consumer) => {
                    second.run(store, executor, close_policy, consumer).await
                }
                stopped @ ScanOutcome::Stopped(_) => Ok(stopped),
            }
        }
    }
}

/// Runs the scan if there is one. Lets a scan leave out a read with nothing to
/// read, e.g. a legacy table that was migrated away.
impl<S: ScanSequence> ScanSequence for Option<S> {
    type Item<'a> = S::Item<'a>;

    async fn run<'s, C>(
        self,
        store: &'s PartitionStore,
        executor: &'s (impl IoExecutor + Sync),
        close_policy: ScanClosePolicy,
        consumer: C,
    ) -> Result<ScanOutcome<C>>
    where
        C: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>,
    {
        match self {
            Some(scan) => scan.run(store, executor, close_policy, consumer).await,
            None => Ok(ScanOutcome::Exhausted(consumer)),
        }
    }
}

/// Runs the ops one after the other, handing the consumer along.
impl<O: ScanOp> ScanSequence for Vec<O> {
    type Item<'a> = O::Item<'a>;

    async fn run<'s, C>(
        self,
        store: &'s PartitionStore,
        executor: &'s (impl IoExecutor + Sync),
        close_policy: ScanClosePolicy,
        mut consumer: C,
    ) -> Result<ScanOutcome<C>>
    where
        C: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>,
    {
        for op in self {
            match store
                .run_scan_op(executor, op, close_policy, consumer)
                .await?
            {
                ScanOutcome::Exhausted(c) => consumer = c,
                stopped @ ScanOutcome::Stopped(_) => return Ok(stopped),
            }
        }
        Ok(ScanOutcome::Exhausted(consumer))
    }
}

#[derive(Debug)]
pub enum TableScan<K> {
    /// Scan an inclusive partition key-range.
    ScanPartitionKeyRange(KeyRange),
    /// Scan within the same prefix.
    Prefix(K),
    /// Inclusive key range.
    RangeInclusive(K, K),
}

impl<K: EncodeTableKeyPrefix> TableScan<K> {
    pub(crate) fn encode(self, arena: &mut BytesMut) -> PhysicalScan<Bytes> {
        match self {
            Prefix(key) => {
                key.serialize_to(arena);
                PhysicalScan::Prefix(K::KEY_KIND, arena.split().freeze())
            }
            RangeInclusive(start, end) => {
                arena.reserve(start.serialized_length() + end.serialized_length());
                start.serialize_to(arena);
                let start = arena.split().freeze();
                end.serialize_to(arena);
                let mut end = arena.split();
                if start == end {
                    return PhysicalScan::Prefix(K::KEY_KIND, start);
                }

                if !convert_to_upper_bound(&mut end) {
                    // Not allowed to happen since we guarantee that KeyKind is
                    // always incrementable.
                    std::hint::cold_path();
                    panic!("Key range end overflowed, start key {:x?}", start);
                }
                let end = end.freeze();
                // RocksDB requires the exclusive upper bound to share the seek prefix when
                // total-order seek is disabled.
                let scan_mode = ScanMode::from_range(&start, &end);

                PhysicalScan::RangeExclusive(K::KEY_KIND, scan_mode, start, end)
            }
            ScanPartitionKeyRange(range) => {
                let start = range.start();
                let end = range.end();
                // A single partition key can use a prefix scan over the start key.
                if start == end {
                    arena.reserve(start.serialized_length() + KeyKind::SERIALIZED_LENGTH);
                    K::serialize_key_kind(arena);
                    start.encode(arena);
                    return PhysicalScan::Prefix(K::KEY_KIND, arena.split().freeze());
                }

                arena.reserve(2 * (start.serialized_length() + KeyKind::SERIALIZED_LENGTH));
                K::serialize_key_kind(arena);
                start.encode(arena);
                let start_bytes = arena.split().freeze();

                K::serialize_key_kind(arena);
                end.encode(arena);
                let mut end_bytes = arena.split();
                if !convert_to_upper_bound(&mut end_bytes) {
                    // not allowed to happen since we guarantee that KeyKind is
                    // always incrementable.
                    std::hint::cold_path();
                    panic!("Key range end overflowed, start key {:x?}", start);
                }
                let end_bytes = end_bytes.freeze();
                PhysicalScan::RangeExclusive(
                    K::KEY_KIND,
                    ScanMode::TotalOrder,
                    start_bytes,
                    end_bytes,
                )
            }
        }
    }
}

/// A scan over encoded keys of one key kind. `B` holds the encoded bytes; it can
/// be borrowed, because iterators copy what they need.
pub enum PhysicalScan<B> {
    /// All keys that start with the given bytes.
    Prefix(KeyKind, B),
    /// All keys from the start (inclusive) to the end (exclusive).
    RangeExclusive(KeyKind, ScanMode, B, B),
}

impl<B: AsRef<[u8]>> PhysicalScan<B> {
    /// The kind of keys this scan reads, which also decides its column family.
    pub(crate) fn key_kind(&self) -> KeyKind {
        match self {
            Self::Prefix(key_kind, _) | Self::RangeExclusive(key_kind, ..) => *key_kind,
        }
    }

    /// The key an iterator for this scan seeks to first.
    pub(crate) fn start(&self) -> &B {
        match self {
            Self::Prefix(_, prefix) => prefix,
            Self::RangeExclusive(_, _, start, _) => start,
        }
    }

    /// Like [`Self::start`], but takes the scan apart.
    pub(crate) fn into_start(self) -> B {
        match self {
            Self::Prefix(_, prefix) => prefix,
            Self::RangeExclusive(_, _, start, _) => start,
        }
    }

    /// Sets the iteration bounds this scan needs on `opts`. It also decides whether
    /// seeks can use the column family's prefix extractor or must use total order,
    /// replacing whatever `opts` said about either.
    pub(crate) fn configure(&self, opts: &mut ReadOptions) {
        match self {
            Self::Prefix(_, prefix) => configure_prefix_iterator_opts(opts, prefix.as_ref()),
            Self::RangeExclusive(_, scan_mode, start, end) => {
                configure_range_iterator_opts(opts, *scan_mode, start.as_ref(), end.as_ref())
            }
        }
    }
}

impl<K: EncodeTableKeyPrefix> From<TableScan<K>> for PhysicalScan<Bytes> {
    fn from(scan: TableScan<K>) -> Self {
        let mut arena = BytesMut::new();
        scan.encode(&mut arena)
    }
}

/// The lease policy that implements a scan's close policy.
pub(crate) fn lease_policy(close_policy: ScanClosePolicy) -> LeasePolicy {
    match close_policy {
        ScanClosePolicy::CancelOnClose => LeasePolicy::StopOnSeal,
        ScanClosePolicy::FinishBeforeClose => LeasePolicy::WholeOperation,
    }
}

/// Decodes the physical rows of `op` for its typed row consumer.
pub(crate) struct DecodingVisitor<O, C> {
    pub op: O,
    pub consumer: C,
}

impl<O, C> IteratorVisitor for DecodingVisitor<O, C>
where
    O: ScanOp,
    C: for<'a> RowConsumer<Item<'a> = O::Item<'a>>,
{
    type Error = StorageError;

    async fn visit(&mut self, key: &[u8], value: &[u8]) -> Result<IterAction> {
        Ok(match self.op.visit(key, value, &mut self.consumer).await? {
            ControlFlow::Continue(()) => IterAction::Next,
            ControlFlow::Break(()) => IterAction::Stop,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Add;

    use bytes::{BufMut, Bytes, BytesMut};
    use num_bigint::BigUint;

    use restate_types::sharding::KeyRange;

    use crate::keys::{EncodeTableKey, KeyKind};
    use crate::scan::{PhysicalScan, TableScan};
    use crate::{DB_PREFIX_LENGTH, ScanMode, TableKind, convert_to_upper_bound};

    /// Same layout as invocation status keys: partition key and a 16 bytes id
    struct TestKey(u64, u128);

    impl EncodeTableKey for TestKey {
        const TABLE: TableKind = TableKind::InvocationStatus;
        const KEY_KIND: KeyKind = KeyKind::InvocationStatus;

        fn serialize_to<B: BufMut>(&self, bytes: &mut B) {
            Self::KEY_KIND.serialize(bytes);
            bytes.put_u64(self.0);
            bytes.put_u128(self.1);
        }

        fn serialized_length(&self) -> usize {
            KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<u64>() + std::mem::size_of::<u128>()
        }
    }

    fn scan_mode(scan: TableScan<TestKey>) -> ScanMode {
        let PhysicalScan::RangeExclusive(_, scan_mode, _, _) = scan.into() else {
            panic!("expected range scan");
        };
        scan_mode
    }

    fn verify_binary_increment(bytes: &mut BytesMut) {
        let as_number = BigUint::from_bytes_be(bytes);
        let expected_successor = as_number.add(1u64);

        assert!(convert_to_upper_bound(bytes));
        let got_successor = BigUint::from_bytes_be(bytes);

        assert_eq!(got_successor, expected_successor);
    }

    #[test]
    fn simple_increment() {
        let mut bytes = BytesMut::new();
        for i in 0..1024 {
            bytes.clear();
            bytes.put_u64(i);
            verify_binary_increment(&mut bytes);
        }
    }

    fn verify_partition_covers_exactly(partition_id: u64) {
        let next_partition_id = partition_id + 1;
        let mut db = BTreeMap::new();
        let keys_to_insert = 10;

        // add few keys from the current partition id
        for i in 0..keys_to_insert {
            let mut key = BytesMut::new();
            key.put_u64(partition_id);
            key.put_u64(i);

            db.insert(key, "partition-1");
        }
        // add few keys from the next partition id
        for i in 0..2 {
            let mut key = BytesMut::new();
            key.put_u64(next_partition_id);
            key.put_u64(i);

            db.insert(key, "partition-2");
        }

        // compute bounds
        let mut lower_bound = BytesMut::new();
        lower_bound.put_u64(partition_id);
        lower_bound.put_u64(0);

        let mut upper_bound_inclusive = BytesMut::new();
        upper_bound_inclusive.put_u64(partition_id);
        upper_bound_inclusive.put_u64(u64::MAX);

        assert!(convert_to_upper_bound(&mut upper_bound_inclusive));

        let mut seen_values = 0;
        for (_, &value) in db.range(lower_bound..upper_bound_inclusive) {
            assert_eq!(value, "partition-1");
            seen_values += 1;
        }

        assert_eq!(seen_values, keys_to_insert);
    }

    #[test]
    fn scan_stays_within_partition_bounds() {
        verify_partition_covers_exactly(0);
        verify_partition_covers_exactly(255);
        verify_partition_covers_exactly(256);
        verify_partition_covers_exactly(1024);
        verify_partition_covers_exactly(32 * 1024);
        verify_partition_covers_exactly(u32::MAX as u64);
        verify_partition_covers_exactly(u64::MAX - 1);
    }

    #[test]
    fn inclusive_key_ranges_crossing_prefix_use_total_order() {
        assert_eq!(
            scan_mode(TableScan::RangeInclusive(TestKey(1, 0), TestKey(2, 0))),
            ScanMode::TotalOrder
        );
        assert_eq!(
            scan_mode(TableScan::RangeInclusive(
                TestKey(1, 0),
                TestKey(1, u128::MAX),
            )),
            ScanMode::TotalOrder
        );
        // Same partition key, but the ids differ within the prefix extractor length
        assert_eq!(
            scan_mode(TableScan::RangeInclusive(
                TestKey(1, 0),
                TestKey(1, 1 << 64)
            )),
            ScanMode::TotalOrder
        );
    }

    #[test]
    fn inclusive_key_range_sharing_extractor_prefix_stays_within_prefix() {
        assert_eq!(
            scan_mode(TableScan::RangeInclusive(TestKey(1, 0), TestKey(1, 9))),
            ScanMode::WithinPrefix
        );
    }

    #[test]
    fn equal_inclusive_key_range_uses_prefix_scan() {
        let scan = TableScan::RangeInclusive(TestKey(1, 9), TestKey(1, 9));
        let physical_scan: PhysicalScan<Bytes> = scan.into();

        let PhysicalScan::Prefix(_, prefix) = physical_scan else {
            panic!("expected prefix scan");
        };

        let mut expected_prefix = BytesMut::new();
        TestKey(1, 9).serialize_to(&mut expected_prefix);
        assert_eq!(prefix, expected_prefix);
    }

    #[test]
    fn full_scan_ranges_use_prefix_or_total_order_as_appropriate() {
        let singleton_scan = TableScan::<TestKey>::ScanPartitionKeyRange(KeyRange::new(42, 42));
        let singleton_physical_scan: PhysicalScan<Bytes> = singleton_scan.into();

        let PhysicalScan::Prefix(_, singleton_prefix) = singleton_physical_scan else {
            panic!("expected prefix scan");
        };

        let mut expected_prefix = BytesMut::from(&KeyKind::InvocationStatus.as_bytes()[..]);
        expected_prefix.put_u64(42);
        assert_eq!(singleton_prefix, expected_prefix);

        let scan = TableScan::<TestKey>::ScanPartitionKeyRange(KeyRange::FULL);
        let physical_scan: PhysicalScan<Bytes> = scan.into();

        let PhysicalScan::RangeExclusive(_, scan_mode, _, end) = physical_scan else {
            panic!("expected range scan");
        };

        let mut expected_end =
            BytesMut::from(&KeyKind::InvocationStatus.exclusive_upper_bound()[..]);
        expected_end.resize(DB_PREFIX_LENGTH, 0);

        assert_eq!(scan_mode, ScanMode::TotalOrder);
        assert_eq!(end, expected_end);
    }

    #[test]
    fn binary_increment_suffix() {
        let mut bytes = BytesMut::new();
        bytes.put_u64(257);
        bytes.put_u64(u64::MAX);
        bytes.put_u64(u64::MAX);
        bytes.put_u64(u64::MAX);
        bytes.put_u64(u64::MAX);

        verify_binary_increment(&mut bytes);
    }
}
