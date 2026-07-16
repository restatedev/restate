// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Bytes, BytesMut};

use restate_types::sharding::KeyRange;

use crate::keys::{EncodeTableKeyPrefix, KeyEncode, KeyKind};
use crate::scan::TableScan::{Prefix, RangeInclusive, ScanPartitionKeyRange};
use crate::{ScanMode, TableKind, convert_to_upper_bound};

#[derive(Debug)]
pub enum TableScan<K> {
    /// Scan an inclusive partition key-range.
    ScanPartitionKeyRange(KeyRange),
    /// Scan within the same prefix.
    Prefix(K),
    /// Inclusive key range.
    RangeInclusive(K, K),
}

pub(crate) enum PhysicalScan<B> {
    Prefix(TableKind, B),
    RangeExclusive(TableKind, ScanMode, B, B),
}

impl PhysicalScan<Bytes> {
    pub fn from<K: EncodeTableKeyPrefix>(scan: TableScan<K>, arena: &mut BytesMut) -> Self {
        match scan {
            Prefix(key) => {
                key.serialize_to(arena);
                PhysicalScan::Prefix(K::TABLE, arena.split().freeze())
            }
            RangeInclusive(start, end) => {
                arena.reserve(start.serialized_length() + end.serialized_length());
                start.serialize_to(arena);
                let start = arena.split().freeze();
                end.serialize_to(arena);
                let mut end = arena.split();
                if start == end {
                    return PhysicalScan::Prefix(K::TABLE, start);
                }

                if !convert_to_upper_bound(&mut end) {
                    // Not allowed to happen since we guarantee that KeyKind is
                    // always incrementable.
                    std::hint::cold_path();
                    panic!("Key range end overflowed, start key {:x?}", &start);
                }
                let end = end.freeze();
                // RocksDB requires the exclusive upper bound to share the seek prefix when
                // total-order seek is disabled.
                let scan_mode = ScanMode::from_range(&start, &end);

                PhysicalScan::RangeExclusive(K::TABLE, scan_mode, start, end)
            }
            ScanPartitionKeyRange(range) => {
                let start = range.start();
                let end = range.end();
                // A single partition key can use a prefix scan over the start key.
                if start == end {
                    arena.reserve(start.serialized_length() + KeyKind::SERIALIZED_LENGTH);
                    K::serialize_key_kind(arena);
                    start.encode(arena);
                    return PhysicalScan::Prefix(K::TABLE, arena.split().freeze());
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
                    panic!("Key range end overflowed, start key {:x?}", &start);
                }
                let end_bytes = end_bytes.freeze();
                PhysicalScan::RangeExclusive(K::TABLE, ScanMode::TotalOrder, start_bytes, end_bytes)
            }
        }
    }
}

impl<K: EncodeTableKeyPrefix> From<TableScan<K>> for PhysicalScan<Bytes> {
    fn from(scan: TableScan<K>) -> Self {
        let mut arena = BytesMut::new();
        PhysicalScan::from(scan, &mut arena)
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

    struct TestKey(u64, u64);

    impl EncodeTableKey for TestKey {
        const TABLE: TableKind = TableKind::InvocationStatus;
        const KEY_KIND: KeyKind = KeyKind::InvocationStatus;

        fn serialize_to<B: BufMut>(&self, bytes: &mut B) {
            Self::KEY_KIND.serialize(bytes);
            bytes.put_u64(self.0);
            bytes.put_u64(self.1);
        }

        fn serialized_length(&self) -> usize {
            KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<u64>() * 2
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
                TestKey(1, u64::MAX),
            )),
            ScanMode::TotalOrder
        );
    }

    #[test]
    fn single_partition_inclusive_key_range_stays_within_prefix() {
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
