// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{KeyCodec, TableKey};
use crate::scan::TableScan::{KeyPrefix, KeyRangeInclusive, Partition, PartitionKeyRange};
use crate::TableKind;
use bytes::BytesMut;
use restate_types::identifiers::{PartitionId, PartitionKey};
use std::ops::RangeInclusive;

pub enum TableScan<K> {
    /// Scan an entire partition of a given table.
    Partition(PartitionId),
    /// Scan an inclusive key-range.
    PartitionKeyRange(RangeInclusive<PartitionKey>),
    /// Key Prefix
    KeyPrefix(K),
    /// Inclusive Key Range
    KeyRangeInclusive(K, K),
}

pub(crate) enum PhysicalScan {
    Prefix(TableKind, BytesMut),
    RangeExclusive(TableKind, BytesMut, BytesMut),
    RangeOpen(TableKind, BytesMut),
}

impl<K: TableKey> From<TableScan<K>> for PhysicalScan {
    fn from(scan: TableScan<K>) -> Self {
        match scan {
            KeyPrefix(key) => PhysicalScan::Prefix(K::table(), key.serialize()),
            KeyRangeInclusive(start, end) => {
                let start = start.serialize();
                let mut end = end.serialize();
                if try_increment(&mut end) {
                    PhysicalScan::RangeExclusive(K::table(), start, end)
                } else {
                    PhysicalScan::RangeOpen(K::table(), start)
                }
            }
            Partition(partition_id) => {
                let mut prefix = BytesMut::with_capacity(
                    partition_id.serialized_length() + K::serialized_key_prefix_length(),
                );
                K::serialize_key_prefix(&mut prefix);
                partition_id.encode(&mut prefix);
                PhysicalScan::Prefix(K::table(), prefix)
            }
            PartitionKeyRange(range) => {
                let (start, end) = (range.start(), range.end());
                let mut start_bytes = BytesMut::with_capacity(
                    start.serialized_length() + K::serialized_key_prefix_length(),
                );
                K::serialize_key_prefix(&mut start_bytes);
                start.encode(&mut start_bytes);
                match end.checked_add(1) {
                    None => PhysicalScan::RangeOpen(K::table(), start_bytes),
                    Some(end) => {
                        let mut end_bytes = BytesMut::with_capacity(
                            end.serialized_length() + K::serialized_key_prefix_length(),
                        );
                        K::serialize_key_prefix(&mut end_bytes);
                        end.encode(&mut end_bytes);
                        PhysicalScan::RangeExclusive(K::table(), start_bytes, end_bytes)
                    }
                }
            }
        }
    }
}

impl<K: TableKey> TableScan<K> {
    pub fn table(&self) -> TableKind {
        K::table()
    }
}

/// Binary increment the number represented by the given bytes.
/// This function computes the next lexicographical byte string
/// that comes after this string.
///
/// RocksDB ranges are exclusive, yet in restate we treat the partition
/// ranges as inclusive.
///
/// RocksDB rows can be considered as sorted, big, unsigned
/// integers stored as big endian byte strings.
/// To compute the successor of a key, we do a binary increment of
/// the number represented by the input bytes.
///
///```ignore
/// [aBytes, bBytes] = [aBytes, successor(bBytes) ) =
///     [aBytes, (BigUint(bBytes)+1).to_big_endian_bytes() )
///```
/// returns true iff the successor doesn't generate a carry.
#[inline]
fn try_increment(bytes: &mut BytesMut) -> bool {
    for byte in bytes.iter_mut().rev() {
        if let Some(incremented) = byte.checked_add(1) {
            *byte = incremented;
            return true;
        } else {
            *byte = 0;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::scan::try_increment;
    use bytes::{BufMut, BytesMut};
    use num_bigint::BigUint;
    use std::collections::BTreeMap;
    use std::ops::Add;

    fn verify_binary_increment(bytes: &mut BytesMut) {
        let as_number = BigUint::from_bytes_be(bytes);
        let expected_successor = as_number.add(1u64);

        try_increment(bytes);
        let got_successor = BigUint::from_bytes_be(bytes);

        assert_eq!(got_successor, expected_successor);
    }

    #[test]
    fn test_simple_increment() {
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

        assert!(try_increment(&mut upper_bound_inclusive));

        let mut seen_values = 0;
        for (_, &value) in db.range(lower_bound..upper_bound_inclusive) {
            assert_eq!(value, "partition-1");
            seen_values += 1;
        }

        assert_eq!(seen_values, keys_to_insert);
    }

    #[test]
    fn test_scan_stays_within_partition_bounds() {
        verify_partition_covers_exactly(0);
        verify_partition_covers_exactly(255);
        verify_partition_covers_exactly(256);
        verify_partition_covers_exactly(1024);
        verify_partition_covers_exactly(32 * 1024);
        verify_partition_covers_exactly(u32::MAX as u64);
        verify_partition_covers_exactly(u64::MAX - 1);
    }

    #[test]
    fn test_binary_increment_suffix() {
        let mut bytes = BytesMut::new();
        bytes.put_u64(257);
        bytes.put_u64(u64::MAX);
        bytes.put_u64(u64::MAX);
        bytes.put_u64(u64::MAX);
        bytes.put_u64(u64::MAX);

        verify_binary_increment(&mut bytes);
    }
}
