// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::PartitionKey;
use restate_types::sharding::KeyRange;

/// The partition keys an index read admits: one more condition an index key
/// answers on its own.
///
/// A key of a partition-local index ends with the key of the record it names,
/// and that key leads with the record's partition key. So a read that the
/// query restricts to some partition keys, such as those of one virtual
/// object's invocations, checks each index key before fetching its record,
/// like any condition on a column the index key holds:
///
/// ```text
/// query: target_service_key = 'k' AND scope IS NULL    -> partition key hash("k")
/// index run of service "Counter": t9 (pk 17)  t8 (pk hash("k"))  t7 (pk 4) …
///                                  skipped     fetched            skipped
/// ```
///
/// It filters, it doesn't seek: the index is ordered by its own fields, so the
/// read still walks the run, but fetches only the records the query can return.
/// The caller still checks every fetched record against its predicate.
#[derive(Clone, Debug, Default)]
pub struct PartitionKeyFilter(Option<Box<[KeyRange]>>);

impl PartitionKeyFilter {
    /// Admits every partition key.
    pub const ALL: Self = Self(None);

    /// Admits the keys of `ranges`, which must be sorted and disjoint, as a
    /// normalized domain's ranges are. No ranges admit nothing.
    pub fn new(ranges: impl IntoIterator<Item = KeyRange>) -> Self {
        let ranges: Box<[KeyRange]> = ranges.into_iter().collect();
        debug_assert!(
            ranges
                .windows(2)
                .all(|pair| pair[0].end() < pair[1].start()),
            "partition key ranges must be sorted and disjoint"
        );
        Self(Some(ranges))
    }

    /// Whether a record under `key` may be read.
    pub fn admits(&self, key: PartitionKey) -> bool {
        let Some(ranges) = &self.0 else {
            return true;
        };
        let at = ranges.partition_point(|range| range.end() < key);
        ranges.get(at).is_some_and(|range| range.start() <= key)
    }

    /// The bytes its ranges hold beyond the filter itself.
    pub fn heap_bytes(&self) -> usize {
        self.0.as_deref().map_or(0, std::mem::size_of_val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_only_keys_inside_its_ranges() {
        let filter = PartitionKeyFilter::new([KeyRange::new(3, 3), KeyRange::new(10, 20)]);
        let admitted: Vec<_> = [0, 3, 4, 9, 10, 15, 20, 21, u64::MAX]
            .into_iter()
            .filter(|key| filter.admits(*key))
            .collect();
        assert_eq!(admitted, [3, 10, 15, 20]);
        assert!(PartitionKeyFilter::ALL.admits(42));
        assert!(!PartitionKeyFilter::new([]).admits(42));
        assert_eq!(filter.heap_bytes(), 2 * std::mem::size_of::<KeyRange>());
        assert_eq!(PartitionKeyFilter::ALL.heap_bytes(), 0);
    }
}
