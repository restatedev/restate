// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use bytes::BufMut;

use restate_clock::time::MillisSinceEpoch;
use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::filter::ValuePredicate;

use crate::keys::IndexFieldEncode;
use crate::keys::predicate::{PreparedIndexPredicate, PreparedIndexRanges};

use super::{IndexFilterCodec, PreparedFieldPredicate};

impl IndexFieldEncode for MillisSinceEpoch {
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        self.as_u64().encode_field(target);
    }

    fn serialized_length(&self) -> usize {
        size_of::<u64>()
    }
}

impl IndexFilterCodec for Reverse<UniqueTimestamp> {
    type Value = MillisSinceEpoch;

    fn prepare_value(
        predicate: &ValuePredicate<MillisSinceEpoch>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<PreparedFieldPredicate> {
        Ok(prepare_timestamp_predicate(
            predicate,
            literals,
            encoded_range,
        ))
    }
}

impl IndexFilterCodec for RoughTimestamp {
    type Value = MillisSinceEpoch;

    fn prepare_value(
        predicate: &ValuePredicate<MillisSinceEpoch>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<PreparedFieldPredicate> {
        Ok(prepare_timestamp_predicate(
            predicate,
            literals,
            rough_range,
        ))
    }
}

fn prepare_timestamp_predicate<V: IndexFieldEncode>(
    predicate: &ValuePredicate<MillisSinceEpoch>,
    literals: &mut Vec<u8>,
    encoded_range: impl Fn(Bound<&MillisSinceEpoch>, Bound<&MillisSinceEpoch>) -> Option<(V, V)>,
) -> PreparedFieldPredicate {
    let empty = |literals: &mut Vec<u8>| {
        PreparedFieldPredicate::Value(PreparedIndexPredicate::new(
            &ValuePredicate::<V>::In(Vec::new()),
            literals,
        ))
    };
    let range = match predicate {
        ValuePredicate::Equal(millis) => encoded_range(Included(millis), Included(millis)),
        ValuePredicate::Range { lower, upper } => encoded_range(lower.as_ref(), upper.as_ref()),
        ValuePredicate::In(values) => {
            let ranges = values
                .iter()
                .filter_map(|millis| encoded_range(Included(millis), Included(millis)));
            return PreparedIndexRanges::new(ranges, literals)
                .map(PreparedFieldPredicate::Ranges)
                .unwrap_or_else(|| empty(literals));
        }
    };
    match range {
        Some((lower, upper)) => PreparedFieldPredicate::Value(PreparedIndexPredicate::new(
            &ValuePredicate::Range {
                lower: Included(lower),
                upper: Included(upper),
            },
            literals,
        )),
        None => empty(literals),
    }
}

/// Converts millisecond predicates to whole-second bounds without widening them.
/// A fractional-second equality is empty; ranges round inward after clipping.
fn rough_range(
    lower: Bound<&MillisSinceEpoch>,
    upper: Bound<&MillisSinceEpoch>,
) -> Option<(RoughTimestamp, RoughTimestamp)> {
    let min = RoughTimestamp::RESTATE_EPOCH.as_unix_millis().as_u64();
    let max = RoughTimestamp::MAX.as_unix_millis().as_u64();
    let first = match lower {
        Included(millis) => millis.as_u64(),
        Excluded(millis) => millis.as_u64().checked_add(1)?,
        Unbounded => min,
    }
    .max(min);
    let last = match upper {
        Included(millis) => millis.as_u64(),
        Excluded(millis) => millis.as_u64().checked_sub(1)?,
        Unbounded => max,
    }
    .min(max);
    if first > last {
        return None;
    }
    let first = (first - min).div_ceil(1_000);
    let last = (last - min) / 1_000;
    (first <= last).then(|| {
        (
            RoughTimestamp::new(first as u32),
            RoughTimestamp::new(last as u32),
        )
    })
}

/// Converts a logical millisecond interval into inclusive, descending HLC bounds.
/// Equality includes every logical counter in that millisecond. Clipping before
/// conversion handles timestamps outside HLC's epoch/domain without overflow.
fn encoded_range(
    lower: Bound<&MillisSinceEpoch>,
    upper: Bound<&MillisSinceEpoch>,
) -> Option<(u64, u64)> {
    let min = UniqueTimestamp::MIN.to_unix_millis().as_u64();
    let max = UniqueTimestamp::MAX.to_unix_millis().as_u64();
    let first = match lower {
        Included(millis) => millis.as_u64(),
        Excluded(millis) => millis.as_u64().checked_add(1)?,
        Unbounded => min,
    }
    .max(min);
    let last = match upper {
        Included(millis) => millis.as_u64(),
        Excluded(millis) => millis.as_u64().checked_sub(1)?,
        Unbounded => max,
    }
    .min(max);
    if first > last {
        return None;
    }
    let start = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(first))
        .expect("millisecond bounds were clipped to the HLC domain")
        .as_u64();
    let end = if last == max {
        UniqueTimestamp::MAX.as_u64()
    } else {
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(last + 1))
            .expect("next millisecond is within the HLC domain")
            .as_u64()
            - 1
    };
    Some((!end, !start))
}

#[cfg(test)]
mod tests {
    use std::ops::RangeBounds;

    use super::*;

    #[test]
    fn rough_timestamp_predicates_preserve_millisecond_boundaries() {
        let min = RoughTimestamp::RESTATE_EPOCH.as_unix_millis().as_u64();
        let max = RoughTimestamp::MAX.as_unix_millis().as_u64();
        let timestamps = [
            RoughTimestamp::RESTATE_EPOCH,
            RoughTimestamp::new(1),
            RoughTimestamp::new(2),
            RoughTimestamp::new(u32::MAX - 2),
            RoughTimestamp::MAX,
        ];
        let mut predicates = vec![ValuePredicate::In(Vec::new())];
        let mut bounds = vec![Unbounded];
        for ms in [
            0,
            min - 1,
            min,
            min + 1,
            min + 999,
            min + 1000,
            min + 1001,
            max - 1,
            max,
            max + 1,
            u64::MAX,
        ] {
            let value = MillisSinceEpoch::new(ms);
            bounds.extend([Included(value), Excluded(value)]);
            predicates.push(ValuePredicate::Equal(value));
            predicates.push(ValuePredicate::In(vec![
                value,
                MillisSinceEpoch::new(min + 1000),
                value,
            ]));
        }
        for lower in &bounds {
            for upper in &bounds {
                predicates.push(ValuePredicate::Range {
                    lower: *lower,
                    upper: *upper,
                });
            }
        }
        for (case, predicate) in predicates.into_iter().enumerate() {
            let mut literals = Vec::new();
            let prepared = RoughTimestamp::prepare_value(&predicate, &mut literals).unwrap();
            for timestamp in timestamps {
                let millis = timestamp.as_unix_millis();
                let expected = match &predicate {
                    ValuePredicate::Equal(value) => millis == *value,
                    ValuePredicate::In(values) => values.contains(&millis),
                    ValuePredicate::Range { lower, upper } => (*lower, *upper).contains(&millis),
                };
                let mut encoded = Vec::new();
                timestamp.encode_field(&mut encoded);
                assert_eq!(
                    prepared.matches(&literals, &encoded).unwrap(),
                    expected,
                    "predicate {case}, {timestamp:?}"
                );
            }
        }
    }

    #[test]
    fn millisecond_predicates_match_logical_time_across_counters_and_domain_edges() {
        let min = UniqueTimestamp::MIN.to_unix_millis().as_u64();
        let max = UniqueTimestamp::MAX.to_unix_millis().as_u64();
        let mut timestamps = Vec::new();
        for ms in [min, min + 1, min + 2, min + 3, max - 1, max] {
            let first = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(ms)).unwrap();
            let last = if ms == max {
                UniqueTimestamp::MAX
            } else {
                UniqueTimestamp::try_from(
                    UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(ms + 1))
                        .unwrap()
                        .as_u64()
                        - 1,
                )
                .unwrap()
            };
            timestamps.extend([
                first,
                UniqueTimestamp::try_from(first.as_u64() + 1).unwrap(),
                last,
            ]);
        }
        let mut predicates = vec![
            ValuePredicate::In(Vec::new()),
            ValuePredicate::Range {
                lower: Unbounded,
                upper: Unbounded,
            },
            ValuePredicate::Range {
                lower: Included(MillisSinceEpoch::new(min + 3)),
                upper: Included(MillisSinceEpoch::new(min)),
            },
        ];
        for ms in [
            0,
            min - 1,
            min,
            min + 1,
            min + 2,
            min + 3,
            max,
            max + 1,
            u64::MAX,
        ] {
            let value = MillisSinceEpoch::new(ms);
            predicates.extend([
                ValuePredicate::Equal(value),
                ValuePredicate::In(vec![value, MillisSinceEpoch::new(min + 1), value]),
                ValuePredicate::Range {
                    lower: Included(value),
                    upper: Unbounded,
                },
                ValuePredicate::Range {
                    lower: Excluded(value),
                    upper: Unbounded,
                },
                ValuePredicate::Range {
                    lower: Unbounded,
                    upper: Included(value),
                },
                ValuePredicate::Range {
                    lower: Unbounded,
                    upper: Excluded(value),
                },
                ValuePredicate::Range {
                    lower: Included(value),
                    upper: Excluded(value),
                },
            ]);
        }
        for (index, predicate) in predicates.iter().enumerate() {
            let mut literals = Vec::new();
            let prepared = <Reverse<UniqueTimestamp> as IndexFilterCodec>::prepare_value(
                predicate,
                &mut literals,
            )
            .unwrap();
            for timestamp in &timestamps {
                let millis = timestamp.to_unix_millis();
                let expected = match predicate {
                    ValuePredicate::Equal(value) => millis == *value,
                    ValuePredicate::In(values) => values.contains(&millis),
                    ValuePredicate::Range { lower, upper } => (*lower, *upper).contains(&millis),
                };
                let mut encoded = Vec::new();
                Reverse(*timestamp).encode_field(&mut encoded);
                assert_eq!(
                    prepared.matches(&literals, &encoded).unwrap(),
                    expected,
                    "predicate {index}, timestamp {timestamp:?}"
                );
            }
        }
    }
}
