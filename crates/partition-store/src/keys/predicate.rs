// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Bound;

use restate_storage_api::filter::ValuePredicate;

use super::IndexFieldEncode;

/// A value predicate prepared for comparison with **one encoded index field**.
///
/// Literals use the field's order-preserving index encoding and are stored in
/// the owning key filter's shared buffer. Offsets remain valid when that buffer
/// grows or moves. Evaluation borrows one field without decoding its value.
pub(crate) struct PreparedIndexPredicate {
    predicate: ValuePredicate<LiteralSpan>,
}

/// The byte range of one encoded literal in the shared buffer.
#[derive(Clone, Copy)]
struct LiteralSpan {
    /// Inclusive offset.
    start: usize,
    /// Exclusive offset.
    end: usize,
}

impl LiteralSpan {
    /// Borrows the literal from its owning filter's buffer.
    fn get(self, literals: &[u8]) -> &[u8] {
        &literals[self.start..self.end]
    }
}

impl PreparedIndexPredicate {
    /// Appends literals to the shared buffer and records their offsets.
    /// Existing predicates' literals are preserved.
    pub(crate) fn new<V: IndexFieldEncode>(
        predicate: &ValuePredicate<V>,
        literals: &mut Vec<u8>,
    ) -> Self {
        let predicate = match predicate {
            ValuePredicate::Equal(value) => ValuePredicate::Equal(encode(value, literals)),
            ValuePredicate::In(values) => {
                let start = literals.len();
                literals.reserve(values.iter().map(IndexFieldEncode::serialized_length).sum());
                let mut spans: Vec<_> =
                    values.iter().map(|value| encode(value, literals)).collect();
                // Value order may differ from the order of offsets in the buffer.
                spans.sort_unstable_by(|a, b| a.get(literals).cmp(b.get(literals)));
                spans.dedup_by(|a, b| a.get(literals) == b.get(literals));
                if spans.len() != values.len() {
                    // Pack the surviving values in their original offset order so
                    // moving bytes left cannot overwrite a value still to be copied.
                    // Only this predicate's newly appended region is modified.
                    spans.sort_unstable_by_key(|span| span.start);
                    let mut end = start;
                    for span in &mut spans {
                        let len = span.end - span.start;
                        literals.copy_within(span.start..span.end, end);
                        *span = LiteralSpan {
                            start: end,
                            end: end + len,
                        };
                        end += len;
                    }
                    literals.truncate(end);
                    // Restore value order for binary search.
                    spans.sort_unstable_by(|a, b| a.get(literals).cmp(b.get(literals)));
                }
                ValuePredicate::In(spans)
            }
            ValuePredicate::Range { lower, upper } => ValuePredicate::Range {
                lower: lower.as_ref().map(|value| encode(value, literals)),
                upper: upper.as_ref().map(|value| encode(value, literals)),
            },
        };

        Self { predicate }
    }

    fn finite_values(&self) -> Option<&[LiteralSpan]> {
        match &self.predicate {
            ValuePredicate::Equal(value) => Some(std::slice::from_ref(value)),
            ValuePredicate::In(values) => Some(values),
            ValuePredicate::Range { .. } => None,
        }
    }

    /// Borrows the exact candidates for equality or IN; ranges return `None`.
    pub(super) fn values<'a>(
        &'a self,
        literals: &'a [u8],
    ) -> Option<impl ExactSizeIterator<Item = &'a [u8]>> {
        let spans = self.finite_values()?;
        Some(spans.iter().map(move |span| span.get(literals)))
    }

    /// Finds finite candidates strictly after this encoded value without rescanning
    /// earlier members. IN literals are already sorted and deduplicated.
    pub(super) fn values_after<'a>(
        &'a self,
        literals: &'a [u8],
        encoded: &[u8],
    ) -> Option<impl Iterator<Item = &'a [u8]>> {
        let spans = self.finite_values()?;
        let next = spans.partition_point(|span| span.get(literals) <= encoded);
        Some(spans[next..].iter().map(move |span| span.get(literals)))
    }

    /// Returns enclosing bounds, including any IN-list holes.
    /// Empty IN lists must be handled through `values` before calling this.
    pub(super) fn bounds<'a>(&self, literals: &'a [u8]) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        match &self.predicate {
            ValuePredicate::Equal(value) => (
                Bound::Included(value.get(literals)),
                Bound::Included(value.get(literals)),
            ),
            ValuePredicate::In(values) => match (values.first(), values.last()) {
                (Some(first), Some(last)) => (
                    Bound::Included(first.get(literals)),
                    Bound::Included(last.get(literals)),
                ),
                // Empty sets are handled before requesting their bounds.
                _ => unreachable!("an empty set has no bounds"),
            },
            ValuePredicate::Range { lower, upper } => (
                lower.as_ref().map(|span| span.get(literals)),
                upper.as_ref().map(|span| span.get(literals)),
            ),
        }
    }

    /// Tests exactly one complete, validated encoded field.
    ///
    /// The field must use the same codec as the predicate values.
    /// Do not pass the remaining key or a segment containing trailing fields.
    /// `literals` must be the shared buffer this predicate was prepared into.
    pub(crate) fn matches(&self, literals: &[u8], encoded: &[u8]) -> bool {
        match &self.predicate {
            ValuePredicate::Equal(value) => encoded == value.get(literals),
            ValuePredicate::In(values) => values
                .binary_search_by(|value| value.get(literals).cmp(encoded))
                .is_ok(),
            ValuePredicate::Range { lower, upper } => {
                let above_lower = match lower {
                    Bound::Included(value) => encoded >= value.get(literals),
                    Bound::Excluded(value) => encoded > value.get(literals),
                    Bound::Unbounded => true,
                };

                let below_upper = match upper {
                    Bound::Included(value) => encoded <= value.get(literals),
                    Bound::Excluded(value) => encoded < value.get(literals),
                    Bound::Unbounded => true,
                };

                above_lower && below_upper
            }
        }
    }
}

/// Appends one index-encoded value and returns its byte range.
fn encode<V: IndexFieldEncode>(value: &V, literals: &mut Vec<u8>) -> LiteralSpan {
    let start = literals.len();
    literals.reserve(value.serialized_length());
    value.encode_field(literals);
    LiteralSpan {
        start,
        end: literals.len(),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::ops::RangeBounds;

    use restate_types::vqueues::EntryKind;
    use restate_util_string::ReString;

    use crate::keys::{FieldDecoder, IndexFieldDecode};

    use super::*;

    /// Compare encoded matching with logical matching, including every pairing
    /// of included, excluded, and unbounded endpoints from the sample values.
    fn check_predicates<C: IndexFieldDecode>(values: &[C::Owned])
    where
        C::Owned: Clone + Debug + Ord,
    {
        let mut predicates: Vec<_> = values.iter().cloned().map(ValuePredicate::Equal).collect();
        predicates.push(ValuePredicate::In(Vec::new()));

        // Keep holes and supply members in reverse order, with duplicates.
        let mut members: Vec<_> = values.iter().step_by(2).rev().cloned().collect();
        members.extend_from_within(..);
        predicates.push(ValuePredicate::In(members));

        let bounds: Vec<_> = std::iter::once(Unbounded)
            .chain(
                values
                    .iter()
                    .flat_map(|value| [Included(value.clone()), Excluded(value.clone())]),
            )
            .collect();
        for lower in &bounds {
            for upper in &bounds {
                predicates.push(ValuePredicate::Range {
                    lower: lower.clone(),
                    upper: upper.clone(),
                });
            }
        }

        for (case, predicate) in predicates.iter().enumerate() {
            let mut literals = Vec::new();
            let prepared = PreparedIndexPredicate::new(predicate, &mut literals);
            for value in values {
                let expected = match predicate {
                    ValuePredicate::Equal(expected) => value == expected,
                    ValuePredicate::In(members) => members.contains(value),
                    ValuePredicate::Range { lower, upper } => {
                        (lower.clone(), upper.clone()).contains(value)
                    }
                };

                // Test a borrowed field inside a compound key, rather than a
                // standalone encoding that could hide accidental suffix use.
                let mut key = Vec::new();
                value.encode_field(&mut key);
                let field_len = key.len();
                42_u64.encode_field(&mut key);
                let mut remaining = key.as_slice();
                let field = FieldDecoder::<C>::take(&mut remaining).unwrap();
                assert_eq!(field.as_bytes().as_ptr(), key.as_ptr());
                assert_eq!(field.as_bytes().len(), field_len);
                assert_eq!(remaining, &42_u64.to_be_bytes());
                assert_eq!(
                    prepared.matches(&literals, field.as_bytes()),
                    expected,
                    "case {case}, value {value:?}"
                );
            }
        }
    }

    #[test]
    fn string_predicates_match_logical_values() {
        check_predicates::<ReString>(
            &[
                "",
                "\0",
                "a",
                "a\0",
                "aa",
                "alpha",
                "alphabet",
                "b",
                "1234567",
                "12345678",
                "12345678\0",
                "123456789",
                "é",
                "🦀",
            ]
            .map(ReString::from),
        );
    }

    #[test]
    fn nullable_string_predicates_distinguish_null_and_empty() {
        check_predicates::<Option<ReString>>(
            &[None, Some(""), Some("\0"), Some("alpha"), Some("alphabet")]
                .map(|value| value.map(ReString::from)),
        );
    }

    #[test]
    fn integer_and_entry_kind_predicates_match_logical_values() {
        check_predicates::<u64>(&[0, 1, 255, 256, u64::MAX]);
        check_predicates::<EntryKind>(&[EntryKind::Invocation, EntryKind::StateMutation]);
    }

    #[test]
    fn shared_literals_survive_growth_compaction_and_finalization() {
        let encoded = |value: &str| {
            let mut bytes = Vec::new();
            value.encode_field(&mut bytes);
            bytes
        };
        let mut literals = Vec::new();
        let equal = PreparedIndexPredicate::new(
            &ValuePredicate::Equal(ReString::from("alpha")),
            &mut literals,
        );
        let first_len = literals.len();
        let first_capacity = literals.capacity();
        let long = "z".repeat(first_capacity + 128);
        let predicate = ValuePredicate::In(vec![
            ReString::from(long.as_str()),
            ReString::from("a\0"),
            ReString::from(long.as_str()),
            ReString::from("beta"),
            ReString::from("a\0"),
        ]);
        let in_list = PreparedIndexPredicate::new(&predicate, &mut literals);
        drop(predicate);
        assert!(literals.capacity() > first_capacity);
        // Deduplication must remove duplicate payloads, not just duplicate spans,
        // and must leave literals belonging to earlier predicates untouched.
        let unique = [encoded("a\0"), encoded("beta"), encoded(&long)];
        assert_eq!(
            literals.len(),
            first_len + unique.iter().map(Vec::len).sum::<usize>()
        );
        assert_eq!(&literals[..first_len], encoded("alpha"));

        let range = PreparedIndexPredicate::new(
            &ValuePredicate::Range {
                lower: Excluded(255_u64),
                upper: Included(256),
            },
            &mut literals,
        );
        let literals = literals.into_boxed_slice();
        assert!(equal.matches(&literals, &encoded("alpha")));
        assert!(!equal.matches(&literals, &encoded("alphabet")));
        assert!(range.matches(&literals, &256_u64.to_be_bytes()));
        assert!(!range.matches(&literals, &255_u64.to_be_bytes()));
        assert!(!range.matches(&literals, &257_u64.to_be_bytes()));
        assert!(!in_list.matches(&literals, &encoded("alpha")));

        let values: Vec<_> = in_list.values(&literals).unwrap().collect();
        assert_eq!(values, unique.iter().map(Vec::as_slice).collect::<Vec<_>>());
        for value in values {
            assert!(literals.as_ptr_range().contains(&value.as_ptr()));
            assert!(in_list.matches(&literals, value));
        }
        assert_eq!(
            in_list.bounds(&literals),
            (
                Included(unique[0].as_slice()),
                Included(unique[2].as_slice())
            )
        );
        let first = encoded("alpha");
        assert_eq!(
            equal.bounds(&literals),
            (Included(first.as_slice()), Included(first.as_slice()))
        );
    }
}
