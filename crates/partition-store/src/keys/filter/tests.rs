// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use bytes::BufMut;

use restate_storage_api::filter::{Filter, ValuePredicate};
use restate_util_string::ReString;

use crate::TableKind;
use crate::keys::macros::define_index_key;

use super::KeyMatch;

restate_storage_api::define_table! { pub(crate) Navigation; }
restate_storage_api::define_filter! {
    pub(crate) Navigation {
        parent: u64,
        child: u64,
        label: Option<ReString> => starts_with,
    }
}

fn start<B: BufMut>(identity: &u8, buffer: &mut B) {
    buffer.put_u8(*identity);
}

define_index_key!(
    NavigationKey,
    table: TableKind::State,
    context: u8,
    start: start,
    fields { parent: u64, child: u64, label: Option<ReString> => str }
    filter: Navigation
);

fn matches(predicate: &ValuePredicate<u64>, value: u64) -> bool {
    match predicate {
        ValuePredicate::Equal(expected) => value == *expected,
        ValuePredicate::In(values) => values.contains(&value),
        ValuePredicate::Range { lower, upper } => (*lower, *upper).contains(&value),
    }
}

fn copy_predicate(predicate: &ValuePredicate<u64>) -> ValuePredicate<u64> {
    match predicate {
        ValuePredicate::Equal(value) => ValuePredicate::Equal(*value),
        ValuePredicate::In(values) => ValuePredicate::In(values.clone()),
        ValuePredicate::Range { lower, upper } => ValuePredicate::Range {
            lower: *lower,
            upper: *upper,
        },
    }
}

#[test]
fn lexicographic_navigation_agrees_with_cartesian_reference() {
    // Adjacent values around a byte carry and the maximum encoded integer catch
    // accidental carry into the parent or appending a suffix behind a boundary.
    let numbers = [0, 1, 254, 255, 256, u64::MAX - 1, u64::MAX];
    let labels = [
        None,
        Some(""),
        Some("a"),
        Some("a\0"),
        Some("abcdefgh"),
        Some("abcdefghi"),
        Some("b"),
    ];
    let mut rows = Vec::new();
    for parent in numbers {
        for child in numbers {
            for label in labels {
                let mut key = Vec::new();
                NavigationKey::prefix(1, &mut key)
                    .parent(parent)
                    .child(child)
                    .label(label);
                rows.push(((parent, child, label), key));
            }
        }
    }
    rows.sort_by(|(_, a), (_, b)| a.cmp(b));

    let bounds = [
        Unbounded,
        Included(0),
        Excluded(0),
        Included(255),
        Excluded(255),
        Included(u64::MAX),
        Excluded(u64::MAX),
    ];
    let mut predicates: Vec<_> = numbers.into_iter().map(ValuePredicate::Equal).collect();
    predicates.push(ValuePredicate::In(vec![]));
    // Unstored candidates, duplicates, and holes, at either depth.
    predicates.push(ValuePredicate::In(vec![u64::MAX, 256, 253, 1, 1]));
    for lower in bounds {
        for upper in bounds {
            predicates.push(ValuePredicate::Range { lower, upper });
        }
    }

    for (parent_case, parent) in predicates.iter().enumerate() {
        for (child_case, child) in predicates.iter().enumerate() {
            for prefix in [None, Some(""), Some("a"), Some("abcdefgh")] {
                let mut filter = Filter::default()
                    .and(NavigationClause::Parent(copy_predicate(parent)))
                    .and(NavigationClause::Child(copy_predicate(child)));
                if let Some(prefix) = prefix {
                    filter = filter.and(NavigationClause::LabelStartsWith(prefix.into()));
                }
                let expected: Vec<_> = rows
                    .iter()
                    .filter(|((p, c, label), _)| {
                        matches(parent, *p)
                            && matches(child, *c)
                            && prefix.is_none_or(|prefix| {
                                label.is_some_and(|label| label.starts_with(prefix))
                            })
                    })
                    .map(|(row, _)| *row)
                    .collect();

                let mut actual = Vec::new();
                if let Some(mut cursor) = NavigationKey::prepare_filter(&filter)
                    .unwrap()
                    .into_cursor(&[1])
                    .unwrap()
                {
                    let mut position = 0;
                    while let Some((row, key)) = rows.get(position) {
                        if !cursor.scan().contains_key(key) {
                            position += 1;
                            continue;
                        }
                        match cursor.evaluate(key).unwrap() {
                            KeyMatch::Match => {
                                actual.push(*row);
                                position += 1;
                            }
                            KeyMatch::Seek(target) => {
                                assert!(target.as_ref() > key.as_slice());
                                assert!(cursor.scan().contains_key(&target));
                                assert!(target.starts_with(&[1]));
                                position = rows
                                    .partition_point(|(_, key)| key.as_slice() < target.as_ref());
                            }
                            KeyMatch::Done => break,
                        }
                    }
                }
                assert_eq!(
                    actual, expected,
                    "parent case={parent_case}, child case={child_case}, prefix={prefix:?}"
                );
            }
        }
    }
}
