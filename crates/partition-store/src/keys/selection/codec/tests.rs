// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::Bound::{Excluded, Included, Unbounded};

use super::*;

#[test]
fn nullable_custom_translations_reject_non_null_literal_encoding() {
    use restate_clock::RoughTimestamp;

    let mut literals = vec![123];
    let predicate = ValuePredicate::Range {
        lower: Included(Some(2)),
        upper: Unbounded,
    };
    assert!(Option::<ReverseU64Codec>::prepare_value(&predicate, &mut literals).is_err());
    let predicate = ValuePredicate::Equal(Some(RoughTimestamp::RESTATE_EPOCH.as_unix_millis()));
    assert!(Option::<RoughTimestampCodec>::prepare_value(&predicate, &mut literals).is_err());
    assert!(
        Option::<ReverseUniqueTimestampCodec>::prepare_value(&predicate, &mut literals).is_err()
    );
    assert_eq!(literals[0], 123);

    // NULL itself needs no inner-codec translation and retains NULL-first semantics.
    let prepared =
        Option::<ReverseU64Codec>::prepare_value(&ValuePredicate::Equal(None), &mut literals)
            .unwrap();
    let mut encoded = Vec::new();
    Option::<ReverseU64Codec>::encode_field(&None, &mut encoded);
    assert!(prepared.matches(&literals, &encoded).unwrap());
    encoded.clear();
    Option::<ReverseU64Codec>::encode_field(&Some(Reverse(2)), &mut encoded);
    assert!(!prepared.matches(&literals, &encoded).unwrap());
}

#[test]
fn numeric_enum_filters_match_string_predicates_including_nullable_fields() {
    let stages: Vec<_> = Stage::VARIANTS
        .iter()
        .copied()
        .filter(|stage| *stage != Stage::Unknown)
        .collect();
    check_string_enum::<StageRankCodec>(&stages, Stage::as_str);
    check_string_enum::<StatusCodec>(Status::VARIANTS, Status::as_str);
}

fn check_string_enum<C>(variants: &[C::Value], name: fn(C::Value) -> &'static str)
where
    C: FieldPredicateCodec<Literal = ReString>,
    C::Value: Copy + Debug,
{
    let values: Vec<_> = variants
        .iter()
        .map(|&value| (ReString::from_static(name(value)), value))
        .collect();
    let literals: Vec<_> = variants
        .iter()
        .map(|&value| name(value))
        .chain([
            "",
            "a",
            "pending",
            "run",
            "running\0",
            "s",
            "sta",
            "unknown",
            "z",
            "é",
        ])
        .map(ReString::from_static)
        .collect();
    check_filters::<C>(&values, &literals, |value| Some(value.as_str()));

    let optional_values: Vec<_> = std::iter::once((None, None))
        .chain(
            values
                .into_iter()
                .map(|(name, value)| (Some(name), Some(value))),
        )
        .collect();
    let optional_literals: Vec<_> = std::iter::once(None)
        .chain(literals.into_iter().map(Some))
        .collect();
    check_filters::<Option<C>>(&optional_values, &optional_literals, |value| {
        value.as_ref().map(ReString::as_str)
    });
}

fn check_filters<C>(
    values: &[(C::Literal, C::Value)],
    literals: &[C::Literal],
    name: fn(&C::Literal) -> Option<&str>,
) where
    C: FieldPredicateCodec,
    C::Literal: Clone + Debug,
    C::Value: Debug,
{
    let mut predicates = vec![ValuePredicate::In(Vec::new())];
    let mut bounds = vec![Unbounded];
    for literal in literals {
        predicates.push(ValuePredicate::Equal(literal.clone()));
        predicates.push(ValuePredicate::In(vec![
            literal.clone(),
            literals[0].clone(),
            literal.clone(),
        ]));
        bounds.extend([Included(literal.clone()), Excluded(literal.clone())]);
    }
    for lower in &bounds {
        for upper in &bounds {
            predicates.push(ValuePredicate::Range {
                lower: lower.clone(),
                upper: upper.clone(),
            });
        }
    }

    // Existing literals must survive preparation into the shared buffer.
    let mut encoded_literals = vec![123];
    for (case, predicate) in predicates.iter().enumerate() {
        encoded_literals.truncate(1);
        let prepared = C::prepare_value(predicate, &mut encoded_literals).unwrap();
        let mut matches = 0;
        for (value, physical) in values {
            let expected = match predicate {
                ValuePredicate::Equal(expected) => value == expected,
                ValuePredicate::In(values) => values.contains(value),
                ValuePredicate::Range { lower, upper } => {
                    (lower.as_ref(), upper.as_ref()).contains(&value)
                }
            };
            let mut encoded = Vec::new();
            C::encode_field(physical, &mut encoded);
            assert_eq!(
                prepared.matches(&encoded_literals, &encoded).unwrap(),
                expected,
                "predicate {case}, value {value:?}"
            );
            matches += usize::from(expected);
        }
        let PreparedFieldPredicate::Value(prepared) = prepared else {
            panic!("expected finite candidates");
        };
        assert_eq!(prepared.values(&encoded_literals).unwrap().len(), matches);
        assert_eq!(encoded_literals[0], 123);
    }

    for prefix in ["", "r", "s", "st", "su", "unknown", "x", "é", "\0"] {
        encoded_literals.truncate(1);
        let prepared = C::prepare_prefix(prefix, &mut encoded_literals).unwrap();
        let mut matches = 0;
        for (value, physical) in values {
            let expected = name(value).is_some_and(|name| name.starts_with(prefix));
            let mut encoded = Vec::new();
            C::encode_field(physical, &mut encoded);
            assert_eq!(
                prepared.matches(&encoded_literals, &encoded).unwrap(),
                expected,
                "prefix {prefix:?}, value {value:?}"
            );
            matches += usize::from(expected);
        }
        let PreparedFieldPredicate::Value(prepared) = prepared else {
            panic!("expected finite prefix candidates");
        };
        assert_eq!(prepared.values(&encoded_literals).unwrap().len(), matches);
        assert_eq!(encoded_literals[0], 123);
    }
}
