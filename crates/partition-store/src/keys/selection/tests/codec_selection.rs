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

use restate_storage_api::StorageError;
use restate_storage_api::selection::{Selection, ValuePredicate};
use restate_storage_api::vqueue_table::Stage;
use restate_util_string::ReString;

use crate::keys::codecs::{StageRankCodec, StringCodec, U64Codec};
use crate::keys::macros::define_index_key;
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{IndexKeySchema, KeyMatch, PreparedKeySelection};
use crate::keys::{FieldCodec, KeyDecode, KeyDecoder, KeyEncode, KeyKind, OrderedFieldCodec};

use super::start;

// The same stored value type, but variable-length name bytes instead of ranks.
pub enum StageNameCodec {}

impl FieldCodec for StageNameCodec {
    type Value = Stage;
    type Ref<'a> = &'a Stage;
    type Encoded<'a> = <StringCodec as FieldCodec>::Encoded<'a>;

    fn borrow(value: &Stage) -> &Stage {
        value
    }

    fn encode_ref<B: BufMut>(value: &Stage, target: &mut B) {
        assert_ne!(*value, Stage::Unknown);
        StringCodec::encode_ref(value.as_str(), target);
    }

    fn serialized_length_ref(value: &Stage) -> usize {
        StringCodec::serialized_length_ref(value.as_str())
    }

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Stage> {
        let name = StringCodec::decode_encoded(encoded)?;
        StageRankCodec::finite_domain()
            .unwrap()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
            .ok_or(StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        StringCodec::take_encoded(buf)
    }
}

impl OrderedFieldCodec for StageNameCodec {}

impl FieldPredicateCodec for StageNameCodec {
    type Literal = ReString;

    fn finite_domain() -> Option<impl Iterator<Item = (ReString, Stage)>> {
        StageRankCodec::finite_domain()
    }

    fn prefix_values(prefix: &str) -> Option<impl Iterator<Item = Stage>> {
        StageRankCodec::prefix_values(prefix)
    }
}

restate_storage_api::define_table! { pub(crate) CodecSelection; }
restate_storage_api::define_selector! {
    pub(crate) CodecSelection {
        parent: u64,
        stage: ReString => starts_with,
        child: u64,
        label: Option<ReString> => starts_with,
    }
}

define_index_key!(
    RankKey,
    key_kind: KeyKind::State,
    context: u8,
    start: start,
    fields { parent: U64Codec, stage: StageRankCodec, child: U64Codec, label: Option<StringCodec> => str }
    selector: CodecSelection
);
define_index_key!(
    NameKey,
    key_kind: KeyKind::State,
    context: u8,
    start: start,
    fields { parent: U64Codec, stage: StageNameCodec, child: U64Codec, label: Option<StringCodec> => str }
);

// Separate bindings also inherit codec selection from the key schema.
crate::keys::macros::define_key_selection!(
    NameKey; [CodecSelection]; parent, stage, child, label,
);

type Row = (u64, Stage, u64, Option<&'static str>);

fn scan<K: IndexKeySchema>(
    prepared: PreparedKeySelection<K>,
    rows: &[(Row, Vec<u8>)],
    live: bool,
) -> (Vec<Row>, usize) {
    let mut actual = Vec::new();
    let mut seeks = 0;
    let cursor = if live {
        let mut cursor = PreparedKeySelection::<K>::new(
            &Selection::<CodecSelection>::All,
            |_, _| unreachable!(),
        )
        .unwrap()
        .into_cursor(&[1])
        .unwrap()
        .unwrap();
        cursor.set_live(prepared);
        Some(cursor)
    } else {
        prepared.into_cursor(&[1]).unwrap()
    };
    if let Some(mut cursor) = cursor {
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
                    seeks += 1;
                    position = rows.partition_point(|(_, key)| key.as_slice() < target.as_ref());
                }
                KeyMatch::Done => break,
            }
        }
    }
    (actual, seeks)
}

#[test]
fn schema_selected_codecs_preserve_logical_results_through_seeks() {
    let mut logical = Vec::new();
    for parent in [0, 255, 256, u64::MAX] {
        for stage in [
            Stage::Finished,
            Stage::Inbox,
            Stage::Paused,
            Stage::Running,
            Stage::Suspended,
        ] {
            for child in [0, 1, 255, 256, u64::MAX] {
                for label in [None, Some(""), Some("a"), Some("abcdefgh"), Some("b")] {
                    logical.push((parent, stage, child, label));
                }
            }
        }
    }
    macro_rules! rows {
        ($key:ident) => {{
            let mut rows = Vec::new();
            for &(parent, stage, child, label) in &logical {
                let owned = $key {
                    parent,
                    stage,
                    child,
                    label: label.map(ReString::from_static),
                };
                let mut bytes = vec![1];
                owned.encode(&mut bytes);
                assert_eq!(owned.serialized_length() + 1, bytes.len());
                let borrowed = $key::borrowed(parent, stage, child, label);
                let mut other = vec![1];
                borrowed.encode(&mut other);
                assert_eq!(bytes, other);
                assert_eq!(borrowed.serialized_length(), owned.serialized_length());
                other.clear();
                $key::prefix(1, &mut other)
                    .parent(parent)
                    .stage(stage)
                    .child(child)
                    .label(label);
                assert_eq!(bytes, other);
                let decoded = $key::decode(&mut &bytes[1..]).unwrap();
                assert_eq!(
                    (
                        decoded.parent,
                        decoded.stage,
                        decoded.child,
                        decoded.label.as_deref()
                    ),
                    (parent, stage, child, label)
                );
                let view = KeyDecoder::<$key>::from_payload(&bytes[1..])
                    .take_all()
                    .unwrap();
                assert_eq!(view.stage.decode().unwrap(), stage);
                assert_eq!(view.child.decode().unwrap(), child);
                rows.push(((parent, stage, child, label), bytes));
            }
            rows.sort_by(|(_, a), (_, b)| a.cmp(b));
            rows
        }};
    }
    let ranked = rows!(RankKey);
    let named = rows!(NameKey);
    assert_ne!(ranked[0].1, named[0].1);

    let mut predicates = vec![
        ValuePredicate::Equal(ReString::from_static("running")),
        ValuePredicate::Equal(ReString::from_static("pending")),
        ValuePredicate::In(vec![]),
        ValuePredicate::In(
            ["suspended", "inbox", "inbox", "pending"]
                .map(ReString::from_static)
                .to_vec(),
        ),
    ];
    let bounds = [
        Unbounded,
        Included(""),
        Excluded("inbox"),
        Included("pending"),
        Included("running"),
        Excluded("suspended"),
        Included("z"),
    ];
    for lower in bounds {
        for upper in bounds {
            predicates.push(ValuePredicate::Range {
                lower: lower.map(ReString::from_static),
                upper: upper.map(ReString::from_static),
            });
        }
    }
    let mut seeks = [0, 0];
    for predicate in &predicates {
        for prefix in ["", "a", "abcdefgh"] {
            for stage_prefix in ["", "r", "s", "x"] {
                let filter = Selection::All
                    .and(CodecSelectionClause::Stage(match predicate {
                        ValuePredicate::Equal(value) => ValuePredicate::Equal(value.clone()),
                        ValuePredicate::In(values) => ValuePredicate::In(values.clone()),
                        ValuePredicate::Range { lower, upper } => ValuePredicate::Range {
                            lower: lower.clone(),
                            upper: upper.clone(),
                        },
                    }))
                    .and(CodecSelectionClause::StageStartsWith(stage_prefix.into()))
                    .and(CodecSelectionClause::Child(ValuePredicate::In(vec![
                        256, 1, 253,
                    ])))
                    .and(CodecSelectionClause::LabelStartsWith(prefix.into()));
                let expected: Vec<_> = ranked
                    .iter()
                    .map(|(row, _)| *row)
                    .filter(|(_, stage, child, label)| {
                        let name = ReString::from_static(stage.as_str());
                        let matches = match predicate {
                            ValuePredicate::Equal(value) => name == *value,
                            ValuePredicate::In(values) => values.contains(&name),
                            ValuePredicate::Range { lower, upper } => {
                                (lower.as_ref(), upper.as_ref()).contains(&&name)
                            }
                        };
                        matches
                            && stage.as_str().starts_with(stage_prefix)
                            && [1, 256].contains(child)
                            && label.is_some_and(|label| label.starts_with(prefix))
                    })
                    .collect();
                for live in [false, true] {
                    let (actual, count) =
                        scan(RankKey::prepare_selection(&filter).unwrap(), &ranked, live);
                    assert_eq!(actual, expected);
                    seeks[0] += count;
                    let (actual, count) =
                        scan(NameKey::prepare_selection(&filter).unwrap(), &named, live);
                    assert_eq!(actual, expected);
                    seeks[1] += count;
                }
            }
        }
    }
    assert!(seeks.into_iter().all(|count| count > 0));

    // Nullable finite-domain translation also uses the selected inner codec.
    let mut literals = vec![123];
    let prepared = Option::<StageNameCodec>::prepare_value(
        &ValuePredicate::Range {
            lower: Included(Some("pending".into())),
            upper: Unbounded,
        },
        &mut literals,
    )
    .unwrap();
    for (value, expected) in [
        (None, false),
        (Some(Stage::Paused), false),
        (Some(Stage::Running), true),
    ] {
        let mut encoded = Vec::new();
        Option::<StageNameCodec>::encode_field(&value, &mut encoded);
        assert_eq!(prepared.matches(&literals, &encoded).unwrap(), expected);
    }
    assert_eq!(literals[0], 123);
}
