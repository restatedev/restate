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

use bytes::{BufMut, BytesMut};

use restate_storage_api::filter::{Filter, ValuePredicate};
use restate_storage_api::vqueue_table::Stage;
use restate_types::ServiceName;
use restate_types::sharding::PartitionId;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::TableKind;
use crate::keys::filter::KeyMatch;
use crate::keys::{DecodeIndexKey, EncodeIndexKey, KeyDecoder, KeyKind};
use crate::scan::PhysicalScan;

restate_storage_api::define_table! { pub(crate) Record; }

restate_storage_api::define_filter! {
    pub(crate) Record {
        name: ReString => starts_with,
        category: Option<ReString> => starts_with,
        kind: EntryKind => starts_with,
        // Deliberately requests a prefix on a codec that cannot support it.
        sequence: u64 => starts_with,
    }
}

/// A non-stat fixed identity with a different prefix context.
fn start_index<B: BufMut>(identity: &(PartitionId, u16), buffer: &mut B) {
    buffer.put_slice(KeyKind::ScopedState.as_bytes());
    buffer.put_u32(u32::from(u16::from(identity.0)));
    buffer.put_u16(identity.1);
    buffer.put_u16(0);
}

// Physical order deliberately differs from the logical declaration.
define_index_key!(
    RecordKey,
    table: TableKind::State,
    context: (PartitionId, u16),
    start: start_index,
    fields {
        sequence: u64,
        category: Option<ReString> => str,
        name: ServiceName => str,
        kind: EntryKind,
    }
    filter: Record
);

// A second access path contains only the name field.
define_index_key!(
    NameKey,
    table: TableKind::State,
    context: (PartitionId, u16),
    start: start_index,
    fields { name: ServiceName => str }
    filter: Record
);

#[test]
fn unfiltered_keys_decode_owned_and_progressively() {
    // Codec-only keys use the lower-level generator without a filter target.
    define_index_key!(
        DemoKey,
        table: TableKind::State,
        context: (PartitionId, u16),
        start: start_index,
        fields {
            stage: Stage,
            service_name1: ReString,
            service_name2: ReString,
            datum: u64,
        }
    );

    let mut scratch = BytesMut::new();
    DemoKey {
        stage: Stage::Inbox,
        service_name1: ReString::from("Timo"),
        service_name2: ReString::from("Kimo"),
        datum: 18,
    }
    .encode(&mut scratch);

    let mut payload = scratch.as_ref();
    let key = DemoKey::decode(&mut payload).unwrap();
    assert_eq!(key.stage, Stage::Inbox);
    assert_eq!(key.service_name1, "Timo");
    assert_eq!(key.service_name2, "Kimo");
    assert_eq!(key.datum, 18);
    assert!(payload.is_empty());

    let decoder = KeyDecoder::<DemoKey>::from_payload(&scratch);
    assert_eq!(decoder.tag(), "stage");
    let (stage, decoder) = decoder.decode_stage().unwrap();
    assert_eq!(stage, Stage::Inbox);
    assert_eq!(decoder.tag(), "service_name1");
    let (service_name1, decoder) = decoder.decode_service_name1().unwrap();
    assert_eq!(service_name1, "Timo");
    let (service_name2, decoder) = decoder.decode_service_name2().unwrap();
    assert_eq!(service_name2, "Kimo");
    assert_eq!(decoder.decode_datum().unwrap(), 18);

    let decoder = KeyDecoder::<DemoKey>::from_payload(&scratch);
    let (stage, decoder) = decoder.take_stage().unwrap();
    assert_eq!(
        stage.encoded(),
        crate::encoded_mem_cmp_str!(Stage::Inbox.as_str())
    );
    assert_eq!(stage.as_bytes(), Stage::Inbox.as_mem_cmp_str().as_bytes());
    assert_eq!(stage.decode().unwrap(), Stage::Inbox);
    let (service_name1, decoder) = decoder.take_service_name1().unwrap();
    assert_eq!(service_name1.encoded(), crate::encoded_mem_cmp_str!("Timo"));
    assert_eq!(service_name1.decode().unwrap(), "Timo");
    let (service_name2, decoder) = decoder.take_service_name2().unwrap();
    assert_eq!(service_name2.encoded(), crate::encoded_mem_cmp_str!("Kimo"));
    assert_eq!(service_name2.decode().unwrap(), "Kimo");
    let datum = decoder.take_datum().unwrap();
    assert_eq!(datum.as_bytes(), 18_u64.to_be_bytes());
    assert_eq!(datum.encoded(), &18_u64.to_be_bytes());
    assert_eq!(datum.decode().unwrap(), 18);

    let view: DemoKeyView<'_> = KeyDecoder::<DemoKey>::from_payload(&scratch)
        .take_all()
        .unwrap();
    assert_eq!(view.stage.as_bytes().as_ptr(), scratch.as_ptr());
    assert_eq!(view.stage.decode().unwrap(), Stage::Inbox);
    assert_eq!(view.service_name1.decode().unwrap(), "Timo");
    assert_eq!(view.service_name2.decode().unwrap(), "Kimo");
    assert_eq!(view.datum.decode().unwrap(), 18);
    for len in 0..scratch.len() {
        assert!(
            KeyDecoder::<DemoKey>::from_payload(&scratch[..len])
                .take_all()
                .is_err()
        );
    }

    let mut trailing = scratch.to_vec();
    trailing.push(0);
    assert!(
        KeyDecoder::<DemoKey>::from_payload(&trailing)
            .take_all()
            .is_err()
    );
    let decoder = KeyDecoder::<DemoKey>::from_payload(&trailing);
    let (_, decoder) = decoder.decode_stage().unwrap();
    let (_, decoder) = decoder.decode_service_name1().unwrap();
    let (_, decoder) = decoder.decode_service_name2().unwrap();
    assert!(decoder.decode_datum().is_err());

    let decoder = KeyDecoder::<DemoKey>::from_payload(&trailing);
    let (_, decoder) = decoder.take_stage().unwrap();
    let (_, decoder) = decoder.take_service_name1().unwrap();
    let (_, decoder) = decoder.take_service_name2().unwrap();
    assert!(decoder.take_datum().is_err());

    // Taking a view validates boundaries, leaving semantic checks until decode.
    scratch.clear();
    RecordKey::borrowed(7, None::<&str>, "", EntryKind::Invocation).encode(&mut scratch);
    let view = KeyDecoder::<RecordKey>::from_payload(&scratch)
        .take_all()
        .unwrap();
    assert_eq!(view.sequence.decode().unwrap(), 7);
    assert_eq!(view.category.decode().unwrap(), None);
    assert!(view.name.decode().is_err());
    assert_eq!(view.kind.decode().unwrap(), EntryKind::Invocation);
    scratch.clear();
    NameKey::borrowed("svc").encode(&mut scratch);
    let view = KeyDecoder::<NameKey>::from_payload(&scratch)
        .take_all()
        .unwrap();
    assert_eq!(view.name.decode().unwrap().as_str(), "svc");
}

#[test]
fn non_stat_keys_share_codecs_prefixes_and_filter_binding() {
    let identity = (PartitionId::from(8), 3);
    let mut fixed = Vec::new();
    RecordKey::prefix(identity, &mut fixed);
    assert_eq!(fixed, b"sS\x00\x00\x00\x08\x00\x03\x00\x00");
    let mut other = Vec::new();
    RecordKey::prefix((identity.0, 4), &mut other);
    assert_ne!(fixed, other);
    other.clear();
    RecordKey::prefix((PartitionId::from(9), 3), &mut other);
    assert_ne!(fixed, other);

    let owned = RecordKey {
        sequence: 7,
        category: Some("alpha".into()),
        name: ServiceName::new("svc"),
        kind: EntryKind::Invocation,
    };
    let mut payload = Vec::new();
    owned.encode(&mut payload);
    assert_eq!(owned.encoded_len(), payload.len());
    let borrowed = RecordKey::borrowed(7, Some("alpha"), "svc", EntryKind::Invocation);
    let mut borrowed_bytes = Vec::new();
    borrowed.encode(&mut borrowed_bytes);
    assert_eq!(borrowed_bytes, payload);
    assert_eq!(borrowed.encoded_len(), payload.len());
    let mut full = BytesMut::new();
    RecordKey::prefix(identity, &mut full)
        .sequence(7)
        .category(Some("alpha"))
        .name("svc")
        .kind(EntryKind::Invocation);
    assert!(full.starts_with(&fixed));
    assert_eq!(&full[fixed.len()..], payload);

    // Prefix construction appends to reusable buffers and does not retain field inputs.
    other.clear();
    other.push(0xff);
    let builder = RecordKey::prefix(identity, &mut other).sequence(7);
    let builder = {
        let category = String::from("alpha");
        builder.category(Some(&category))
    };
    builder.name("svc").kind(EntryKind::Invocation);
    assert_eq!(other[0], 0xff);
    assert_eq!(&other[1..], full.as_ref());

    // A fixed-size slice cursor also works, without an intermediate growable buffer.
    let mut storage = [0u8; 128];
    let capacity = storage.len();
    let written = {
        let mut cursor = &mut storage[..];
        RecordKey::prefix(identity, &mut cursor)
            .sequence(7)
            .category(Some("alpha"))
            .name("svc")
            .kind(EntryKind::Invocation);
        capacity - cursor.len()
    };
    assert_eq!(&storage[..written], full.as_ref());

    let decoded = KeyDecoder::<RecordKey>::from_payload(&payload)
        .decode_all()
        .unwrap();
    assert_eq!(decoded.sequence, owned.sequence);
    assert_eq!(decoded.category, owned.category);
    assert_eq!(decoded.name, owned.name);
    assert_eq!(decoded.kind, owned.kind);
    let (sequence, decoder) = KeyDecoder::<RecordKey>::from_payload(&payload)
        .take_sequence()
        .unwrap();
    assert_eq!(sequence.decode().unwrap(), 7);
    let (category, decoder) = decoder.decode_category().unwrap();
    assert_eq!(category.unwrap().as_str(), "alpha");
    let (name, decoder) = decoder.take_name().unwrap();
    assert_eq!(name.decode().unwrap().as_str(), "svc");
    assert_eq!(decoder.decode_kind().unwrap(), EntryKind::Invocation);
    let mut trailing = payload.clone();
    trailing.push(0);
    assert!(
        KeyDecoder::<RecordKey>::from_payload(&trailing)
            .decode_all()
            .is_err()
    );

    let filter = Filter::default()
        .and(RecordClause::Sequence(ValuePredicate::Equal(7)))
        .and(RecordClause::CategoryStartsWith("alp".into()))
        .and(RecordClause::NameStartsWith("s".into()))
        .and(RecordClause::Kind(ValuePredicate::Equal(
            EntryKind::Invocation,
        )));
    let prepared = RecordKey::prepare_filter(&filter).unwrap();
    let PhysicalScan::RangeExclusive(table, _, lower, upper) =
        prepared.scan(&fixed).unwrap().unwrap()
    else {
        panic!("expected range")
    };
    assert_eq!(table, TableKind::State);
    let mut expected = Vec::new();
    RecordKey::prefix(identity, &mut expected)
        .sequence(7)
        .category(Some("alp"))
        .name("s")
        .kind(EntryKind::Invocation);
    assert_eq!(lower.as_ref(), expected);
    expected.clear();
    RecordKey::prefix(identity, &mut expected).sequence(7);
    expected.extend_from_slice(b"\x01alq");
    assert_eq!(upper.as_ref(), expected);
    let mut cursor = prepared.into_cursor(&fixed).unwrap().unwrap();
    assert_eq!(cursor.evaluate(&full).unwrap(), KeyMatch::Match);

    for (category, name, kind, matches) in [
        (None, "svc", EntryKind::Invocation, false),
        (Some("alphabet"), "service", EntryKind::Invocation, true),
        (Some("alpha"), "svc", EntryKind::StateMutation, false),
        (Some("alpha"), "other", EntryKind::Invocation, false),
        (Some("beta"), "svc", EntryKind::Invocation, false),
    ] {
        let mut bytes = fixed.clone();
        RecordKey::borrowed(7, category, name, kind).encode(&mut bytes);
        assert_eq!(cursor.evaluate(&bytes).unwrap() == KeyMatch::Match, matches);
    }

    let filter = Filter::default().and(RecordClause::Sequence(ValuePredicate::Range {
        lower: Excluded(7),
        upper: Included(9),
    }));
    let PhysicalScan::RangeExclusive(_, _, lower, upper) = RecordKey::prepare_filter(&filter)
        .unwrap()
        .scan(&fixed)
        .unwrap()
        .unwrap()
    else {
        panic!("expected range")
    };
    expected.clear();
    RecordKey::prefix(identity, &mut expected).sequence(8);
    assert_eq!(lower.as_ref(), expected);
    expected.clear();
    RecordKey::prefix(identity, &mut expected).sequence(10);
    assert_eq!(upper.as_ref(), expected);
}

#[test]
fn generated_binding_rejects_invalid_literals_and_unsupported_clauses() {
    for predicate in [
        ValuePredicate::Equal(EntryKind::Unknown),
        ValuePredicate::In(vec![EntryKind::Invocation, EntryKind::Unknown]),
        ValuePredicate::Range {
            lower: Included(EntryKind::Unknown),
            upper: Unbounded,
        },
        ValuePredicate::Range {
            lower: Unbounded,
            upper: Excluded(EntryKind::Unknown),
        },
    ] {
        assert!(
            RecordKey::prepare_filter(&Filter::default().and(RecordClause::Kind(predicate)))
                .is_err()
        );
    }
    assert!(
        NameKey::prepare_filter(
            &Filter::default().and(RecordClause::Name(ValuePredicate::Equal("svc".into())))
        )
        .is_ok()
    );
    let prepared =
        NameKey::prepare_filter(&Filter::default().and(RecordClause::NameStartsWith("s".into())))
            .unwrap();
    let mut cursor = prepared.into_cursor(&[1]).unwrap().unwrap();
    let mut payload = vec![1];
    NameKey::borrowed("svc").encode(&mut payload);
    assert_eq!(cursor.evaluate(&payload).unwrap(), KeyMatch::Match);
    payload.truncate(1);
    NameKey::borrowed("z").encode(&mut payload);
    assert_eq!(cursor.evaluate(&payload).unwrap(), KeyMatch::Done);
    let mut cursor = RecordKey::prepare_filter(
        &Filter::default().and(RecordClause::KindStartsWith("inv".into())),
    )
    .unwrap()
    .into_cursor(&[1])
    .unwrap()
    .unwrap();
    for kind in [EntryKind::Invocation, EntryKind::StateMutation] {
        let mut key = vec![1];
        RecordKey::borrowed(7, None::<&str>, "svc", kind).encode(&mut key);
        assert_eq!(
            cursor.evaluate(&key).unwrap() == KeyMatch::Match,
            kind == EntryKind::Invocation
        );
    }
    assert!(
        RecordKey::prepare_filter(
            &Filter::default().and(RecordClause::SequenceStartsWith("7".into()))
        )
        .is_err()
    );
    assert!(
        NameKey::prepare_filter(
            &Filter::default().and(RecordClause::CategoryStartsWith("a".into()))
        )
        .is_err()
    );
    assert!(
        NameKey::prepare_filter(
            &Filter::default().and(RecordClause::Sequence(ValuePredicate::Equal(7)))
        )
        .is_err()
    );
}
