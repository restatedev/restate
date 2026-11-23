// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query reads of locks. The lock key is not ordered by its logical fields (see
//! the module docs), but its optional scope has a canonical, prefix-free encoding
//! and the raw lock name runs to the end of the key. Equality and IN on scope and
//! lock name therefore position the iterator, with the lock name declared as a
//! terminal field; ranges and prefixes remain residual.

use std::num::NonZeroUsize;
use std::ops::ControlFlow;

use anyhow::{Context, ensure};
use bilrost::OwnedMessage;

use restate_storage_api::StorageError;
use restate_storage_api::lock_table::LockState;
use restate_storage_query_api::ordering::{NativeOrder, OrderTerm};
use restate_storage_query_api::schema::{RecordProgress, row_written};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_storage_query_api::tables::locks::{
    LockView, Locks, LocksClause, LocksColumn, LocksDomains, MAX_LOOKUP_PAIRS, partition_keys,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::identifiers::{PartitionKey, WithPartitionKey};
use restate_types::{CanonicalLockId, LockName, Scope};
use restate_util_string::ReString;

use crate::keys::codecs::{LockNameCodec, OptionalScopeCodec};
use crate::keys::header::{KeySchema, PartitionKeyHeader, restrict_partition_keys};
use crate::keys::selection::codec::prepare_members;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKey, FieldCodec, KeyDecode, KeyDecoder, KeyKind};
use crate::lookup::{LookupBinding, LookupTable};
use crate::table::PhysicalTable;
use crate::{Result, break_on_err};

use super::LockKey;

/// Physical lock records using the existing key and value encodings.
pub enum LocksTable {}

impl Table for LocksTable {
    type Record<'a> = (KeyDecoder<'a, LockKey>, &'a [u8]);
    type Scan = KeySelection<LockKey>;
    /// Keys start with the partition key; the scope and lock name after it don't
    /// sort like the columns showing them.
    const NATIVE_ORDERS: &'static [NativeOrder] = &[&[OrderTerm::Column("partition_key")]];
}

impl PhysicalTable for LocksTable {
    type Key = LockKey;
}

/// A lock is named by its scope (NULL when unscoped) and its lock name, as the
/// `scope` and `lock_name` columns show them.
impl LookupTable for LocksTable {
    type Id = (Option<ReString>, ReString);
    /// A starting point for small lock values, not measured.
    const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(500).unwrap();

    /// `None` for an invalid scope or a name without `service/`. Empty keys
    /// (`Svc/`), which older versions allowed, stay valid.
    fn key((scope, lock_name): &Self::Id) -> Option<LockKey> {
        let optional_scope = scope
            .as_deref()
            .map(Scope::try_non_interned)
            .transpose()
            .ok()?;
        let lock_name = LockName::parse_unchecked(lock_name).ok()?;
        let partition_key = CanonicalLockId {
            scope: &optional_scope,
            lock_name: &lock_name,
        }
        .partition_key();
        Some(LockKey {
            partition_key,
            optional_scope,
            lock_name,
        })
    }

    /// Reads the fields with checked bounds and UTF-8 (the stored-key decoders
    /// don't check either), rebuilds the key from them, and requires the same
    /// partition key and bytes.
    fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
        let rest = bytes
            .strip_prefix(KeyKind::Lock.as_bytes())
            .context("not a lock key")?;
        let (stored, mut rest) = rest
            .split_first_chunk()
            .context("lock key without a partition key")?;
        let scope = OptionalScopeCodec::take_encoded(&mut rest)
            .and_then(OptionalScopeCodec::decode_encoded)
            .context("invalid lock scope")?;
        let lock_name = LockNameCodec::decode_encoded(rest).context("invalid lock name")?;
        let key = Self::key(&(scope, lock_name)).context("invalid lock identity")?;
        ensure!(
            key.partition_key == PartitionKey::from_be_bytes(*stored),
            "lock key stored under another partition key"
        );
        let mut canonical = Vec::with_capacity(bytes.len());
        key.serialize_to(&mut canonical);
        ensure!(canonical == bytes, "lock key isn't canonical");
        Ok(key.partition_key)
    }
}

/// A branch that pins both `scope` and `lock_name` to finite sets can only match
/// their combinations, up to [`MAX_LOOKUP_PAIRS`] of them; otherwise it needs a
/// scan.
impl LookupBinding for LocksBinding {
    fn lookup_ids(domains: &LocksDomains) -> Domain<(Option<ReString>, ReString)> {
        domains
            .scope
            .bounded_product(&domains.lock_name, MAX_LOOKUP_PAIRS)
    }
}

impl KeySchema for LockKey {
    type Header = PartitionKeyHeader<Self>;

    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        // The existing owned key includes its partition key. Retain the full key.
        Ok(KeyDecoder::from_payload(key))
    }
}

impl KeyDecode for LockKey {
    fn decode(source: &mut &[u8]) -> Result<Self> {
        Self::deserialize_from(source)
    }
}

impl OrderedKeySchema for LockKey {
    const KEY_KIND: KeyKind = KeyKind::Lock;
    const FIELDS: &'static [OrderedKeyField] = &[
        OrderedKeyField::new::<OptionalScopeCodec>("scope"),
        OrderedKeyField::terminal::<LockNameCodec>("lock_name"),
    ];
}

/// Binds scope and lock-name clauses to the key's suffix fields.
fn prepare_fields(selection: &Selection<Locks>) -> Result<KeySelection<LockKey>> {
    KeySelection::for_key_fields(selection, |clause, literals| {
        Ok(match clause {
            LocksClause::Scope(predicate) => {
                prepare_members::<OptionalScopeCodec, _>(predicate, literals, |scope| {
                    Some(scope.as_deref())
                })
            }
            // The column renders `service/key`, exactly the stored remainder.
            LocksClause::LockName(predicate) => {
                prepare_members::<LockNameCodec, _>(predicate, literals, |name| Some(name.as_str()))
            }
            _ => None,
        })
    })
}

pub enum LocksBinding {}

impl SchemaBinding for LocksBinding {
    type Schema = Locks;
    type Table = LocksTable;
    type Materializer = ();

    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        _: &mut (),
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress> {
        row_written(Self::materialize_projected(record, emit))?;
        Ok(RecordProgress::Done)
    }

    fn prepare(selection: &Selection<Locks>) -> Result<PreparedRead<LocksTable>> {
        let request = prepare_fields(selection)?;
        let columns = match selection {
            Selection::All | Selection::Empty => {
                return Ok(PreparedRead::new(request, SelectionPrecision::Exact));
            }
            Selection::Predicates(columns) => columns,
        };
        let (mut keys, mut scope, mut lock_name, mut acquired_by) =
            (Domain::all(), Domain::all(), Domain::all(), Domain::all());
        for column in [
            LocksColumn::PartitionKey,
            LocksColumn::Scope,
            LocksColumn::LockName,
            LocksColumn::AcquiredBy,
        ] {
            for clause in columns.for_column(column) {
                match clause {
                    LocksClause::PartitionKey(predicate) => keys = keys.intersect(predicate.into()),
                    LocksClause::Scope(predicate) => scope = scope.intersect(predicate.into()),
                    LocksClause::LockName(predicate) => {
                        lock_name = lock_name.intersect(predicate.into())
                    }
                    LocksClause::AcquiredBy(predicate) => {
                        acquired_by = acquired_by.intersect(predicate.into())
                    }
                    _ => {}
                }
            }
        }
        // Residuals retain gaps of non-finite key sets, cross-header candidates
        // of the suffix fields, and unsupported predicates.
        let request = restrict_partition_keys(
            request,
            &partition_keys(keys, scope, lock_name, acquired_by),
        );
        Ok(PreparedRead::new(request, SelectionPrecision::Inexact))
    }

    fn materialize(
        (key, mut value): <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(LockView<'a>) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        let (partition_key, scope, lock_name) = break_on_err(key.decode_all())?.split();
        let state =
            break_on_err(LockState::decode(&mut value).map_err(StorageError::BilrostDecode))?;
        emit(LockView {
            partition_key,
            scope: scope.as_ref(),
            lock_name: &lock_name,
            state: &state,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use restate_storage_query_api::selection::ValuePredicate;
    use restate_types::identifiers::partitioner::HashPartitioner;
    use restate_types::sharding::KeyRange;
    use restate_types::{LockName, Scope};
    use restate_util_string::ReString;

    use crate::keys::EncodeTableKey;
    use crate::keys::predicate::EncodedValuePredicate;
    use crate::keys::selection::validate_fields;
    use crate::keys::selection::{OrderedKeyField, PreparedFieldPredicate};
    use crate::table::navigate_live;

    use super::*;

    #[test]
    fn terminal_lock_names_position_reads() {
        // Prefix-related names among 64 neighbors in each of two scopes.
        let names: Vec<_> = ["Cart/a", "Cart/ab", "Cart/b"]
            .into_iter()
            .map(str::to_owned)
            .chain((0..64).map(|i| format!("Svc/k{i}")))
            .collect();
        let mut rows = Vec::new();
        for scope in ["t1", "t2"] {
            for name in &names {
                let key = LockKey {
                    partition_key: HashPartitioner::compute_partition_key(scope),
                    optional_scope: Some(Scope::try_non_interned(scope).unwrap()),
                    lock_name: LockName::parse_unchecked(name).unwrap(),
                };
                let mut bytes = Vec::new();
                EncodeTableKey::serialize_to(&key, &mut bytes);
                rows.push((bytes, (scope, name.clone())));
            }
        }
        rows.sort();
        let keys: Vec<_> = rows.iter().map(|(key, _)| key).collect();
        let read = |selection: Selection<Locks>, live: Option<Selection<Locks>>| {
            let prepared = LocksBinding::prepare(&selection).unwrap();
            let live = live.map(|live| prepare_fields(&live).unwrap());
            let (matches, visited) =
                navigate_live(Arc::clone(prepared.request()), live, KeyRange::FULL, &keys);
            let found: Vec<_> = matches
                .into_iter()
                .map(|index| (rows[index].1.0, rows[index].1.1.as_str()))
                .collect();
            (found, visited)
        };
        let scope = |scope: &str| LocksClause::Scope(ValuePredicate::Equal(Some(scope.into())));
        let names = |names: &[&str]| {
            LocksClause::LockName(ValuePredicate::In(
                names.iter().copied().map(ReString::from).collect(),
            ))
        };
        let select =
            |clauses: Vec<LocksClause>| clauses.into_iter().fold(Selection::All, Selection::and);

        // An extension of the requested name may be visited and rejected.
        let (found, visited) = read(select(vec![scope("t1"), names(&["Cart/a"])]), None);
        assert_eq!(found, [("t1", "Cart/a")]);
        assert!(
            visited <= 2,
            "a_name_reads_at_most_its_extension: {visited}"
        );
        let (found, visited) = read(
            select(vec![scope("t1"), names(&["Cart/a", "Cart/ab"])]),
            None,
        );
        assert_eq!(found, [("t1", "Cart/a"), ("t1", "Cart/ab")]);
        assert!(visited <= 3, "a_name_and_its_extension: {visited}");
        let (found, visited) = read(select(vec![scope("t1"), names(&["Cart/aa"])]), None);
        assert!(found.is_empty());
        assert!(visited <= 1, "an_absent_name_between_neighbors: {visited}");

        // Both scopes' headers: terminal exhaustion carries into the scope field.
        let (mut found, visited) = read(select(vec![names(&["Cart/b", "Svc/k7"])]), None);
        found.sort_unstable();
        assert_eq!(
            found,
            [
                ("t1", "Cart/b"),
                ("t1", "Svc/k7"),
                ("t2", "Cart/b"),
                ("t2", "Svc/k7")
            ]
        );
        assert!(
            visited <= 10,
            "names_across_headers_carry_into_scope: {visited}"
        );

        // A live finite restriction navigates within the static selection's bounds,
        // which it does not tighten: the scope's first name seeks to the match,
        // and the next name exhausts it.
        let (found, visited) = read(
            select(vec![scope("t2")]),
            Some(select(vec![names(&["Cart/ab"])])),
        );
        assert_eq!(found, [("t2", "Cart/ab")]);
        assert!(visited <= 3, "live_names_seek: {visited}");

        // Continuous predicates never reach a terminal field, whatever the binder returns.
        let range = KeySelection::<LockKey>::for_key_fields(
            &select(vec![names(&["Cart/a"])]),
            |_, literals| {
                let predicate = ValuePredicate::Range {
                    lower: Bound::Excluded(b"Cart/a".as_slice()),
                    upper: Bound::Unbounded,
                };
                EncodedValuePredicate::try_new(&predicate, literals, |value, literals| {
                    literals.extend_from_slice(value);
                    Ok::<_, StorageError>(())
                })
                .map(|predicate| Some(PreparedFieldPredicate::Value(predicate)))
            },
        )
        .unwrap();
        assert_eq!(range.precision(), SelectionPrecision::Inexact);
        assert_eq!(
            navigate_live(range, None, KeyRange::FULL, &keys).1,
            keys.len()
        );

        // A terminal field anywhere but last is rejected.
        let misplaced = [
            OrderedKeyField::terminal::<LockNameCodec>("lock_name"),
            OrderedKeyField::new::<OptionalScopeCodec>("scope"),
        ];
        assert!(std::panic::catch_unwind(|| validate_fields(&misplaced)).is_err());
    }

    /// A branch that pins both columns to finite sets names every combination;
    /// one that doesn't names no finite set, so the source is scanned.
    #[test]
    fn lock_ids_combine_finite_scopes_and_names() {
        let mut domains = LocksDomains {
            scope: Domain::values([None, Some(ReString::from("tenant-1"))]),
            lock_name: Domain::values(["Cart/a", "Cart/b"].map(ReString::from)),
            ..Default::default()
        };
        let ids = LocksBinding::lookup_ids(&domains).into_values().unwrap();
        assert_eq!(ids.len(), 4);
        assert!(ids.contains(&(Some("tenant-1".into()), "Cart/b".into())));
        domains.lock_name = Domain::all();
        assert!(LocksBinding::lookup_ids(&domains).is_all());

        // 100 × 100 is the limit; one more name makes the alternative scan.
        let names = |n| Domain::values((0..n).map(|i| ReString::from(format!("Svc/k{i}"))));
        domains.scope = Domain::values((0..100).map(|i| Some(ReString::from(format!("t{i}")))));
        domains.lock_name = names(100);
        let ids = LocksBinding::lookup_ids(&domains).into_values().unwrap();
        assert_eq!(ids.len(), MAX_LOOKUP_PAIRS);
        domains.lock_name = names(101);
        assert!(LocksBinding::lookup_ids(&domains).is_all());
    }

    /// Received lock keys are checked field by field before anything decodes
    /// them: a malformed key is an error, never a panic. Every valid name is
    /// accepted with its own partition key, including an empty key and names
    /// that extend one another.
    #[test]
    fn received_lock_keys_are_checked_before_decoding() {
        let id =
            |scope: Option<&str>, name: &str| (scope.map(ReString::from), ReString::from(name));
        let encode = |scope, name| {
            let key = LocksTable::key(&id(scope, name)).unwrap();
            let mut bytes = Vec::new();
            key.serialize_to(&mut bytes);
            (bytes, key.partition_key)
        };
        for (scope, name) in [
            (None, "Cart/alice"),
            (Some("tenant-1"), "Cart/alice"),
            (None, "Svc/"),
            (None, "Svc/a"),
            (None, "Svc/a/b"),
            (None, "Svc/ab"),
            (Some("tenant-1"), "Svc/a"),
            (Some("tenant-1"), "Svc/ab"),
        ] {
            let (bytes, partition_key) = encode(scope, name);
            assert_eq!(
                LocksTable::check_key(&bytes).unwrap(),
                partition_key,
                "{scope:?} {name}"
            );
        }
        // IDs that name no lock: a query naming them scans instead.
        for (scope, name) in [
            (Some(""), "Cart/a"),
            (Some("bad scope"), "Cart/a"),
            (None, "noslash"),
            (None, "/key"),
        ] {
            assert!(
                LocksTable::key(&id(scope, name)).is_none(),
                "{scope:?} {name}"
            );
        }

        // Scoped: kind (2) | partition key (8) | `s` | length (4) | `tenant-1` | name.
        let (scoped, partition_key) = encode(Some("tenant-1"), "Cart/alice");
        let (unscoped, _) = encode(None, "Cart/alice");
        let header = |rest: &[u8]| {
            let mut bytes = KeyKind::Lock.as_bytes().to_vec();
            bytes.extend_from_slice(&partition_key.to_be_bytes());
            bytes.extend_from_slice(rest);
            bytes
        };
        let mut wrong_kind = scoped.clone();
        wrong_kind[..KeyKind::SERIALIZED_LENGTH].copy_from_slice(KeyKind::State.as_bytes());
        let mut unknown_tag = scoped.clone();
        unknown_tag[10] = b'x';
        let mut moved = scoped.clone();
        moved[9] ^= 1;
        for (case, bytes) in [
            ("another key kind", wrong_kind),
            ("short key kind", scoped[..1].to_vec()),
            ("no partition key", scoped[..2].to_vec()),
            ("short partition key", scoped[..9].to_vec()),
            ("no scope tag", scoped[..10].to_vec()),
            ("short scope length", scoped[..12].to_vec()),
            ("truncated scope", scoped[..17].to_vec()),
            ("name without a slash", unscoped[..15].to_vec()),
            ("unknown scope tag", unknown_tag),
            ("zero scope length", header(b"s\0\0\0\0Cart/alice")),
            (
                "oversized scope length",
                header(b"s\xff\xff\xff\xffCart/alice"),
            ),
            ("non-UTF-8 scope", header(b"s\0\0\0\x02\xff\xfeCart/alice")),
            ("invalid scope", header(b"s\0\0\0\x09bad scopeCart/alice")),
            ("non-UTF-8 name", header(b"uCart/\xff")),
            ("another partition key", moved),
        ] {
            assert!(LocksTable::check_key(&bytes).is_err(), "{case}");
        }
    }
}
