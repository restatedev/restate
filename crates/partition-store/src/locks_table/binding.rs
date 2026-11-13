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

use std::ops::ControlFlow;

use bilrost::OwnedMessage;

use restate_storage_api::StorageError;
use restate_storage_api::lock_table::LockState;
use restate_storage_query_api::schema::{RecordProgress, row_written};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_storage_query_api::tables::locks::{
    LockView, Locks, LocksClause, LocksColumn, partition_keys,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};

use crate::keys::codecs::{LockNameCodec, OptionalScopeCodec};
use crate::keys::header::{KeySchema, PartitionKeyHeader, partition_key_envelope};
use crate::keys::selection::codec::prepare_members;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder, KeyKind};
use crate::table::PhysicalTable;
use crate::{Result, break_on_err};

use super::LockKey;

/// Physical lock records using the existing key and value encodings.
pub enum LocksTable {}

impl Table for LocksTable {
    type Record<'a> = (KeyDecoder<'a, LockKey>, &'a [u8]);
    type Scan = KeySelection<LockKey>;
}

impl PhysicalTable for LocksTable {
    type Key = LockKey;
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
        // The header accepts one interval; residuals retain gaps, cross-header
        // candidates of the suffix fields, and unsupported predicates.
        let request =
            match partition_key_envelope(&partition_keys(keys, scope, lock_name, acquired_by)) {
                None => KeySelection::empty(),
                Some(range) => request.restrict_scope(range),
            };
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
}
