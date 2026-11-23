// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query reads of user state over both stored layouts. Neither key is ordered by
//! its logical field values (see the module docs of `state_table`), but each
//! field has a canonical, prefix-free legacy encoding: equality and IN on scope,
//! service name, service key and state key position the iterator, while ranges
//! and prefixes remain residual. The unscoped layout is read until its migration
//! completes.

use std::num::NonZeroUsize;
use std::ops::ControlFlow;

use anyhow::{Context, ensure};
use bytes::Bytes;

use restate_storage_query_api::schema::{RecordProgress, row_written};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_storage_query_api::tables::state::{
    ObjectKeyId, State, StateClause, StateColumn, StateDomains, StateView, named_state_keys,
    partition_keys,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::ServiceName;
use restate_types::identifiers::{PartitionKey, WithPartitionKey};

use crate::keys::codecs::{DelimitedBytesCodec, OptionalScopeCodec};
use crate::keys::header::{PartitionKeyed, restrict_partition_keys};
use crate::keys::selection::KeySelection;
use crate::keys::selection::codec::prepare_members;
use crate::keys::{FieldCodec, KeyKind};
use crate::lookup::{LookupBinding, LookupTable, ensure_canonical, object_id, utf8_field};
use crate::{Result, break_on_err};

use super::{ScopedStateKey, ScopedStateTable, StateKey, StateTable, write_state_entry_key};

/// `unscoped` marks the legacy layout, whose records all have a NULL scope: its
/// header then follows from the service key alone.
fn prepare<K: PartitionKeyed>(
    selection: &Selection<State>,
    unscoped: bool,
) -> Result<(KeySelection<K>, SelectionPrecision)> {
    let request = KeySelection::for_key_fields(selection, |clause, literals| {
        Ok(match clause {
            StateClause::Scope(predicate) => {
                prepare_members::<OptionalScopeCodec, _>(predicate, literals, |scope| {
                    Some(scope.as_deref())
                })
            }
            StateClause::ServiceName(predicate) | StateClause::ServiceKey(predicate) => {
                prepare_members::<DelimitedBytesCodec, _>(predicate, literals, |value| {
                    Some(value.as_bytes())
                })
            }
            // A NULL key is a non-UTF-8 state key: no literal names its bytes.
            StateClause::Key(predicate) => {
                prepare_members::<DelimitedBytesCodec, _>(predicate, literals, |key| {
                    key.as_deref().map(str::as_bytes)
                })
            }
            _ => None,
        })
    })?;
    let columns = match selection {
        Selection::All | Selection::Empty => return Ok((request, SelectionPrecision::Exact)),
        Selection::Predicates(columns) => columns,
    };
    let mut keys = Domain::all();
    let mut scope = if unscoped {
        Domain::values([None])
    } else {
        Domain::all()
    };
    let mut service_key = Domain::all();
    for clause in columns.for_column(StateColumn::PartitionKey) {
        if let StateClause::PartitionKey(predicate) = clause {
            keys = keys.intersect(predicate.into());
        }
    }
    for clause in columns.for_column(StateColumn::Scope) {
        if let StateClause::Scope(predicate) = clause {
            scope = scope.intersect(predicate.into());
        }
    }
    for clause in columns.for_column(StateColumn::ServiceKey) {
        if let StateClause::ServiceKey(predicate) = clause {
            service_key = service_key.intersect(predicate.into());
        }
    }
    // Residuals retain gaps of non-finite key sets, cross-header candidates of
    // the suffix fields, and unsupported predicates.
    let request = restrict_partition_keys(request, &partition_keys(keys, scope, service_key));
    Ok((request, SelectionPrecision::Inexact))
}

pub enum StateBinding {}
pub enum ScopedStateBinding {}

impl SchemaBinding for StateBinding {
    type Schema = State;
    type Table = StateTable;
    type Materializer = ();

    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        _: &mut (),
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress> {
        row_written(Self::materialize_projected(record, emit))?;
        Ok(RecordProgress::Done)
    }

    fn prepare(selection: &Selection<State>) -> Result<PreparedRead<StateTable>> {
        let (request, precision) = prepare(selection, true)?;
        Ok(PreparedRead::new(request, precision))
    }

    fn materialize(
        (key, value): <StateTable as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(StateView<'a>) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        let key = break_on_err(key.decode_all())?;
        emit(StateView {
            partition_key: key.partition_key,
            scope: None,
            service_name: &key.service_name,
            service_key: &key.service_key,
            key: &key.state_key,
            value,
        })
    }
}

impl SchemaBinding for ScopedStateBinding {
    type Schema = State;
    type Table = ScopedStateTable;
    type Materializer = ();

    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        _: &mut (),
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress> {
        row_written(Self::materialize_projected(record, emit))?;
        Ok(RecordProgress::Done)
    }

    fn prepare(selection: &Selection<State>) -> Result<PreparedRead<ScopedStateTable>> {
        let (request, precision) = prepare(selection, false)?;
        Ok(PreparedRead::new(request, precision))
    }

    fn materialize(
        (key, value): <ScopedStateTable as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(StateView<'a>) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        let key = break_on_err(key.decode_all())?;
        emit(StateView {
            partition_key: key.partition_key,
            scope: key.scope.as_ref().map(|scope| scope.as_str()),
            service_name: key.service_name.as_str(),
            service_key: key.service_key.as_str(),
            key: &key.state_key,
            value,
        })
    }
}

/// A state key is named by its scope, service name, service key and UTF-8 state
/// key, as the `scope`, `service_name`, `service_key` and `key` columns show them.
impl LookupTable for StateTable {
    type Id = ObjectKeyId;
    /// Smaller than for locks: state values can be large. Not measured.
    const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

    /// `None` for a scoped key: this layout holds unscoped state only.
    fn key((scope, service_name, service_key, key): &ObjectKeyId) -> Option<StateKey> {
        if scope.is_some() {
            return None;
        }
        let service_id = object_id(scope, service_name, service_key)?;
        Some(write_state_entry_key(
            &service_id,
            &Bytes::copy_from_slice(key.as_bytes()),
        ))
    }

    fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
        let rest = bytes
            .strip_prefix(KeyKind::State.as_bytes())
            .context("not a state key")?;
        let (stored, mut rest) = rest
            .split_first_chunk::<8>()
            .context("state key without a partition key")?;
        let service_name = utf8_field(&mut rest, "service name")?;
        let service_key = utf8_field(&mut rest, "service key")?;
        let key = utf8_field(&mut rest, "state key")?;
        ensure!(rest.is_empty(), "state key with trailing bytes");
        let key =
            Self::key(&(None, service_name, service_key, key)).context("invalid state key")?;
        ensure_canonical(bytes, *stored, &key)?;
        Ok(key.partition_key)
    }
}

/// As [`StateTable`], for scoped and unscoped state alike.
impl LookupTable for ScopedStateTable {
    type Id = ObjectKeyId;
    /// Smaller than for locks: state values can be large. Not measured.
    const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

    /// `None` for an invalid scope or an empty service name, which no key holds.
    fn key((scope, service_name, service_key, key): &ObjectKeyId) -> Option<ScopedStateKey> {
        if service_name.is_empty() {
            return None;
        }
        let service_id = object_id(scope, service_name, service_key)?;
        Some(ScopedStateKey {
            partition_key: service_id.partition_key(),
            scope: service_id.scope,
            // Not interned: interned names live as long as the process, and these
            // come from queries.
            service_name: ServiceName::new_non_interned(service_name),
            service_key: service_key.clone(),
            state_key: Bytes::copy_from_slice(key.as_bytes()),
        })
    }

    fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
        let rest = bytes
            .strip_prefix(KeyKind::ScopedState.as_bytes())
            .context("not a scoped state key")?;
        let (stored, mut rest) = rest
            .split_first_chunk::<8>()
            .context("state key without a partition key")?;
        let scope = OptionalScopeCodec::take_encoded(&mut rest)
            .and_then(OptionalScopeCodec::decode_encoded)
            .context("invalid state scope")?;
        let service_name = utf8_field(&mut rest, "service name")?;
        let service_key = utf8_field(&mut rest, "service key")?;
        let key = utf8_field(&mut rest, "state key")?;
        ensure!(rest.is_empty(), "state key with trailing bytes");
        let key =
            Self::key(&(scope, service_name, service_key, key)).context("invalid state key")?;
        ensure_canonical(bytes, *stored, &key)?;
        Ok(key.partition_key)
    }
}

/// A branch that pins every key column to finitely many values can only match
/// their combinations, up to `MAX_LOOKUP_KEYS` of them; this layout holds
/// unscoped state only, so only the NULL scope can match.
impl LookupBinding for StateBinding {
    fn lookup_ids(domains: &StateDomains) -> Domain<ObjectKeyId> {
        named_state_keys(
            domains.scope.clone().intersect(Domain::values([None])),
            domains,
        )
    }
}

/// As for [`StateBinding`], with every scope.
impl LookupBinding for ScopedStateBinding {
    fn lookup_ids(domains: &StateDomains) -> Domain<ObjectKeyId> {
        named_state_keys(domains.scope.clone(), domains)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use bytes::Bytes;

    use restate_storage_query_api::selection::ValuePredicate;
    use restate_types::Scope;
    use restate_types::identifiers::partitioner::HashPartitioner;
    use restate_types::sharding::KeyRange;
    use restate_util_string::ReString;

    use crate::keys::EncodeTableKey;
    use crate::state_table::{ScopedStateKey, StateKey};
    use crate::table::navigate;

    use super::*;

    #[test]
    fn natural_keys_position_state_reads() {
        // Two services with 64 objects of four state keys each, all in one scope
        // and therefore under one partition key.
        let tenant = HashPartitioner::compute_partition_key("tenant");
        let mut rows = Vec::new();
        for service in ["Cart", "Profile"] {
            for object in 0..64 {
                for state in 0..4 {
                    let key = ScopedStateKey {
                        partition_key: tenant,
                        scope: Some(Scope::try_non_interned("tenant").unwrap()),
                        service_name: service.into(),
                        service_key: format!("o{object}").into(),
                        state_key: Bytes::from(format!("k{state}")),
                    };
                    let mut bytes = Vec::new();
                    key.serialize_to(&mut bytes);
                    rows.push((bytes, (service, object, state)));
                }
            }
        }
        rows.sort();
        let keys: Vec<_> = rows.iter().map(|(key, _)| key).collect();
        let read = |clauses: Vec<StateClause>| {
            let selection = clauses.into_iter().fold(Selection::All, Selection::and);
            let prepared = ScopedStateBinding::prepare(&selection).unwrap();
            let (matches, visited) =
                navigate(Arc::clone(prepared.request()), KeyRange::FULL, &keys);
            let mut found: Vec<_> = matches.into_iter().map(|index| rows[index].1).collect();
            found.sort_unstable();
            (found, visited)
        };
        let text = |value: &str| ValuePredicate::Equal(ReString::from(value));
        let scope = || StateClause::Scope(ValuePredicate::Equal(Some("tenant".into())));

        let (found, visited) = read(vec![
            scope(),
            StateClause::ServiceName(text("Cart")),
            StateClause::ServiceKey(text("o7")),
        ]);
        assert_eq!(
            found,
            (0..4).map(|state| ("Cart", 7, state)).collect::<Vec<_>>()
        );
        assert_eq!(visited, 4, "an_object_reads_only_its_state");

        let (found, visited) = read(vec![
            scope(),
            StateClause::ServiceName(text("Cart")),
            StateClause::ServiceKey(text("o7")),
            StateClause::Key(ValuePredicate::Equal(Some("k2".into()))),
        ]);
        assert_eq!(
            (found, visited),
            (vec![("Cart", 7, 2)], 1),
            "a_state_key_reads_one_key"
        );

        // Without a service name, each service is positioned at the objects.
        let (found, visited) = read(vec![
            scope(),
            StateClause::ServiceKey(ValuePredicate::In(vec!["o1".into(), "o9".into()])),
        ]);
        assert_eq!(found.len(), 16);
        assert!(visited <= 24, "objects_seek_within_each_service: {visited}");

        // A string range is not codec order: it stays residual.
        let (found, visited) = read(vec![
            scope(),
            StateClause::ServiceName(ValuePredicate::Range {
                lower: Bound::Excluded("C".into()),
                upper: Bound::Unbounded,
            }),
        ]);
        assert_eq!(
            (found.len(), visited),
            (512, 512),
            "service_name_ranges_read_the_header"
        );

        // Legacy unscoped keys: objects of one key share a partition key across
        // services; a service name skips the other service's state.
        let alice = HashPartitioner::compute_partition_key("alice");
        let mut legacy: Vec<_> = ["Cart", "Profile"]
            .into_iter()
            .flat_map(|service| (0..4).map(move |state| (service, state)))
            .map(|(service, state)| {
                let mut bytes = Vec::new();
                StateKey {
                    partition_key: alice,
                    service_name: service.into(),
                    service_key: "alice".into(),
                    state_key: Bytes::from(format!("k{state}")),
                }
                .serialize_to(&mut bytes);
                bytes
            })
            .collect();
        legacy.sort();
        let selection = Selection::All
            .and(StateClause::ServiceName(text("Profile")))
            .and(StateClause::ServiceKey(text("alice")));
        let prepared = StateBinding::prepare(&selection).unwrap();
        let (matches, visited) = navigate(Arc::clone(prepared.request()), KeyRange::FULL, &legacy);
        assert_eq!(
            (matches.len(), visited),
            (4, 4),
            "legacy_service_names_seek"
        );
    }

    /// Complete state keys are named by every combination of finitely many scopes,
    /// service names, service keys and state keys, up to the cap; the legacy layout
    /// keeps only the NULL scope. Received keys of either layout are checked field
    /// by field before anything decodes them.
    #[test]
    fn complete_state_keys_are_named_and_checked() {
        use restate_storage_query_api::tables::state::MAX_LOOKUP_KEYS;

        let texts = |values: &[&str]| Domain::values(values.iter().map(|v| ReString::from(*v)));
        let mut domains = StateDomains {
            scope: Domain::values([None, Some(ReString::from("tenant"))]),
            service_name: texts(&["Counter"]),
            service_key: texts(&["a", "b"]),
            key: Domain::values([Some(ReString::from("count"))]),
            ..Default::default()
        };
        let scoped = ScopedStateBinding::lookup_ids(&domains)
            .into_values()
            .unwrap();
        assert_eq!(scoped.len(), 4);
        let legacy = StateBinding::lookup_ids(&domains).into_values().unwrap();
        assert_eq!(legacy.len(), 2);
        assert!(legacy.iter().all(|(scope, ..)| scope.is_none()));
        // A NULL state key names no bytes: the alternative is read instead.
        domains.key = Domain::values([None, Some(ReString::from("count"))]);
        assert!(ScopedStateBinding::lookup_ids(&domains).is_all());
        // 100 × 100 is the cap; one more service key makes the alternative read.
        let many = |n| Domain::values((0..n).map(|i| ReString::from(format!("k{i}"))));
        domains.scope = Domain::values([None]);
        domains.service_key = many(100);
        domains.key = Domain::values((0..100).map(|i| Some(ReString::from(format!("s{i}")))));
        let ids = ScopedStateBinding::lookup_ids(&domains)
            .into_values()
            .unwrap();
        assert_eq!(ids.len(), MAX_LOOKUP_KEYS);
        domains.service_key = many(101);
        assert!(ScopedStateBinding::lookup_ids(&domains).is_all());

        // Round trips: each key is accepted with the partition key it's stored under.
        let id = |scope: Option<&str>, key: &str| {
            (
                scope.map(ReString::from),
                ReString::from("Counter"),
                ReString::from("alice"),
                ReString::from(key),
            )
        };
        fn encode(key: &impl EncodeTableKey) -> Vec<u8> {
            let mut bytes = Vec::new();
            key.serialize_to(&mut bytes);
            bytes
        }
        let unscoped = StateTable::key(&id(None, "count")).unwrap();
        let unscoped_bytes = encode(&unscoped);
        assert_eq!(
            StateTable::check_key(&unscoped_bytes).unwrap(),
            unscoped.partition_key
        );
        assert!(StateTable::key(&id(Some("tenant"), "count")).is_none());
        let scoped = ScopedStateTable::key(&id(Some("tenant"), "count")).unwrap();
        let scoped_bytes = encode(&scoped);
        assert_eq!(
            ScopedStateTable::check_key(&scoped_bytes).unwrap(),
            scoped.partition_key
        );
        let null_scope = ScopedStateTable::key(&id(None, "")).unwrap();
        assert_eq!(
            ScopedStateTable::check_key(&encode(&null_scope)).unwrap(),
            null_scope.partition_key
        );
        assert!(ScopedStateTable::key(&id(Some("bad scope"), "count")).is_none());
        let unnamed = (None, ReString::default(), "alice".into(), "count".into());
        assert!(ScopedStateTable::key(&unnamed).is_none());

        // Malformed keys are errors, never panics.
        let mut moved = scoped_bytes.clone();
        moved[9] ^= 1;
        let raw = StateKey {
            state_key: Bytes::from_static(&[0xff]),
            ..unscoped
        };
        let mut trailing = unscoped_bytes.clone();
        trailing.push(0);
        for (case, bytes) in [
            ("another key kind", scoped_bytes.clone()),
            ("short partition key", unscoped_bytes[..9].to_vec()),
            (
                "truncated field",
                unscoped_bytes[..unscoped_bytes.len() - 1].to_vec(),
            ),
            ("non-UTF-8 state key", encode(&raw)),
            ("trailing bytes", trailing),
        ] {
            assert!(StateTable::check_key(&bytes).is_err(), "{case}");
        }
        for (case, bytes) in [
            ("another key kind", unscoped_bytes),
            ("truncated scope", scoped_bytes[..13].to_vec()),
        ] {
            assert!(ScopedStateTable::check_key(&bytes).is_err(), "{case}");
        }
        // Canonical, but under a partition key its identity doesn't derive.
        let error = ScopedStateTable::check_key(&moved).unwrap_err();
        assert!(
            error.to_string().contains("another partition key"),
            "{error}"
        );
    }

    /// The UI's object listing: the state of a few objects of one service, named
    /// by their keys, with a NULL scope. Each object has its own partition key,
    /// spread over the key space among other objects of this and other services.
    /// The read visits the requested objects' partition keys only, however far
    /// apart they are, and an owner reads only the ones it owns.
    ///
    /// Service names encode with their length first, so under each object's key
    /// "Other" sorts before "Counter" (a seek skips it) and "CounterArchive"
    /// after it (the read moves on to the next listed key).
    #[test]
    fn object_lists_visit_only_their_partition_keys() {
        let mut rows = Vec::new();
        for service in ["Counter", "CounterArchive", "Other"] {
            for object in 0..200 {
                let service_key = format!("{object}");
                for state in 0..2 {
                    let mut bytes = Vec::new();
                    StateKey {
                        partition_key: HashPartitioner::compute_partition_key(&service_key),
                        service_name: service.into(),
                        service_key: service_key.clone().into(),
                        state_key: Bytes::from(format!("k{state}")),
                    }
                    .serialize_to(&mut bytes);
                    rows.push((bytes, (service, object, state)));
                }
            }
        }
        rows.sort();
        let keys: Vec<_> = rows.iter().map(|(key, _)| key).collect();
        let objects = [3, 57, 101, 150, 199];
        let selection = Selection::All
            .and(StateClause::ServiceName(ValuePredicate::Equal(
                "Counter".into(),
            )))
            .and(StateClause::ServiceKey(ValuePredicate::In(
                objects
                    .iter()
                    .map(|object| format!("{object}").into())
                    .collect(),
            )))
            .and(StateClause::Scope(ValuePredicate::Equal(None)));
        let prepared = StateBinding::prepare(&selection).unwrap();
        let read = |range| {
            let (matches, visited) = navigate(Arc::clone(prepared.request()), range, &keys);
            let mut found: Vec<_> = matches.into_iter().map(|index| rows[index].1).collect();
            found.sort_unstable();
            (found, visited)
        };

        let (found, visited) = read(KeyRange::FULL);
        let expected: Vec<_> = objects
            .iter()
            .flat_map(|&object| (0..2).map(move |state| ("Counter", object, state)))
            .collect();
        assert_eq!(found, expected);
        // Each object's partition key holds two state keys under each service.
        // The read visits the first object's "Other" key, then per object the
        // two "Counter" keys and one "CounterArchive" key, from which it seeks
        // to the next object's "Counter" keys: 16 visits against 1,200 keys.
        assert_eq!(visited, 3 * objects.len() + 1, "visits only the objects");

        // An owner of half the key space reads the requested objects it owns.
        let middle = u64::MAX / 2;
        let (found, _) = read(KeyRange::new(0, middle));
        let owned: Vec<_> = expected
            .iter()
            .copied()
            .filter(|(_, object, _)| {
                HashPartitioner::compute_partition_key(format!("{object}")) <= middle
            })
            .collect();
        assert!(!owned.is_empty() && owned.len() < expected.len());
        assert_eq!(found, owned);
    }
}
