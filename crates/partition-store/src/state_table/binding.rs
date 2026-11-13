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

use std::ops::ControlFlow;

use restate_storage_query_api::schema::{RecordProgress, row_written};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_storage_query_api::tables::state::{
    State, StateClause, StateColumn, StateView, partition_keys,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};

use crate::keys::codecs::{DelimitedBytesCodec, OptionalScopeCodec};
use crate::keys::header::{PartitionKeyed, partition_key_envelope};
use crate::keys::selection::KeySelection;
use crate::keys::selection::codec::prepare_members;
use crate::{Result, break_on_err};

use super::{ScopedStateTable, StateTable};

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
    // The header accepts one interval; residuals retain gaps, cross-header
    // candidates of the suffix fields, and unsupported predicates.
    let request = match partition_key_envelope(&partition_keys(keys, scope, service_key)) {
        None => KeySelection::empty(),
        Some(range) => request.restrict_scope(range),
    };
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
}
