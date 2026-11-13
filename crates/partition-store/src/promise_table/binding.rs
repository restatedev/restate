// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query reads of promises over both stored layouts. Neither key is ordered by
//! its logical field values (see the module docs), but each field has a canonical,
//! prefix-free legacy encoding: equality and IN on scope, service name, workflow
//! ID, and promise key position the iterator, while ranges and prefixes remain
//! residual. The legacy layout is read until its migration completes.

use std::ops::ControlFlow;

use restate_storage_api::StorageError;
use restate_storage_api::promise_table::Promise as StoredPromise;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_query_api::schema::{RecordProgress, row_written};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_storage_query_api::tables::promise::{
    Promise, PromiseClause, PromiseColumn, PromiseView,
};
use restate_storage_query_api::tables::state::partition_keys;
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};

use crate::keys::codecs::{DelimitedBytesCodec, OptionalScopeCodec};
use crate::keys::header::{KeySchema, PartitionKeyHeader, PartitionKeyed, partition_key_envelope};
use crate::keys::selection::codec::prepare_members;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};
use crate::table::{PhysicalTable, ReadMetadata};
use crate::{Result, break_on_err};

use super::{PromiseKey, ScopedPromiseKey};

macro_rules! promise_table {
    (
        $(#[$meta:meta])* $table:ident, $key:ty, $active:expr,
        fields [$($tag:literal: $codec:ty),+ $(,)?]
    ) => {
        $(#[$meta])*
        pub enum $table {}

        impl Table for $table {
            type Record<'a> = (KeyDecoder<'a, $key>, &'a [u8]);
            type Scan = KeySelection<$key>;
        }

        impl PhysicalTable for $table {
            type Key = $key;

            fn is_active(metadata: &ReadMetadata<'_>) -> bool {
                ($active)(metadata)
            }
        }

        impl KeySchema for $key {
            type Header = PartitionKeyHeader<Self>;

            fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
                // The existing owned key includes its partition key. Retain the full key.
                Ok(KeyDecoder::from_payload(key))
            }
        }

        impl KeyDecode for $key {
            fn decode(source: &mut &[u8]) -> Result<Self> {
                Self::deserialize_from(source)
            }
        }

        impl OrderedKeySchema for $key {
            const KEY_KIND: KeyKind = <$key as EncodeTableKeyPrefix>::KEY_KIND;
            const FIELDS: &'static [OrderedKeyField] =
                &[$(OrderedKeyField::new::<$codec>($tag)),+];
        }
    };
}

promise_table!(
    /// Promises of unscoped workflows, in the layout before scoped promises.
    PromiseTable,
    PromiseKey,
    |metadata: &ReadMetadata<'_>| !metadata.features.is_migrated_to_scoped_promise_table,
    fields [
        "service_name": DelimitedBytesCodec,
        "service_key": DelimitedBytesCodec,
        "key": DelimitedBytesCodec,
    ]
);
promise_table!(
    /// Promises in the scoped layout.
    ScopedPromiseTable,
    ScopedPromiseKey,
    |_: &ReadMetadata<'_>| true,
    fields [
        "scope": OptionalScopeCodec,
        "service_name": DelimitedBytesCodec,
        "service_key": DelimitedBytesCodec,
        "key": DelimitedBytesCodec,
    ]
);

/// Narrows the header to the partition keys of the requested scopes and workflow IDs.
/// `unscoped` marks the legacy layout, whose records all have a NULL scope: its
/// header then follows from the workflow ID alone.
fn prepare<K: PartitionKeyed>(
    selection: &Selection<Promise>,
    unscoped: bool,
) -> Result<(KeySelection<K>, SelectionPrecision)> {
    let request = KeySelection::for_key_fields(selection, |clause, literals| {
        Ok(match clause {
            PromiseClause::Scope(predicate) => {
                prepare_members::<OptionalScopeCodec, _>(predicate, literals, |scope| {
                    Some(scope.as_deref())
                })
            }
            PromiseClause::ServiceName(predicate)
            | PromiseClause::ServiceKey(predicate)
            | PromiseClause::Key(predicate) => {
                prepare_members::<DelimitedBytesCodec, _>(predicate, literals, |value| {
                    Some(value.as_bytes())
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
    for column in [
        PromiseColumn::PartitionKey,
        PromiseColumn::Scope,
        PromiseColumn::ServiceKey,
    ] {
        for clause in columns.for_column(column) {
            match clause {
                PromiseClause::PartitionKey(predicate) => keys = keys.intersect(predicate.into()),
                PromiseClause::Scope(predicate) => scope = scope.intersect(predicate.into()),
                PromiseClause::ServiceKey(predicate) => {
                    service_key = service_key.intersect(predicate.into())
                }
                _ => {}
            }
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

macro_rules! promise_binding {
    (
        $binding:ident, $table:ty, unscoped: $unscoped:expr,
        |$key:ident, $promise:ident, $emit:ident| $materialize:expr
    ) => {
        pub enum $binding {}

        impl SchemaBinding for $binding {
            type Schema = Promise;
            type Table = $table;
            type Materializer = ();

            fn materialize_next(
                record: <Self::Table as Table>::Record<'_>,
                _: &mut (),
                emit: &mut impl RowSink<Self::Schema>,
            ) -> Result<RecordProgress> {
                row_written(Self::materialize_projected(record, emit))?;
                Ok(RecordProgress::Done)
            }

            fn prepare(selection: &Selection<Promise>) -> Result<PreparedRead<$table>> {
                let (request, precision) = prepare(selection, $unscoped)?;
                Ok(PreparedRead::new(request, precision))
            }

            fn materialize(
                (key, mut value): <Self::Table as Table>::Record<'_>,
                $emit: &mut impl for<'a> FnMut(PromiseView<'a>) -> ControlFlow<Result<()>>,
            ) -> ControlFlow<Result<()>> {
                let $key = break_on_err(key.decode_all())?;
                let $promise = break_on_err(StoredPromise::decode(&mut value))?;
                $materialize
            }
        }
    };
}

promise_binding!(PromiseBinding, PromiseTable, unscoped: true, |key, promise, emit| {
    // As in V1, a legacy workflow ID that is not UTF-8 fails the read.
    let service_key = break_on_err(std::str::from_utf8(&key.service_key).map_err(|error| {
        StorageError::Generic(anyhow::anyhow!("Cannot convert to string {error}"))
    }))?;
    emit(PromiseView {
        partition_key: key.partition_key,
        scope: None,
        service_name: &key.service_name,
        service_key,
        key: &key.key,
        promise: &promise,
    })
});
promise_binding!(
    ScopedPromiseBinding,
    ScopedPromiseTable,
    unscoped: false,
    |key, promise, emit| {
        emit(PromiseView {
            partition_key: key.partition_key,
            scope: key.scope.as_ref().map(|scope| scope.as_str()),
            service_name: key.service_name.as_str(),
            service_key: &key.service_key,
            key: &key.key,
            promise: &promise,
        })
    }
);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use restate_storage_query_api::selection::ValuePredicate;
    use restate_types::Scope;
    use restate_types::identifiers::partitioner::HashPartitioner;
    use restate_types::sharding::KeyRange;
    use restate_util_string::ReString;

    use crate::keys::EncodeTableKey;
    use crate::table::navigate;

    use super::*;

    #[test]
    fn natural_keys_position_promise_reads() {
        // 64 workflows of four promises each, all in one scope.
        let tenant = HashPartitioner::compute_partition_key("tenant");
        let mut scoped: Vec<_> = (0..64)
            .flat_map(|workflow| (0..4).map(move |promise| (workflow, promise)))
            .map(|(workflow, promise)| {
                let mut bytes = Vec::new();
                let key = ScopedPromiseKey {
                    partition_key: tenant,
                    scope: Some(Scope::try_non_interned("tenant").unwrap()),
                    service_name: "Signup".into(),
                    service_key: format!("w{workflow}").into(),
                    key: format!("p{promise}").into(),
                };
                EncodeTableKey::serialize_to(&key, &mut bytes);
                bytes
            })
            .collect();
        scoped.sort();
        let text = |value: &str| ValuePredicate::Equal(ReString::from(value));
        let scope = || PromiseClause::Scope(ValuePredicate::Equal(Some("tenant".into())));
        let workflow = || {
            Selection::All
                .and(scope())
                .and(PromiseClause::ServiceName(text("Signup")))
                .and(PromiseClause::ServiceKey(text("w7")))
        };
        let visit = |selection: Selection<Promise>| {
            let prepared = ScopedPromiseBinding::prepare(&selection).unwrap();
            let (matches, visited) =
                navigate(Arc::clone(prepared.request()), KeyRange::FULL, &scoped);
            (matches.len(), visited)
        };
        assert_eq!(
            visit(workflow()),
            (4, 4),
            "a_workflow_reads_only_its_promises"
        );
        assert_eq!(
            visit(workflow().and(PromiseClause::Key(text("p2")))),
            (1, 1),
            "a_promise_key_reads_one_promise"
        );
        assert_eq!(
            visit(Selection::All.and(scope())),
            (256, 256),
            "scope_only_reads_the_header"
        );

        // Legacy keys of one workflow ID share a partition key across services.
        let alice = HashPartitioner::compute_partition_key("alice");
        let mut legacy: Vec<_> = ["Checkout", "Signup"]
            .into_iter()
            .flat_map(|service| (0..4).map(move |promise| (service, promise)))
            .map(|(service, promise)| {
                let mut bytes = Vec::new();
                let key = PromiseKey {
                    partition_key: alice,
                    service_name: service.into(),
                    service_key: Bytes::from_static(b"alice"),
                    key: format!("p{promise}").into(),
                };
                EncodeTableKey::serialize_to(&key, &mut bytes);
                bytes
            })
            .collect();
        legacy.sort();
        let selection = Selection::All
            .and(PromiseClause::ServiceName(text("Signup")))
            .and(PromiseClause::ServiceKey(text("alice")));
        let prepared = PromiseBinding::prepare(&selection).unwrap();
        let (matches, visited) = navigate(Arc::clone(prepared.request()), KeyRange::FULL, &legacy);
        assert_eq!(
            (matches.len(), visited),
            (4, 4),
            "legacy_headers_and_service_names_seek"
        );
    }
}
