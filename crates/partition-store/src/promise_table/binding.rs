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

use std::num::NonZeroUsize;
use std::ops::ControlFlow;

use anyhow::{Context, ensure};
use bytestring::ByteString;

use restate_storage_api::StorageError;
use restate_storage_api::promise_table::Promise as StoredPromise;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_query_api::schema::{RecordProgress, row_written};
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision};
use restate_storage_query_api::tables::promise::{
    Promise, PromiseClause, PromiseColumn, PromiseDomains, PromiseView, named_promise_keys,
};
use restate_storage_query_api::tables::state::{ObjectKeyId, partition_keys};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::ServiceName;
use restate_types::identifiers::{PartitionKey, WithPartitionKey};

use crate::keys::codecs::{DelimitedBytesCodec, OptionalScopeCodec};
use crate::keys::header::{KeySchema, PartitionKeyHeader, PartitionKeyed, restrict_partition_keys};
use crate::keys::selection::codec::prepare_members;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{
    DecodeTableKey, EncodeTableKeyPrefix, FieldCodec, KeyDecode, KeyDecoder, KeyKind,
};
use crate::lookup::{LookupBinding, LookupTable, ensure_canonical, object_id, utf8_field};
use crate::table::{PhysicalTable, ReadMetadata};
use crate::{Result, break_on_err};

use super::{PromiseKey, ScopedPromiseKey, create_key};

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
    // Residuals retain gaps of non-finite key sets, cross-header candidates of
    // the suffix fields, and unsupported predicates.
    let request = restrict_partition_keys(request, &partition_keys(keys, scope, service_key));
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

/// A promise is named by its workflow's scope, service name and workflow ID, and
/// its key, as the `scope`, `service_name`, `service_key` and `key` columns show
/// them.
impl LookupTable for PromiseTable {
    type Id = ObjectKeyId;
    /// Smaller than for locks: completed promises hold their values. Not measured.
    const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

    /// `None` for a scoped promise: this layout holds unscoped workflows only.
    fn key((scope, service_name, service_key, key): &ObjectKeyId) -> Option<PromiseKey> {
        if scope.is_some() {
            return None;
        }
        let service_id = object_id(scope, service_name, service_key)?;
        Some(create_key(&service_id, &ByteString::from(key.as_str())))
    }

    fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
        let rest = bytes
            .strip_prefix(KeyKind::Promise.as_bytes())
            .context("not a promise key")?;
        let (stored, mut rest) = rest
            .split_first_chunk::<8>()
            .context("promise key without a partition key")?;
        let service_name = utf8_field(&mut rest, "service name")?;
        let service_key = utf8_field(&mut rest, "workflow ID")?;
        let key = utf8_field(&mut rest, "promise key")?;
        ensure!(rest.is_empty(), "promise key with trailing bytes");
        let key =
            Self::key(&(None, service_name, service_key, key)).context("invalid promise key")?;
        ensure_canonical(bytes, *stored, &key)?;
        Ok(key.partition_key)
    }
}

/// As [`PromiseTable`], for scoped and unscoped workflows alike.
impl LookupTable for ScopedPromiseTable {
    type Id = ObjectKeyId;
    /// Smaller than for locks: completed promises hold their values. Not measured.
    const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

    /// `None` for an invalid scope or an empty service name, which no key holds.
    fn key((scope, service_name, service_key, key): &ObjectKeyId) -> Option<ScopedPromiseKey> {
        if service_name.is_empty() {
            return None;
        }
        let service_id = object_id(scope, service_name, service_key)?;
        Some(ScopedPromiseKey {
            partition_key: service_id.partition_key(),
            scope: service_id.scope,
            // Not interned: interned names live as long as the process, and these
            // come from queries.
            service_name: ServiceName::new_non_interned(service_name),
            service_key: service_key.clone(),
            key: key.clone(),
        })
    }

    fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
        let rest = bytes
            .strip_prefix(KeyKind::ScopedPromise.as_bytes())
            .context("not a scoped promise key")?;
        let (stored, mut rest) = rest
            .split_first_chunk::<8>()
            .context("promise key without a partition key")?;
        let scope = OptionalScopeCodec::take_encoded(&mut rest)
            .and_then(OptionalScopeCodec::decode_encoded)
            .context("invalid promise scope")?;
        let service_name = utf8_field(&mut rest, "service name")?;
        let service_key = utf8_field(&mut rest, "workflow ID")?;
        let key = utf8_field(&mut rest, "promise key")?;
        ensure!(rest.is_empty(), "promise key with trailing bytes");
        let key =
            Self::key(&(scope, service_name, service_key, key)).context("invalid promise key")?;
        ensure_canonical(bytes, *stored, &key)?;
        Ok(key.partition_key)
    }
}

/// A branch that pins every key column to finitely many values can only match
/// their combinations, up to `MAX_LOOKUP_KEYS` of them; this layout holds
/// unscoped workflows only, so only the NULL scope can match.
impl LookupBinding for PromiseBinding {
    fn lookup_ids(domains: &PromiseDomains) -> Domain<ObjectKeyId> {
        named_promise_keys(
            domains.scope.clone().intersect(Domain::values([None])),
            domains,
        )
    }
}

/// As for [`PromiseBinding`], with every scope.
impl LookupBinding for ScopedPromiseBinding {
    fn lookup_ids(domains: &PromiseDomains) -> Domain<ObjectKeyId> {
        named_promise_keys(domains.scope.clone(), domains)
    }
}

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

    /// Complete promise keys are named by every combination of finite columns; the
    /// legacy layout keeps only the NULL scope. Received keys of either layout are
    /// checked before anything decodes them.
    #[test]
    fn complete_promise_keys_are_named_and_checked() {
        let texts = |values: &[&str]| Domain::values(values.iter().map(|v| ReString::from(*v)));
        let domains = PromiseDomains {
            scope: Domain::values([None, Some(ReString::from("tenant"))]),
            service_name: texts(&["Signup"]),
            service_key: texts(&["alice", "bob"]),
            key: texts(&["email"]),
            ..Default::default()
        };
        assert_eq!(
            ScopedPromiseBinding::lookup_ids(&domains)
                .into_values()
                .unwrap()
                .len(),
            4
        );
        let legacy = PromiseBinding::lookup_ids(&domains).into_values().unwrap();
        assert!(legacy.len() == 2 && legacy.iter().all(|(scope, ..)| scope.is_none()));

        fn encode(key: &impl EncodeTableKey) -> Vec<u8> {
            let mut bytes = Vec::new();
            key.serialize_to(&mut bytes);
            bytes
        }
        let id = |scope: Option<&str>| {
            (
                scope.map(ReString::from),
                ReString::from("Signup"),
                ReString::from("alice"),
                ReString::from("email"),
            )
        };
        let legacy = PromiseTable::key(&id(None)).unwrap();
        let legacy_bytes = encode(&legacy);
        assert_eq!(
            PromiseTable::check_key(&legacy_bytes).unwrap(),
            legacy.partition_key
        );
        assert!(PromiseTable::key(&id(Some("tenant"))).is_none());
        let scoped = ScopedPromiseTable::key(&id(Some("tenant"))).unwrap();
        let scoped_bytes = encode(&scoped);
        assert_eq!(
            ScopedPromiseTable::check_key(&scoped_bytes).unwrap(),
            scoped.partition_key
        );
        let mut moved = scoped_bytes.clone();
        moved[9] ^= 1;
        let mut trailing = legacy_bytes.clone();
        trailing.push(0);
        assert!(PromiseTable::check_key(&scoped_bytes).is_err());
        assert!(PromiseTable::check_key(&legacy_bytes[..9]).is_err());
        assert!(PromiseTable::check_key(&trailing).is_err());
        assert!(ScopedPromiseTable::check_key(&legacy_bytes).is_err());
        let error = ScopedPromiseTable::check_key(&moved).unwrap_err();
        assert!(
            error.to_string().contains("another partition key"),
            "{error}"
        );
        let unnamed = (None, ReString::default(), "alice".into(), "email".into());
        assert!(ScopedPromiseTable::key(&unnamed).is_none());
    }

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
