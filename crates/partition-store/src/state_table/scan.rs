// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical reads over existing state encodings. The partition-key header bounds
//! a read, and the legacy suffix fields (optional scope, then length-delimited
//! service name, service key, and state key) are declared as ordered fields: an
//! equality or IN restriction positions the iterator at matching objects instead
//! of reading every object that shares the partition key (e.g. all objects of one
//! scope). Their codec order is not string order, so ranges and prefixes remain
//! residual.

use restate_storage_query_api::Table;

use crate::Result;
use crate::keys::codecs::{DelimitedBytesCodec, OptionalScopeCodec};
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};
use crate::table::PhysicalTable;

use super::{ScopedStateKey, StateKey};

/// Declares a state table over one of the existing key types: the physical
/// table, its `kind | partition key` header and the legacy suffix fields as
/// ordered codecs, with the column tag each field binds to.
macro_rules! state_table {
    ($table:ident, $key:ty, $active:expr, fields [$($tag:literal: $codec:ty),+ $(,)?]) => {
        /// Physical state records using the existing key and value encodings.
        pub enum $table {}

        impl Table for $table {
            type Record<'a> = (KeyDecoder<'a, $key>, &'a [u8]);
            type Scan = KeySelection<$key>;
        }

        impl PhysicalTable for $table {
            type Key = $key;

            fn is_active(metadata: &crate::table::ReadMetadata<'_>) -> bool {
                ($active)(metadata)
            }
        }

        impl KeySchema for $key {
            type Header = PartitionKeyHeader<Self>;

            fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
                // Existing owned state keys include the partition key and their
                // decoder validates the key kind. Retain the borrowed full key.
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

state_table!(
    StateTable,
    StateKey,
    |metadata: &crate::table::ReadMetadata<'_>| !metadata
        .features
        .is_migrated_to_scoped_state_table,
    fields [
        "service_name": DelimitedBytesCodec,
        "service_key": DelimitedBytesCodec,
        "key": DelimitedBytesCodec,
    ]
);
state_table!(
    ScopedStateTable,
    ScopedStateKey,
    |_: &crate::table::ReadMetadata<'_>| true,
    fields [
        "scope": OptionalScopeCodec,
        "service_name": DelimitedBytesCodec,
        "service_key": DelimitedBytesCodec,
        "key": DelimitedBytesCodec,
    ]
);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tokio_util::sync::CancellationToken;

    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor};
    use restate_storage_query_api::selection::{Selection, ValuePredicate};
    use restate_storage_query_api::tables::state::{State, StateClause};
    use restate_types::Scope;
    use restate_types::identifiers::{PartitionKey, partitioner::HashPartitioner};
    use restate_types::partitions::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_util_string::ReString;

    use crate::keys::selection::codec::prepare_members;
    use crate::keys::{EncodeTableKey, KeyKind};
    use crate::{PartitionStoreManager, ReadSnapshot};

    use super::*;

    /// A read's clauses as the scoped state table prepares them: equality and
    /// membership on the key's fields, the rest residual.
    fn prepare(clauses: Vec<StateClause>) -> KeySelection<ScopedStateKey> {
        let selection = clauses
            .into_iter()
            .fold(Selection::<State>::All, Selection::and);
        KeySelection::for_key_fields(&selection, |clause, literals| {
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
                StateClause::Key(predicate) => {
                    prepare_members::<DelimitedBytesCodec, _>(predicate, literals, |key| {
                        key.as_deref().map(str::as_bytes)
                    })
                }
                _ => None,
            })
        })
        .unwrap()
    }

    type Found = (PartitionKey, String, String, String);

    /// Runs `request` through a cursor on `snapshot`: the records in the order the
    /// cursor yields them, and the keys it evaluated.
    fn read(snapshot: &ReadSnapshot, request: KeySelection<ScopedStateKey>) -> (Vec<Found>, u64) {
        let mut cursor = <ReadSnapshot as CursorScan<ScopedStateTable>>::scan_cursor(
            snapshot,
            Arc::new(request),
        )
        .unwrap();
        let mut found = Vec::new();
        loop {
            match cursor.advance().unwrap() {
                CursorStep::Record => {
                    let (decoder, _) = cursor.current().unwrap();
                    let key = decoder.decode_all().unwrap();
                    found.push((
                        key.partition_key,
                        key.service_name.to_string(),
                        key.service_key.to_string(),
                        String::from_utf8(key.state_key.to_vec()).unwrap(),
                    ));
                }
                CursorStep::Skipped => {}
                CursorStep::Done => break,
            }
        }
        (found, cursor.evaluated())
    }

    #[restate_core::test]
    async fn field_restrictions_seek_within_state_keys() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        // Two scopes, each under its own partition key, with two services of 16
        // objects and three state keys each.
        let tenants = ["tenant-a", "tenant-b"]
            .map(|tenant| (tenant, HashPartitioner::compute_partition_key(tenant)));
        let mut tx = store.transaction();
        for (tenant, partition_key) in tenants {
            for service in ["Cart", "Profile"] {
                for object in 0..16 {
                    for state in 0..3 {
                        let mut bytes = Vec::new();
                        let key = ScopedStateKey {
                            partition_key,
                            scope: Some(Scope::try_non_interned(tenant).unwrap()),
                            service_name: service.into(),
                            service_key: format!("o{object:02}").into(),
                            state_key: Bytes::from(format!("k{state}")),
                        };
                        EncodeTableKey::serialize_to(&key, &mut bytes);
                        tx.raw_put_cf(KeyKind::ScopedState, bytes, b"value");
                    }
                }
            }
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, _) = store.query_read(CancellationToken::new()).unwrap();
        let (tenant_a, tenant_b) = (tenants[0].1, tenants[1].1);
        let text = |value: &str| ValuePredicate::Equal(ReString::from(value));
        let scope = |tenant: &str| StateClause::Scope(ValuePredicate::Equal(Some(tenant.into())));
        let rows = |partition_key, service: &str, objects: &[usize]| -> Vec<Found> {
            objects
                .iter()
                .flat_map(|object| {
                    (0..3).map(move |state| {
                        (
                            partition_key,
                            service.to_owned(),
                            format!("o{object:02}"),
                            format!("k{state}"),
                        )
                    })
                })
                .collect()
        };

        // Over the partition's whole key range (a varying header), every leading
        // field fixed: the cursor seeks to the object and stops after it, carrying
        // past the other scope's partition key.
        let (found, evaluated) = read(
            &snapshot,
            prepare(vec![
                scope("tenant-a"),
                StateClause::ServiceName(text("Cart")),
                StateClause::ServiceKey(text("o07")),
            ]),
        );
        assert_eq!(found, rows(tenant_a, "Cart", &[7]));
        assert!(
            evaluated <= 8,
            "an object seeks within its service: {evaluated}"
        );

        // A membership restriction without a service name: positioned at the
        // objects of each service in turn.
        let (found, evaluated) = read(
            &snapshot,
            prepare(vec![
                scope("tenant-b"),
                StateClause::ServiceKey(ValuePredicate::In(vec!["o01".into(), "o09".into()])),
            ]),
        );
        let mut expected = rows(tenant_b, "Cart", &[1, 9]);
        expected.extend(rows(tenant_b, "Profile", &[1, 9]));
        assert_eq!(found, expected);
        assert!(
            evaluated <= 20,
            "objects seek within each service: {evaluated}"
        );

        // One partition key pinned (a fixed header): a service reads only its own
        // state, in key order.
        let (found, evaluated) = read(
            &snapshot,
            prepare(vec![StateClause::ServiceName(text("Profile"))])
                .restrict_scope(KeyRange::new(tenant_a, tenant_a)),
        );
        let objects: Vec<usize> = (0..16).collect();
        assert_eq!(found, rows(tenant_a, "Profile", &objects));
        assert!(
            evaluated <= 50,
            "a pinned header reads its service once: {evaluated}"
        );

        // A string range is not the legacy codec's order: it stays residual, so
        // the read visits every key of the scope.
        let (found, evaluated) = read(
            &snapshot,
            prepare(vec![
                scope("tenant-a"),
                StateClause::ServiceName(ValuePredicate::Range {
                    lower: std::ops::Bound::Excluded("C".into()),
                    upper: std::ops::Bound::Unbounded,
                }),
            ]),
        );
        assert_eq!((found.len(), evaluated), (96, 97), "ranges stay residual");
    }
}
