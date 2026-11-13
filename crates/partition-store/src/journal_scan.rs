// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical journal and journal-event reads for queries.
//!
//! Both journal layouts store `kind | partition_key | invocation_uuid | journal_index`,
//! with the UUID as 16 raw bytes and the index as a big-endian `u32`; events store
//! `kind | partition_key | invocation_uuid | event_type | timestamp | lsn`, all
//! fixed width. These suffixes are mem-comparable, so they are also declared as
//! ordered fields without changing the stored format: an invocation-ID restriction
//! positions the iterator at that invocation's records, and a journal-index
//! restriction positions it within them.
//!
//! For example, `id = X` restricts the header to `X`'s partition key and the UUID
//! field to `X`'s UUID. Other invocations sharing that partition key (all
//! invocations of one virtual-object key) are skipped by seeking, not read.

use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision, ValuePredicate};
use restate_storage_query_api::tables::journal::{Journal, JournalClause, JournalColumn};
use restate_storage_query_api::tables::journal_events::{
    JournalEvents, JournalEventsClause, JournalEventsColumn,
};
use restate_storage_query_api::{PreparedRead, Table};
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};

use crate::Result;
use crate::keys::codecs::{InvocationUuidCodec, U8Codec, U32Codec, U64Codec};
use crate::keys::header::{KeySchema, PartitionKeyHeader, PartitionKeyed, prepare_partition_keyed};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};
use crate::table::PhysicalTable;

macro_rules! journal_table {
    (
        $(#[$meta:meta])* $table:ident, $key:ty, $schema:ty, $prepare:path,
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
        }

        impl $table {
            /// Prepares the key restrictions for a logical selection.
            pub fn prepare(selection: &Selection<$schema>) -> Result<PreparedRead<Self>> {
                let (request, precision) = $prepare(selection)?;
                Ok(PreparedRead::new(request, precision))
            }
        }

        impl KeySchema for $key {
            type Header = PartitionKeyHeader<Self>;

            fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
                // The existing owned key includes the partition key that the
                // invocation ID needs. Retain the borrowed full key.
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

journal_table!(
    /// Journal entries of service protocol versions below 4.
    JournalTable, crate::journal_table::JournalKey, Journal, prepare_journal,
    fields ["id": InvocationUuidCodec, "index": U32Codec]
);
journal_table!(
    /// Journal entries of service protocol version 4 and later.
    JournalV2Table, crate::journal_table_v2::JournalKey, Journal, prepare_journal,
    fields ["id": InvocationUuidCodec, "index": U32Codec]
);
journal_table!(
    /// Invocation events. Only `id` binds a logical column; the remaining fields
    /// are declared so key evaluation can find their boundaries.
    JournalEventsTable, crate::journal_events::JournalEventKey, JournalEvents, prepare_events,
    fields [
        "id": InvocationUuidCodec,
        "event_type_code": U8Codec,
        "timestamp": U64Codec,
        "lsn": U64Codec,
    ]
);

fn prepare_journal<K: PartitionKeyed>(
    selection: &Selection<Journal>,
) -> Result<(KeySelection<K>, SelectionPrecision)> {
    prepare_partition_keyed(
        selection,
        &[JournalColumn::PartitionKey, JournalColumn::Id],
        |clause| match clause {
            JournalClause::PartitionKey(predicate) => Some(predicate.into()),
            JournalClause::Id(predicate) => Some(partition_keys(predicate)),
            _ => None,
        },
        |clause, literals| match clause {
            JournalClause::Id(predicate) => {
                InvocationUuidCodec::try_prepare_value(predicate, literals)
            }
            JournalClause::Index(predicate) => U32Codec::try_prepare_value(predicate, literals),
            _ => Ok(None),
        },
    )
}

fn prepare_events<K: PartitionKeyed>(
    selection: &Selection<JournalEvents>,
) -> Result<(KeySelection<K>, SelectionPrecision)> {
    prepare_partition_keyed(
        selection,
        &[JournalEventsColumn::PartitionKey, JournalEventsColumn::Id],
        |clause| match clause {
            JournalEventsClause::PartitionKey(predicate) => Some(predicate.into()),
            JournalEventsClause::Id(predicate) => Some(partition_keys(predicate)),
            _ => None,
        },
        |clause, literals| match clause {
            JournalEventsClause::Id(predicate) => {
                InvocationUuidCodec::try_prepare_value(predicate, literals)
            }
            _ => Ok(None),
        },
    )
}

fn partition_keys(ids: &ValuePredicate<InvocationId>) -> Domain<PartitionKey> {
    Domain::from(ids).map_values(|id| id.partition_key())
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use restate_storage_query_api::selection::ValuePredicate;
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::sharding::KeyRange;

    use crate::journal_table_v2::JournalKey;
    use crate::keys::EncodeTableKey;
    use crate::table::navigate;

    use super::*;

    #[test]
    fn invocation_ids_and_indexes_position_journal_reads() {
        // Four invocations with ten entries each on partition keys 7, 8 and 9, read
        // by an owner of many partition keys. UUIDs are distinct, as in practice.
        let id = |key: u64, n: u64| {
            InvocationId::from_parts(key, InvocationUuid::from_u128((key * 100 + n).into()))
        };
        let mut entries: Vec<_> = (7..=9)
            .flat_map(|key| (1..=4).map(move |n| id(key, n)))
            .flat_map(|id| (0..10).map(move |index| (id, index)))
            .map(|(id, index)| {
                let key = JournalKey {
                    partition_key: id.partition_key(),
                    invocation_uuid: id.invocation_uuid(),
                    journal_index: index,
                };
                let mut bytes = Vec::new();
                EncodeTableKey::serialize_to(&key, &mut bytes);
                (bytes, (id, index))
            })
            .collect();
        entries.sort();
        let keys: Vec<_> = entries.iter().map(|(key, _)| key).collect();
        let read = |selection: Selection<Journal>| {
            let (request, _) = prepare_journal::<JournalKey>(&selection).unwrap();
            let (matches, visited) = navigate(request, KeyRange::new(0, 100), &keys);
            let rows: Vec<_> = matches.into_iter().map(|index| entries[index].1).collect();
            (rows, visited)
        };
        let ids = |ids: &[InvocationId]| JournalClause::Id(ValuePredicate::In(ids.to_vec()));

        let target = id(8, 2);
        let (rows, visited) = read(Selection::All.and(ids(&[target])));
        assert_eq!(
            rows,
            (0..10).map(|index| (target, index)).collect::<Vec<_>>()
        );
        assert_eq!(visited, 10, "an_id_reads_only_its_entries");

        let (rows, visited) = read(Selection::All.and(ids(&[target])).and(JournalClause::Index(
            ValuePredicate::Range {
                lower: Bound::Included(3),
                upper: Bound::Included(4),
            },
        )));
        assert_eq!(rows, [(target, 3), (target, 4)]);
        assert_eq!(visited, 2, "an_index_range_positions_within_an_invocation");

        // Partition keys 7..=9 lie between the requested IDs: each is positioned
        // past by seeks rather than read.
        let (first, last) = (id(7, 1), id(9, 4));
        let (rows, visited) = read(Selection::All.and(ids(&[first, last])));
        let expected: Vec<_> = [first, last]
            .into_iter()
            .flat_map(|id| (0..10).map(move |index| (id, index)))
            .collect();
        assert_eq!(rows, expected);
        assert!(
            visited <= 24,
            "id_lists_seek_between_partition_keys: {visited}"
        );
    }
}
