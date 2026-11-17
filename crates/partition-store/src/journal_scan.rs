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

use std::num::NonZeroUsize;

use anyhow::Context;

use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision, ValuePredicate};
use restate_storage_query_api::tables::journal::{Journal, JournalClause, JournalColumn};
use restate_storage_query_api::tables::journal_events::{
    JournalEvents, JournalEventsClause, JournalEventsColumn,
};
use restate_storage_query_api::{PreparedRead, Table};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};

use crate::Result;
use crate::journal_table::JournalKey as JournalV1Key;
use crate::journal_table_v2::JournalKey as JournalV2Key;
use crate::keys::codecs::{InvocationUuidCodec, U8Codec, U32Codec, U64Codec};
use crate::keys::header::{KeySchema, PartitionKeyHeader, PartitionKeyed, prepare_partition_keyed};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};
use crate::lookup::{LookupTable, ensure_canonical};
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

/// A journal entry is named by its invocation ID and index, as the `id` and
/// `index` columns show them. Both layouts store `kind | partition key | UUID |
/// index`, so a received key must be exactly that long.
macro_rules! journal_lookup {
    ($table:ty, $key:ident, $kind:expr) => {
        impl LookupTable for $table {
            type Id = (InvocationId, u32);
            /// Smaller than for locks: entries can be large. Not measured.
            const BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

            fn key((id, index): &(InvocationId, u32)) -> Option<$key> {
                Some($key {
                    partition_key: id.partition_key(),
                    invocation_uuid: id.invocation_uuid(),
                    journal_index: *index,
                })
            }

            fn check_key(bytes: &[u8]) -> anyhow::Result<PartitionKey> {
                let rest = bytes
                    .strip_prefix($kind.as_bytes())
                    .context("not a journal key")?;
                let (stored, rest) = rest
                    .split_first_chunk::<8>()
                    .context("journal key without a partition key")?;
                let (uuid, rest) = rest
                    .split_first_chunk::<{ InvocationUuid::RAW_BYTES_LEN }>()
                    .context("journal key without an invocation UUID")?;
                let index: [u8; 4] = rest.try_into().context("journal key without an index")?;
                let id = InvocationId::from_parts(
                    PartitionKey::from_be_bytes(*stored),
                    InvocationUuid::from_bytes(*uuid),
                );
                let key =
                    Self::key(&(id, u32::from_be_bytes(index))).context("invalid journal key")?;
                ensure_canonical(bytes, *stored, &key)?;
                Ok(key.partition_key)
            }
        }
    };
}

journal_lookup!(JournalTable, JournalV1Key, KeyKind::Journal);
journal_lookup!(JournalV2Table, JournalV2Key, KeyKind::JournalV2);

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

    /// Received journal keys of either layout are checked before anything decodes
    /// them: exactly `kind | partition key | UUID | index`, accepted with the
    /// partition key of the invocation they name.
    #[test]
    fn received_journal_keys_are_checked_before_decoding() {
        let id = InvocationId::from_parts(7, InvocationUuid::from_u128(42));
        let v1 = JournalTable::key(&(id, 3)).unwrap();
        let mut v1_bytes = Vec::new();
        EncodeTableKey::serialize_to(&v1, &mut v1_bytes);
        assert_eq!(JournalTable::check_key(&v1_bytes).unwrap(), 7);
        let v2 = JournalV2Table::key(&(id, 3)).unwrap();
        let mut v2_bytes = Vec::new();
        EncodeTableKey::serialize_to(&v2, &mut v2_bytes);
        assert_eq!(JournalV2Table::check_key(&v2_bytes).unwrap(), 7);

        let mut moved = v2_bytes.clone();
        moved[9] ^= 1;
        let mut long = v1_bytes.clone();
        long.push(0);
        for (case, bytes) in [
            ("another layout", v2_bytes.clone()),
            ("short partition key", v1_bytes[..9].to_vec()),
            ("short UUID", v1_bytes[..20].to_vec()),
            ("short index", v1_bytes[..v1_bytes.len() - 1].to_vec()),
            ("trailing bytes", long),
        ] {
            assert!(JournalTable::check_key(&bytes).is_err(), "{case}");
        }
        // The partition key is part of the invocation ID: the same UUID under
        // another partition key is another invocation's entry, stored under its own.
        assert_eq!(JournalV2Table::check_key(&moved).unwrap(), 6);
    }

    #[test]
    fn invocation_ids_and_indexes_position_journal_reads() {
        // Four invocations with ten entries each on partition keys 7 to 29, read
        // by an owner of many partition keys. UUIDs are distinct, as in practice.
        let id = |key: u64, n: u64| {
            InvocationId::from_parts(key, InvocationUuid::from_u128((key * 100 + n).into()))
        };
        let mut entries: Vec<_> = (7..=29)
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

        // The requested IDs are on partition keys 7 and 29: the read lists those
        // two, so the invocations on the 21 keys between are never visited, and
        // within each key it seeks past the other invocations.
        let (first, last) = (id(7, 1), id(29, 4));
        let (rows, visited) = read(Selection::All.and(ids(&[first, last])));
        let expected: Vec<_> = [first, last]
            .into_iter()
            .flat_map(|id| (0..10).map(move |index| (id, index)))
            .collect();
        assert_eq!(rows, expected);
        assert_eq!(visited, 23, "id_lists_seek_between_partition_keys");
    }
}
