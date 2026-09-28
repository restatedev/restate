// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Registers an aggregate and adapts the shared ordered-key generators to the
/// statistic's fixed identity.
///
/// - `table` is the existing storage-api marker. Its `FilterTarget` implementation
///   supplies the logical fields and clause type; filter preparation is always generated.
/// - `value` selects the `Aggregate` implementation and its stored output type.
/// - `key` declares physical field order and codecs, independently of logical field order.
///
/// This macro implements `Stat` and `AggregatedStat` for the table marker and
/// delegates key generation to `define_index_key!`. The generated key's
/// `prepare_filter` accepts `Filter<table>` and binds its clauses to those codecs.
macro_rules! define_aggregated_stat {
    (
        table: $stat:ident,
        value: $kind:ty,
        key: $key:ident(
            $($field:ident: $codec:ty $(=> $borrowed:ty)?),+ $(,)?
        ) $(,)?
    ) => {
        impl crate::stats::Stat for $stat {
            const STAT_ID: crate::stats::StatId = crate::stats::StatId::$stat;
            const STAT_KIND: crate::stats::StatKind = <$kind as crate::stats::aggregated::Aggregate>::STAT_KIND;
            type OwnedKey = $key;
            type Value = <$kind as crate::stats::aggregated::Aggregate>::Output;
        }

        impl crate::stats::aggregated::AggregatedStat for $stat {
            type Aggregation = $kind;
        }

        crate::keys::macros::define_index_key!(
            $key,
            table: crate::TableKind::Stats,
            context: restate_types::sharding::PartitionId,
            start: |partition_id: &restate_types::sharding::PartitionId, buffer| {
                let prefix = crate::stats::StatKeyPrefix::of::<$stat>(*partition_id);
                crate::keys::EncodeTableKeyPrefix::serialize_to(&prefix, buffer);
            },
            fields { $($field: $codec $(=> $borrowed)?),+ }
            filter: $stat
        );

        impl crate::stats::EncodeStatKey<$stat> for $key {
            fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                crate::keys::EncodeIndexKey::encode(self, bytes);
            }
            fn encoded_len(&self) -> usize { crate::keys::EncodeIndexKey::encoded_len(self) }
        }

        impl crate::stats::DecodeStatKey<$stat> for $key {
            fn decode(bytes: &mut &[u8]) -> crate::Result<Self> {
                crate::keys::DecodeIndexKey::decode(bytes)
            }
        }

        paste::paste! {
            impl crate::stats::EncodeStatKey<$stat> for [<$key Ref>]<'_> {
                fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                    crate::keys::EncodeIndexKey::encode(self, bytes);
                }
                fn encoded_len(&self) -> usize { crate::keys::EncodeIndexKey::encoded_len(self) }
            }
        }
    };
}

pub(crate) use define_aggregated_stat;
