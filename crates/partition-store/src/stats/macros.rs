// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Implements a registered statistic using the shared ordered-key generators.
///
/// - `table` names an existing storage-api marker and its matching `StatId` variant.
///   Its `Selector` implementation supplies the logical fields and clause type;
///   selection preparation is always generated. This macro does not allocate an ID.
/// - `value` selects an `Aggregate` implementation, not its decoded output type.
///   That implementation supplies both the persisted `StatKind` and stored value codec.
/// - `key` declares physical field order and codecs, independently of logical field order.
///   Every field requires `OrderedFieldCodec`; selection preparation additionally requires
///   `FieldPredicateCodec` with the matching logical literal type. `=> str` selects a
///   borrowed encoding input, and `Option<C>` selects the nullable form of codec `C`.
///
/// This macro implements `Stat` and `AggregatedStat` for the table marker and
/// delegates owned keys, borrowed inputs, views, prefix builders, and decoding to
/// `define_index_key!`. Encoding through `EncodeStatKey` writes only the payload;
/// `Stat::encode_key` and the generated prefix builder prepend the same
/// `StatKeyPrefix`. The generated `prepare_selection` accepts `Selection<table>` and
/// inherits its physical codecs from the key schema.
/// Attributes before `table` document the generated owned key.
macro_rules! define_aggregated_stat {
    (
        $(#[$meta:meta])*
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
            $(#[$meta])*
            $key,
            key_kind: crate::keys::KeyKind::Stats,
            context: restate_types::sharding::PartitionId,
            start: |partition_id: &restate_types::sharding::PartitionId, buffer| {
                let prefix = crate::stats::StatKeyPrefix::of::<$stat>(*partition_id);
                crate::keys::EncodeTableKeyPrefix::serialize_to(&prefix, buffer);
            },
            fields { $($field: $codec $(=> $borrowed)?),+ }
            selector: $stat
        );

        impl crate::stats::EncodeStatKey<$stat> for $key {
            fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                crate::keys::KeyEncode::encode(self, bytes);
            }
            fn serialized_length(&self) -> usize { crate::keys::KeyEncode::serialized_length(self) }
        }

        impl crate::stats::DecodeStatKey<$stat> for $key {
            fn decode(bytes: &mut &[u8]) -> crate::Result<Self> {
                crate::keys::KeyDecode::decode(bytes)
            }
        }

        paste::paste! {
            impl crate::stats::EncodeStatKey<$stat> for [<$key Ref>]<'_> {
                fn encode<B: bytes::BufMut>(&self, bytes: &mut B) {
                    crate::keys::KeyEncode::encode(self, bytes);
                }
                fn serialized_length(&self) -> usize { crate::keys::KeyEncode::serialized_length(self) }
            }
        }
    };
}

pub(crate) use define_aggregated_stat;
