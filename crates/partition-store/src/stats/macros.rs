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
/// - `table` declares a physical marker. `definition` names its registry
///   definition, including its version; each replica's registry issues the
///   index ID of its index.
/// - `schema` supplies logical columns for the separate, codec-driven selection
///   binding. It does not become an associated schema on the physical table.
/// - `value` selects an `Aggregate` implementation, not its decoded output type.
///   That implementation supplies both the persisted `StatKind` and stored value codec.
/// - `key` declares physical field order and codecs, independently of logical column order.
///   Every field requires `OrderedFieldCodec`; selection preparation additionally requires
///   `FieldPredicateCodec` with the matching logical literal type. `=> str` selects a
///   borrowed encoding input, and `Option<C>` selects the nullable form of codec `C`.
///
/// This macro declares a physical table marker and implements `Stat`,
/// `AggregatedStat`, `Table`, and `PhysicalTable`. It
/// delegates owned keys, borrowed inputs, views, prefix builders, and decoding to
/// `define_ordered_key!`. Encoding through `EncodeStatKey` writes only the payload;
/// `Stat::encode_key` and the generated prefix builder prepend the same header
/// for an `IndexScope`. The generated `prepare_selection` accepts `Selection<schema>` and
/// inherits its physical codecs from the key schema.
/// Attributes before `table` document the generated owned key.
macro_rules! define_aggregated_stat {
    (
        $(#[$meta:meta])*
        table: $stat:ident,
        definition: $definition:literal,
        schema: $schema:ty,
        value: $kind:ty,
        key: $key:ident(
            $($field:ident: $codec:ty $(=> $borrowed:ty)?),+ $(,)?
        ) $(,)?
    ) => {
        #[derive(Debug)]
        pub enum $stat {}

        impl crate::stats::Stat for $stat {
            const DEFINITION: &'static str = $definition;
            const STAT_KIND: crate::stats::StatKind = <$kind as crate::stats::aggregated::Aggregate>::STAT_KIND;
            type OwnedKey = $key;
            type Value = <$kind as crate::stats::aggregated::Aggregate>::Output;
        }

        impl crate::stats::aggregated::AggregatedStat for $stat {
            type Aggregation = $kind;
        }

        impl restate_storage_query_api::Table for $stat {
            type Record<'a> = (crate::keys::KeyDecoder<'a, <Self as crate::stats::Stat>::OwnedKey>, &'a [u8]);
            type Scan = crate::keys::selection::KeySelection<<Self as crate::stats::Stat>::OwnedKey>;
        }

        impl crate::table::PhysicalTable for $stat {
            type Key = <Self as crate::stats::Stat>::OwnedKey;
        }

        crate::keys::macros::define_ordered_key!(
            $(#[$meta])*
            $key,
            key_kind: <crate::stats::AggregateKeyHeader<$stat> as crate::keys::header::KeyHeader>::KEY_KIND,
            context: crate::keys::header::IndexScope,
            start: |scope: &crate::keys::header::IndexScope, buffer| {
                crate::stats::encode_header::<$stat, _>(*scope, buffer);
            },
            fields { $($field: $codec $(=> $borrowed)?),+ }
            schema: $schema
        );

        impl crate::keys::header::KeySchema for $key {
            type Header = crate::stats::AggregateKeyHeader<$stat>;
        }

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
