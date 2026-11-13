// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Declares a statistic: a registered index of counters, keyed by the declared
/// fields, whose values an aggregation updates with merge operands.
///
/// ```ignore
/// define_aggregated_stat!(
///     /// Current entry counts by service, handler and entry kind.
///     table: ServiceStats,                    // the physical table, a marker type
///     definition: "service_stats/1",          // the registry definition
///     schema: ServiceStatsSchema,             // the logical columns the fields bind to
///     value: StageStatusGauge,                // the `Aggregate`: counters per stage and status
///     key: ServiceStatsKey(
///         service_name: ServiceNameCodec => str,
///         handler: Option<StringCodec> => str, // nullable field
///         kind: EntryKindCodec,
///     ),
/// );
/// ```
///
/// A key on disk is the statistic's header, `kind | pad | partition | index ID |
/// aggregation kind` (`AggregateKeyPrefix` plus the `StatKind`), followed by the
/// fields in this order. `value` names an `Aggregate` implementation, which
/// fixes both the persisted aggregation kind and the stored value's codec, so
/// that writes and merges agree on the format.
///
/// The declaration generates, through `define_ordered_key!`, the key type
/// `ServiceStatsKey` with its borrowing input, view, prefix builder and decoder,
/// and implements `Stat`, `AggregatedStat`, the physical `Table` and
/// `PhysicalTable` for `ServiceStats`. `Stat::encode_key` and the prefix builder
/// prepend the header for an `IndexScope`; `EncodeStatKey` writes the payload
/// alone. The generated `prepare_selection` takes a `Selection<ServiceStatsSchema>`;
/// every field codec must implement `FieldPredicateCodec` for its column's
/// literal type. Attributes before `table:` document the generated key.
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
