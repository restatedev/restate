// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Defines a physical statistic and its existing ordered key encoding.
#[allow(unused_macros)]
macro_rules! define_aggregated_stat {
    (
        $(#[$meta:meta])*
        table: $stat:ident,
        value: $kind:ty,
        key: $key:ident(
            $($field:ident: $codec:ty $(=> $borrowed:ty)?),+ $(,)?
        ) $(,)?
    ) => {
        #[derive(Debug)]
        pub enum $stat {}

        impl crate::stats::Stat for $stat {
            const STAT_ID: crate::stats::StatId = crate::stats::StatId::$stat;
            const STAT_KIND: crate::stats::StatKind = <$kind as crate::stats::aggregated::Aggregate>::STAT_KIND;
            type OwnedKey = $key;
            type Value = <$kind as crate::stats::aggregated::Aggregate>::Output;
        }

        impl crate::stats::aggregated::AggregatedStat for $stat {
            type Aggregation = $kind;
        }

        crate::keys::macros::define_ordered_key!(
            $(#[$meta])*
            $key,
            key_kind: <crate::stats::StatKeyHeader<$stat> as crate::keys::header::KeyHeader>::KEY_KIND,
            context: restate_types::sharding::PartitionId,
            start: |partition_id: &restate_types::sharding::PartitionId, buffer| {
                let prefix = crate::stats::StatKeyPrefix::of::<$stat>(*partition_id);
                crate::keys::EncodeTableKeyPrefix::serialize_to(&prefix, buffer);
            },
            fields { $($field: $codec $(=> $borrowed)?),+ }
        );

        impl crate::keys::header::KeySchema for $key {
            type Header = crate::stats::StatKeyHeader<$stat>;
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

#[allow(unused_imports)]
pub(crate) use define_aggregated_stat;
