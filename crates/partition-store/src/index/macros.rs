// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Declares an index and its complete key layout. The final field must be
/// annotated `(primary_key)`; its codec's `Value` determines the primary-key type.
/// Only complete owned/borrowed keys implement SecondaryIndexKey.
/// Generates a physical `Table` and prepared `Scan` implementation, without a
/// logical-schema association. Reads enforce both index-header and primary-key ownership.
///
/// `definition: "name/version"` declares a registered index: each replica's
/// registry issues the index ID of its index, and reads resolve the Ready
/// one. Without it, the index keeps its static `IndexId` of the same name.
macro_rules! define_secondary_index {
    (
        $(#[$meta:meta])*
        $index:ident,
        definition: $definition:literal,
        key: $key:ident { $($fields:tt)* } $(,)?
    ) => {
        crate::index::macros::define_secondary_index!(
            @annotation [$(#[$meta])* $index, $key, [registered $definition]] []; $($fields)*
        );
    };
    (
        $(#[$meta:meta])*
        $index:ident,
        key: $key:ident { $($fields:tt)* } $(,)?
    ) => {
        crate::index::macros::define_secondary_index!(
            @annotation [$(#[$meta])* $index, $key, [static]] []; $($fields)*
        );
    };

    // A `ty` macro fragment cannot be followed by `(`. Strip the terminal
    // annotation before parsing the field types, preserving arbitrary Rust types.
    (@annotation [$($context:tt)*] [$($fields:tt)*]; (primary_key) $(,)?) => {
        crate::index::macros::define_secondary_index!(
            @fields [$($context)*] []; $($fields)*
        );
    };
    (@annotation [$($context:tt)*] [$($fields:tt)*]; (primary_key) $($rest:tt)+) => {
        compile_error!("the primary-key field must be the last field");
    };
    (@annotation [$($context:tt)*] [$($fields:tt)*]; $next:tt $($rest:tt)*) => {
        crate::index::macros::define_secondary_index!(
            @annotation [$($context)*] [$($fields)* $next]; $($rest)*
        );
    };
    (@annotation [$($context:tt)*] [$($fields:tt)*];) => {
        compile_error!("the final field must be annotated (primary_key)");
    };

    (@fields [$($context:tt)*] [$($fields:tt)*]; $primary_key:ident: $primary_type:ty) => {
        crate::index::macros::define_secondary_index!(
            @emit [$($context)*] [$($fields)*]; $primary_key: $primary_type
        );
    };
    (@fields [$($context:tt)*] [$($fields:tt)*];
        $field:ident: $codec:ty $(=> $borrowed:ty)?, $($rest:tt)+
    ) => {
        crate::index::macros::define_secondary_index!(
            @fields [$($context)*] [$($fields)* $field: $codec $(=> $borrowed)?,]; $($rest)+
        );
    };

    (@identity $index:ident, $primary_type:ty, [registered $definition:literal]) => {
        impl crate::index::SecondaryIndex for $index {
            type PrimaryKey = <$primary_type as crate::keys::FieldCodec>::Value;
            type Scope = crate::keys::header::IndexScope;

            fn scope(
                metadata: &crate::table::ReadMetadata<'_>,
            ) -> crate::Result<crate::keys::header::IndexScope> {
                Ok(crate::keys::header::IndexScope {
                    partition: metadata.partition.partition_id,
                    index: metadata.ready_index(
                        $definition,
                        crate::registry::IndexKind::Secondary,
                    )?,
                })
            }

            fn prefix(scope: crate::keys::header::IndexScope) -> crate::keys::IndexKeyPrefix {
                crate::keys::IndexKeyPrefix::new(scope.partition, scope.index)
            }
        }

        impl crate::index::RegisteredIndex for $index {
            const DEFINITION: &'static str = $definition;
        }
    };
    (@identity $index:ident, $primary_type:ty, [static]) => {
        impl crate::index::SecondaryIndex for $index {
            type PrimaryKey = <$primary_type as crate::keys::FieldCodec>::Value;
            type Scope = restate_types::sharding::PartitionId;

            fn scope(
                metadata: &crate::table::ReadMetadata<'_>,
            ) -> crate::Result<restate_types::sharding::PartitionId> {
                Ok(metadata.partition.partition_id)
            }

            fn prefix(partition: restate_types::sharding::PartitionId) -> crate::keys::IndexKeyPrefix {
                crate::keys::IndexKeyPrefix::new(
                    partition,
                    crate::index::IndexId::$index.as_u32(),
                )
            }
        }
    };

    (@emit [$(#[$meta:meta])* $index:ident, $key:ident, [$($identity:tt)*]]
        [$($field:ident: $codec:ty $(=> $borrowed:ty)?,)*];
        $primary_key:ident: $primary_type:ty
    ) => {
        $(#[$meta])*
        pub enum $index {}

        crate::index::macros::define_secondary_index!(
            @identity $index, $primary_type, [$($identity)*]
        );

        crate::keys::macros::define_ordered_key!(
            $key,
            key_kind: crate::keys::KeyKind::SecondaryIndex,
            context: <$index as crate::index::SecondaryIndex>::Scope,
            start: |scope: &<$index as crate::index::SecondaryIndex>::Scope, buffer: &mut _| {
                let prefix = <$index as crate::index::SecondaryIndex>::prefix(*scope);
                bytes::BufMut::put_slice(buffer, zerocopy::IntoBytes::as_bytes(&prefix));
            },
            fields {
                $($field: $codec $(=> $borrowed)?,)*
                $primary_key: $primary_type,
            }
        );

        impl crate::keys::header::KeySchema for $key {
            type Header = crate::keys::IndexKeyHeader<$index>;
        }

        impl restate_storage_query_api::Table for $index {
            type Record<'a> = (crate::keys::KeyDecoder<'a, $key>, &'a [u8]);
            type Scan = crate::keys::selection::KeySelection<$key>;
        }

        impl crate::index::SecondaryIndexKey for $key {
            type Index = $index;

            fn primary_key(&self) -> &<$index as crate::index::SecondaryIndex>::PrimaryKey {
                &self.$primary_key
            }
        }

        impl crate::table::PhysicalTable for $index {
            type Key = $key;

            fn owns_record(
                key: crate::keys::KeyDecoder<'_, $key>,
                metadata: &crate::table::ReadMetadata<'_>,
            ) -> crate::Result<bool> {
                use std::ops::RangeBounds as _;

                // Index-header ownership alone does not exclude foreign
                // primary keys retained by a partition split.
                let view = key.take_all()?;
                let id = view.$primary_key.decode()?;
                Ok(metadata.partition.key_range.contains(&id.partition_key()))
            }
        }

        paste::paste! {
            impl crate::index::SecondaryIndexKey for [<$key Ref>]<'_> {
                type Index = $index;

                fn primary_key(&self) -> &<$index as crate::index::SecondaryIndex>::PrimaryKey {
                    &self.$primary_key
                }
            }
        }
    };
}

pub(crate) use define_secondary_index;
