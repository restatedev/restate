// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Declares an index, its primary table, and its complete key layout. The final
/// field must be annotated `(primary_key)` and have the table's primary-key type.
/// Only complete owned/borrowed keys implement SecondaryIndexKey.
macro_rules! define_secondary_index {
    (
        $(#[$meta:meta])*
        $index:ident,
        table: $table:ty,
        key: $key:ident { $($fields:tt)* } $(,)?
    ) => {
        crate::index::macros::define_secondary_index!(
            @annotation [$(#[$meta])* $index, $table, $key] []; $($fields)*
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

    (@emit [$(#[$meta:meta])* $index:ident, $table:ty, $key:ident]
        [$($field:ident: $codec:ty $(=> $borrowed:ty)?,)*];
        $primary_key:ident: $primary_type:ty
    ) => {
        $(#[$meta])*
        pub enum $index {}

        impl crate::index::SecondaryIndex for $index {
            type Table = $table;

            const INDEX_ID: crate::index::IndexId = crate::index::IndexId::$index;
        }

        static_assertions::assert_type_eq_all!(
            $primary_type,
            <$table as restate_storage_api::Table>::PrimaryKey
        );

        crate::keys::macros::define_index_key!(
            $key,
            table: crate::TableKind::SecondaryIndex,
            context: restate_types::sharding::PartitionId,
            start: |partition_id: &restate_types::sharding::PartitionId, buffer: &mut _| {
                let prefix = crate::keys::IndexKeyPrefix::of::<$index>(*partition_id);
                bytes::BufMut::put_slice(buffer, zerocopy::IntoBytes::as_bytes(&prefix));
            },
            fields {
                $($field: $codec $(=> $borrowed)?,)*
                $primary_key: $primary_type,
            }
        );

        impl crate::index::SecondaryIndexKey for $key {
            type Index = $index;

            fn primary_key(&self) -> &<<$index as crate::index::SecondaryIndex>::Table as restate_storage_api::Table>::PrimaryKey {
                &self.$primary_key
            }
        }

        paste::paste! {
            impl crate::index::SecondaryIndexKey for [<$key Ref>]<'_> {
                type Index = $index;

                fn primary_key(&self) -> &<<$index as crate::index::SecondaryIndex>::Table as restate_storage_api::Table>::PrimaryKey {
                    &self.$primary_key
                }
            }
        }
    };
}

pub(crate) use define_secondary_index;
