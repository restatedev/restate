// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Declares a secondary index: its name, its definition in the registry and its
/// key, the ordered fields ending with the entry's own key.
///
/// ```ignore
/// define_secondary_index!(
///     /// Find the most recently transitioned entries for a service within a stage.
///     EntryByStageService,
///     definition: "entry_by_stage_service/1",   // registered under this name
///     key: EntryByStageServiceKey {
///         stage: StageRankCodec,
///         service_name: ServiceNameCodec => str, // `=> str`: the encoder borrows a &str
///         transitioned_at: ReverseUniqueTimestampCodec,
///         canonical_id: CanonicalEntryIdCodec (primary_key), // last: the entry's identity
///     }
/// );
/// ```
///
/// A key on disk is the index header, `xI | pad | partition | index ID`,
/// followed by the fields in this order (see the `index` module). The last field
/// must be annotated `(primary_key)`: its codec's value type is the index's
/// `PrimaryKey`, and a read checks that it belongs to the partition it runs on.
///
/// The index is **registered** under `definition:`: each replica's registry
/// issues the index ID of its build, and a read resolves the Ready one.
///
/// The declaration generates, through `define_ordered_key!`:
/// - `EntryByStageServiceKey`, its borrowing `…KeyRef` and `…KeyView`, the prefix
///   builder and the field-by-field decoder;
/// - `EntryByStageService`, the physical table over that key, with
///   `PhysicalTable` and the snapshot reads every physical table gets;
/// - `SecondaryIndex` for the index and `SecondaryIndexKey` for its complete
///   keys (a prefix is not a key).
///
/// Logical columns bind separately with `define_key_selection!`, so that one key
/// can serve several schemas:
///
/// ```ignore
/// define_key_selection!(
///     EntryByStageServiceKey; [EntryByService];
///     stage, service_name => non_null, transitioned_at, canonical_id,
/// );
/// ```
// The index definitions, in their own commit, and tests declare indexes with it.
#[allow(unused_macros)]
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

#[allow(unused_imports)] // See above.
pub(crate) use define_secondary_index;
