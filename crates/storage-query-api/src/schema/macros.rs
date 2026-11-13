// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Declares a logical table schema from one column list.
///
/// A declaration names the schema, its columns and their native types, and
/// optionally each column's Arrow type for the SQL view. The macro generates the
/// [`SelectionSchema`](crate::SelectionSchema) implementation and, for the
/// row-producing form below, [`LogicalSchema`](crate::LogicalSchema) with its row
/// type and batch builder; see [`schema`](crate::schema) for the concepts. The
/// selection-only form is:
///
/// ```text
/// define_schema! {
///     <attributes>
///     <vis> <Schema> {
///         <column>: <NativeType>,
///         <column>: <NativeType> => <ArrowDataType>,
///         #[starts_with]
///         <column>: <NativeType> => <ArrowDataType>,
///         ...
///     }
/// }
/// ```
///
/// Generates an uninhabited schema marker, its [`SelectionSchema`](crate::SelectionSchema) implementation,
/// `<Schema>Column` and `<Schema>Clause` enums, `<Schema>Domains` candidate domains,
/// and a `<Schema>ColumnTypes` trait exposing each native type as an associated type.
/// The selection-only form does not define a physical table, storage row, or key.
///
/// - Native types must be `Ord + Clone`. SQL-bound columns also implement
///   [`SelectionValue`](crate::selection::SelectionValue).
///   In particular, `Option<V>` gives the column nullable selection semantics.
///   Native `Ord` represents candidate sets; it does not imply SQL ordering support.
/// - Arrow representations are optional. The generated `arrow_schema` method
///   includes only represented columns, with permissive Arrow nullability.
/// - `#[starts_with]` adds a literal prefix clause. Its operand is always `ReString`,
///   including for nullable columns. `starts_with_clause` instead produces native
///   membership when `SelectionValue::finite_strings` is available. Physical
///   codecs decide how to prepare either representation.
/// - Column identifiers are canonical tags and become CamelCase enum variants.
///   Declaration order determines enumeration order, not index sort order.
/// - Schema attributes are copied to the marker; column doc comments are copied to
///   the corresponding column and clause variants.
/// - `#[column("alias")]` changes the SQL binding and Arrow name, leaving the
///   canonical column tag intact. `#[values(expression)]` declares the **complete**
///   possible native values, never samples or estimates. Analysis intersects it
///   before deriving, so exclusions can become finite candidates or contradictions.
///   These attributes precede `#[starts_with]` in this provisional syntax.
/// - `#[internal]` declares a modeled column with no SQL binding or Arrow view.
///   It still has a column identity, native clauses, and a candidate domain, and
///   does not need SQL literal conversion. Its domain initially admits everything.
/// - An optional `derive [hook, ...]` after the columns runs each hook once, in
///   order, on `&mut <Schema>Domains`, returning `anyhow::Result<()>`. Hooks may
///   intersect domains with sound consequences, never erase existing constraints.
///   Use `Domain::map_values` for forward derivation from finite sets; projecting
///   exclusions through a lossy mapping is unsafe. Empty domains reject a branch
///   before hooks run and after each hook. Alternatives are merged only explicitly.
///
/// # Example
///
/// ```
/// use datafusion::arrow::datatypes::DataType;
/// use restate_storage_query_api::{SelectionSchema, define_schema};
/// use restate_storage_query_api::selection::{Selection, ValuePredicate};
/// use restate_types::vqueues::EntryKind;
/// use restate_util_string::ReString;
///
/// define_schema! {
///     /// Example logical table.
///     pub Entries {
///         /// The handler name, or NULL when absent.
///         #[starts_with]
///         handler: Option<ReString> => DataType::LargeUtf8,
///         /// A column with no declared Arrow representation.
///         kind: EntryKind,
///     }
/// }
///
/// assert_eq!(Entries::tag(EntriesColumn::Handler), "handler");
/// assert_eq!(Entries::column_from_tag("kind"), Some(EntriesColumn::Kind));
/// assert_eq!(Entries::arrow_schema().fields().len(), 1);
/// let prefix = Entries::starts_with_clause(EntriesColumn::Handler, "get").unwrap();
/// assert_eq!(Entries::column(&prefix), EntriesColumn::Handler);
/// assert_eq!(Entries::starts_with_prefix(&prefix), Some("get"));
/// assert!(Entries::starts_with_clause(EntriesColumn::Kind, "inv").is_none());
/// let selection = Selection::<Entries>::All
///     .and(EntriesClause::Handler(ValuePredicate::Equal(None)));
/// ```
///
/// # Native rows and Arrow builders
///
/// The row-producing form derives its schema marker, row, and batch-builder names.
/// For example, `invocation_status` generates `InvocationStatus`,
/// `InvocationStatusRow`, and `InvocationStatusBatchBuilder`. The syntax remains
/// provisional until the proc-macro conversion. Both forms share the same selection
/// metadata generation.
///
/// ```text
/// define_schema! {
///     /// Source documentation.
///     invocation_status {
///         name: "sys_invocation_status",
///         sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
///         sharding_columns { partition_key: PartitionKey, id: InvocationId, scope: Scope },
///         shard_keys: |domains: InvocationStatusDomains| {
///             let service_keys = domains.target_service_key.try_map_values(Clone::clone);
///             partition_keys(Domain::all(), domains.scope, service_keys)
///         },
///         columns {
///             partition_key: PartitionKey => DataType::UInt64,
///             id: InvocationId => DataType::LargeUtf8,
///             scope: Option<ReString> => DataType::LargeUtf8,
///             target_service_key: Option<ReString> => DataType::LargeUtf8,
///         }
///     }
/// }
/// ```
///
/// `name` is the stable source-schema identity; registration may choose a session
/// alias. `sharding` declares the shard key and hard serving eligibility without
/// selecting concrete nodes; the declaration implements `LogicalSchema` and derives
/// the representation of candidate shard keys from the `ShardingScheme` variant.
/// Partition variants carry `is_leader_required`; node variants carry an optional
/// required role. Session replica preferences cannot weaken these requirements.
///
/// ## Sharding columns
///
/// `sharding_columns { column: Derivation, ... }` lists the columns whose non-NULL
/// value alone determines a row's shard key (see
/// [`schema::sharding`](crate::schema::sharding) for the concepts). Each
/// `Derivation` names a [`shard_key`](crate::schema::sharding::shard_key) type of the
/// schema's own scheme, and the column's native type must match it; an `Option`
/// column is a nullable sharding column. The declaration is a fact about every row,
/// so list a column only if it *always* determines the key. In the example above:
///
/// - `id: InvocationId` holds because every invocation ID encodes its partition key.
/// - `scope: Scope` holds for scoped invocations, and NULL determines nothing.
/// - `target_service_key` is *not* a sharding column: it determines the key only when
///   `scope` is NULL.
///
/// The generated `LogicalSchema::branch_shard_keys` maps each sharding column's
/// candidate domain through its derivation and intersects the results. With
/// `id IN ('inv_1…', 'inv_2…')`, the candidates are exactly the two encoded keys.
/// `LogicalSchema::SHARDING_COLUMNS` exposes the declaration for shard-local JOINs.
///
/// Partition and node schemes must declare `sharding_columns`; an empty block
/// states that no column alone determines the shard key, so every shard is a
/// candidate. `Coordinator` and `Unsharded` have a single shard and declare none.
///
/// `shard_keys: |domains| ...` is optional. It adds candidate shard keys that no
/// single column determines, such as the NULL-scope fallback above, and is
/// intersected with the generated result. It must return a superset of the shard
/// keys of the rows that can satisfy the alternative; it only prunes, and never
/// proves colocation. Only `Unsharded` permits remote placement of its single
/// shard. Candidate shard keys only prune shards; physical bindings still prepare
/// the original selection for reads.
///
/// The declaration is checked at compile time. This one is valid:
///
/// ```
/// use datafusion::arrow::datatypes::DataType;
/// use restate_storage_query_api::{ShardingScheme, define_schema};
/// use restate_types::identifiers::InvocationId;
///
/// define_schema! {
///     invocations {
///         name: "invocations",
///         sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
///         sharding_columns { id: InvocationId },
///         columns { id: InvocationId => DataType::LargeUtf8 }
///     }
/// }
/// ```
///
/// A derivation of another scheme does not compile:
///
/// ```compile_fail
/// # use datafusion::arrow::datatypes::DataType;
/// # use restate_storage_query_api::{ShardingScheme, define_schema};
/// # use restate_types::identifiers::InvocationId;
/// define_schema! {
///     invocations {
///         name: "invocations",
///         sharding: ShardingScheme::ByNode(None),
///         sharding_columns { id: InvocationId },
///         columns { id: InvocationId => DataType::LargeUtf8 }
///     }
/// }
/// ```
///
/// Neither does a derivation of another native type:
///
/// ```compile_fail
/// # use datafusion::arrow::datatypes::DataType;
/// # use restate_storage_query_api::{ShardingScheme, define_schema};
/// # use restate_types::identifiers::InvocationId;
/// define_schema! {
///     invocations {
///         name: "invocations",
///         sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
///         sharding_columns { id: VQueueId },
///         columns { id: InvocationId => DataType::LargeUtf8 }
///     }
/// }
/// ```
///
/// Nor a partition or node scheme without a declaration:
///
/// ```compile_fail
/// # use datafusion::arrow::datatypes::DataType;
/// # use restate_storage_query_api::{ShardingScheme, define_schema};
/// # use restate_types::identifiers::InvocationId;
/// define_schema! {
///     invocations {
///         name: "invocations",
///         sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
///         columns { id: InvocationId => DataType::LargeUtf8 }
///     }
/// }
/// ```
///
/// Native columns also drive selection clauses and complete `#[values(...)]`
/// domains. The generated row owns native values; `LogicalSchema::Row<'a>` borrows
/// that row for the callback. Builders reuse their projection across batches.
/// A declaration may use `row: for<'a> BorrowedRow<'a>, append: append_row,`
/// to supply a borrowed row family and its projection-aware writer instead.
/// An optional `internal { column: NativeType, ... }` block adds native columns
/// without SQL exposure. `primary_key: column,` identifies a complete native key.
/// Exposed columns require an Arrow representation; the selection-only form also
/// supports aliases and optional Arrow views.
///
/// Arrow representations follow the V1/V2 table syntax: `DataType::Utf8`,
/// `DataType::LargeUtf8`, `DataType::Binary`, `DataType::LargeBinary`,
/// `DataType::UInt32`, `DataType::UInt64`, `DataType::Int32`, `DataType::Boolean`,
/// and `DataType::Duration` (milliseconds), with `DataType` imported from Arrow.
/// `TimestampMillisecond` (UTC), `UInt32List`, `Utf8List`, and `LargeUtf8List`
/// are marker syntax for parameterized Arrow types. Lists use 32-bit offsets
/// and nullable `item` children; `LargeUtf8List` has large UTF-8 children,
/// not large list offsets.
///
/// Native values include `ReString`/`String`, IDs, `EntryKind`, `Stage`, and `Status`
/// for SQL strings, `Vec<u8>` for binary data, integer/bool scalars, `MillisSinceEpoch`
/// (or `i64`) for millisecond timestamps,
/// and `Vec<V>` for lists. `Option<V>` preserves NULL for columns and list items.
/// `builder.row()` also provides a projection-aware writer for borrowed or formatted
/// values. Omitted projected columns are NULL; sources must populate required native
/// columns. Formatting writes directly into Arrow buffers without allocating a string.
/// Native aliases work without macro-level type inspection. Literal conversions
/// remain the responsibility of `SelectionValue`; unsupported SQL comparisons
/// (including binary/list literals) remain residual.
///
/// # Secondary index views
///
/// An index form uses `primary: module::Schema, reference: column => lookup,` instead of
/// `sharding`/`shard_keys` declarations. Its `columns { column => ArrowType, ... }` reuse
/// native types from the primary, including internal columns. Explicitly typed
/// `column: NativeType => ArrowType` columns belong only to the index and do not
/// contribute to the primary's candidate shard keys. A trailing
/// `key { column: asc nulls_last, other: desc nulls_first, ... }` describes key
/// order; the complete reference is appended automatically. `lookup` maps a borrowed
/// reference to the primary's native lookup key; callers must validate any additional
/// incarnation identity after fetching the primary record. The index inherits
/// the primary's sharding and hard serving requirements. SQL ordering still requires a
/// binding's codec-specific proof, and primary-query substitution requires
/// independent completeness evidence. This form exposes persisted-record inspection.
#[macro_export]
macro_rules! define_schema {
    (
        $(#[doc = $doc:expr])*
        $schema:ident {
            name: $name:literal,
            primary: $module:ident :: $primary:ident,
            reference: $reference:ident => $lookup:expr,
            $(row: for<$row_lifetime:lifetime> $row_type:ty, append: $append:path,)?
            columns {
                $($(#[doc = $column_doc:expr])* $(#[starts_with $($prefix:tt)*])?
                  $column:ident $(: $native:ty)? => $arrow:ident $(:: $variant:ident)?),+ $(,)?
            }
            key { $($key:ident: $direction:ident $nulls:ident),* $(,)? }
        }
    ) => {
        $crate::schema::__private::paste! {
            $crate::define_schema! { @sharded
                (<$module::$primary as $crate::LogicalSchema>::Sharding)
                $(#[doc = $doc])*
                $schema {
                    name: $name,
                    sharding: <$module::$primary as $crate::LogicalSchema>::SHARDING,
                    shard: {
                        columns [{}]
                        shard_keys [|domains: [<$schema:camel Domains>]| {
                            let mut primary = $module::[<$primary Domains>]::default();
                            $($crate::define_schema!(@index_domain primary, domains, $column $(, $native)?);)+
                            <$module::$primary as $crate::LogicalSchema>::branch_shard_keys(primary)
                        }]
                    },
                    $(row: for<$row_lifetime> $row_type, append: $append,)?
                    columns {
                        $($(#[doc = $column_doc])* $(#[starts_with $($prefix)*])?
                          $column: $crate::define_schema!(@index_type $module, $primary, $column $(, $native)?)
                              => $arrow $(:: $variant)?,)+
                    }
                }
            }

            impl $crate::schema::index::IndexSchema for [<$schema:camel>] {
                type Primary = $module::$primary;
                type Reference = <Self as [<$schema:camel ColumnTypes>]>::[<$reference:camel>];
                const REFERENCE_COLUMN: Self::Column = [<$schema:camel Column>]::[<$reference:camel>];
                const KEY_COLUMNS: &'static [$crate::schema::index::IndexColumn<Self::Column>] = &[
                    $($crate::schema::index::IndexColumn {
                        column: [<$schema:camel Column>]::[<$key:camel>],
                        options: $crate::schema::__private::datafusion::arrow::compute::SortOptions {
                            descending: $crate::define_schema!(@descending $direction),
                            nulls_first: $crate::define_schema!(@nulls_first $nulls),
                        },
                    },)*
                    $crate::schema::index::IndexColumn {
                        column: Self::REFERENCE_COLUMN,
                        options: $crate::schema::__private::datafusion::arrow::compute::SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    },
                ];

                fn primary_key(reference: &Self::Reference) -> <Self::Primary as $crate::schema::index::PrimarySchema>::PrimaryKey {
                    let lookup: fn(&Self::Reference) -> <Self::Primary as $crate::schema::index::PrimarySchema>::PrimaryKey = $lookup;
                    lookup(reference)
                }

                fn primary_column(column: Self::Column) -> Option<<Self::Primary as $crate::SelectionSchema>::Column> {
                    match column {
                        $([<$schema:camel Column>]::[<$column:camel>] => $crate::define_schema!(@index_column $module, $primary, $column $(, $native)?),)+
                    }
                }

                fn from_primary_domains(
                    primary: &<Self::Primary as $crate::SelectionSchema>::Domains,
                ) -> <Self as $crate::SelectionSchema>::Domains {
                    #[allow(unused_mut)] // An index may consist of index-only columns.
                    let mut domains = <Self as $crate::SelectionSchema>::Domains::default();
                    $($crate::define_schema!(@primary_domain domains, primary, $column $(, $native)?);)+
                    domains
                }
            }
        }
    };
    (@index_type $module:ident, $primary:ident, $column:ident, $native:ty) => { $native };
    (@index_type $module:ident, $primary:ident, $column:ident) => {
        $crate::schema::__private::paste! {
            <$module::$primary as $module::[<$primary ColumnTypes>]>::[<$column:camel>]
        }
    };
    (@index_domain $primary:ident, $domains:ident, $column:ident, $native:ty) => {};
    (@index_domain $primary:ident, $domains:ident, $column:ident) => {
        $primary.$column = $domains.$column;
    };
    (@primary_domain $domains:ident, $primary:ident, $column:ident, $native:ty) => {};
    (@primary_domain $domains:ident, $primary:ident, $column:ident) => {
        $domains.$column = $primary.$column.clone();
    };
    (@index_column $module:ident, $primary:ident, $column:ident, $native:ty) => { None };
    (@index_column $module:ident, $primary:ident, $column:ident) => {
        $crate::schema::__private::paste! {
            Some($module::[<$primary Column>]::[<$column:camel>])
        }
    };
    (@descending asc) => { false };
    (@descending desc) => { true };
    (@nulls_first nulls_first) => { true };
    (@nulls_first nulls_last) => { false };
    (@sharding Coordinator) => { $crate::schema::sharding::Unsharded };
    (@sharding Unsharded) => { $crate::schema::sharding::Unsharded };
    (@sharding ByNode) => { $crate::schema::sharding::NodeSharding };
    (@sharding ByPartitionId) => { $crate::schema::sharding::PartitionIdSharding };
    (@sharding ByPartitionKey) => { $crate::schema::sharding::PartitionKeySharding };
    // Partition and node schemes state their sharding columns explicitly, possibly
    // none, so that a forgotten declaration cannot silently read every shard.
    (@require_sharding_columns Coordinator $($columns:tt)?) => {};
    (@require_sharding_columns Unsharded $($columns:tt)?) => {};
    (@require_sharding_columns $kind:ident $columns:tt) => {};
    (@require_sharding_columns $kind:ident) => {
        ::core::compile_error!(concat!(
            "`", stringify!($kind), "` schemas must declare `sharding_columns { ... }`; ",
            "declare an empty block when no column alone determines the shard key"
        ));
    };
    (
        $(#[doc = $doc:expr])*
        $schema:ident {
            name: $name:literal,
            sharding: $namespace:ident :: $kind:ident $(($($args:tt)*))? $({$($fields:tt)*})?,
            $(sharding_columns $sharding_columns:tt,)?
            $(shard_keys: $shard_keys:expr,)?
            $(primary_key: $primary_key:ident,)?
            $(row: for<$row_lifetime:lifetime> $row_type:ty, append: $append:path,)?
            columns { $($columns:tt)* }
            $(internal { $($internal:tt)* })?
        }
    ) => {
        $crate::define_schema!(@require_sharding_columns $kind $($sharding_columns)?);
        $crate::define_schema! { @sharded ($crate::define_schema!(@sharding $kind))
            $(#[doc = $doc])*
            $schema {
                name: $name,
                sharding: $namespace::$kind $(($($args)*))? $({$($fields)*})?,
                shard: {
                    columns [$($sharding_columns)?]
                    shard_keys [$($shard_keys)?]
                },
                $(primary_key: $primary_key,)?
                $(row: for<$row_lifetime> $row_type, append: $append,)?
                columns { $($columns)* }
                $(internal { $($internal)* })?
            }
        }
    };
    (@sharded ($sharding:ty)
        $(#[doc = $doc:expr])*
        $schema:ident {
            name: $name:literal,
            sharding: $scheme:expr,
            shard: $shard:tt,
            $(primary_key: $primary_key:ident,)?
            $(row: for<$row_lifetime:lifetime> $row_type:ty, append: $append:path,)?
            columns { $($columns:tt)* }
            $(internal { $($internal:tt)* })?
        }
    ) => {
        $crate::schema::__private::paste! {
            $crate::define_schema!(@define
                $(#[doc = $doc])*
                [<$schema:camel>], [<$schema:camel Row>], [<$schema:camel BatchBuilder>],
                $name, $scheme, $sharding, $shard, [$(for<$row_lifetime> $row_type, $append)?], {
                    $($columns)*
                } $(internal { $($internal)* })?
            );
            $(impl $crate::schema::index::PrimarySchema for [<$schema:camel>] {
                type PrimaryKey = <Self as [<$schema:camel ColumnTypes>]>::[<$primary_key:camel>];
                const PRIMARY_KEY_COLUMN: Self::Column = [<$schema:camel Column>]::[<$primary_key:camel>];
            })?
        }
    };
    (
        $(#[$meta:meta])*
        $vis:vis $target:ident { $($fields:tt)+ }
        $(derive [$($hook:expr),* $(,)?])?
    ) => {
        $(#[$meta])*
        $vis enum $target {}

        $crate::define_schema!(@fields [$vis] [$target] [$($($hook,)*)?]
            [] [] [] [] [] [] [];
            $($fields)+ ,
        );
    };

    // Accumulate enum variants and match arms before emitting complete items.
    (@fields [$vis:vis] [$target:ident] [$($hook:expr,)*]
        [$($fields:tt)*] [$($clauses:tt)*] [$($arms:tt)*] [$($prefixes:tt)*]
        [$($prefix_views:tt)*] [$($arrow_types:tt)*] [$($bindings:tt)*];
        $(#[doc = $doc:expr])* $(#[column($column:literal)])? $(#[values($values:expr)])?
        #[starts_with] $field:ident: $ty:ty $(=> $arrow:expr)?, $($rest:tt)*
    ) => {
        $crate::define_schema!(@fields [$vis] [$target] [$($hook,)*]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)*
                $(#[doc = $doc])*
                [<$field:camel>]($crate::selection::ValuePredicate<$ty>),
                $(#[doc = $doc])*
                #[doc = ""]
                #[doc = "Matches a case-sensitive literal prefix; NULL never matches."]
                #[doc = "An empty prefix matches every non-NULL value. No LIKE syntax is interpreted."]
                [<$field:camel StartsWith>]($crate::schema::__private::ReString),
            ]
            [$($arms)*
                [<$target Clause>]::[<$field:camel>](_)
                | [<$target Clause>]::[<$field:camel StartsWith>](_) => [<$target Column>]::[<$field:camel>],
            ]
            [$($prefixes)*
                [<$target Column>]::[<$field:camel>] => ::core::option::Option::Some(
                    (|prefix: &str| {
                        if let Some(values) = <$ty as $crate::selection::SelectionValue>::finite_strings() {
                            [<$target Clause>]::[<$field:camel>]($crate::selection::ValuePredicate::In(
                                values.filter_map(|(name, value)| name.starts_with(prefix).then_some(value)).collect()
                            ))
                        } else {
                            [<$target Clause>]::[<$field:camel StartsWith>](prefix.into())
                        }
                    }) as fn(&str) -> Self::Clause
                ),
            ]
            [$($prefix_views)*
                [<$target Clause>]::[<$field:camel>](_) => ::core::option::Option::None,
                [<$target Clause>]::[<$field:camel StartsWith>](prefix) => ::core::option::Option::Some(prefix.as_str()),
            ]
            [$($arrow_types)* [<$target Column>]::[<$field:camel>] => $crate::define_schema!(@arrow $($arrow)?),]
            [$($bindings)* ($field: $ty [input] [$($column)?] [$($values)?])];
            $($rest)*
        );
    };
    (@fields [$vis:vis] [$target:ident] [$($hook:expr,)*]
        [$($fields:tt)*] [$($clauses:tt)*] [$($arms:tt)*] [$($prefixes:tt)*]
        [$($prefix_views:tt)*] [$($arrow_types:tt)*] [$($bindings:tt)*];
        $(#[doc = $doc:expr])* $(#[column($column:literal)])? $(#[values($values:expr)])?
        $field:ident: $ty:ty $(=> $arrow:expr)?, $($rest:tt)*
    ) => {
        $crate::define_schema!(@fields [$vis] [$target] [$($hook,)*]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)* $(#[doc = $doc])* [<$field:camel>]($crate::selection::ValuePredicate<$ty>),]
            [$($arms)* [<$target Clause>]::[<$field:camel>](_) => [<$target Column>]::[<$field:camel>],]
            [$($prefixes)* [<$target Column>]::[<$field:camel>] => ::core::option::Option::None,]
            [$($prefix_views)* [<$target Clause>]::[<$field:camel>](_) => ::core::option::Option::None,]
            [$($arrow_types)* [<$target Column>]::[<$field:camel>] => $crate::define_schema!(@arrow $($arrow)?),]
            [$($bindings)* ($field: $ty [input] [$($column)?] [$($values)?])];
            $($rest)*
        );
    };
    (@fields [$vis:vis] [$target:ident] [$($hook:expr,)*]
        [$($fields:tt)*] [$($clauses:tt)*] [$($arms:tt)*] [$($prefixes:tt)*]
        [$($prefix_views:tt)*] [$($arrow_types:tt)*] [$($bindings:tt)*];
        $(#[doc = $doc:expr])* #[internal] $(#[values($values:expr)])?
        $field:ident: $ty:ty, $($rest:tt)*
    ) => {
        $crate::define_schema!(@fields [$vis] [$target] [$($hook,)*]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)* $(#[doc = $doc])* [<$field:camel>]($crate::selection::ValuePredicate<$ty>),]
            [$($arms)* [<$target Clause>]::[<$field:camel>](_) => [<$target Column>]::[<$field:camel>],]
            [$($prefixes)* [<$target Column>]::[<$field:camel>] => ::core::option::Option::None,]
            [$($prefix_views)* [<$target Clause>]::[<$field:camel>](_) => ::core::option::Option::None,]
            [$($arrow_types)* [<$target Column>]::[<$field:camel>] => ::core::option::Option::None,]
            [$($bindings)* ($field: $ty [internal] [] [$($values)?])];
            $($rest)*
        );
    };
    (@fields [$vis:vis] [$target:ident] [$($hook:expr,)*]
        [$($fields:tt)*] [$($clauses:tt)*] [$($arms:tt)*] [$($prefixes:tt)*]
        [$($prefix_views:tt)*] [$($arrow_types:tt)*]
        [$(($tags:ident: $types:ty [$input:ident] [$($column:literal)?] [$($values:expr)?]))+]; $(,)*
    ) => {
        $crate::schema::__private::paste! {
            #[doc = concat!("Logical columns of [`", stringify!($target), "`].")]
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            $vis enum [<$target Column>] { $($fields)* }

            // Emit enum-map support directly to avoid a caller-side dependency.
            impl $crate::schema::__private::enum_map::Enum for [<$target Column>] {
                type Array<V> = [V; [$(::core::stringify!($tags)),+].len()];

                fn from_usize(value: usize) -> Self {
                    [$(Self::[<$tags:camel>]),+][value]
                }

                fn into_usize(self) -> usize { self as usize }
            }

            #[doc = concat!("Conditions on [`", stringify!($target), "`], combined with logical AND.")]
            #[allow(dead_code)] // Some schema consumers use only column metadata or candidate domains.
            $vis enum [<$target Clause>] { $($clauses)* }

            #[doc = concat!("Native column types declared by [`", stringify!($target), "`].")]
            $vis trait [<$target ColumnTypes>] {
                $(
                    #[doc = concat!("The native type of `", stringify!($tags), "`.")]
                    type [<$tags:camel>]: ::core::cmp::Ord + ::core::clone::Clone;
                )+
            }

            impl [<$target ColumnTypes>] for $target {
                $(type [<$tags:camel>] = $types;)+
            }

            #[doc = concat!("Necessary candidate restrictions for one alternative on [`", stringify!($target), "`].")]
            #[derive(Clone)]
            $vis struct [<$target Domains>] {
                $(pub $tags: $crate::selection::domain::Domain<<$target as [<$target ColumnTypes>]>::[<$tags:camel>]>,)+
            }

            impl Default for [<$target Domains>] {
                fn default() -> Self {
                    Self { $($tags: $crate::selection::domain::Domain::all(),)+ }
                }
            }

            impl [<$target Domains>] {
                /// Whether a modeled column proves this alternative impossible.
                $vis fn is_empty(&self) -> bool {
                    false $(|| self.$tags.is_empty())+
                }

                /// These restrictions as a conjunction of native predicates, for key
                /// preparation. It admits at least the domains' values: see
                /// [`Domain::to_predicate`]. An empty domain selects nothing.
                ///
                /// [`Domain::to_predicate`]: $crate::selection::domain::Domain::to_predicate
                #[allow(dead_code)] // Only schemas navigated through restrictions use this.
                $vis fn to_selection(&self) -> $crate::selection::Selection<$target> {
                    if self.is_empty() {
                        return $crate::selection::Selection::Empty;
                    }
                    #[allow(unused_mut)]
                    let mut selection = $crate::selection::Selection::All;
                    $(
                        if let ::core::option::Option::Some(predicate) = self.$tags.to_predicate() {
                            selection = selection.and([<$target Clause>]::[<$tags:camel>](predicate));
                        }
                    )+
                    selection
                }
            }

            impl $target {
                #[allow(dead_code)]
                pub fn arrow_type(column: [<$target Column>]) -> ::core::option::Option<$crate::schema::__private::DataType> {
                    match column { $($arrow_types)* }
                }

                #[allow(dead_code)]
                pub fn arrow_schema() -> $crate::schema::__private::SchemaRef {
                    // This static belongs to this concrete generated schema,
                    // rather than a generic function shared by multiple schemas.
                    static SCHEMA: ::std::sync::OnceLock<$crate::schema::__private::SchemaRef> = ::std::sync::OnceLock::new();
                    ::std::sync::Arc::clone(SCHEMA.get_or_init(|| {
                        $crate::schema::__private::arrow_schema::<Self>(Self::arrow_type)
                    }))
                }
            }

            impl $crate::SelectionSchema for $target {
                type Column = [<$target Column>];
                type Clause = [<$target Clause>];
                type Domains = [<$target Domains>];

                const COLUMNS: &'static [Self::Column] = &[$([<$target Column>]::[<$tags:camel>],)+];

                fn column(clause: &Self::Clause) -> Self::Column {
                    match clause { $($arms)* }
                }

                fn tag(column: Self::Column) -> &'static str {
                    match column { $([<$target Column>]::[<$tags:camel>] => stringify!($tags),)+ }
                }

                fn column_from_tag(tag: &str) -> ::core::option::Option<Self::Column> {
                    match tag {
                        $(stringify!($tags) => ::core::option::Option::Some([<$target Column>]::[<$tags:camel>]),)+
                        _ => ::core::option::Option::None,
                    }
                }

                fn sql_name(column: Self::Column) -> ::core::option::Option<&'static str> {
                    match column {
                        $([<$target Column>]::[<$tags:camel>] =>
                            $crate::define_schema!(@column $input stringify!($tags) $(, $column)?),)+
                    }
                }

                fn value_clause<P: $crate::selection::ValuePredicateBuilder>(
                    column: Self::Column,
                    builder: P,
                ) -> ::core::option::Option<Self::Clause> {
                    match column {
                        $([<$target Column>]::[<$tags:camel>] =>
                            $crate::define_schema!(@value $input builder, $types, [<$target Clause>]::[<$tags:camel>]),)+
                    }
                }

                fn starts_with_clause(column: Self::Column, prefix: &str) -> ::core::option::Option<Self::Clause> {
                    let constructor: fn(&str) -> Self::Clause =
                        match column { $($prefixes)* }?;
                    ::core::option::Option::Some(constructor(prefix))
                }

                fn starts_with_prefix(clause: &Self::Clause) -> Option<&str> {
                    match clause { $($prefix_views)* }
                }

                fn analyze_branch(
                    filters: &[::std::sync::Arc<dyn $crate::schema::__private::PhysicalExpr>],
                ) -> $crate::schema::__private::anyhow::Result<$crate::selection::BranchOutcome<Self::Domains>> {
                    $(
                        let $tags = $crate::define_schema!(@domain $input $types, filters,
                            $crate::define_schema!(@name stringify!($tags) $(, $column)?));
                        $(let $tags = $tags.intersect($crate::selection::domain::Domain::values($values));)?
                    )+
                    #[allow(unused_mut)] // Tables may have no derivation hooks.
                    let mut result = [<$target Domains>] { $($tags,)+ };
                    if result.is_empty() {
                        return Ok($crate::selection::BranchOutcome::Impossible);
                    }
                    $(
                        ($hook)(&mut result)?;
                        if result.is_empty() {
                            return Ok($crate::selection::BranchOutcome::Impossible);
                        }
                    )*
                    Ok($crate::selection::BranchOutcome::Candidates(result))
                }

                fn union_domains(left: Self::Domains, right: Self::Domains) -> Self::Domains {
                    [<$target Domains>] { $($tags: left.$tags.union(right.$tags),)+ }
                }
            }
        }
    };

    (@arrow $arrow:expr) => { ::core::option::Option::Some($arrow) };
    (@arrow) => { ::core::option::Option::None };
    (@name $default:expr, $alias:literal) => { $alias };
    (@name $default:expr) => { $default };
    (@column input $default:expr $(, $alias:literal)?) => {
        ::core::option::Option::Some($crate::define_schema!(@name $default $(, $alias)?))
    };
    (@column internal $default:expr) => { ::core::option::Option::None };
    (@value input $builder:ident, $ty:ty, $variant:path) => {
        $builder.build::<$ty>().map($variant)
    };
    (@value internal $builder:ident, $ty:ty, $variant:path) => {{
        let _ = $builder;
        ::core::option::Option::None
    }};
    (@domain input $ty:ty, $filters:ident, $column:expr) => {
        $crate::schema::__private::column_domain::<$ty>($filters, $column)?
    };
    (@domain internal $ty:ty, $filters:ident, $column:expr) => {{
        let _ = $filters;
        $crate::selection::domain::Domain::<$ty>::all()
    }};
    (
        @define
        $(#[doc = $doc:expr])*
        $table:ident, $row:ident, $builder:ident, $name:literal, $scheme:expr, $sharding:ty, $shard:tt, [$($row_spec:tt)*], {
            $($(#[doc = $column_doc:expr])*
              $(#[values($values:expr)])? $(#[starts_with $($prefix:tt)*])?
              $column:ident: $native:ty => $arrow:ident $(:: $variant:ident)?),+ $(,)?
        }
        $(internal { $($(#[doc = $internal_doc:expr])* $internal:ident: $internal_type:ty),* $(,)? })?
    ) => {
        $crate::define_schema! {
            $(#[doc = $doc])*
            #[derive(Debug)]
            pub $table {
                $($(#[doc = $column_doc])* $(#[values($values)])? $(#[starts_with $($prefix)*])?
                  $column: $native => $crate::define_schema!(@data_type $arrow $(:: $variant)?),)+
                $($($(#[doc = $internal_doc])* #[internal] $internal: $internal_type,)*)?
            }
        }

        #[derive(Debug, Clone)]
        pub struct $row { $(pub $column: $native,)+ $($(pub $internal: $internal_type,)*)? }

        $crate::define_schema!(@logical $table, $row, $builder, $name, $scheme, $sharding, $shard, [$($row_spec)*]);

        $crate::schema::__private::paste! {
            /// Writes one projected row without constructing an owned native row.
            pub struct [<$table RowWriter>]<'a> {
                builder: &'a mut $builder,
            }

            impl $builder {
                pub fn row(&mut self) -> [<$table RowWriter>]<'_> {
                    [<$table RowWriter>] { builder: self }
                }
            }

            #[allow(dead_code)] // Consumers may use native rows or only some projected setters.
            impl [<$table RowWriter>]<'_> {
                $(
                    pub fn [<is_ $column _defined>](&self) -> bool {
                        self.builder.$column.is_some()
                    }

                    $crate::define_schema!(@setter $column $native, $arrow $(:: $variant)?);
                )+
            }

            impl Drop for [<$table RowWriter>]<'_> {
                fn drop(&mut self) {
                    use $crate::schema::__private::datafusion::arrow::array::ArrayBuilder as _;
                    $(if let Some(builder) = &mut self.builder.$column {
                        if builder.len() == self.builder.num_rows {
                            builder.append_null();
                        }
                    })+
                    self.builder.num_rows += 1;
                }
            }

            impl $row {
                /// Enforces every clause, including columns absent from the key.
                pub fn matches(&self, selection: &$crate::selection::Selection<$table>) -> bool {
                    use $crate::selection::Selection;
                    #[allow(unused_imports)] // Only schemas with prefix clauses need this.
                    use $crate::schema::rows::StringValue as _;
                    match selection {
                        Selection::All => true,
                        Selection::Empty => false,
                        Selection::Predicates(columns) => columns.iter()
                            .flat_map(|(_, clauses)| clauses)
                            .all(|clause| match clause {
                                $(
                                    [<$table Clause>]::[<$column:camel>](predicate) => predicate.matches(&self.$column),
                                    $(
                                        $($prefix)*
                                        [<$table Clause>]::[<$column:camel StartsWith>](prefix) =>
                                            self.$column.string_value().is_some_and(|value| value.starts_with(prefix.as_str())),
                                    )?
                                )+
                                $($([<$table Clause>]::[<$internal:camel>](predicate) => predicate.matches(&self.$internal),)*)?
                            }),
                    }
                }
            }
        }

        impl $table {
            #[allow(dead_code)] // Documentation generation is optional for private declarations.
            pub const DESCRIPTION: &'static str = concat!($($doc, "\n",)*);
            pub const COLUMN_DOCS: &'static [$crate::schema::rows::TableColumn] = &[
                $($crate::schema::rows::TableColumn {
                    name: stringify!($column),
                    column_type: $crate::define_schema!(@type_name $arrow $(:: $variant)?),
                    description: concat!($($column_doc, "\n",)*),
                },)+
            ];
        }

        /// Projection-aware Arrow builder. Rows can be appended without cloning.
        pub struct $builder {
            schema: $crate::schema::__private::datafusion::arrow::datatypes::SchemaRef,
            num_rows: usize,
            $($column: Option<$crate::define_schema!(@builder $arrow $(:: $variant)?)>,)+
        }

        impl $builder {
            pub fn schema() -> $crate::schema::__private::datafusion::arrow::datatypes::SchemaRef {
                <$table as $crate::LogicalSchema>::arrow_schema()
            }

            pub fn new(schema: $crate::schema::__private::datafusion::arrow::datatypes::SchemaRef) -> Self {
                Self {
                    $($column: schema.column_with_name(stringify!($column)).map(|_| $crate::define_schema!(@new $arrow $(:: $variant)?)),)+
                    schema,
                    num_rows: 0,
                }
            }

            pub fn num_rows(&self) -> usize { self.num_rows }

            pub fn append(&mut self, row: &$row) {
                $(if let Some(builder) = &mut self.$column {
                    $crate::define_schema!(@append $arrow $(:: $variant)? builder row $column);
                })+
                self.num_rows += 1;
            }

            pub fn finish(&mut self) -> $crate::schema::__private::datafusion::common::Result<$crate::schema::__private::datafusion::arrow::record_batch::RecordBatch> {
                self.finish_batch(true)
            }

            /// Finishes the final batch without reserving buffers for another one.
            pub fn into_batch(mut self) -> $crate::schema::__private::datafusion::common::Result<$crate::schema::__private::datafusion::arrow::record_batch::RecordBatch> {
                self.finish_batch(false)
            }

            fn finish_batch(&mut self, reserve_next: bool) -> $crate::schema::__private::datafusion::common::Result<$crate::schema::__private::datafusion::arrow::record_batch::RecordBatch> {
                let arrays = self.schema.fields().iter().map(|field| {
                    match field.name().as_str() {
                        $(stringify!($column) => {
                            let builder = self.$column.as_mut().expect("projected column has a builder");
                            Ok(if reserve_next {
                                $crate::schema::rows::BuilderCapacity::finish_and_new(builder)
                            } else {
                                $crate::schema::__private::datafusion::arrow::array::ArrayBuilder::finish(builder)
                            })
                        },)+
                        name => Err($crate::schema::__private::datafusion::common::DataFusionError::Internal(format!("unknown projected column {name}"))),
                    }
                }).collect::<$crate::schema::__private::datafusion::common::Result<Vec<_>>>()?;
                let options = $crate::schema::__private::datafusion::arrow::record_batch::RecordBatchOptions::new()
                    .with_row_count(Some(std::mem::take(&mut self.num_rows)));
                Ok($crate::schema::__private::datafusion::arrow::record_batch::RecordBatch::try_new_with_options(
                    ::std::sync::Arc::clone(&self.schema), arrays, &options,
                )?)
            }
        }

    };
    (@logical $table:ident, $row:ident, $builder:ident, $name:literal, $scheme:expr, $sharding:ty, $shard:tt, []) => {
        $crate::define_schema!(@logical $table, $row, $builder, $name, $scheme, $sharding, $shard,
            [for<'a> &'a $row, $builder::append]);
    };
    (@logical $table:ident, $row:ident, $builder:ident, $name:literal, $scheme:expr, $sharding:ty, $shard:tt,
        [for<$row_lifetime:lifetime> $row_type:ty, $append:path]) => {
        impl $crate::LogicalSchema for $table {
            const NAME: &'static str = $name;
            const SHARDING: $crate::ShardingScheme = $scheme;
            type Sharding = $sharding;
            type Row<$row_lifetime> = $row_type;
            type RowBuilder = $builder;
            $crate::define_schema!(@shard_items $table $shard);
            fn arrow_type(column: Self::Column) -> Option<$crate::schema::__private::datafusion::arrow::datatypes::DataType> { Self::arrow_type(column) }
            fn arrow_schema() -> $crate::schema::__private::datafusion::arrow::datatypes::SchemaRef { Self::arrow_schema() }
        }
        impl $crate::RowBuilder<$table> for $builder {
            fn new(schema: $crate::schema::__private::datafusion::arrow::datatypes::SchemaRef) -> Self { Self::new(schema) }
            fn num_rows(&self) -> usize { self.num_rows() }
            fn append(&mut self, row: <$table as $crate::LogicalSchema>::Row<'_>) { $append(self, row) }
            fn finish(&mut self) -> $crate::schema::__private::datafusion::common::Result<$crate::schema::__private::datafusion::arrow::record_batch::RecordBatch> { self.finish() }
            fn into_batch(self) -> $crate::schema::__private::datafusion::common::Result<$crate::schema::__private::datafusion::arrow::record_batch::RecordBatch> { self.into_batch() }
        }
    };
    // Sharding columns and the generated candidate shard keys of one alternative.
    (@shard_items $table:ident {
        columns [$({ $($column:ident: $derivation:ident),* $(,)? })?]
        shard_keys [$($shard_keys:expr)?]
    }) => {
        $crate::schema::__private::paste! {
            const SHARDING_COLUMNS: &'static [$crate::schema::sharding::ShardingColumn<Self::Column>] = &[
                $($($crate::schema::sharding::ShardingColumn {
                    column: [<$table Column>]::[<$column:camel>],
                    kind: <$crate::schema::sharding::shard_key::$derivation
                        as $crate::schema::sharding::shard_key::Derivation>::KIND,
                    nullable: <<$table as [<$table ColumnTypes>]>::[<$column:camel>]
                        as $crate::schema::sharding::shard_key::Input<$crate::schema::sharding::shard_key::$derivation>>::NULLABLE,
                },)*)?
            ];

            fn branch_shard_keys(domains: Self::Domains) -> <Self::Sharding as $crate::schema::sharding::Sharding>::Candidates {
                use $crate::schema::sharding::Candidates;
                let _ = &domains; // Unused without sharding columns or a closure.
                #[allow(unused_mut)]
                let mut shard_keys = <<Self::Sharding as $crate::schema::sharding::Sharding>::Candidates as Candidates>::all();
                $($(shard_keys = Candidates::intersect(
                    shard_keys,
                    <<$table as [<$table ColumnTypes>]>::[<$column:camel>]
                        as $crate::schema::sharding::shard_key::Input<$crate::schema::sharding::shard_key::$derivation>>
                        ::shard_keys(&domains.$column),
                );)*)?
                $(shard_keys = Candidates::intersect(shard_keys, ($shard_keys)(domains));)?
                shard_keys
            }
        }
    };
    (@data_type TimestampMillisecond) => {
        $crate::schema::__private::datafusion::arrow::datatypes::DataType::Timestamp(
            $crate::schema::__private::datafusion::arrow::datatypes::TimeUnit::Millisecond, Some("+00:00".into()),
        )
    };
    (@data_type $namespace:ident :: Duration) => {
        $namespace::Duration($crate::schema::__private::datafusion::arrow::datatypes::TimeUnit::Millisecond)
    };
    (@data_type UInt32List) => { $crate::define_schema!(@list_type UInt32) };
    (@data_type Utf8List) => { $crate::define_schema!(@list_type Utf8) };
    (@data_type LargeUtf8List) => { $crate::define_schema!(@list_type LargeUtf8) };
    (@data_type $namespace:ident :: $arrow:ident) => { $namespace::$arrow };
    (@list_type $arrow:ident) => {
        $crate::schema::__private::datafusion::arrow::datatypes::DataType::List(std::sync::Arc::new(
            $crate::schema::__private::datafusion::arrow::datatypes::Field::new("item", $crate::schema::__private::datafusion::arrow::datatypes::DataType::$arrow, true),
        ))
    };

    // The reference names types as the introspection API presents them: the
    // large variants read as their plain counterparts, and durations and lists
    // by their units and items.
    (@type_name $namespace:ident :: LargeUtf8) => { "Utf8" };
    (@type_name $namespace:ident :: LargeBinary) => { "Binary" };
    (@type_name $namespace:ident :: Duration) => { "DurationMillisecond" };
    (@type_name $namespace:ident :: $arrow:ident) => { stringify!($arrow) };
    (@type_name UInt32List) => { "UInt32 List" };
    (@type_name Utf8List) => { "Utf8 List" };
    (@type_name LargeUtf8List) => { "Utf8 List" };
    (@type_name $arrow:ident) => { stringify!($arrow) };

    (@builder $namespace:ident :: Utf8) => { $crate::schema::__private::datafusion::arrow::array::StringBuilder };
    (@builder $namespace:ident :: LargeUtf8) => { $crate::schema::__private::datafusion::arrow::array::LargeStringBuilder };
    (@builder $namespace:ident :: Binary) => { $crate::schema::__private::datafusion::arrow::array::BinaryBuilder };
    (@builder $namespace:ident :: LargeBinary) => { $crate::schema::__private::datafusion::arrow::array::LargeBinaryBuilder };
    (@builder $namespace:ident :: UInt32) => { $crate::schema::__private::datafusion::arrow::array::UInt32Builder };
    (@builder $namespace:ident :: UInt64) => { $crate::schema::__private::datafusion::arrow::array::UInt64Builder };
    (@builder $namespace:ident :: Int32) => { $crate::schema::__private::datafusion::arrow::array::Int32Builder };
    (@builder $namespace:ident :: Boolean) => { $crate::schema::__private::datafusion::arrow::array::BooleanBuilder };
    (@builder TimestampMillisecond) => { $crate::schema::__private::datafusion::arrow::array::TimestampMillisecondBuilder };
    (@builder $namespace:ident :: Duration) => { $crate::schema::__private::datafusion::arrow::array::DurationMillisecondBuilder };
    (@builder UInt32List) => { $crate::schema::__private::datafusion::arrow::array::ListBuilder<$crate::define_schema!(@builder DataType::UInt32)> };
    (@builder Utf8List) => { $crate::schema::__private::datafusion::arrow::array::ListBuilder<$crate::define_schema!(@builder DataType::Utf8)> };
    (@builder LargeUtf8List) => { $crate::schema::__private::datafusion::arrow::array::ListBuilder<$crate::define_schema!(@builder DataType::LargeUtf8)> };

    (@new TimestampMillisecond) => {
        $crate::schema::__private::datafusion::arrow::array::TimestampMillisecondBuilder::new().with_timezone("+00:00")
    };
    (@new $arrow:ident $(:: $variant:ident)?) => { <$crate::define_schema!(@builder $arrow $(:: $variant)?)>::default() };

    (@append $namespace:ident :: Utf8 $builder:ident $row:ident $column:ident) => { $crate::define_schema!(@string $builder $row $column) };
    (@append $namespace:ident :: LargeUtf8 $builder:ident $row:ident $column:ident) => { $crate::define_schema!(@string $builder $row $column) };
    (@append $namespace:ident :: Binary $builder:ident $row:ident $column:ident) => { $crate::define_schema!(@binary $builder $row $column) };
    (@append $namespace:ident :: LargeBinary $builder:ident $row:ident $column:ident) => { $crate::define_schema!(@binary $builder $row $column) };
    (@append UInt32List $builder:ident $row:ident $column:ident) => {
        $crate::define_schema!(@list $builder $row $column PrimitiveValue primitive_value)
    };
    (@append Utf8List $builder:ident $row:ident $column:ident) => {
        $crate::define_schema!(@list $builder $row $column StringValue string_value)
    };
    (@append LargeUtf8List $builder:ident $row:ident $column:ident) => {
        $crate::define_schema!(@list $builder $row $column StringValue string_value)
    };
    (@append $namespace:ident :: $arrow:ident $builder:ident $row:ident $column:ident) => {
        $crate::define_schema!(@primitive $builder $row $column)
    };
    (@append TimestampMillisecond $builder:ident $row:ident $column:ident) => {
        $crate::define_schema!(@primitive $builder $row $column)
    };
    (@primitive $builder:ident $row:ident $column:ident) => {{
        use $crate::schema::rows::PrimitiveValue as _;
        $builder.append_option($row.$column.primitive_value())
    }};
    (@string $builder:ident $row:ident $column:ident) => {{
        $crate::schema::rows::ArrowStringValue::append_to(&$row.$column, $builder)
    }};
    (@binary $builder:ident $row:ident $column:ident) => {{
        use $crate::schema::rows::SliceValue as _;
        $builder.append_option($row.$column.slice_value())
    }};
    (@list $builder:ident $row:ident $column:ident $trait:ident $method:ident) => {{
        use $crate::schema::rows::{SliceValue as _, $trait as _};
        $builder.append_option($row.$column.slice_value().map(|items| items.iter().map(|item| item.$method())))
    }};

    (@setter $column:ident $native:ty, $namespace:ident :: Utf8) => {
        $crate::define_schema!(@string_setter $column);
    };
    (@setter $column:ident $native:ty, $namespace:ident :: LargeUtf8) => {
        $crate::define_schema!(@string_setter $column);
    };
    (@setter $column:ident $native:ty, $namespace:ident :: Binary) => {
        $crate::define_schema!(@binary_setter $column);
    };
    (@setter $column:ident $native:ty, $namespace:ident :: LargeBinary) => {
        $crate::define_schema!(@binary_setter $column);
    };
    (@binary_setter $column:ident) => {
        pub fn $column(&mut self, value: impl $crate::schema::rows::SliceValue<Item = u8>) {
            if let Some(builder) = &mut self.builder.$column {
                $crate::define_schema!(@unwritten self builder);
                builder.append_option(value.slice_value());
            }
        }
    };
    (@string_setter $column:ident) => {
        pub fn $column(&mut self, value: impl AsRef<str>) {
            if let Some(builder) = &mut self.builder.$column {
                $crate::define_schema!(@unwritten self builder);
                builder.append_value(value);
            }
        }

        $crate::schema::__private::paste! {
            pub fn [<fmt_ $column>](&mut self, value: impl ::std::fmt::Display) {
                if let Some(builder) = &mut self.builder.$column {
                    use ::std::fmt::Write as _;
                    $crate::define_schema!(@unwritten self builder);
                    write!(builder, "{value}").expect("formatting into Arrow is infallible");
                    builder.append_value("");
                }
            }
        }
    };
    (@setter $column:ident $native:ty, $namespace:ident :: UInt64) => { $crate::define_schema!(@scalar_setter $column u64); };
    (@setter $column:ident $native:ty, $namespace:ident :: UInt32) => { $crate::define_schema!(@scalar_setter $column u32); };
    (@setter $column:ident $native:ty, $namespace:ident :: Int32) => { $crate::define_schema!(@scalar_setter $column i32); };
    (@setter $column:ident $native:ty, $namespace:ident :: Boolean) => { $crate::define_schema!(@scalar_setter $column bool); };
    (@setter $column:ident $native:ty, $namespace:ident :: Duration) => { $crate::define_schema!(@scalar_setter $column i64); };
    (@setter $column:ident $native:ty, TimestampMillisecond) => { $crate::define_schema!(@scalar_setter $column i64); };
    (@scalar_setter $column:ident $ty:ty) => {
        pub fn $column(&mut self, value: impl Into<Option<$ty>>) {
            if let Some(builder) = &mut self.builder.$column {
                $crate::define_schema!(@unwritten self builder);
                builder.append_option(value.into());
            }
        }
    };
    (@setter $column:ident $native:ty, UInt32List) => {
        $crate::define_schema!(@native_setter $column $native, UInt32List);
        $crate::schema::__private::paste! {
            pub fn [<$column _iter>](&mut self, values: impl IntoIterator<Item = Option<u32>>) {
                if let Some(builder) = &mut self.builder.$column {
                    $crate::define_schema!(@unwritten self builder);
                    builder.append_value(values);
                }
            }
        }
    };
    (@setter $column:ident $native:ty, $arrow:ident $(:: $variant:ident)?) => {
        $crate::define_schema!(@native_setter $column $native, $arrow $(:: $variant)?);
    };
    (@native_setter $column:ident $native:ty, $arrow:ident $(:: $variant:ident)?) => {
        pub fn $column(&mut self, value: $native) {
            if let Some(builder) = &mut self.builder.$column {
                $crate::define_schema!(@unwritten self builder);
                struct Value { $column: $native }
                let row = Value { $column: value };
                $crate::define_schema!(@append $arrow $(:: $variant)? builder row $column);
            }
        }
    };
    (@unwritten $writer:ident $builder:ident) => {
        assert_eq!(
            $crate::schema::__private::datafusion::arrow::array::ArrayBuilder::len($builder),
            $writer.builder.num_rows,
            "a projected column must be written at most once per row",
        );
    };
}
