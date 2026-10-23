// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Declares logical selection metadata from one authoritative column list.
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
/// - Arrow representations are optional. The generated `arrow_schema` method
///   includes only represented columns, with permissive Arrow nullability.
/// - `#[starts_with]` adds a literal prefix clause. Its operand is always `ReString`,
///   including for nullable columns. Physical codecs decide how to prepare it.
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
/// For example, `service_stats` generates `ServiceStats`, `ServiceStatsRow`, and
/// `ServiceStatsBatchBuilder`. The syntax remains provisional until the proc-macro
/// conversion. Both forms share the same selection metadata generation.
///
/// ```text
/// define_schema! {
///     /// Source documentation.
///     service_stats {
///         name: "sys_service_stats",
///         sharding: ShardingScheme::PartitionId,
///         columns {
///             #[starts_with]
///             service_name: ReString => DataType::LargeUtf8,
///             num_entries: u64 => DataType::UInt64,
///         }
///     }
/// }
/// ```
///
/// `name` is the stable source-schema identity; registration may choose a session
/// alias. `sharding` classifies the shard domain without selecting serving nodes.
/// Native columns also drive selection clauses and complete `#[values(...)]`
/// domains. The generated row owns native values; `LogicalSchema::Row<'a>` borrows
/// that row for the callback. Builders reuse their projection across batches.
/// This form currently requires an Arrow representation for every column;
/// the selection-only form supports aliases, internal columns, and optional Arrow views.
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
/// Native values are `ReString`/`String` (also `EntryKind` for strings), `Vec<u8>`
/// for binary data, the corresponding integer/bool for scalars (`i64` for time),
/// and `Vec<V>` for lists. `Option<V>` preserves NULL for columns and list items.
/// Native aliases work without macro-level type inspection. Literal conversions
/// remain the responsibility of `SelectionValue`; unsupported SQL comparisons
/// (including binary/list literals) remain residual.
#[macro_export]
macro_rules! define_schema {
    (
        $(#[doc = $doc:expr])*
        $schema:ident {
            name: $name:literal,
            sharding: $sharding:expr,
            columns { $($columns:tt)* }
        }
    ) => {
        $crate::schema::__private::paste! {
            $crate::define_schema!(@define
                $(#[doc = $doc])*
                [<$schema:camel>], [<$schema:camel Row>], [<$schema:camel BatchBuilder>],
                $name, $sharding, {
                    $($columns)*
                }
            );
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
                    [<$target Clause>]::[<$field:camel StartsWith>]
                        as fn($crate::schema::__private::ReString) -> Self::Clause
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

            impl [<$target Domains>] {
                /// Whether a modeled column proves this alternative impossible.
                $vis fn is_empty(&self) -> bool {
                    false $(|| self.$tags.is_empty())+
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
                    let constructor: fn($crate::schema::__private::ReString) -> Self::Clause =
                        match column { $($prefixes)* }?;
                    ::core::option::Option::Some(constructor(prefix.into()))
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
        $table:ident, $row:ident, $builder:ident, $name:literal, $sharding:expr, {
            $($(#[doc = $column_doc:expr])*
              $(#[values($values:expr)])? $(#[starts_with $($prefix:tt)*])?
              $column:ident: $native:ty => $arrow:ident $(:: $variant:ident)?),+ $(,)?
        }
    ) => {
        $crate::define_schema! {
            $(#[doc = $doc])*
            #[derive(Debug)]
            pub $table {
                $($(#[doc = $column_doc])* $(#[values($values)])? $(#[starts_with $($prefix)*])?
                  $column: $native => $crate::define_schema!(@data_type $arrow $(:: $variant)?),)+
            }
        }

        #[derive(Debug, Clone)]
        pub struct $row { $(pub $column: $native,)+ }

        impl $crate::LogicalSchema for $table {
            const NAME: &'static str = $name;
            const SHARDING: $crate::ShardingScheme = $sharding;
            type Row<'a> = &'a $row;
            type RowBuilder = $builder;

            fn arrow_type(column: Self::Column) -> Option<$crate::schema::__private::datafusion::arrow::datatypes::DataType> {
                Self::arrow_type(column)
            }

            fn arrow_schema() -> $crate::schema::__private::datafusion::arrow::datatypes::SchemaRef {
                Self::arrow_schema()
            }
        }

        $crate::schema::__private::paste! {
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
                            }),
                    }
                }
            }
        }

        impl $table {
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

        impl $crate::RowBuilder<$table> for $builder {
            fn new(schema: $crate::schema::__private::datafusion::arrow::datatypes::SchemaRef) -> Self { Self::new(schema) }
            fn num_rows(&self) -> usize { self.num_rows() }
            fn append(&mut self, row: <$table as $crate::LogicalSchema>::Row<'_>) { self.append(row) }
            fn finish(&mut self) -> $crate::schema::__private::datafusion::common::Result<$crate::schema::__private::datafusion::arrow::record_batch::RecordBatch> { self.finish() }
            fn into_batch(self) -> $crate::schema::__private::datafusion::common::Result<$crate::schema::__private::datafusion::arrow::record_batch::RecordBatch> { self.into_batch() }
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

    (@type_name $namespace:ident :: $arrow:ident) => { stringify!($arrow) };
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
        use $crate::schema::rows::StringValue as _;
        $builder.append_option($row.$column.string_value())
    }};
    (@binary $builder:ident $row:ident $column:ident) => {{
        use $crate::schema::rows::SliceValue as _;
        $builder.append_option($row.$column.slice_value())
    }};
    (@list $builder:ident $row:ident $column:ident $trait:ident $method:ident) => {{
        use $crate::schema::rows::{SliceValue as _, $trait as _};
        $builder.append_option($row.$column.slice_value().map(|items| items.iter().map(|item| item.$method())))
    }};
}
