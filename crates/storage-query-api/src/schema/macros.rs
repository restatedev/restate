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
/// define_table! {
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
/// `<Schema>Column` and `<Schema>Clause` enums, and a `<Schema>ColumnTypes` trait exposing each native type as an associated type.
/// It does not define a physical table, storage row, or key. The macro's historical
/// name will be consolidated into `define_schema!` during the proc-macro conversion.
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
/// # Example
///
/// ```
/// use datafusion::arrow::datatypes::DataType;
/// use restate_storage_query_api::{SelectionSchema, define_table};
/// use restate_storage_query_api::selection::{Selection, ValuePredicate};
/// use restate_types::vqueues::EntryKind;
/// use restate_util_string::ReString;
///
/// define_table! {
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
#[macro_export]
macro_rules! define_table {
    (
        $(#[$meta:meta])*
        $vis:vis $target:ident { $($fields:tt)+ }
        $(derive [$($hook:expr),* $(,)?])?
    ) => {
        $(#[$meta])*
        $vis enum $target {}

        $crate::define_table!(@fields [$vis] [$target] [$($($hook,)*)?]
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
        $crate::define_table!(@fields [$vis] [$target] [$($hook,)*]
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
            [$($arrow_types)* [<$target Column>]::[<$field:camel>] => $crate::define_table!(@arrow $($arrow)?),]
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
        $crate::define_table!(@fields [$vis] [$target] [$($hook,)*]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)* $(#[doc = $doc])* [<$field:camel>]($crate::selection::ValuePredicate<$ty>),]
            [$($arms)* [<$target Clause>]::[<$field:camel>](_) => [<$target Column>]::[<$field:camel>],]
            [$($prefixes)* [<$target Column>]::[<$field:camel>] => ::core::option::Option::None,]
            [$($prefix_views)* [<$target Clause>]::[<$field:camel>](_) => ::core::option::Option::None,]
            [$($arrow_types)* [<$target Column>]::[<$field:camel>] => $crate::define_table!(@arrow $($arrow)?),]
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
        $crate::define_table!(@fields [$vis] [$target] [$($hook,)*]
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
                            $crate::define_table!(@column $input stringify!($tags) $(, $column)?),)+
                    }
                }

                fn value_clause<P: $crate::selection::ValuePredicateBuilder>(
                    column: Self::Column,
                    builder: P,
                ) -> ::core::option::Option<Self::Clause> {
                    match column {
                        $([<$target Column>]::[<$tags:camel>] =>
                            $crate::define_table!(@value $input builder, $types, [<$target Clause>]::[<$tags:camel>]),)+
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

            }
        }
    };

    (@arrow $arrow:expr) => { ::core::option::Option::Some($arrow) };
    (@arrow) => { ::core::option::Option::None };
    (@name $default:expr, $alias:literal) => { $alias };
    (@name $default:expr) => { $default };
    (@column input $default:expr $(, $alias:literal)?) => {
        ::core::option::Option::Some($crate::define_table!(@name $default $(, $alias)?))
    };
    (@column internal $default:expr) => { ::core::option::Option::None };
    (@value input $builder:ident, $ty:ty, $variant:path) => {
        $builder.build::<$ty>().map($variant)
    };
    (@value internal $builder:ident, $ty:ty, $variant:path) => {{
        let _ = $builder;
        ::core::option::Option::None
    }};
}
