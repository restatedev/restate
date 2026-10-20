// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Implements [`Selector`](crate::selection::Selector) for a table marker
/// declared with [`define_table!`](crate::define_table).
///
/// ```text
/// define_selector! {
///     /// `Selector` impl documentation.
///     <vis> <Table> {
///         /// Field documentation.
///         <field>: <Type>,                 // value clauses
///         <field>: <Type> => starts_with,  // value and prefix clauses
///         ...
///     }
/// }
/// ```
///
/// Generates `<Table>Field` and `<Table>Clause` enums with visibility `<vis>`.
/// The fields describe the selectable schema, not the fields of a Rust record
/// struct or the layout of a storage key. For example, declaring
/// `handler: Option<ReString> => starts_with` on `Entries` produces:
///
/// | Generated | Example |
/// | --- | --- |
/// | Tag: the field identifier, verbatim | `"handler"` |
/// | Field variant: the identifier in CamelCase | `EntriesField::Handler` |
/// | Value clause over `ValuePredicate<Type>` | `EntriesClause::Handler(ValuePredicate<Option<ReString>>)` |
/// | Prefix clause, only with `=> starts_with` | `EntriesClause::HandlerStartsWith(ReString)` |
///
/// - Adapters and physical key schemas find fields by tag, so field identifiers
///   must match the names they use, such as snake_case SQL column names.
/// - `<Type>` must implement [`SelectionValue`](crate::selection::SelectionValue).
/// - `=> starts_with` declares that the field supports literal string prefixes;
///   the macro does not check `<Type>`. The prefix is always a `ReString`,
///   including for `Option<ReString>`. Storage may still reject a prefix clause
///   on a field whose encoding cannot match prefixes.
/// - Declaration order sets each field's enum-map index, not physical key order.
/// - Only doc comments are accepted as attributes. Docs before `<Table>` go on
///   the `Selector` impl; document the marker itself in `define_table!`.
///   Field docs are copied to the field variant and each of its clause variants.
/// - Invoke it in the crate that declares the marker; the enums are generated at
///   the invocation site.
///
/// Generic adapters build clauses by tag instead of naming `<Table>Clause`; see
/// the [selection module example](crate::selection#example).
///
/// # Example
///
/// ```
/// use restate_storage_api::selection::{Selector, ValuePredicate};
/// use restate_storage_api::{define_selector, define_table};
/// use restate_types::vqueues::EntryKind;
/// use restate_util_string::ReString;
///
/// define_table! {
///     /// Example table.
///     pub Entries;
/// }
///
/// define_selector! {
///     /// Selects entries by handler and kind.
///     pub Entries {
///         /// The handler name, or NULL when absent.
///         handler: Option<ReString> => starts_with,
///         /// The entry kind.
///         kind: EntryKind,
///     }
/// }
///
/// assert_eq!(Entries::tag(EntriesField::Handler), "handler");
/// assert_eq!(Entries::field_from_tag("kind"), Some(EntriesField::Kind));
///
/// let prefix = Entries::starts_with_clause(EntriesField::Handler, "get").unwrap();
/// assert_eq!(Entries::field(&prefix), EntriesField::Handler);
/// assert_eq!(Entries::starts_with_prefix(&prefix), Some("get"));
///
/// let is_null = EntriesClause::Handler(ValuePredicate::Equal(None));
/// assert_eq!(Entries::starts_with_prefix(&is_null), None);
/// assert!(Entries::starts_with_clause(EntriesField::Kind, "inv").is_none());
/// ```
///
/// # Expansion
///
/// The example above expands to the following, with paths shortened by imports
/// and the field count written as a literal.
///
/// ```
/// use restate_storage_api::selection::__private::enum_map;
/// use restate_storage_api::selection::{Selector, ValuePredicate, ValuePredicateBuilder};
/// use restate_types::vqueues::EntryKind;
/// use restate_util_string::ReString;
/// # restate_storage_api::define_table! { pub Entries; }
///
/// /// Logical fields of [`Entries`].
/// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// pub enum EntriesField {
///     /// The handler name, or NULL when absent.
///     Handler,
///     /// The entry kind.
///     Kind,
/// }
///
/// // Implemented directly: enum-map's derive would require the invoking crate
/// // to depend on enum-map.
/// impl enum_map::Enum for EntriesField {
///     type Array<V> = [V; 2];
///
///     fn from_usize(value: usize) -> Self {
///         [Self::Handler, Self::Kind][value]
///     }
///
///     fn into_usize(self) -> usize {
///         self as usize
///     }
/// }
///
/// /// Conditions on [`Entries`], combined with logical AND.
/// pub enum EntriesClause {
///     /// The handler name, or NULL when absent.
///     Handler(ValuePredicate<Option<ReString>>),
///     /// The handler name, or NULL when absent.
///     ///
///     /// Matches a case-sensitive literal prefix; NULL never matches.
///     /// An empty prefix matches every non-NULL value. No LIKE syntax is interpreted.
///     HandlerStartsWith(ReString),
///     /// The entry kind.
///     Kind(ValuePredicate<EntryKind>),
/// }
///
/// /// Selects entries by handler and kind.
/// impl Selector for Entries {
///     type Field = EntriesField;
///     type Clause = EntriesClause;
///
///     fn field(clause: &Self::Clause) -> Self::Field {
///         match clause {
///             EntriesClause::Handler(_) | EntriesClause::HandlerStartsWith(_) => {
///                 EntriesField::Handler
///             }
///             EntriesClause::Kind(_) => EntriesField::Kind,
///         }
///     }
///
///     fn tag(field: Self::Field) -> &'static str {
///         match field {
///             EntriesField::Handler => "handler",
///             EntriesField::Kind => "kind",
///         }
///     }
///
///     fn field_from_tag(tag: &str) -> Option<Self::Field> {
///         match tag {
///             "handler" => Some(EntriesField::Handler),
///             "kind" => Some(EntriesField::Kind),
///             _ => None,
///         }
///     }
///
///     fn value_clause<P: ValuePredicateBuilder>(
///         field: Self::Field,
///         builder: P,
///     ) -> Option<Self::Clause> {
///         match field {
///             EntriesField::Handler => builder.build().map(EntriesClause::Handler),
///             EntriesField::Kind => builder.build().map(EntriesClause::Kind),
///         }
///     }
///
///     fn starts_with_clause(field: Self::Field, prefix: &str) -> Option<Self::Clause> {
///         // Copies the prefix only for fields that support it.
///         let constructor: fn(ReString) -> Self::Clause = match field {
///             EntriesField::Handler => {
///                 Some(EntriesClause::HandlerStartsWith as fn(ReString) -> Self::Clause)
///             }
///             EntriesField::Kind => None,
///         }?;
///         Some(constructor(prefix.into()))
///     }
///
///     fn starts_with_prefix(clause: &Self::Clause) -> Option<&str> {
///         match clause {
///             EntriesClause::Handler(_) => None,
///             EntriesClause::HandlerStartsWith(prefix) => Some(prefix.as_str()),
///             EntriesClause::Kind(_) => None,
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_selector {
    (
        $(#[doc = $doc:expr])*
        $vis:vis $target:ident { $($fields:tt)+ }
    ) => {
        $crate::define_selector!(@fields
            [$(#[doc = $doc])*] [$vis] [$target]
            [] [] [] [] [] [];
            $($fields)+ ,
        );
    };

    // Accumulate enum variants and match arms before emitting the items. Nested
    // macro calls cannot expand directly to enum variants or match arms.
    (@fields [$($docs:tt)*] [$vis:vis] [$target:ident]
        [$($fields:tt)*] [$($clauses:tt)*] [$($arms:tt)*] [$($prefixes:tt)*] [$($prefix_views:tt)*] [$($tags:ident)*];
        $(#[doc = $doc:expr])* $field:ident: $ty:ty => starts_with, $($rest:tt)*
    ) => {
        $crate::define_selector!(@fields [$($docs)*] [$vis] [$target]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)*
                $(#[doc = $doc])*
                [<$field:camel>]($crate::selection::ValuePredicate<$ty>),
                $(#[doc = $doc])*
                #[doc = ""]
                #[doc = "Matches a case-sensitive literal prefix; NULL never matches."]
                #[doc = "An empty prefix matches every non-NULL value. No LIKE syntax is interpreted."]
                [<$field:camel StartsWith>]($crate::selection::__private::ReString),
            ]
            [$($arms)*
                [<$target Clause>]::[<$field:camel>](_)
                | [<$target Clause>]::[<$field:camel StartsWith>](_) => [<$target Field>]::[<$field:camel>],
            ]
            [$($prefixes)*
                [<$target Field>]::[<$field:camel>] => ::core::option::Option::Some(
                    [<$target Clause>]::[<$field:camel StartsWith>]
                        as fn($crate::selection::__private::ReString) -> Self::Clause
                ),
            ]
            [$($prefix_views)*
                [<$target Clause>]::[<$field:camel>](_) => ::core::option::Option::None,
                [<$target Clause>]::[<$field:camel StartsWith>](prefix) => ::core::option::Option::Some(prefix.as_str()),
            ]
            [$($tags)* $field];
            $($rest)*
        );
    };
    (@fields [$($docs:tt)*] [$vis:vis] [$target:ident]
        [$($fields:tt)*] [$($clauses:tt)*] [$($arms:tt)*] [$($prefixes:tt)*] [$($prefix_views:tt)*] [$($tags:ident)*];
        $(#[doc = $doc:expr])* $field:ident: $ty:ty, $($rest:tt)*
    ) => {
        $crate::define_selector!(@fields [$($docs)*] [$vis] [$target]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)* $(#[doc = $doc])* [<$field:camel>]($crate::selection::ValuePredicate<$ty>),]
            [$($arms)* [<$target Clause>]::[<$field:camel>](_) => [<$target Field>]::[<$field:camel>],]
            [$($prefixes)* [<$target Field>]::[<$field:camel>] => ::core::option::Option::None,]
            [$($prefix_views)* [<$target Clause>]::[<$field:camel>](_) => ::core::option::Option::None,]
            [$($tags)* $field];
            $($rest)*
        );
    };
    (@fields [$($docs:tt)*] [$vis:vis] [$target:ident]
        [$($fields:tt)*] [$($clauses:tt)*] [$($arms:tt)*] [$($prefixes:tt)*] [$($prefix_views:tt)*] [$($tags:ident)+]; $(,)*
    ) => {
        $crate::selection::__private::paste! {
            #[doc = concat!("Logical fields of [`", stringify!($target), "`].")]
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            $vis enum [<$target Field>] { $($fields)* }

            // Emit enum-map support directly: its derive uses an absolute
            // `::enum_map` path, which would require a caller-side dependency.
            impl $crate::selection::__private::enum_map::Enum for [<$target Field>] {
                type Array<V> = [V; [$(::core::stringify!($tags)),+].len()];

                fn from_usize(value: usize) -> Self {
                    [$(Self::[<$tags:camel>]),+][value]
                }

                fn into_usize(self) -> usize { self as usize }
            }

            #[doc = concat!("Conditions on [`", stringify!($target), "`], combined with logical AND.")]
            $vis enum [<$target Clause>] { $($clauses)* }

            $($docs)*
            impl $crate::selection::Selector for $target {
                type Field = [<$target Field>];
                type Clause = [<$target Clause>];

                fn field(clause: &Self::Clause) -> Self::Field {
                    match clause { $($arms)* }
                }

                fn tag(field: Self::Field) -> &'static str {
                    match field { $([<$target Field>]::[<$tags:camel>] => stringify!($tags),)+ }
                }

                fn field_from_tag(tag: &str) -> ::core::option::Option<Self::Field> {
                    match tag {
                        $(stringify!($tags) => ::core::option::Option::Some([<$target Field>]::[<$tags:camel>]),)+
                        _ => ::core::option::Option::None,
                    }
                }

                fn value_clause<P: $crate::selection::ValuePredicateBuilder>(
                    field: Self::Field,
                    builder: P,
                ) -> ::core::option::Option<Self::Clause> {
                    match field {
                        $([<$target Field>]::[<$tags:camel>] =>
                            builder.build().map([<$target Clause>]::[<$tags:camel>]),)+
                    }
                }

                fn starts_with_clause(field: Self::Field, prefix: &str) -> ::core::option::Option<Self::Clause> {
                    let constructor: fn($crate::selection::__private::ReString) -> Self::Clause =
                        match field { $($prefixes)* }?;
                    ::core::option::Option::Some(constructor(prefix.into()))
                }

                fn starts_with_prefix(clause: &Self::Clause) -> ::core::option::Option<&str> {
                    match clause { $($prefix_views)* }
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use restate_util_string::ReString;

    use crate::selection::{Selection, Selector, ValuePredicate};

    type NullableName = Option<ReString>;

    crate::define_table! { pub(crate) Example; }
    crate::define_table! { Single; }

    crate::define_selector! {
        /// An example with string capabilities and a type alias.
        pub(crate) Example {
            /// The service name.
            #[doc = "Additional field documentation."]
            service_name: ReString => starts_with,
            /// The optional handler name.
            handler: NullableName => starts_with,
            /// The entry kind.
            kind: u64,
        }
    }

    crate::define_selector! { Single { entry_id: u64 } }

    #[test]
    fn generated_clauses_map_to_fields_and_preserve_grouping() {
        let mut selection = Selection::<Example>::default();
        for (clause, field) in [
            (
                ExampleClause::ServiceName(ValuePredicate::Equal("svc".into())),
                ExampleField::ServiceName,
            ),
            (
                Example::starts_with_clause(ExampleField::ServiceName, "s").unwrap(),
                ExampleField::ServiceName,
            ),
            (
                ExampleClause::Handler(ValuePredicate::Equal(None)),
                ExampleField::Handler,
            ),
            (
                Example::starts_with_clause(ExampleField::Handler, "get").unwrap(),
                ExampleField::Handler,
            ),
            (
                ExampleClause::Kind(ValuePredicate::In(vec![1, 2])),
                ExampleField::Kind,
            ),
        ] {
            assert_eq!(Example::field(&clause), field);
            assert_eq!(Example::field_from_tag(Example::tag(field)), Some(field));
            let prefix = Example::starts_with_prefix(&clause);
            match &clause {
                ExampleClause::ServiceNameStartsWith(value)
                | ExampleClause::HandlerStartsWith(value) => {
                    assert_eq!(prefix, Some(value.as_str()));
                    assert!(std::ptr::eq(
                        prefix.unwrap().as_ptr(),
                        value.as_str().as_ptr()
                    ));
                }
                _ => assert!(prefix.is_none()),
            }
            selection = selection.and(clause);
        }
        let Selection::Predicates(fields) = selection else {
            panic!("expected predicates")
        };
        assert_eq!(
            fields
                .iter()
                .map(|(field, clauses)| (Example::tag(field), clauses.len()))
                .collect::<Vec<_>>(),
            [("service_name", 2), ("handler", 2), ("kind", 1)]
        );
        assert_eq!(Example::field_from_tag("missing"), None);
        assert_eq!(Example::field_from_tag("ServiceName"), None);
        assert!(Example::starts_with_clause(ExampleField::Kind, "").is_none());
        assert!(Single::starts_with_clause(SingleField::EntryId, "").is_none());
        let empty = Example::starts_with_clause(ExampleField::Handler, "").unwrap();
        assert_eq!(Example::starts_with_prefix(&empty), Some(""));

        let [
            ExampleClause::ServiceName(ValuePredicate::Equal(name)),
            ExampleClause::ServiceNameStartsWith(prefix),
        ] = fields.for_field(ExampleField::ServiceName)
        else {
            panic!("expected both service-name clauses")
        };
        assert_eq!((name.as_str(), prefix.as_str()), ("svc", "s"));
        let [
            ExampleClause::Handler(ValuePredicate::Equal(None)),
            ExampleClause::HandlerStartsWith(prefix),
        ] = fields.for_field(ExampleField::Handler)
        else {
            panic!("expected nullable handler clauses")
        };
        assert_eq!(prefix.as_str(), "get");
        let [ExampleClause::Kind(ValuePredicate::In(kinds))] = fields.for_field(ExampleField::Kind)
        else {
            panic!("expected kind membership")
        };
        assert_eq!(kinds, &[1, 2]);

        let clause = SingleClause::EntryId(ValuePredicate::Equal(42));
        assert!(Single::starts_with_prefix(&clause).is_none());
        assert_eq!(Single::field(&clause), SingleField::EntryId);
        assert_eq!(Single::tag(SingleField::EntryId), "entry_id");
        assert_eq!(
            Single::field_from_tag("entry_id"),
            Some(SingleField::EntryId)
        );
        let SingleClause::EntryId(ValuePredicate::Equal(value)) = clause else {
            panic!("expected entry id")
        };
        assert_eq!(value, 42);
    }
}
