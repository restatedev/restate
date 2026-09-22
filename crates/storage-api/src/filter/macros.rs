// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Defines a field enum, typed clauses, and a `FilterTarget` implementation for
/// an existing table marker declared with [`define_table!`](crate::define_table).
///
/// Field names become snake-case tags and CamelCase variants. `=> starts_with`
/// adds a `StartsWith` clause carrying a literal `ReString` prefix. Prefix support
/// is explicit and does not depend on recognizing the spelling of a field type.
///
/// The visibility applies to the generated field and clause enums. Document the
/// marker on its `define_table!` declaration. Field doc comments (including
/// `#[doc = ...]`) are copied to the field variant and each clause variant.
/// Declaration order determines enum-map indexing, not physical key order.
/// Field types must implement [`FilterValue`](crate::filter::FilterValue) so
/// expression adapters can build typed value clauses through the target.
///
/// # Usage
///
/// ```
/// use restate_storage_api::{define_filter, define_table};
/// use restate_storage_api::filter::{Filter, FilterTarget};
/// use restate_types::vqueues::EntryKind;
/// use restate_util_string::ReString;
///
/// // define a data table
/// define_table! {
///     /// Service-load dimensions.
///     pub ServiceLoad;
/// }
///
///
/// // define a way to filter data from a data table
/// define_filter! {
///     pub ServiceLoad {
///         /// The service name.
///         service_name: ReString => starts_with,
///         /// The handler name, or NULL when absent.
///         handler: Option<ReString> => starts_with,
///         /// The entry kind.
///         kind: EntryKind,
///     }
/// }
///
/// let clause = ServiceLoadClause::HandlerStartsWith("get".into());
/// assert_eq!(ServiceLoad::field(&clause), ServiceLoadField::Handler);
/// assert_eq!(ServiceLoad::starts_with_prefix(&clause), Some("get"));
/// let filter = Filter::<ServiceLoad>::default().and(clause);
/// ```
///
/// # Expanded output
///
/// The filter declaration above generates these items for the existing marker.
/// Dependency paths are shortened with imports below; ordinary `derive`
/// expansions are not shown.
///
/// ```
/// use restate_storage_api::filter::{FilterTarget, ValuePredicate, ValuePredicateBuilder};
/// use restate_storage_api::filter::__private::enum_map;
/// use restate_types::vqueues::EntryKind;
/// use restate_util_string::ReString;
///
/// # restate_storage_api::define_table! { pub ServiceLoad; }
/// /// Logical fields of [`ServiceLoad`].
/// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// pub enum ServiceLoadField {
///     /// The service name.
///     ServiceName,
///     /// The handler name, or NULL when absent.
///     Handler,
///     /// The entry kind.
///     Kind,
/// }
///
/// // These implementations provide enum-map support without requiring the
/// // invocation site to depend directly on enum-map's derive macro.
/// impl enum_map::Enum for ServiceLoadField {
///     const LENGTH: usize = [Self::ServiceName, Self::Handler, Self::Kind].len();
///
///     fn from_usize(value: usize) -> Self {
///         [Self::ServiceName, Self::Handler, Self::Kind][value]
///     }
///
///     fn into_usize(self) -> usize {
///         self as usize
///     }
/// }
///
/// impl<V> enum_map::EnumArray<V> for ServiceLoadField {
///     type Array = [V; <Self as enum_map::Enum>::LENGTH];
/// }
///
/// /// Conditions on [`ServiceLoad`], combined with logical AND.
/// pub enum ServiceLoadClause {
///     /// The service name.
///     ServiceName(ValuePredicate<ReString>),
///     /// The service name.
///     ///
///     /// Matches a case-sensitive literal prefix; NULL never matches.
///     /// An empty prefix matches every non-NULL value. No LIKE syntax is interpreted.
///     ServiceNameStartsWith(ReString),
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
/// impl FilterTarget for ServiceLoad {
///     type Field = ServiceLoadField;
///     type Clause = ServiceLoadClause;
///
///     fn field(clause: &Self::Clause) -> Self::Field {
///         match clause {
///             ServiceLoadClause::ServiceName(_)
///             | ServiceLoadClause::ServiceNameStartsWith(_) => ServiceLoadField::ServiceName,
///             ServiceLoadClause::Handler(_)
///             | ServiceLoadClause::HandlerStartsWith(_) => ServiceLoadField::Handler,
///             ServiceLoadClause::Kind(_) => ServiceLoadField::Kind,
///         }
///     }
///
///     fn tag(field: Self::Field) -> &'static str {
///         match field {
///             ServiceLoadField::ServiceName => "service_name",
///             ServiceLoadField::Handler => "handler",
///             ServiceLoadField::Kind => "kind",
///         }
///     }
///
///     fn field_from_tag(tag: &str) -> Option<Self::Field> {
///         match tag {
///             "service_name" => Some(ServiceLoadField::ServiceName),
///             "handler" => Some(ServiceLoadField::Handler),
///             "kind" => Some(ServiceLoadField::Kind),
///             _ => None,
///         }
///     }
///
///     fn value_clause<P: ValuePredicateBuilder>(field: Self::Field, builder: P) -> Option<Self::Clause> {
///         match field {
///             ServiceLoadField::ServiceName => builder.build().map(ServiceLoadClause::ServiceName),
///             ServiceLoadField::Handler => builder.build().map(ServiceLoadClause::Handler),
///             ServiceLoadField::Kind => builder.build().map(ServiceLoadClause::Kind),
///         }
///     }
///
///     fn starts_with_clause(field: Self::Field, prefix: &str) -> Option<Self::Clause> {
///         let constructor = match field {
///             ServiceLoadField::ServiceName => Some(ServiceLoadClause::ServiceNameStartsWith as fn(ReString) -> Self::Clause),
///             ServiceLoadField::Handler => Some(ServiceLoadClause::HandlerStartsWith as fn(ReString) -> Self::Clause),
///             ServiceLoadField::Kind => None,
///         }?;
///         Some(constructor(prefix.into()))
///     }
///
///     fn starts_with_prefix(clause: &Self::Clause) -> Option<&str> {
///         match clause {
///             ServiceLoadClause::ServiceName(_) => None,
///             ServiceLoadClause::ServiceNameStartsWith(prefix) => Some(prefix.as_str()),
///             ServiceLoadClause::Handler(_) => None,
///             ServiceLoadClause::HandlerStartsWith(prefix) => Some(prefix.as_str()),
///             ServiceLoadClause::Kind(_) => None,
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_filter {
    (
        $(#[doc = $doc:expr])*
        $vis:vis $target:ident { $($fields:tt)+ }
    ) => {
        $crate::define_filter!(@fields
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
        $crate::define_filter!(@fields [$($docs)*] [$vis] [$target]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)*
                $(#[doc = $doc])*
                [<$field:camel>]($crate::filter::ValuePredicate<$ty>),
                $(#[doc = $doc])*
                #[doc = ""]
                #[doc = "Matches a case-sensitive literal prefix; NULL never matches."]
                #[doc = "An empty prefix matches every non-NULL value. No LIKE syntax is interpreted."]
                [<$field:camel StartsWith>]($crate::filter::__private::ReString),
            ]
            [$($arms)*
                [<$target Clause>]::[<$field:camel>](_)
                | [<$target Clause>]::[<$field:camel StartsWith>](_) => [<$target Field>]::[<$field:camel>],
            ]
            [$($prefixes)*
                [<$target Field>]::[<$field:camel>] => ::core::option::Option::Some(
                    [<$target Clause>]::[<$field:camel StartsWith>]
                        as fn($crate::filter::__private::ReString) -> Self::Clause
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
        $crate::define_filter!(@fields [$($docs)*] [$vis] [$target]
            [$($fields)* $(#[doc = $doc])* [<$field:camel>],]
            [$($clauses)* $(#[doc = $doc])* [<$field:camel>]($crate::filter::ValuePredicate<$ty>),]
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
        $crate::filter::__private::paste! {
            #[doc = concat!("Logical fields of [`", stringify!($target), "`].")]
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            $vis enum [<$target Field>] { $($fields)* }

            // Emit enum-map support directly: its derive uses an absolute
            // `::enum_map` path, which would require a caller-side dependency.
            impl $crate::filter::__private::enum_map::Enum for [<$target Field>] {
                const LENGTH: usize = [$(Self::[<$tags:camel>]),+].len();

                fn from_usize(value: usize) -> Self {
                    [$(Self::[<$tags:camel>]),+][value]
                }

                fn into_usize(self) -> usize { self as usize }
            }

            impl<V> $crate::filter::__private::enum_map::EnumArray<V> for [<$target Field>] {
                type Array = [V; <Self as $crate::filter::__private::enum_map::Enum>::LENGTH];
            }

            #[doc = concat!("Conditions on [`", stringify!($target), "`], combined with logical AND.")]
            $vis enum [<$target Clause>] { $($clauses)* }

            impl $crate::filter::FilterTarget for $target {
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

                fn value_clause<P: $crate::filter::ValuePredicateBuilder>(
                    field: Self::Field,
                    builder: P,
                ) -> ::core::option::Option<Self::Clause> {
                    match field {
                        $([<$target Field>]::[<$tags:camel>] =>
                            builder.build().map([<$target Clause>]::[<$tags:camel>]),)+
                    }
                }

                fn starts_with_clause(field: Self::Field, prefix: &str) -> ::core::option::Option<Self::Clause> {
                    let constructor: fn($crate::filter::__private::ReString) -> Self::Clause =
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

    use crate::filter::{Filter, FilterTarget, ValuePredicate};

    type NullableName = Option<ReString>;

    crate::define_table! { pub(crate) Example; }
    crate::define_table! { Single; }

    crate::define_filter! {
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

    crate::define_filter! { Single { entry_id: u64 } }

    #[test]
    fn generated_clauses_map_to_fields_and_preserve_grouping() {
        let mut filter = Filter::<Example>::default();
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
            filter = filter.and(clause);
        }
        let Filter::Predicates(fields) = filter else {
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
