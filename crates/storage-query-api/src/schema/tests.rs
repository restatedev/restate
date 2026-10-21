// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_util_string::ReString;

use crate::selection::{Selection, ValuePredicate};

use super::*;

type NullableName = Option<ReString>;

crate::define_schema! {
    /// An example with optional Arrow representation and a native type alias.
    pub(crate) Example {
        /// The service name.
        #[doc = "Additional column documentation."]
        #[starts_with]
        service_name: ReString => DataType::LargeUtf8,
        /// The optional handler name.
        #[starts_with]
        handler: NullableName => DataType::LargeUtf8,
        /// A modeled column without an Arrow representation.
        kind: u64,
    }
}

crate::define_schema! { Single { entry_id: u64 } }

#[test]
fn declared_columns_drive_arrow_views_and_typed_selection() {
    let schema = Example::arrow_schema();
    assert_eq!(
        Example::COLUMNS,
        &[
            ExampleColumn::ServiceName,
            ExampleColumn::Handler,
            ExampleColumn::Kind
        ]
    );
    assert_eq!(
        schema.fields().len(),
        2,
        "Arrow view omits unrepresented logical columns"
    );
    assert_eq!(schema.field(0).name(), "service_name");
    assert_eq!(schema.field(1).name(), "handler");
    assert!(
        schema.fields().iter().all(|field| field.is_nullable()),
        "Arrow nullability remains permissive"
    );
    assert_eq!(Example::arrow_type(ExampleColumn::Kind), None);
    assert!(Single::arrow_schema().fields().is_empty());

    let mut selection = Selection::<Example>::default();
    for (clause, field) in [
        (
            ExampleClause::ServiceName(ValuePredicate::Equal("svc".into())),
            ExampleColumn::ServiceName,
        ),
        (
            Example::starts_with_clause(ExampleColumn::ServiceName, "s").unwrap(),
            ExampleColumn::ServiceName,
        ),
        (
            ExampleClause::Handler(ValuePredicate::Equal(None)),
            ExampleColumn::Handler,
        ),
        (
            Example::starts_with_clause(ExampleColumn::Handler, "get").unwrap(),
            ExampleColumn::Handler,
        ),
        (
            ExampleClause::Kind(ValuePredicate::In(vec![1, 2])),
            ExampleColumn::Kind,
        ),
    ] {
        assert_eq!(Example::column(&clause), field);
        assert_eq!(Example::column_from_tag(Example::tag(field)), Some(field));
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
        [("service_name", 2), ("handler", 2), ("kind", 1)],
        "every modeled column supports selection, including columns outside the Arrow view"
    );
    assert_eq!(Example::column_from_tag("missing"), None);
    assert_eq!(Example::column_from_tag("ServiceName"), None);
    assert!(Example::starts_with_clause(ExampleColumn::Kind, "").is_none());
    assert!(Single::starts_with_clause(SingleColumn::EntryId, "").is_none());
    let empty = Example::starts_with_clause(ExampleColumn::Handler, "").unwrap();
    assert_eq!(Example::starts_with_prefix(&empty), Some(""));

    let [
        ExampleClause::ServiceName(ValuePredicate::Equal(name)),
        ExampleClause::ServiceNameStartsWith(prefix),
    ] = fields.for_column(ExampleColumn::ServiceName)
    else {
        panic!("expected both service-name clauses")
    };
    assert_eq!((name.as_str(), prefix.as_str()), ("svc", "s"));
    let [
        ExampleClause::Handler(ValuePredicate::Equal(None)),
        ExampleClause::HandlerStartsWith(prefix),
    ] = fields.for_column(ExampleColumn::Handler)
    else {
        panic!("expected nullable handler clauses")
    };
    assert_eq!(prefix.as_str(), "get");
    let [ExampleClause::Kind(ValuePredicate::In(kinds))] = fields.for_column(ExampleColumn::Kind)
    else {
        panic!("expected kind membership")
    };
    assert_eq!(kinds, &[1, 2]);

    let clause = SingleClause::EntryId(ValuePredicate::Equal(42));
    assert!(Single::starts_with_prefix(&clause).is_none());
    assert_eq!(Single::column(&clause), SingleColumn::EntryId);
    assert_eq!(Single::tag(SingleColumn::EntryId), "entry_id");
    assert_eq!(
        Single::column_from_tag("entry_id"),
        Some(SingleColumn::EntryId)
    );
    let SingleClause::EntryId(ValuePredicate::Equal(value)) = clause else {
        panic!("expected entry id")
    };
    assert_eq!(value, 42);
}
