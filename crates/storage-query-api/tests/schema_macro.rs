// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_storage_query_api::selection::{Selection, ValuePredicate};

// Qualified invocations must resolve helpers in the defining crate, without
// importing the macro or its implementation traits into the caller's scope.
restate_storage_query_api::define_schema! {
    NativeOnly { id: u32 }
}

restate_storage_query_api::define_schema! {
    external_rows {
        name: "external_rows",
        sharding: restate_storage_query_api::ShardingScheme::None,
        columns {
            id: u32 => DataType::UInt32,
            #[starts_with]
            label: Option<String> => DataType::Utf8,
            values: Vec<Option<u32>> => UInt32List,
        }
    }
}

#[test]
fn both_schema_forms_expand_in_a_downstream_crate() {
    assert!(NativeOnly::arrow_schema().fields().is_empty());
    let row = ExternalRowsRow {
        id: 7,
        label: Some("example".into()),
        values: vec![Some(3), None],
    };
    assert!(row.matches(&Selection::All.and(ExternalRowsClause::LabelStartsWith("ex".into()))));
    assert!(!row.matches(&Selection::All.and(ExternalRowsClause::Id(ValuePredicate::Equal(8)))));

    let mut builder = ExternalRowsBatchBuilder::new(ExternalRowsBatchBuilder::schema());
    builder.append(&row);
    let batch = builder.into_batch().unwrap();
    assert_eq!((batch.num_rows(), batch.num_columns()), (1, 3));
    assert_eq!(batch.column(2).null_count(), 0);
    assert_eq!(ExternalRows::COLUMN_DOCS[0].name, "id");
}
