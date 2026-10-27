// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, DurationMillisecondArray, Int32Array, LargeBinaryArray,
    LargeStringArray, ListArray, StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::prelude::SessionContext;

use restate_util_string::ReString;

use crate::selection::{
    LiteralConversionError, Selection, SelectionLiteral, SelectionValue, ValuePredicate, compile,
};

use crate::define_schema;

type NullableNumbers = Option<Vec<Option<u32>>>;

// Every representation supported by the real V1/V2 table builders, including
// nullable native aliases. The fixture distinguishes NULL lists,
// empty lists, and NULL children.
define_schema! {
    arrow_rows {
        name: "arrow_rows",
        sharding: crate::ShardingScheme::None,
        columns {
            id: u32 => DataType::UInt32,
            #[starts_with]
            text: Option<String> => DataType::Utf8,
            #[starts_with]
            large_text: Option<ReString> => DataType::LargeUtf8,
            binary: Option<Vec<u8>> => DataType::Binary,
            large_binary: Option<Vec<u8>> => DataType::LargeBinary,
            unsigned: Option<u32> => DataType::UInt32,
            large_unsigned: Option<u64> => DataType::UInt64,
            signed: Option<i32> => DataType::Int32,
            flag: Option<bool> => DataType::Boolean,
            timestamp: Option<i64> => TimestampMillisecond,
            duration: Option<i64> => DataType::Duration,
            numbers: NullableNumbers => UInt32List,
            texts: Option<Vec<Option<String>>> => Utf8List,
            large_texts: Option<Vec<Option<ReString>>> => LargeUtf8List,
        }
    }
}

fn rows() -> [ArrowRowsRow; 3] {
    [
        ArrowRowsRow {
            id: 1,
            text: Some("héllo".into()),
            large_text: Some("world".into()),
            binary: Some(vec![0, 255]),
            large_binary: Some(vec![255, 0]),
            unsigned: Some(u32::MAX),
            large_unsigned: Some(u64::MAX),
            signed: Some(i32::MIN),
            flag: Some(true),
            timestamp: Some(-1),
            duration: Some(-2),
            numbers: Some(vec![Some(7), None, Some(u32::MAX)]),
            texts: Some(vec![Some("héllo".into()), None, Some("".into())]),
            large_texts: Some(vec![Some("world".into()), None, Some("".into())]),
        },
        ArrowRowsRow {
            id: 2,
            text: Some("".into()),
            large_text: Some("".into()),
            binary: Some(vec![]),
            large_binary: Some(vec![]),
            unsigned: Some(0),
            large_unsigned: Some(0),
            signed: Some(0),
            flag: Some(false),
            timestamp: Some(0),
            duration: Some(0),
            numbers: Some(vec![]),
            texts: Some(vec![]),
            large_texts: Some(vec![]),
        },
        ArrowRowsRow {
            id: 3,
            text: None,
            large_text: None,
            binary: None,
            large_binary: None,
            unsigned: None,
            large_unsigned: None,
            signed: None,
            flag: None,
            timestamp: None,
            duration: None,
            numbers: None,
            texts: None,
            large_texts: None,
        },
    ]
}

fn lists(values: ArrayRef) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 3, 3, 3].into()),
        values,
        Some(NullBuffer::from(vec![true, true, false])),
    ))
}

fn expected_batch() -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(UInt32Array::from(vec![1, 2, 3]))),
        (
            "text",
            Arc::new(StringArray::from(vec![Some("héllo"), Some(""), None])),
        ),
        (
            "large_text",
            Arc::new(LargeStringArray::from(vec![Some("world"), Some(""), None])),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![
                Some(&[0, 255][..]),
                Some(&[][..]),
                None,
            ])),
        ),
        (
            "large_binary",
            Arc::new(LargeBinaryArray::from(vec![
                Some(&[255, 0][..]),
                Some(&[][..]),
                None,
            ])),
        ),
        (
            "unsigned",
            Arc::new(UInt32Array::from(vec![Some(u32::MAX), Some(0), None])),
        ),
        (
            "large_unsigned",
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
        ),
        (
            "signed",
            Arc::new(Int32Array::from(vec![Some(i32::MIN), Some(0), None])),
        ),
        (
            "flag",
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        ),
        (
            "timestamp",
            Arc::new(
                TimestampMillisecondArray::from(vec![Some(-1), Some(0), None])
                    .with_timezone("+00:00"),
            ),
        ),
        (
            "duration",
            Arc::new(DurationMillisecondArray::from(vec![
                Some(-2),
                Some(0),
                None,
            ])),
        ),
        (
            "numbers",
            lists(Arc::new(UInt32Array::from(vec![
                Some(7),
                None,
                Some(u32::MAX),
            ]))),
        ),
        (
            "texts",
            lists(Arc::new(StringArray::from(vec![
                Some("héllo"),
                None,
                Some(""),
            ]))),
        ),
        (
            "large_texts",
            lists(Arc::new(LargeStringArray::from(vec![
                Some("world"),
                None,
                Some(""),
            ]))),
        ),
    ];
    RecordBatch::try_from_iter_with_nullable(
        columns.into_iter().map(|(name, array)| (name, array, true)),
    )
    .unwrap()
}

#[test]
fn all_table_representations_preserve_values_metadata_and_batch_reuse() {
    let rows = rows();
    let expected = expected_batch();
    let schema = ArrowRowsBatchBuilder::schema();
    assert!(
        Arc::ptr_eq(
            &schema,
            &<ArrowRows as crate::LogicalSchema>::arrow_schema()
        ),
        "generic_and_concrete_schema_access_share_the_cached_schema"
    );
    assert_eq!(
        schema,
        expected.schema(),
        "schema matches existing Arrow representations"
    );
    assert_eq!(ArrowRows::COLUMN_DOCS.len(), schema.fields().len());

    // Full, reordered/subset, and empty projections exercise all representations
    // and the row-count-only batch contract.
    for projection in [(0..14).collect(), vec![13, 9, 3, 11, 1], vec![]] {
        let projected = Arc::new(schema.project(&projection).unwrap());
        let mut builder = ArrowRowsBatchBuilder::new(projected);
        for _ in 0..2 {
            for row in &rows {
                builder.append(row);
            }
            assert_eq!(builder.num_rows(), rows.len(), "append counts logical rows");
            let batch = builder.finish().unwrap();
            assert_eq!(
                batch,
                expected.project(&projection).unwrap(),
                "values, NULLs, units, timezone, and list metadata survive every batch"
            );
            assert_eq!(builder.num_rows(), 0, "finish resets row count");
            let empty = builder.finish().unwrap();
            assert_eq!(empty.num_rows(), 0, "empty finish emits no phantom rows");
            assert_eq!(
                empty.schema(),
                batch.schema(),
                "empty finish retains metadata"
            );
        }
        for row in &rows {
            builder.append(row);
        }
        let final_batch =
            <ArrowRows as crate::LogicalSchema>::RowBuilder::into_batch(builder).unwrap();
        assert_eq!(
            final_batch,
            expected.project(&projection).unwrap(),
            "terminal_finish_preserves_all_values_and_projection_metadata"
        );
    }

    let native = Selection::All
        .and(ArrowRowsClause::Numbers(ValuePredicate::Equal(
            rows[0].numbers.clone(),
        )))
        .and(ArrowRowsClause::Binary(ValuePredicate::Equal(
            rows[0].binary.clone(),
        )))
        .and(ArrowRowsClause::Flag(ValuePredicate::Equal(Some(true))));
    assert_eq!(
        rows.each_ref().map(|row| row.matches(&native)),
        [true, false, false],
        "native binary/list predicates enforce the whole selection"
    );
    assert!(rows.iter().all(|row| row.matches(&Selection::All)));
    assert!(rows.iter().all(|row| !row.matches(&Selection::Empty)));

    for literal in [
        SelectionLiteral::Signed(-1),
        SelectionLiteral::Unsigned(u64::from(u32::MAX) + 1),
    ] {
        assert_eq!(
            u32::from_literal(literal),
            Err(LiteralConversionError::Unrepresentable),
            "narrow integers never wrap"
        );
    }
    for value in [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1] {
        assert_eq!(
            i32::from_literal(SelectionLiteral::Signed(value)),
            Err(LiteralConversionError::Unrepresentable),
            "signed narrowing is checked"
        );
    }
}

#[test]
fn generated_row_selection_preserves_sql_truth_for_all_representations() {
    const T: Option<bool> = Some(true);
    const F: Option<bool> = Some(false);
    const U: Option<bool> = None;
    let rows = rows();
    let mut builder = ArrowRowsBatchBuilder::new(ArrowRows::arrow_schema());
    for row in &rows {
        builder.append(row);
    }
    let batch = builder.finish().unwrap();
    let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let context = SessionContext::new();
    let simplifier = ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(Arc::new(schema.clone()))
            .build(),
    );
    #[rustfmt::skip]
    let cases = [
        // SQL predicate                               SQL truth      exact native selection
        ("text = 'héllo'",                              [T, F, U],     true),
        ("starts_with(large_text, 'wo')",                 [T, F, U],     true),
        ("unsigned = 4294967295",                        [T, F, U],     true),
        ("unsigned IN (0, 4294967295, NULL)",             [T, T, U],     true),
        ("signed < 0",                                  [T, F, U],     true),
        // Optimization rewrites boolean equality to NOT; it remains residual.
        ("flag = false",                                [F, T, U],     false),
        ("flag IS NULL",                                [F, F, T],     true),
        ("large_unsigned IS NULL",                      [F, F, T],     true),
        ("binary IS NULL",                              [F, F, T],     true),
        ("large_binary IS NOT NULL",                     [T, T, F],     false),
        ("numbers IS NULL",                             [F, F, T],     true),
        ("texts IS NOT NULL",                            [T, T, F],     false),
        ("timestamp IS NULL OR duration IS NULL",        [F, F, T],     false),
        ("timestamp < TIMESTAMP '1970-01-01 00:00:00'",   [T, F, U],     false),
        ("duration < arrow_cast(0, 'Duration(Millisecond)')", [T, F, U], false),
        ("array_has(numbers, 7)",                        [T, F, U],     false),
        ("array_has(large_texts, 'world')",               [T, F, U],     false),
        ("binary = X'00ff'",                             [T, F, U],     false),
        ("id = 1 AND array_has(texts, 'héllo')",          [T, F, F],     false),
    ];
    for (sql, truth, exact) in cases {
        let expr = context.parse_sql_expr(sql, &schema).unwrap();
        // As in query planning, fold literal casts and simplify widened integer
        // comparisons before asking the strict compiler for column predicates.
        let expr = simplifier
            .simplify(simplifier.coerce(expr, &schema).unwrap())
            .unwrap();
        let expr = context.create_physical_expr(expr, &schema).unwrap();
        let evaluate = |batch: &RecordBatch| {
            let values = expr
                .evaluate(batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(evaluate(&batch), truth, "SQL truth: {sql}");
        let selection = compile::<ArrowRows>(Some(&expr));
        let candidates = rows.each_ref().map(|row| row.matches(&selection));
        for (candidate, truth) in candidates.iter().zip(truth) {
            assert!(truth != T || *candidate, "candidate soundness: {sql}");
            if exact {
                assert_eq!(*candidate, truth == T, "supported exactness: {sql}");
            }
        }
        let selected =
            filter_record_batch(&batch, &BooleanArray::from(candidates.to_vec())).unwrap();
        let actual = candidates
            .iter()
            .enumerate()
            .filter_map(|(row, keep)| keep.then_some(row))
            .zip(evaluate(&selected))
            .filter_map(|(row, truth)| (truth == T).then_some(row))
            .collect::<Vec<_>>();
        let expected = truth
            .iter()
            .enumerate()
            .filter_map(|(row, truth)| (*truth == T).then_some(row))
            .collect::<Vec<_>>();
        assert_eq!(
            actual, expected,
            "selection plus residual equals SQL: {sql}"
        );
    }
}
