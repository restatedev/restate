// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Differential harness adapted from `23b2a58e38d7` to the prototype baseline.
//! References use independent fixture rows and a broad primary-storage scan.
//! The candidate currently executes locally over one quiescent storage partition.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::ops::ControlFlow;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, LargeBinaryArray, LargeStringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::MemTable;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{StreamExt, stream};

use restate_clock::MockClock;
use restate_platform::sync::Mutex;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::{ScanStateTable, WriteStateTable};
use restate_storage_query_api::{
    AdminUser, QueryEngine, QueryMetadata, QueryOptions, QueryResult, QueryStatus, SessionOptions,
};
use restate_types::Scope;
use restate_types::identifiers::{ServiceId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_util_string::RestateString;

use crate::DataFusionEnv;
use crate::mocks::MockQueryEngine;
use crate::state::schema::StateBuilder;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StateRecord {
    service_id: ServiceId,
    key: Bytes,
    value: Bytes,
}

impl StateRecord {
    fn new(service_id: ServiceId, key: &'static [u8], value: &'static [u8]) -> Self {
        Self {
            service_id,
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        }
    }
}

#[derive(Debug)]
struct StateFixture {
    name: &'static str,
    records: Vec<StateRecord>,
}

impl StateFixture {
    fn deterministic() -> Self {
        let tenant_a = Scope::try_from_static("tenant-a").unwrap();
        let tenant_b = Scope::try_from_static("tenant-b").unwrap();
        let scoped_alpha = ServiceId::new(Some(tenant_a.clone()), "workflow", "alpha");
        let scoped_beta = ServiceId::new(Some(tenant_a), "workflow", "beta");
        let scoped_gamma = ServiceId::new(Some(tenant_b), "workflow", "gamma");
        let unscoped_alpha = ServiceId::new(None, "service", "alpha");
        let unscoped_beta = ServiceId::new(None, "service", "beta");

        Self {
            name: "state-scoped-and-unscoped-v1",
            records: vec![
                StateRecord::new(scoped_alpha.clone(), b"color", b"red"),
                StateRecord::new(scoped_alpha.clone(), b"empty", b""),
                StateRecord::new(scoped_alpha, b"count", b"10"),
                StateRecord::new(scoped_beta.clone(), b"color", b"red"),
                StateRecord::new(scoped_beta, b"empty", b""),
                StateRecord::new(scoped_gamma, b"color", b"blue"),
                StateRecord::new(unscoped_alpha.clone(), b"color", b"red"),
                StateRecord::new(unscoped_alpha, b"binary", b"\0\xff"),
                StateRecord::new(unscoped_beta, b"count", b"10"),
            ],
        }
    }

    async fn populate(&self, store: &mut restate_partition_store::PartitionStore) {
        let mut tx = store.transaction();
        for record in &self.records {
            tx.put_user_state(&record.service_id, &record.key, &record.value)
                .unwrap();
        }
        tx.commit().await.unwrap();
    }

    async fn scan_primary(
        &self,
        store: &mut restate_partition_store::PartitionStore,
    ) -> Vec<StateRecord> {
        let records = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&records);
        store
            .for_each_user_state(KeyRange::FULL, move |(service_id, key, value)| {
                output.lock().push(StateRecord {
                    service_id,
                    key,
                    value: Bytes::copy_from_slice(value),
                });
                ControlFlow::Continue(())
            })
            .unwrap()
            .await
            .unwrap();

        records.lock().clone()
    }

    fn describe(&self) -> String {
        format!("fixture={} records={:#?}", self.name, self.records)
    }
}

#[derive(Clone, Copy, Debug)]
enum Comparison {
    Bag,
    Ordered,
}

#[derive(Debug)]
struct Case {
    name: &'static str,
    sql: &'static str,
    comparison: Comparison,
    expected_rows: usize,
    anchor: Anchor,
    outcome: ExpectedOutcome,
}

#[derive(Clone, Copy, Debug)]
enum Anchor {
    None,
    ScalarCount,
    GroupedLengths,
}

#[derive(Clone, Copy, Debug)]
enum ExpectedOutcome {
    Success,
    ErrorContains(&'static str),
}

const CASES: &[Case] = &[
    Case {
        name: "full-row-bag",
        sql: "SELECT partition_key, scope, service_name, service_key, key, key_length, \
              value_utf8, value, value_length FROM state",
        comparison: Comparison::Bag,
        expected_rows: 9,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "duplicate-valued-projection",
        sql: "SELECT value_utf8 FROM state WHERE value_utf8 = 'red'",
        comparison: Comparison::Bag,
        expected_rows: 3,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "scope-equality-in-filter",
        sql: "SELECT scope, service_key, key, value FROM state \
              WHERE scope = 'tenant-a' AND service_name = 'workflow' \
              AND service_key IN ('alpha', 'beta') AND key = 'color'",
        comparison: Comparison::Bag,
        expected_rows: 2,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "empty-result",
        sql: "SELECT service_key FROM state WHERE scope = 'missing'",
        comparison: Comparison::Bag,
        expected_rows: 0,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "scalar-count",
        sql: "SELECT COUNT(*) AS row_count FROM state",
        comparison: Comparison::Bag,
        expected_rows: 1,
        anchor: Anchor::ScalarCount,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "grouped-integer-aggregate",
        sql: "SELECT value_length, COUNT(*) AS row_count, SUM(key_length) AS key_bytes \
              FROM state GROUP BY value_length ORDER BY value_length",
        comparison: Comparison::Ordered,
        expected_rows: 4,
        anchor: Anchor::GroupedLengths,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "total-order",
        sql: "SELECT partition_key, scope, service_name, service_key, key, value \
              FROM state ORDER BY partition_key, scope NULLS FIRST, service_name, service_key, key",
        comparison: Comparison::Ordered,
        expected_rows: 9,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "ordered-limit-offset",
        sql: "SELECT partition_key, scope, service_name, service_key, key \
              FROM state ORDER BY partition_key, scope NULLS FIRST, service_name, service_key, key \
              LIMIT 4 OFFSET 2",
        comparison: Comparison::Ordered,
        expected_rows: 4,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "constant-zero-column-source-projection",
        sql: "SELECT 1 AS one FROM state",
        comparison: Comparison::Bag,
        expected_rows: 9,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "scope-in-point-ranges",
        sql: "SELECT partition_key, scope, service_key, key, value FROM state \
              WHERE scope IN ('tenant-a', 'tenant-b') \
              ORDER BY partition_key, scope, service_name, service_key, key",
        comparison: Comparison::Ordered,
        expected_rows: 6,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "repeated-source-union-all",
        sql: "SELECT value_utf8 FROM state WHERE value_utf8 = 'red' \
              UNION ALL SELECT value_utf8 FROM state WHERE value_utf8 = 'red'",
        comparison: Comparison::Bag,
        expected_rows: 6,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
    Case {
        name: "repeated-source-join",
        sql: "SELECT a.service_key, a.value_utf8, b.key AS other_key FROM state a \
              JOIN state b ON a.scope = b.scope AND a.service_name = b.service_name \
              AND a.service_key = b.service_key \
              WHERE a.scope = 'tenant-a' AND a.key = 'color'",
        comparison: Comparison::Bag,
        expected_rows: 5,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    },
];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Cell {
    Null,
    Int64(i64),
    UInt64(u64),
    LargeUtf8(String),
    LargeBinary(Vec<u8>),
}

type Row = Vec<Cell>;

#[derive(Debug)]
struct CapturedResult {
    schema: SchemaRef,
    rows: Vec<Row>,
    terminal_error: Option<String>,
}

impl CapturedResult {
    fn from_items(
        schema: SchemaRef,
        items: impl IntoIterator<Item = Result<RecordBatch, DataFusionError>>,
    ) -> Result<Self, String> {
        validate_supported_schema(&schema)?;
        let mut rows = Vec::new();
        let mut terminal_error = None;

        for (batch_index, item) in items.into_iter().enumerate() {
            match item {
                Ok(batch) => {
                    if schema != batch.schema() {
                        return Err(format!(
                            "batch {batch_index} schema differs from declared stream schema\nexpected: {schema:#?}\nactual: {:#?}",
                            batch.schema()
                        ));
                    }
                    append_batch(&mut rows, &batch)?;
                }
                Err(error) => {
                    terminal_error.get_or_insert_with(|| error.to_string());
                }
            }
        }

        Ok(Self {
            schema,
            rows,
            terminal_error,
        })
    }
}

fn validate_supported_schema(schema: &Schema) -> Result<(), String> {
    for field in schema.fields() {
        if !matches!(
            field.data_type(),
            DataType::Int64 | DataType::UInt64 | DataType::LargeUtf8 | DataType::LargeBinary
        ) {
            return Err(format!(
                "unsupported comparison type {:?} for column {}",
                field.data_type(),
                field.name()
            ));
        }
    }
    Ok(())
}

async fn capture_stream(mut stream: SendableRecordBatchStream) -> Result<CapturedResult, String> {
    let schema = stream.schema();
    validate_supported_schema(&schema)?;
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item);
    }
    CapturedResult::from_items(schema, items)
}

fn append_batch(rows: &mut Vec<Row>, batch: &RecordBatch) -> Result<(), String> {
    for row_index in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(batch.num_columns());
        for (column_index, field) in batch.schema().fields().iter().enumerate() {
            let array = batch.column(column_index);
            if array.is_null(row_index) {
                row.push(Cell::Null);
                continue;
            }

            let value = match field.data_type() {
                DataType::Int64 => Cell::Int64(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row_index),
                ),
                DataType::UInt64 => Cell::UInt64(
                    array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(row_index),
                ),
                DataType::LargeUtf8 => Cell::LargeUtf8(
                    array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap()
                        .value(row_index)
                        .to_owned(),
                ),
                DataType::LargeBinary => Cell::LargeBinary(
                    array
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .unwrap()
                        .value(row_index)
                        .to_vec(),
                ),
                unsupported => {
                    return Err(format!(
                        "unsupported comparison type {unsupported:?} for column {}",
                        field.name()
                    ));
                }
            };
            row.push(value);
        }
        rows.push(row);
    }
    Ok(())
}

fn compare_results(
    case: &Case,
    expected: &CapturedResult,
    actual: &CapturedResult,
) -> Result<(), String> {
    validate_outcome(case.outcome, expected)
        .map_err(|error| format!("expected result outcome: {error}"))?;
    validate_outcome(case.outcome, actual)
        .map_err(|error| format!("actual result outcome: {error}"))?;
    if expected.schema != actual.schema {
        return Err(format!(
            "schema mismatch\nexpected: {:#?}\nactual: {:#?}",
            expected.schema, actual.schema
        ));
    }
    match case.comparison {
        Comparison::Bag => compare_bags(&expected.rows, &actual.rows),
        Comparison::Ordered => {
            if expected.rows == actual.rows {
                Ok(())
            } else {
                let first_difference = expected
                    .rows
                    .iter()
                    .zip(&actual.rows)
                    .position(|(expected, actual)| expected != actual)
                    .unwrap_or_else(|| expected.rows.len().min(actual.rows.len()));
                Err(format!(
                    "ordered sequence mismatch at row {first_difference}\nexpected: {:#?}\nactual: {:#?}",
                    expected.rows, actual.rows
                ))
            }
        }
    }
}

fn compare_bags(expected: &[Row], actual: &[Row]) -> Result<(), String> {
    let expected = row_counts(expected);
    let actual = row_counts(actual);
    if expected == actual {
        return Ok(());
    }

    let mut difference = String::new();
    for (row, expected_count) in &expected {
        let actual_count = actual.get(row).copied().unwrap_or_default();
        if *expected_count > actual_count {
            writeln!(
                difference,
                "missing {} occurrence(s) of {row:?}",
                expected_count - actual_count
            )
            .unwrap();
        }
    }
    for (row, actual_count) in &actual {
        let expected_count = expected.get(row).copied().unwrap_or_default();
        if *actual_count > expected_count {
            writeln!(
                difference,
                "excess {} occurrence(s) of {row:?}",
                actual_count - expected_count
            )
            .unwrap();
        }
    }
    Err(difference)
}

fn row_counts(rows: &[Row]) -> BTreeMap<&Row, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row).or_default() += 1;
    }
    counts
}

fn record_counts(records: &[StateRecord]) -> BTreeMap<&StateRecord, usize> {
    let mut counts = BTreeMap::new();
    for record in records {
        *counts.entry(record).or_default() += 1;
    }
    counts
}

fn state_batch(records: &[StateRecord]) -> RecordBatch {
    let partition_key = UInt64Array::from_iter_values(
        records
            .iter()
            .map(|record| record.service_id.partition_key()),
    );
    let scope = LargeStringArray::from_iter(
        records
            .iter()
            .map(|record| record.service_id.scope.as_ref().map(|scope| scope.as_str())),
    );
    let service_name = LargeStringArray::from_iter_values(
        records
            .iter()
            .map(|record| &*record.service_id.service_name),
    );
    let service_key =
        LargeStringArray::from_iter_values(records.iter().map(|record| &*record.service_id.key));
    let key = LargeStringArray::from_iter(
        records
            .iter()
            .map(|record| std::str::from_utf8(&record.key).ok()),
    );
    let key_length = UInt64Array::from_iter_values(
        records
            .iter()
            .map(|record| u64::try_from(record.key.len()).unwrap()),
    );
    let value_utf8 = LargeStringArray::from_iter(
        records
            .iter()
            .map(|record| std::str::from_utf8(&record.value).ok()),
    );
    let value =
        LargeBinaryArray::from_iter_values(records.iter().map(|record| record.value.as_ref()));
    let value_length = UInt64Array::from_iter_values(
        records
            .iter()
            .map(|record| u64::try_from(record.value.len()).unwrap()),
    );

    RecordBatch::try_new(
        StateBuilder::schema(),
        vec![
            Arc::new(partition_key),
            Arc::new(scope),
            Arc::new(service_name),
            Arc::new(service_key),
            Arc::new(key),
            Arc::new(key_length),
            Arc::new(value_utf8),
            Arc::new(value),
            Arc::new(value_length),
        ],
    )
    .unwrap()
}

async fn run_memtable(sql: &str, batch: RecordBatch) -> Result<CapturedResult, String> {
    let context = SessionContext::new();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).map_err(|e| e.to_string())?;
    context
        .register_table("state", Arc::new(table))
        .map_err(|e| e.to_string())?;
    let state = context.state();
    let statement = state
        .sql_to_statement(sql, &datafusion::config::Dialect::PostgreSQL)
        .map_err(|e| e.to_string())?;
    let plan = state
        .statement_to_plan(statement)
        .await
        .map_err(|e| e.to_string())?;
    let stream = context
        .execute_logical_plan(plan)
        .await
        .map_err(|e| e.to_string())?
        .execute_stream()
        .await
        .map_err(|e| e.to_string())?;
    capture_stream(stream).await
}

async fn run_candidate(result: QueryResult) -> Result<CapturedResult, String> {
    let captured = capture_stream(result.stream).await?;
    let stats = result.diagnostics.snapshot();
    let expected_status = if captured.terminal_error.is_some() {
        QueryStatus::Failed
    } else {
        QueryStatus::Completed
    };
    if stats.status != expected_status || stats.output_rows != captured.rows.len() as u64 {
        return Err(format!(
            "diagnostics disagree with captured output: {stats:?}, rows={}, terminal_error={:?}",
            captured.rows.len(),
            captured.terminal_error,
        ));
    }
    let warnings = result.diagnostics.warnings();
    if !warnings.is_empty() {
        return Err(format!("unexpected candidate warnings: {warnings:?}"));
    }
    Ok(captured)
}

async fn run_case(
    case: &Case,
    fixture: &StateFixture,
    primary_records: &[StateRecord],
    result: QueryResult,
) -> Result<(), String> {
    let logical = run_memtable(case.sql, state_batch(&fixture.records))
        .await
        .map_err(|error| format!("logical oracle: {error}"))?;
    let primary = run_memtable(case.sql, state_batch(primary_records))
        .await
        .map_err(|error| format!("primary oracle: {error}"))?;
    let candidate = run_candidate(result)
        .await
        .map_err(|error| format!("candidate: {error}"))?;

    validate_case_outcomes(case, &logical, &primary, &candidate)?;
    if logical.rows.len() != case.expected_rows {
        return Err(format!(
            "hand-calculated row-count mismatch: expected {}, got {}",
            case.expected_rows,
            logical.rows.len()
        ));
    }
    check_anchor(case.anchor, &logical.rows)?;
    compare_results(case, &logical, &primary)
        .map_err(|error| format!("primary oracle: {error}"))?;
    compare_results(case, &logical, &candidate).map_err(|error| format!("candidate: {error}"))
}

fn validate_case_outcomes(
    case: &Case,
    logical: &CapturedResult,
    primary: &CapturedResult,
    candidate: &CapturedResult,
) -> Result<(), String> {
    let mut failures = Vec::new();
    for (source, result) in [
        ("logical oracle", logical),
        ("primary oracle", primary),
        ("candidate", candidate),
    ] {
        if let Err(error) = validate_outcome(case.outcome, result) {
            failures.push(format!("{source}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

fn validate_outcome(expected: ExpectedOutcome, result: &CapturedResult) -> Result<(), String> {
    match (expected, result.terminal_error.as_deref()) {
        (ExpectedOutcome::Success, None) => Ok(()),
        (ExpectedOutcome::Success, Some(actual)) => {
            Err(format!("unexpected terminal failure: {actual}"))
        }
        (ExpectedOutcome::ErrorContains(expected), None) => Err(format!(
            "expected a terminal failure containing {expected:?}, but the stream succeeded"
        )),
        (ExpectedOutcome::ErrorContains(expected), Some(actual)) if actual.contains(expected) => {
            Ok(())
        }
        (ExpectedOutcome::ErrorContains(expected), Some(actual)) => Err(format!(
            "terminal failure did not contain {expected:?}: {actual}"
        )),
    }
}

fn check_anchor(anchor: Anchor, actual: &[Row]) -> Result<(), String> {
    let expected = match anchor {
        Anchor::None => return Ok(()),
        Anchor::ScalarCount => vec![vec![Cell::Int64(9)]],
        Anchor::GroupedLengths => vec![
            vec![Cell::UInt64(0), Cell::Int64(2), Cell::UInt64(10)],
            vec![Cell::UInt64(2), Cell::Int64(3), Cell::UInt64(16)],
            vec![Cell::UInt64(3), Cell::Int64(3), Cell::UInt64(15)],
            vec![Cell::UInt64(4), Cell::Int64(1), Cell::UInt64(5)],
        ],
    };
    if expected == actual {
        Ok(())
    } else {
        Err(format!(
            "hand-calculated value mismatch\nexpected: {expected:#?}\nactual: {actual:#?}"
        ))
    }
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::LargeUtf8, true),
        Field::new("count", DataType::Int64, false),
    ]))
}

fn sample_batch(rows: &[(Option<&str>, i64)]) -> RecordBatch {
    sample_batch_with_schema(sample_schema(), rows)
}

fn sample_batch_with_schema(schema: SchemaRef, rows: &[(Option<&str>, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(LargeStringArray::from_iter(
                rows.iter().map(|(group, _)| *group),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, count)| *count),
            )),
        ],
    )
    .unwrap()
}

fn captured(items: Vec<Result<RecordBatch, DataFusionError>>) -> CapturedResult {
    CapturedResult::from_items(sample_schema(), items).unwrap()
}

#[test]
fn query_correctness_checker_rejects_corruption_and_accepts_legal_layouts() {
    let bag_case = Case {
        name: "checker-bag",
        sql: "synthetic",
        comparison: Comparison::Bag,
        expected_rows: 3,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    };
    let ordered_case = Case {
        name: "checker-ordered",
        sql: "synthetic ORDER BY group",
        comparison: Comparison::Ordered,
        expected_rows: 3,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    };
    let expected = captured(vec![Ok(sample_batch(&[
        (Some("a"), 2),
        (Some("a"), 2),
        (None, 1),
    ]))]);

    let split_and_permuted = captured(vec![
        Ok(sample_batch(&[(None, 1)])),
        Ok(sample_batch(&[(Some("a"), 2), (Some("a"), 2)])),
    ]);
    assert!(compare_results(&bag_case, &expected, &split_and_permuted).is_ok());

    let dropped = captured(vec![Ok(sample_batch(&[(Some("a"), 2), (None, 1)]))]);
    assert!(compare_results(&bag_case, &expected, &dropped).is_err());

    let duplicated = captured(vec![Ok(sample_batch(&[
        (Some("a"), 2),
        (Some("a"), 2),
        (None, 1),
        (None, 1),
    ]))]);
    assert!(compare_results(&bag_case, &expected, &duplicated).is_err());

    let wrong_value = captured(vec![Ok(sample_batch(&[
        (Some("a"), 3),
        (Some("a"), 2),
        (None, 1),
    ]))]);
    assert!(compare_results(&bag_case, &expected, &wrong_value).is_err());

    let wrong_null = captured(vec![Ok(sample_batch(&[
        (Some("a"), 2),
        (Some("a"), 2),
        (Some("a"), 1),
    ]))]);
    assert!(compare_results(&bag_case, &expected, &wrong_null).is_err());

    let wrong_schema = CapturedResult {
        schema: Arc::new(Schema::new(vec![
            Field::new("wrong_group", DataType::LargeUtf8, true),
            Field::new("count", DataType::Int64, false),
        ])),
        rows: expected.rows.clone(),
        terminal_error: None,
    };
    assert!(compare_results(&bag_case, &expected, &wrong_schema).is_err());

    let ordered = captured(vec![Ok(sample_batch(&[
        (None, 1),
        (Some("a"), 2),
        (Some("b"), 2),
    ]))]);
    let ordered_split = captured(vec![
        Ok(sample_batch(&[(None, 1)])),
        Ok(sample_batch(&[(Some("a"), 2), (Some("b"), 2)])),
    ]);
    assert!(compare_results(&ordered_case, &ordered, &ordered_split).is_ok());

    let inverted_across_batches = captured(vec![
        Ok(sample_batch(&[(None, 1), (Some("b"), 2)])),
        Ok(sample_batch(&[(Some("a"), 2)])),
    ]);
    assert!(compare_results(&ordered_case, &ordered, &inverted_across_batches).is_err());

    let late_failure = captured(vec![
        Ok(sample_batch(&[(Some("a"), 2), (Some("a"), 2), (None, 1)])),
        Err(DataFusionError::Execution(
            "injected late failure".to_owned(),
        )),
    ]);
    assert!(compare_results(&bag_case, &expected, &late_failure).is_err());
}

#[test]
fn query_correctness_capture_rejects_batch_schema_drift() {
    let wrong_name = Arc::new(Schema::new(vec![
        Field::new("wrong_group", DataType::LargeUtf8, true),
        Field::new("count", DataType::Int64, false),
    ]));
    let error = CapturedResult::from_items(
        sample_schema(),
        vec![Ok(sample_batch_with_schema(wrong_name, &[]))],
    )
    .unwrap_err();
    assert!(error.contains("batch 0 schema differs"));
    assert!(error.contains("wrong_group"));

    let wrong_type = Arc::new(Schema::new(vec![
        Field::new("group", DataType::LargeUtf8, true),
        Field::new("count", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        wrong_type,
        vec![
            Arc::new(LargeStringArray::from(vec![Some("a")])),
            Arc::new(UInt64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let error = CapturedResult::from_items(sample_schema(), vec![Ok(batch)]).unwrap_err();
    assert!(error.contains("batch 0 schema differs"));
    assert!(error.contains("UInt64"));

    let mut metadata = HashMap::new();
    metadata.insert("semantic".to_owned(), "changed".to_owned());
    let wrong_metadata = Arc::new(Schema::new(vec![
        Field::new("group", DataType::LargeUtf8, true).with_metadata(metadata),
        Field::new("count", DataType::Int64, false),
    ]));
    let error = CapturedResult::from_items(
        sample_schema(),
        vec![Ok(sample_batch_with_schema(
            wrong_metadata,
            &[(Some("a"), 1)],
        ))],
    )
    .unwrap_err();
    assert!(error.contains("batch 0 schema differs"));
    assert!(error.contains("semantic"));
}

#[test]
fn query_correctness_capture_rejects_unsupported_schema_without_values() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "unsupported",
        DataType::Boolean,
        true,
    )]));

    let error = CapturedResult::from_items(
        Arc::clone(&schema),
        Vec::<Result<RecordBatch, DataFusionError>>::new(),
    )
    .unwrap_err();
    assert!(error.contains("unsupported comparison type Boolean"));

    for values in [Vec::<Option<bool>>::new(), vec![None]] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BooleanArray::from(values))],
        )
        .unwrap();
        let error = CapturedResult::from_items(Arc::clone(&schema), vec![Ok(batch)]).unwrap_err();
        assert!(error.contains("unsupported comparison type Boolean"));
    }
}

#[test]
fn query_correctness_success_cases_reject_matching_and_late_failures() {
    let success_case = Case {
        name: "checker-success-outcome",
        sql: "synthetic",
        comparison: Comparison::Bag,
        expected_rows: 0,
        anchor: Anchor::None,
        outcome: ExpectedOutcome::Success,
    };
    let logical = captured(vec![Err(DataFusionError::Execution(
        "matching failure".to_owned(),
    ))]);
    let primary = captured(vec![Err(DataFusionError::Execution(
        "matching failure".to_owned(),
    ))]);
    let candidate = captured(vec![Err(DataFusionError::Execution(
        "matching failure".to_owned(),
    ))]);
    let error = validate_case_outcomes(&success_case, &logical, &primary, &candidate).unwrap_err();
    assert!(error.contains("logical oracle: unexpected terminal failure"));
    assert!(error.contains("primary oracle: unexpected terminal failure"));
    assert!(error.contains("candidate: unexpected terminal failure"));

    let logical = captured(vec![Ok(sample_batch(&[(Some("a"), 1)]))]);
    let primary = captured(vec![Ok(sample_batch(&[(Some("a"), 1)]))]);
    let candidate = captured(vec![
        Ok(sample_batch(&[(Some("a"), 1)])),
        Err(DataFusionError::Execution("late failure".to_owned())),
    ]);
    let error = validate_case_outcomes(&success_case, &logical, &primary, &candidate).unwrap_err();
    assert!(error.contains("candidate: unexpected terminal failure"));
    assert!(error.contains("late failure"));

    let expected_failure = Case {
        outcome: ExpectedOutcome::ErrorContains("declared failure"),
        ..success_case
    };
    let logical = captured(vec![Err(DataFusionError::Execution(
        "declared failure: logical".to_owned(),
    ))]);
    let primary = captured(vec![Err(DataFusionError::Execution(
        "declared failure: primary".to_owned(),
    ))]);
    let candidate = captured(vec![Err(DataFusionError::Execution(
        "declared failure: candidate".to_owned(),
    ))]);
    assert!(validate_case_outcomes(&expected_failure, &logical, &primary, &candidate).is_ok());
}

#[test]
fn query_correctness_checker_counts_zero_column_rows() {
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    let captured = CapturedResult::from_items(schema, vec![Ok(batch)]).unwrap();
    assert_eq!(captured.rows, vec![vec![], vec![], vec![]]);
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_correctness_state_corpus() {
    run_state_corpus(4, 128).await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_correctness_state_corpus_small_batches() {
    run_state_corpus(1, 2).await;
}

async fn run_state_corpus(target_partitions: usize, batch_size: usize) {
    let fixture = StateFixture::deterministic();
    // Exercise the baseline with different parallelism and batch-size settings. Small
    // batches also exercise ordering and aggregate state across batch boundaries.
    let options = HashMap::from([
        (
            "datafusion.execution.target_partitions".to_owned(),
            target_partitions.to_string(),
        ),
        (
            "datafusion.execution.batch_size".to_owned(),
            batch_size.to_string(),
        ),
    ]);
    let env = DataFusionEnv::new(64 * 1024 * 1024, None, None, &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let mut engine = MockQueryEngine::create_with_env(env).await;
    fixture.populate(engine.partition_store()).await;
    let primary_records = fixture.scan_primary(engine.partition_store()).await;

    assert_eq!(
        record_counts(&fixture.records),
        record_counts(&primary_records),
        "broad primary scan disagrees with canonical fixture\n{}",
        fixture.describe()
    );

    for case in CASES {
        let result = engine.execute(case.sql).await.unwrap();
        if let Err(error) = run_case(case, &fixture, &primary_records, result).await {
            panic!(
                "query correctness case failed\ncase={}\nsql={}\ncomparison={:?}\n\
                     target_partitions={target_partitions} batch_size={batch_size}\n{}\n{error}",
                case.name,
                case.sql,
                case.comparison,
                fixture.describe()
            );
        }
    }
}

/// The same independently constructed references gate the real task transport.
pub(crate) async fn run_distributed_state_corpus(
    store: &mut restate_partition_store::PartitionStore,
    engine: &dyn QueryEngine<AdminUser>,
) -> Vec<QueryMetadata> {
    let fixture = StateFixture::deterministic();
    fixture.populate(store).await;
    let primary = fixture.scan_primary(store).await;
    assert_eq!(record_counts(&fixture.records), record_counts(&primary));
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let mut metadata = Vec::new();
    for case in CASES {
        let result = session.execute(case.sql, QueryOptions {}).await.unwrap();
        metadata.push(result.metadata.clone());
        if let Err(error) = run_case(case, &fixture, &primary, result).await {
            panic!(
                "remote correctness case {} failed: {error}\n{}",
                case.name,
                fixture.describe()
            );
        }
    }
    metadata
}

#[test]
fn query_correctness_capture_consumes_batch_splitting_stream() {
    let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        sample_schema(),
        stream::iter(vec![
            Ok(sample_batch(&[(Some("a"), 1)])),
            Ok(sample_batch(&[(Some("b"), 2)])),
        ]),
    ));
    let captured = futures::executor::block_on(capture_stream(stream)).unwrap();
    assert_eq!(captured.rows.len(), 2);
    assert!(captured.terminal_error.is_none());
}
