// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::ensure;
use bytes::Bytes;
use datafusion::common::test_util::{batches_to_sort_string, batches_to_string};
use futures::TryStreamExt;

use restate_partition_store::PartitionStoreTransaction;
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::WriteInvocationStatusTable;
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table::WriteJournalTable;
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_api::vqueue_table::WriteVQueueTable;
use restate_types::Scope;
use restate_types::identifiers::{InvocationId, ServiceId, WithPartitionKey};
use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType, WorkflowHandlerType};
use restate_util_string::RestateString;
use restate_worker_api::invoker::InvocationStatusReport;
use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;

use crate::mocks::{MockQueryEngine, MockSchemas, MockStatusHandle};

use super::data::{
    FixtureFactory, InvocationFixture, InvocationFixtureStatus, InvocationOptions, VQueueFixture,
};

pub(super) struct QueryTest {
    engine: MockQueryEngine,
    invocation_state: MockStatusHandle,
    text_table_factory: FixtureFactory,
}

pub(super) struct QueryExpectation<'a> {
    pub(super) name: &'a str,
    pub(super) sql: &'a str,
    pub(super) expected: &'a [&'a str],
}

impl QueryTest {
    pub(super) async fn create() -> Self {
        let invocation_state = MockStatusHandle::default();
        Self {
            engine: MockQueryEngine::create_with(invocation_state.clone(), MockSchemas::default())
                .await,
            invocation_state,
            text_table_factory: FixtureFactory::for_text_tables(),
        }
    }

    pub(super) async fn populate(
        &mut self,
        populate: impl FnOnce(&mut QueryTestTables<'_, '_>) -> anyhow::Result<()> + Send,
    ) {
        let mut tx = self.engine.partition_store().transaction();
        populate(&mut QueryTestTables {
            transaction: &mut tx,
            invocation_state: &self.invocation_state,
            text_table_factory: &mut self.text_table_factory,
        })
        .unwrap();
        tx.commit().await.unwrap();
    }

    pub(super) async fn assert_query(&self, query: QueryExpectation<'_>) {
        eprintln!("running query test: {}", query.name);
        let batches = match self.try_execute(query.sql).await {
            Ok(batches) => batches,
            Err(error) => {
                let report = self
                    .failure_report(&query, format!("query execution failed:\n{error:#}"))
                    .await;
                panic!("{report}");
            }
        };
        let expected = sorted_expected_table(query.expected);
        let actual = batches_to_sort_string(&batches);

        if expected != actual {
            let report = self
                .failure_report(
                    &query,
                    format!("expected:\n{expected}\n\nactual:\n{actual}"),
                )
                .await;
            panic!("{report}");
        }
    }

    async fn failure_report(&self, query: &QueryExpectation<'_>, mismatch: String) -> String {
        let explain = match self.try_execute(&format!("EXPLAIN {}", query.sql)).await {
            Ok(batches) => batches_to_string(&batches),
            Err(error) => format!("failed to explain query: {error:#}"),
        };
        let explain_analyze = self
            .engine
            .explain_analyze_tree(query.sql)
            .await
            .unwrap_or_else(|error| format!("failed to analyze query: {error:#}"));

        format!(
            "\nquery test `{}` failed\n\nQUERY\n{}\n\nMISMATCH\n{}\n\nEXPLAIN\n{}\n\nEXPLAIN ANALYZE FORMAT TREE\n{}\n",
            query.name, query.sql, mismatch, explain, explain_analyze,
        )
    }

    async fn try_execute(
        &self,
        sql: &str,
    ) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>, crate::context::QueryError> {
        self.engine
            .execute(sql)
            .await?
            .stream
            .try_collect()
            .await
            .map_err(Into::into)
    }
}

fn sorted_expected_table<'a>(expected: &'a [&'a str]) -> String {
    if is_vertical_expected_table(expected) {
        return transpose_expected_table(expected)
            .unwrap_or_else(|error| panic!("invalid vertical expected table: {error:#}"));
    }

    let mut expected = expected.to_vec();
    let num_lines = expected.len();
    if num_lines > 3 {
        expected[2..num_lines - 1].sort_unstable();
    }
    expected.join("\n")
}

fn is_vertical_expected_table(expected: &[&str]) -> bool {
    expected
        .iter()
        .map(|line| line.trim())
        .find(|line| !line.is_empty() && !line.starts_with('+'))
        .filter(|line| line.starts_with('|') && line.ends_with('|'))
        .and_then(|line| parse_text_table_line(line).into_iter().next())
        == Some("column")
}

fn transpose_expected_table<'a>(expected: &'a [&'a str]) -> anyhow::Result<String> {
    let table = TextTable::parse(expected)?;
    ensure!(
        table.columns.first() == Some(&"column"),
        "vertical expected table must start with a `column` header"
    );
    ensure!(
        table.columns.len() > 1,
        "vertical expected table must contain at least one result row"
    );

    let columns = table
        .rows
        .iter()
        .map(|(_, values)| values[0])
        .collect::<Vec<_>>();
    let rows = (1..table.columns.len())
        .map(|column_index| {
            table
                .rows
                .iter()
                .map(|(_, values)| values[column_index])
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    Ok(format_sorted_table(&columns, &rows))
}

fn format_sorted_table(columns: &[&str], rows: &[Vec<&str>]) -> String {
    let widths = columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            rows.iter()
                .map(|row| row[column_index].chars().count())
                .chain(std::iter::once(column.chars().count()))
                .max()
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let separator = widths.iter().fold(String::from("+"), |mut line, width| {
        line.push_str(&"-".repeat(width + 2));
        line.push('+');
        line
    });
    let render_row = |values: &[&str]| {
        values
            .iter()
            .zip(&widths)
            .fold(String::from("|"), |mut line, (value, width)| {
                line.push(' ');
                line.push_str(value);
                line.push_str(&" ".repeat(width - value.chars().count()));
                line.push_str(" |");
                line
            })
    };
    let mut rendered_rows = rows.iter().map(|row| render_row(row)).collect::<Vec<_>>();
    rendered_rows.sort_unstable();

    let mut lines = Vec::with_capacity(rendered_rows.len() + 4);
    lines.push(separator.clone());
    lines.push(render_row(columns));
    lines.push(separator.clone());
    lines.extend(rendered_rows);
    lines.push(separator);
    lines.join("\n")
}

struct TextTable<'input> {
    columns: Vec<&'input str>,
    rows: Vec<(usize, Vec<&'input str>)>,
}

impl<'input> TextTable<'input> {
    fn parse(lines: &'input [&'input str]) -> anyhow::Result<Self> {
        let mut parsed_lines = Vec::new();

        for (index, raw_line) in lines.iter().enumerate() {
            let line_number = index + 1;
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('+') {
                continue;
            }
            ensure!(
                line.starts_with('|') && line.ends_with('|'),
                "text table line {line_number} must start and end with `|`: {line}"
            );
            parsed_lines.push((line_number, parse_text_table_line(line)));
        }

        ensure!(
            parsed_lines.len() >= 2,
            "text table must contain a header and at least one row"
        );
        let (header_line, columns) = parsed_lines.remove(0);
        let mut seen = HashSet::new();
        for column in &columns {
            ensure!(
                !column.is_empty(),
                "text table has an empty column name on line {header_line}"
            );
            ensure!(
                seen.insert(*column),
                "text table has duplicate column `{column}` on line {header_line}"
            );
        }

        for (line_number, values) in &parsed_lines {
            ensure!(
                values.len() == columns.len(),
                "text table row on line {line_number} has {} values but the header has {} columns",
                values.len(),
                columns.len()
            );
        }

        Ok(Self {
            columns,
            rows: parsed_lines,
        })
    }

    fn ensure_exact_columns(&self, expected: &[&str]) -> anyhow::Result<()> {
        ensure!(
            self.columns == expected,
            "text table columns must be exactly: {}",
            expected.join(", ")
        );
        Ok(())
    }

    fn rows(&self) -> impl Iterator<Item = TextTableRow<'_, 'input>> {
        self.rows.iter().map(|(line_number, values)| TextTableRow {
            line_number: *line_number,
            columns: &self.columns,
            values,
        })
    }
}

fn parse_text_table_line(line: &str) -> Vec<&str> {
    line[1..line.len() - 1].split('|').map(str::trim).collect()
}

struct TextTableRow<'table, 'input> {
    line_number: usize,
    columns: &'table [&'input str],
    values: &'table [&'input str],
}

impl<'input> TextTableRow<'_, 'input> {
    fn get(&self, column: &str) -> Option<&'input str> {
        self.columns
            .iter()
            .position(|candidate| *candidate == column)
            .and_then(|index| self.values.get(index).copied())
            .filter(|value| !value.is_empty())
    }

    fn required(&self, column: &str) -> anyhow::Result<&'input str> {
        self.get(column).ok_or_else(|| {
            anyhow::anyhow!(
                "text table row on line {} requires column `{column}`",
                self.line_number
            )
        })
    }

    fn parse<T>(&self, column: &str) -> anyhow::Result<T>
    where
        T: FromStr,
        T::Err: Display,
    {
        let value = self.required(column)?;
        value.parse().map_err(|error| {
            anyhow::anyhow!(
                "text table row on line {} has invalid `{column}` value {value:?}: {error}",
                self.line_number
            )
        })
    }

    fn invocation_id(&self) -> anyhow::Result<InvocationId> {
        let partition_key: u64 = self.parse("partition_key")?;
        let invocation_id: InvocationId = self.parse("id")?;
        ensure!(
            invocation_id.partition_key() == partition_key,
            "text table row on line {} has partition_key {partition_key}, but invocation {invocation_id} belongs to partition {}",
            self.line_number,
            invocation_id.partition_key()
        );
        Ok(invocation_id)
    }
}

fn invocation_target(row: &TextTableRow<'_, '_>) -> anyhow::Result<InvocationTarget> {
    let service_name = row.required("target_service_name")?;
    let service_key = row.get("target_service_key");
    let handler_name = row.required("target_handler_name")?;
    let scope = row.get("scope").map(Scope::try_new).transpose()?;

    match row.required("target_service_ty")? {
        "service" => {
            ensure!(
                service_key.is_none(),
                "text table row on line {} cannot set `target_service_key` for a service target",
                row.line_number
            );
            Ok(match scope {
                Some(scope) => InvocationTarget::scoped_service(service_name, handler_name, scope),
                None => InvocationTarget::service(service_name, handler_name),
            })
        }
        "virtual_object" => {
            let service_key = row.required("target_service_key")?;
            Ok(match scope {
                Some(scope) => InvocationTarget::scoped_virtual_object(
                    service_name,
                    service_key,
                    handler_name,
                    VirtualObjectHandlerType::Exclusive,
                    scope,
                ),
                None => InvocationTarget::virtual_object(
                    service_name,
                    service_key,
                    handler_name,
                    VirtualObjectHandlerType::Exclusive,
                ),
            })
        }
        "workflow" => {
            let service_key = row.required("target_service_key")?;
            Ok(match scope {
                Some(scope) => InvocationTarget::scoped_workflow(
                    service_name,
                    service_key,
                    handler_name,
                    WorkflowHandlerType::Workflow,
                    scope,
                ),
                None => InvocationTarget::workflow(
                    service_name,
                    service_key,
                    handler_name,
                    WorkflowHandlerType::Workflow,
                ),
            })
        }
        value => anyhow::bail!(
            "text table row on line {} has invalid `target_service_ty` value {value:?}",
            row.line_number
        ),
    }
}

pub(super) struct QueryTestTables<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
    invocation_state: &'a MockStatusHandle,
    text_table_factory: &'a mut FixtureFactory,
}

impl<'a, 'store> QueryTestTables<'a, 'store> {
    pub(super) fn state(&mut self) -> StateTableFixture<'_, 'store> {
        StateTableFixture {
            transaction: &mut *self.transaction,
        }
    }

    pub(super) fn sys_invocation_status(&mut self) -> SysInvocationStatusTableFixture<'_, 'store> {
        SysInvocationStatusTableFixture {
            transaction: &mut *self.transaction,
            text_table_factory: &mut *self.text_table_factory,
        }
    }

    pub(super) fn sys_invocation_state(&mut self) -> SysInvocationStateTableFixture<'_> {
        SysInvocationStateTableFixture {
            invocation_state: self.invocation_state,
        }
    }

    pub(super) fn sys_vqueue_meta(&mut self) -> SysVQueueMetaTableFixture<'_, 'store> {
        SysVQueueMetaTableFixture {
            transaction: &mut *self.transaction,
        }
    }

    pub(super) fn sys_vqueues(&mut self) -> SysVQueuesTableFixture<'_, 'store> {
        SysVQueuesTableFixture {
            transaction: &mut *self.transaction,
        }
    }

    pub(super) fn sys_journal(&mut self) -> SysJournalTableFixture<'_, 'store> {
        SysJournalTableFixture {
            transaction: &mut *self.transaction,
        }
    }

    pub(super) fn sys_journal_events(&mut self) -> SysJournalEventsTableFixture<'_, 'store> {
        SysJournalEventsTableFixture {
            transaction: &mut *self.transaction,
        }
    }
}

pub(super) struct StateTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
}

impl StateTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse(lines)?;
        table.ensure_exact_columns(&["scope", "service_name", "service_key", "key", "value"])?;

        for row in table.rows() {
            let service_id = ServiceId::new(
                row.get("scope").map(Scope::try_new).transpose()?,
                row.required("service_name")?,
                row.required("service_key")?,
            );
            let key = Bytes::copy_from_slice(row.required("key")?.as_bytes());
            self.transaction.put_user_state(
                &service_id,
                &key,
                row.required("value")?.as_bytes(),
            )?;
        }
        Ok(())
    }
}

pub(super) struct SysInvocationStatusTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
    text_table_factory: &'a mut FixtureFactory,
}

impl SysInvocationStatusTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, invocation: &InvocationFixture) -> anyhow::Result<()> {
        let status = invocation.invocation_status();
        self.transaction
            .put_invocation_status(&invocation.id, &status)?;
        Ok(())
    }

    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse(lines)?;
        table.ensure_exact_columns(&[
            "partition_key",
            "id",
            "status",
            "completion_result",
            "target_service_name",
            "target_service_key",
            "target_handler_name",
            "target_service_ty",
            "scope",
        ])?;

        for row in table.rows() {
            let mut invocation = self
                .text_table_factory
                .create_invocation(InvocationOptions {
                    service_name: row.required("target_service_name")?,
                    service_key: row.get("target_service_key").unwrap_or(""),
                    handler_name: row.required("target_handler_name")?,
                    status: InvocationFixtureStatus::try_from((
                        row.required("status")?,
                        row.get("completion_result"),
                    ))?,
                    ..InvocationOptions::default()
                });
            invocation.id = row.invocation_id()?;
            invocation.target = invocation_target(&row)?;
            self.populate(&invocation)?;
        }
        Ok(())
    }
}

pub(super) struct SysInvocationStateTableFixture<'a> {
    invocation_state: &'a MockStatusHandle,
}

impl SysInvocationStateTableFixture<'_> {
    pub(super) fn populate(&mut self, invocation: &InvocationFixture) {
        self.invocation_state.push(InvocationStatusReport::new(
            invocation.id,
            invocation.state.clone(),
        ));
    }

    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse(lines)?;
        table.ensure_exact_columns(&["partition_key", "id", "in_flight"])?;

        for row in table.rows() {
            self.invocation_state.push(InvocationStatusReport::new(
                row.invocation_id()?,
                InvocationStatusReportInner {
                    in_flight: row.parse("in_flight")?,
                    ..InvocationStatusReportInner::default()
                },
            ));
        }
        Ok(())
    }
}

pub(super) struct SysVQueueMetaTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
}

impl SysVQueueMetaTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, vqueue: &VQueueFixture) {
        self.transaction
            .create_vqueue(&vqueue.id, &vqueue.metadata());
    }
}

pub(super) struct SysVQueuesTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
}

impl SysVQueuesTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, invocation: &InvocationFixture) -> anyhow::Result<()> {
        let vqueue_id = invocation
            .vqueue_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("invocation fixture has no vqueue"))?;
        let entry = invocation
            .vqueue_entry
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("invocation fixture has no vqueue entry"))?;

        self.transaction
            .put_vqueue_inbox(vqueue_id, entry.stage, &entry.key, &entry.value);
        self.transaction.put_vqueue_entry_status(
            vqueue_id,
            entry.stage,
            &entry.key,
            &entry.value.metadata,
            entry.value.stats.clone(),
            entry.value.status,
        );
        Ok(())
    }
}

pub(super) struct SysJournalTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
}

impl SysJournalTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, invocation: &InvocationFixture) -> anyhow::Result<()> {
        for entry in &invocation.journal_entries {
            self.transaction
                .put_journal_entry(&invocation.id, entry.index, &entry.entry)?;
        }
        Ok(())
    }
}

pub(super) struct SysJournalEventsTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
}

impl SysJournalEventsTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, invocation: &InvocationFixture) -> anyhow::Result<()> {
        for (lsn, event) in invocation.journal_events.iter().enumerate() {
            self.transaction
                .put_journal_event(&invocation.id, event.clone(), lsn as u64)?;
        }
        Ok(())
    }
}

#[test]
fn vertical_expected_table_is_transposed_and_sorted() {
    let actual = sorted_expected_table(&[
        "+--------+---------+-----------+",
        "| column | row 1   | row 2     |",
        "+--------+---------+-----------+",
        "| id     | b       | a         |",
        "| status | running | completed |",
        "+--------+---------+-----------+",
    ]);

    assert_eq!(
        actual,
        [
            "+----+-----------+",
            "| id | status    |",
            "+----+-----------+",
            "| a  | completed |",
            "| b  | running   |",
            "+----+-----------+",
        ]
        .join("\n")
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_report_includes_query_mismatch_and_plans() {
    let test = QueryTest::create().await;
    let query = QueryExpectation {
        name: "diagnostic query",
        sql: "SELECT 1 AS value",
        expected: &[],
    };

    let report = test
        .failure_report(&query, "expected value 2, actual value 1".to_owned())
        .await;

    for section in [
        "QUERY\nSELECT 1 AS value",
        "MISMATCH\nexpected value 2, actual value 1",
        "EXPLAIN\n",
        "logical_plan",
        "physical_plan",
        "EXPLAIN ANALYZE FORMAT TREE\n",
        "Metrics:\n",
        "metrics=[output_rows=1",
    ] {
        assert!(
            report.contains(section),
            "missing {section:?} in:\n{report}"
        );
    }
}
