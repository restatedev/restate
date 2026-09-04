// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{Context, ensure};
use bytes::Bytes;
use bytestring::ByteString;
use datafusion::common::test_util::{batches_to_sort_string, batches_to_string};
use futures::TryStreamExt;
use prost::Message;

use restate_partition_store::PartitionStoreTransaction;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    JournalMetadata, StatusTimestamps, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::{EventView, WriteJournalEventsTable};
use restate_storage_api::journal_table::{JournalEntry, WriteJournalTable as WriteJournalTableV1};
use restate_storage_api::journal_table_v2::WriteJournalTable as WriteJournalTableV2;
use restate_storage_api::lock_table::{AcquiredBy, LockState, WriteLockTable};
use restate_storage_api::promise_table::{Promise, PromiseResult, PromiseState, WritePromiseTable};
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_api::vqueue_table::metadata::{
    Action, MoveMetrics, Update, VQueueLink, VQueueMeta,
};
use restate_storage_api::vqueue_table::stats::{EntryStatistics, WaitStats};
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, EntryValue, Stage, Status, WriteVQueueTable,
};
use restate_types::LimitKey;
use restate_types::clock::{RoughTimestamp, UniqueTimestamp};
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::{InvocationError, InvocationErrorCode};
use restate_types::identifiers::{
    DeploymentId, InvocationId, PartitionId, PartitionProcessorRpcRequestId, ServiceId,
    SubscriptionId, WithPartitionKey,
};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocationSpanContext, ServiceType, Source, VirtualObjectHandlerType,
    WorkflowHandlerType,
};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::{Entry as EntryV1, InputEntry};
use restate_types::journal_events::Event;
use restate_types::journal_v2::{CompletionId, Encoder, Entry, InputCommand, RunCommand};
use restate_types::partition_table::{FindPartition, PartitionTable};
use restate_types::schema::deployment::Deployment;
use restate_types::schema::service::ServiceMetadata;
use restate_types::service_protocol::{self, ServiceProtocolVersion};
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, EntryKind, VQueueEntryId, VQueueId};
use restate_types::{LockName, Scope, ServiceName, Version};
use restate_util_string::{ReString, RestateString};
use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;
use restate_worker_api::invoker::{InvocationErrorReport, InvocationStatusReport};
use restate_worker_api::{BlockedResource, SchedulingStatus, VQueueSchedulerStatus};

use crate::mocks::{MockRemoteQueryEngine, MockSchemas, MockStatusHandle};

use super::data::{FixtureFactory, InvocationFixture, InvocationFixtureStatus, InvocationOptions};

pub(super) struct QueryTest {
    engine: MockRemoteQueryEngine,
    invocation_state: MockStatusHandle,
    schemas: MockSchemas,
    text_table_factory: FixtureFactory,
}

pub(super) struct QueryExpectation<'a> {
    pub(super) name: &'a str,
    pub(super) sql: &'a str,
    pub(super) expected: &'a [&'a str],
}

impl QueryTest {
    pub(super) async fn create_remote() -> Self {
        let invocation_state = MockStatusHandle::default();
        let schemas = MockSchemas::default();
        Self {
            engine: MockRemoteQueryEngine::create_with(invocation_state.clone(), schemas.clone())
                .await,
            invocation_state,
            schemas,
            text_table_factory: FixtureFactory::for_text_tables(),
        }
    }

    pub(super) async fn populate(
        &mut self,
        populate: impl FnOnce(&mut QueryTestTables<'_, '_>) -> anyhow::Result<()> + Send,
    ) {
        self.populate_partition(None, populate).await;
    }

    async fn populate_partition(
        &mut self,
        expected_partition: Option<PartitionId>,
        populate: impl FnOnce(&mut QueryTestTables<'_, '_>) -> anyhow::Result<()> + Send,
    ) {
        let partition_table = self.engine.partition_table().clone();
        let transactions = self
            .engine
            .partition_stores_mut()
            .iter_mut()
            .map(|(partition_id, store)| (*partition_id, store.transaction()))
            .collect::<BTreeMap<_, _>>();
        let mut transactions = PartitionTransactions {
            partition_table,
            expected_partition,
            transactions,
        };
        populate(&mut QueryTestTables {
            transactions: &mut transactions,
            invocation_state: &self.invocation_state,
            schemas: &self.schemas,
            text_table_factory: &mut self.text_table_factory,
        })
        .unwrap();
        for transaction in transactions.transactions.values_mut() {
            transaction.commit().await.unwrap();
        }
    }

    pub(super) async fn assert_query(&self, query: QueryExpectation<'_>) {
        self.assert_query_with_order(query, false).await;
    }

    pub(super) async fn assert_query_ordered(&self, query: QueryExpectation<'_>) {
        self.assert_query_with_order(query, true).await;
    }

    async fn assert_query_with_order(&self, query: QueryExpectation<'_>, ordered: bool) {
        eprintln!("running query test: {}", query.name);
        self.engine.clear_remote_scans();
        let batches = match self.try_execute(query.sql).await {
            Ok(batches) => batches,
            Err(error) => {
                let report = self
                    .failure_report(&query, format!("query execution failed:\n{error:#}"))
                    .await;
                panic!("{report}");
            }
        };
        let expected = expected_table(query.expected, ordered);
        let actual = if ordered {
            batches_to_string(&batches)
        } else {
            batches_to_sort_string(&batches)
        };
        let remote_scans = self.engine.remote_scans();

        let empty_result_without_schema =
            actual == "++\n++" && is_empty_expected_table(query.expected);
        if expected != actual && !empty_result_without_schema {
            let report = self
                .failure_report(
                    &query,
                    format!("expected:\n{expected}\n\nactual:\n{actual}"),
                )
                .await;
            panic!("{report}");
        }
        let unexpected_remote_scans = remote_scans
            .iter()
            .filter(|scan| self.engine.remote_owner(scan.partition_id) != Some(scan.node_id))
            .collect::<Vec<_>>();
        if remote_scans.is_empty() || !unexpected_remote_scans.is_empty() {
            let report = self
                .failure_report(
                    &query,
                    format!(
                        "query did not use the configured remote partition owners\nremote scans: {remote_scans:?}\nunexpected scans: {unexpected_remote_scans:?}"
                    ),
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

fn expected_table<'a>(expected: &'a [&'a str], ordered: bool) -> String {
    if is_vertical_expected_table(expected) {
        return transpose_expected_table(expected, ordered)
            .unwrap_or_else(|error| panic!("invalid vertical expected table: {error:#}"));
    }

    let mut expected = expected.to_vec();
    let num_lines = expected.len();
    if !ordered && num_lines > 3 {
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

fn is_empty_expected_table(expected: &[&str]) -> bool {
    !is_vertical_expected_table(expected)
        && expected
            .iter()
            .map(|line| line.trim())
            .filter(|line| line.starts_with('|') && line.ends_with('|'))
            .count()
            == 1
}

fn transpose_expected_table<'a>(expected: &'a [&'a str], ordered: bool) -> anyhow::Result<String> {
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

    Ok(format_table(&columns, &rows, ordered))
}

fn format_table(columns: &[&str], rows: &[Vec<&str>], ordered: bool) -> String {
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
    if !ordered {
        rendered_rows.sort_unstable();
    }

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

    fn parse_fixture(lines: &'input [&'input str]) -> anyhow::Result<Self> {
        let table = Self::parse(lines)?;
        if table.columns.first() == Some(&"column") {
            table.transpose()
        } else {
            Ok(table)
        }
    }

    fn transpose(self) -> anyhow::Result<Self> {
        ensure!(
            self.columns.len() > 1,
            "vertical text table must contain at least one data row"
        );
        let columns = self
            .rows
            .iter()
            .map(|(_, values)| values[0])
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        for column in &columns {
            ensure!(
                !column.is_empty(),
                "vertical text table has an empty column name"
            );
            ensure!(
                seen.insert(*column),
                "vertical text table has duplicate column `{column}`"
            );
        }
        let rows = (1..self.columns.len())
            .map(|column_index| {
                (
                    column_index + 1,
                    self.rows
                        .iter()
                        .map(|(_, values)| values[column_index])
                        .collect(),
                )
            })
            .collect();
        Ok(Self { columns, rows })
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

    fn parse_optional<T>(&self, column: &str) -> anyhow::Result<Option<T>>
    where
        T: FromStr,
        T::Err: Display,
    {
        self.get(column)
            .map(|value| {
                value.parse().map_err(|error| {
                    anyhow::anyhow!(
                        "text table row on line {} has invalid `{column}` value {value:?}: {error}",
                        self.line_number
                    )
                })
            })
            .transpose()
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

    fn partition_id(&self) -> anyhow::Result<PartitionId> {
        Ok(PartitionId::new_unchecked(self.parse("partition_id")?))
    }

    fn vqueue_id(&self) -> anyhow::Result<VQueueId> {
        let partition_key: u64 = self.parse("partition_key")?;
        let vqueue_id: VQueueId = self.parse("id")?;
        ensure!(
            vqueue_id.partition_key() == partition_key,
            "text table row on line {} has partition_key {partition_key}, but vqueue {vqueue_id} belongs to partition key {}",
            self.line_number,
            vqueue_id.partition_key()
        );
        Ok(vqueue_id)
    }
}

fn unique_timestamp(row: &TextTableRow<'_, '_>, column: &str) -> anyhow::Result<UniqueTimestamp> {
    let millis = MillisSinceEpoch::from(row.parse::<u64>(column)?);
    UniqueTimestamp::try_from_unix_millis(millis).map_err(|error| {
        anyhow::anyhow!(
            "text table row on line {} has invalid `{column}` timestamp {millis}: {error}",
            row.line_number
        )
    })
}

fn optional_unique_timestamp(
    row: &TextTableRow<'_, '_>,
    column: &str,
) -> anyhow::Result<Option<UniqueTimestamp>> {
    row.parse_optional::<u64>(column)?
        .map(|millis| {
            UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::from(millis)).map_err(|error| {
                anyhow::anyhow!(
                    "text table row on line {} has invalid `{column}` timestamp {millis}: {error}",
                    row.line_number
                )
            })
        })
        .transpose()
}

fn required_millis(row: &TextTableRow<'_, '_>, column: &str) -> anyhow::Result<MillisSinceEpoch> {
    Ok(MillisSinceEpoch::from(row.parse::<u64>(column)?))
}

fn optional_millis(
    row: &TextTableRow<'_, '_>,
    column: &str,
) -> anyhow::Result<Option<MillisSinceEpoch>> {
    Ok(row
        .parse_optional::<u64>(column)?
        .map(MillisSinceEpoch::from))
}

fn vqueue_stage(row: &TextTableRow<'_, '_>) -> anyhow::Result<Stage> {
    match row.required("stage")? {
        "inbox" => Ok(Stage::Inbox),
        "running" => Ok(Stage::Running),
        "suspended" => Ok(Stage::Suspended),
        "paused" => Ok(Stage::Paused),
        "finished" => Ok(Stage::Finished),
        value => anyhow::bail!(
            "text table row on line {} has invalid `stage` value {value:?}",
            row.line_number
        ),
    }
}

fn vqueue_status(row: &TextTableRow<'_, '_>) -> anyhow::Result<Status> {
    match row.required("status")? {
        "new" => Ok(Status::New),
        "scheduled" => Ok(Status::Scheduled),
        "started" => Ok(Status::Started),
        "backing-off" => Ok(Status::BackingOff),
        "yielded" => Ok(Status::Yielded),
        "killed" => Ok(Status::Killed),
        "cancelled" => Ok(Status::Cancelled),
        "failed" => Ok(Status::Failed),
        "succeeded" => Ok(Status::Succeeded),
        value => anyhow::bail!(
            "text table row on line {} has invalid `status` value {value:?}",
            row.line_number
        ),
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

fn invocation_source(row: &TextTableRow<'_, '_>) -> anyhow::Result<Source> {
    match row.required("invoked_by")? {
        "service" => {
            ensure!(
                row.get("invoked_by_subscription_id").is_none()
                    && row.get("restarted_from").is_none(),
                "text table row on line {} sets fields that do not belong to a service source",
                row.line_number
            );
            let target = row.required("invoked_by_target")?;
            let mut target_parts = target.split('/');
            let service_name = target_parts.next().unwrap_or_default();
            let handler_name = target_parts.next().unwrap_or_default();
            ensure!(
                !service_name.is_empty()
                    && !handler_name.is_empty()
                    && target_parts.next().is_none(),
                "text table row on line {} has invalid service source target {target:?}",
                row.line_number
            );
            Ok(Source::Service(
                row.parse("invoked_by_id")?,
                InvocationTarget::service(service_name, handler_name),
            ))
        }
        "ingress" => {
            ensure_source_columns_are_empty(row)?;
            Ok(Source::Ingress(PartitionProcessorRpcRequestId::default()))
        }
        "restate" => {
            ensure_source_columns_are_empty(row)?;
            Ok(Source::Internal)
        }
        "subscription" => {
            ensure!(
                row.get("invoked_by_id").is_none()
                    && row.get("invoked_by_target").is_none()
                    && row.get("restarted_from").is_none(),
                "text table row on line {} sets fields that do not belong to a subscription source",
                row.line_number
            );
            Ok(Source::Subscription(
                row.parse::<SubscriptionId>("invoked_by_subscription_id")?,
            ))
        }
        "restart_as_new" => {
            ensure!(
                row.get("invoked_by_id").is_none()
                    && row.get("invoked_by_target").is_none()
                    && row.get("invoked_by_subscription_id").is_none(),
                "text table row on line {} sets fields that do not belong to a restart-as-new source",
                row.line_number
            );
            Ok(Source::RestartAsNew(row.parse("restarted_from")?))
        }
        value => anyhow::bail!(
            "text table row on line {} has unsupported `invoked_by` value {value:?}",
            row.line_number
        ),
    }
}

fn ensure_source_columns_are_empty(row: &TextTableRow<'_, '_>) -> anyhow::Result<()> {
    ensure!(
        row.get("invoked_by_id").is_none()
            && row.get("invoked_by_target").is_none()
            && row.get("invoked_by_subscription_id").is_none()
            && row.get("restarted_from").is_none(),
        "text table row on line {} sets source-specific fields for a source that has none",
        row.line_number
    );
    Ok(())
}

struct PartitionTransactions<'store> {
    partition_table: PartitionTable,
    expected_partition: Option<PartitionId>,
    transactions: BTreeMap<PartitionId, PartitionStoreTransaction<'store>>,
}

impl<'store> PartitionTransactions<'store> {
    fn for_key(
        &mut self,
        key: &impl WithPartitionKey,
    ) -> anyhow::Result<&mut PartitionStoreTransaction<'store>> {
        let partition_id = self.validate_key(key)?;
        self.transactions.get_mut(&partition_id).ok_or_else(|| {
            anyhow::anyhow!(
                "partition {partition_id} for partition key {} has no test store",
                key.partition_key()
            )
        })
    }

    fn validate_key(&self, key: &impl WithPartitionKey) -> anyhow::Result<PartitionId> {
        let partition_key = key.partition_key();
        let partition_id = self.partition_table.find_partition_id(partition_key)?;
        if let Some(expected_partition) = self.expected_partition {
            ensure!(
                partition_id == expected_partition,
                "fixture with partition key {partition_key} belongs to partition {partition_id}, but this population block targets partition {expected_partition}"
            );
        }
        Ok(partition_id)
    }

    fn validate_declared_partition(
        &self,
        declared_partition: PartitionId,
        key: &impl WithPartitionKey,
    ) -> anyhow::Result<()> {
        let actual_partition = self.validate_key(key)?;
        ensure!(
            actual_partition == declared_partition,
            "fixture declares partition {declared_partition}, but partition key {} belongs to partition {actual_partition}",
            key.partition_key()
        );
        Ok(())
    }
}

pub(super) struct QueryTestTables<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
    invocation_state: &'a MockStatusHandle,
    schemas: &'a MockSchemas,
    text_table_factory: &'a mut FixtureFactory,
}

impl<'a, 'store> QueryTestTables<'a, 'store> {
    pub(super) fn state(&mut self) -> StateTableFixture<'_, 'store> {
        StateTableFixture {
            transactions: &mut *self.transactions,
        }
    }

    pub(super) fn sys_promise(&mut self) -> SysPromiseTableFixture<'_, 'store> {
        SysPromiseTableFixture {
            transactions: &mut *self.transactions,
        }
    }

    pub(super) fn sys_invocation_status(&mut self) -> SysInvocationStatusTableFixture<'_, 'store> {
        SysInvocationStatusTableFixture {
            transactions: &mut *self.transactions,
            text_table_factory: &mut *self.text_table_factory,
        }
    }

    pub(super) fn sys_invocation_state(&mut self) -> SysInvocationStateTableFixture<'_, 'store> {
        SysInvocationStateTableFixture {
            invocation_state: self.invocation_state,
            transactions: &*self.transactions,
        }
    }

    pub(super) fn sys_vqueue_meta(&mut self) -> SysVQueueMetaTableFixture<'_, 'store> {
        SysVQueueMetaTableFixture {
            transactions: &mut *self.transactions,
        }
    }

    pub(super) fn sys_vqueues(&mut self) -> SysVQueuesTableFixture<'_, 'store> {
        SysVQueuesTableFixture {
            transactions: &mut *self.transactions,
        }
    }

    pub(super) fn sys_journal(&mut self) -> SysJournalTableFixture<'_, 'store> {
        SysJournalTableFixture {
            transactions: &mut *self.transactions,
        }
    }

    pub(super) fn sys_journal_events(&mut self) -> SysJournalEventsTableFixture<'_, 'store> {
        SysJournalEventsTableFixture {
            transactions: &mut *self.transactions,
        }
    }

    pub(super) fn sys_locks(&mut self) -> SysLocksTableFixture<'_, 'store> {
        SysLocksTableFixture {
            transactions: &mut *self.transactions,
        }
    }

    pub(super) fn sys_scheduler(&mut self) -> SysSchedulerTableFixture<'_, 'store> {
        SysSchedulerTableFixture {
            invocation_state: self.invocation_state,
            transactions: &*self.transactions,
        }
    }

    pub(super) fn sys_deployment(&mut self) -> SysDeploymentTableFixture<'_> {
        SysDeploymentTableFixture {
            schemas: self.schemas,
        }
    }

    pub(super) fn sys_service(&mut self) -> SysServiceTableFixture<'_> {
        SysServiceTableFixture {
            schemas: self.schemas,
        }
    }
}

pub(super) struct StateTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
}

pub(super) struct SysPromiseTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
}

impl SysPromiseTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "scope",
            "service_name",
            "service_key",
            "key",
            "completed",
            "completion_success_value",
            "completion_failure",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let partition_key: u64 = row.parse("partition_key")?;
            let service_id = ServiceId::new(
                row.get("scope").map(Scope::try_new).transpose()?,
                row.required("service_name")?,
                row.required("service_key")?,
            );
            ensure!(
                service_id.partition_key() == partition_key,
                "text table row on line {} has partition_key {partition_key}, but its promise identity belongs to partition key {}",
                row.line_number,
                service_id.partition_key()
            );
            self.transactions
                .validate_declared_partition(partition_id, &service_id)?;

            let success = row.get("completion_success_value");
            let failure = row.get("completion_failure");
            let state = match (row.parse::<bool>("completed")?, success, failure) {
                (false, None, None) => PromiseState::NotCompleted(Vec::new()),
                (true, Some(value), None) => PromiseState::Completed(PromiseResult::Success(
                    Bytes::copy_from_slice(value.as_bytes()),
                )),
                (true, None, Some(value)) => {
                    let error = parse_invocation_error(value)?;
                    PromiseState::Completed(PromiseResult::Failure(
                        error.code(),
                        ByteString::from(error.message()),
                        Vec::new(),
                    ))
                }
                _ => anyhow::bail!(
                    "text table row on line {} must leave both completion columns empty when incomplete, or set exactly one when completed",
                    row.line_number
                ),
            };
            self.transactions.for_key(&service_id)?.put_promise(
                &service_id,
                &ByteString::from(row.required("key")?),
                &Promise { state },
            )?;
        }
        Ok(())
    }
}

impl StateTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "scope",
            "service_name",
            "service_key",
            "key",
            "value",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let partition_key: u64 = row.parse("partition_key")?;
            let service_id = ServiceId::new(
                row.get("scope").map(Scope::try_new).transpose()?,
                row.required("service_name")?,
                row.required("service_key")?,
            );
            ensure!(
                service_id.partition_key() == partition_key,
                "text table row on line {} has partition_key {partition_key}, but its state identity belongs to partition key {}",
                row.line_number,
                service_id.partition_key()
            );
            self.transactions
                .validate_declared_partition(partition_id, &service_id)?;
            let key = Bytes::copy_from_slice(row.required("key")?.as_bytes());
            self.transactions.for_key(&service_id)?.put_user_state(
                &service_id,
                &key,
                row.required("value")?.as_bytes(),
            )?;
        }
        Ok(())
    }
}

pub(super) struct SysInvocationStatusTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
    text_table_factory: &'a mut FixtureFactory,
}

impl SysInvocationStatusTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, invocation: &InvocationFixture) -> anyhow::Result<()> {
        let status = invocation.invocation_status();
        self.transactions
            .for_key(&invocation.id)?
            .put_invocation_status(&invocation.id, &status)?;
        Ok(())
    }

    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        if table.columns.contains(&"target") {
            return self.populate_full_table(table);
        }

        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "id",
            "status",
            "completion_result",
            "created_at",
            "target_service_name",
            "target_service_key",
            "target_handler_name",
            "target_service_ty",
            "scope",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
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
                });
            invocation.id = row.invocation_id()?;
            invocation.target = invocation_target(&row)?;
            invocation.timestamps = StatusTimestamps::new(
                MillisSinceEpoch::from(row.parse::<u64>("created_at")?),
                invocation.timestamps.modification_time(),
                invocation.timestamps.inboxed_transition_time(),
                invocation.timestamps.scheduled_transition_time(),
                invocation.timestamps.running_transition_time(),
                invocation.timestamps.completed_transition_time(),
            );
            self.transactions
                .validate_declared_partition(partition_id, &invocation.id)?;
            self.populate(&invocation)?;
        }
        Ok(())
    }

    fn populate_full_table(&mut self, table: TextTable<'_>) -> anyhow::Result<()> {
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "id",
            "vqueue_id",
            "status",
            "completion_result",
            "completion_failure",
            "target",
            "target_service_name",
            "target_service_key",
            "target_handler_name",
            "target_service_ty",
            "scope",
            "limit_key",
            "idempotency_key",
            "invoked_by",
            "invoked_by_id",
            "invoked_by_subscription_id",
            "invoked_by_target",
            "restarted_from",
            "pinned_deployment_id",
            "pinned_service_protocol_version",
            "journal_size",
            "journal_commands_size",
            "created_at",
            "modified_at",
            "inboxed_at",
            "scheduled_at",
            "scheduled_start_at",
            "running_at",
            "completed_at",
            "completion_retention",
            "journal_retention",
            "suspended_waiting_for_completions",
            "suspended_waiting_for_signals",
            "suspended_waiting_future_json",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let id = row.invocation_id()?;
            let status = InvocationFixtureStatus::try_from((
                row.required("status")?,
                row.get("completion_result"),
            ))?;
            ensure!(
                !matches!(status, InvocationFixtureStatus::Running)
                    || row.get("completion_failure").is_none(),
                "text table row on line {} cannot set `completion_failure` for an invoked invocation",
                row.line_number
            );
            ensure!(
                row.get("suspended_waiting_for_completions").is_none()
                    && row.get("suspended_waiting_for_signals").is_none()
                    && row.get("suspended_waiting_future_json").is_none(),
                "text table row on line {} uses suspended fields, which this fixture does not support",
                row.line_number
            );

            let target = invocation_target(&row)?;
            ensure!(
                row.required("target")? == target.to_string(),
                "text table row on line {} has target {:?}, but its target columns produce {target}",
                row.line_number,
                row.required("target")?
            );
            let pinned_deployment = match (
                row.get("pinned_deployment_id"),
                row.parse_optional::<i32>("pinned_service_protocol_version")?,
            ) {
                (Some(deployment_id), Some(protocol_version)) => Some(PinnedDeployment::new(
                    deployment_id.parse::<DeploymentId>()?,
                    ServiceProtocolVersion::try_from(protocol_version)?,
                )),
                (None, None) => None,
                _ => anyhow::bail!(
                    "text table row on line {} must set both pinned deployment columns or neither",
                    row.line_number
                ),
            };
            let invocation = InvocationFixture {
                id,
                status,
                target,
                vqueue_id: row
                    .get("vqueue_id")
                    .map(str::parse::<VQueueId>)
                    .transpose()?,
                limit_key: row.required("limit_key")?.parse::<LimitKey<ReString>>()?,
                source: invocation_source(&row)?,
                execution_time: optional_millis(&row, "scheduled_start_at")?,
                idempotency_key: row.get("idempotency_key").map(Into::into),
                timestamps: StatusTimestamps::new(
                    required_millis(&row, "created_at")?,
                    required_millis(&row, "modified_at")?,
                    optional_millis(&row, "inboxed_at")?,
                    optional_millis(&row, "scheduled_at")?,
                    optional_millis(&row, "running_at")?,
                    optional_millis(&row, "completed_at")?,
                ),
                completion_retention: Duration::from_millis(row.parse("completion_retention")?),
                journal_retention: Duration::from_millis(row.parse("journal_retention")?),
                journal: JournalMetadata::new(
                    row.parse("journal_size")?,
                    row.parse("journal_commands_size")?,
                    ServiceInvocationSpanContext::empty(),
                ),
                pinned_deployment,
            };
            self.transactions
                .validate_declared_partition(partition_id, &invocation.id)?;
            self.populate(&invocation)?;
        }
        Ok(())
    }
}

pub(super) struct SysInvocationStateTableFixture<'a, 'store> {
    invocation_state: &'a MockStatusHandle,
    transactions: &'a PartitionTransactions<'store>,
}

impl SysInvocationStateTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        let full = table.columns.contains(&"retry_count");
        if full {
            table.ensure_exact_columns(&[
                "partition_id",
                "partition_key",
                "id",
                "in_flight",
                "retry_count",
                "last_start_at",
                "next_retry_at",
                "last_attempt_deployment_id",
                "last_attempt_server",
                "last_failure",
                "last_failure_error_code",
                "last_awaiting_on_future_json",
            ])?;
        } else {
            table.ensure_exact_columns(&["partition_id", "partition_key", "id", "in_flight"])?;
        }

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let invocation_id = row.invocation_id()?;
            self.transactions
                .validate_declared_partition(partition_id, &invocation_id)?;
            if full {
                ensure!(
                    row.get("last_failure_error_code").is_none(),
                    "text table row on line {} uses `last_failure_error_code`, which requires a static documentation-code fixture",
                    row.line_number
                );
            }
            self.invocation_state.push(InvocationStatusReport::new(
                invocation_id,
                if full {
                    InvocationStatusReportInner {
                        in_flight: row.parse("in_flight")?,
                        start_count: row.parse("retry_count")?,
                        last_start_at: UNIX_EPOCH
                            + Duration::from_millis(row.parse("last_start_at")?),
                        next_retry_at: row
                            .parse_optional::<u64>("next_retry_at")?
                            .map(|millis| UNIX_EPOCH + Duration::from_millis(millis)),
                        last_retry_attempt_failure: row
                            .get("last_failure")
                            .map(parse_invocation_error)
                            .transpose()?
                            .map(|err| InvocationErrorReport {
                                err,
                                doc_error_code: None,
                                related_entry_index: None,
                                related_entry_name: None,
                                related_entry_type: None,
                            }),
                        last_awaiting_on_unresolved_future: row
                            .get("last_awaiting_on_future_json")
                            .map(serde_json::from_str)
                            .transpose()
                            .with_context(|| {
                                format!(
                                    "text table row on line {} has invalid `last_awaiting_on_future_json`",
                                    row.line_number
                                )
                            })?,
                        last_attempt_deployment_id: row
                            .get("last_attempt_deployment_id")
                            .map(str::parse::<DeploymentId>)
                            .transpose()?,
                        last_attempt_server: row.get("last_attempt_server").map(str::to_owned),
                        ..InvocationStatusReportInner::default()
                    }
                } else {
                    InvocationStatusReportInner {
                        in_flight: row.parse("in_flight")?,
                        ..InvocationStatusReportInner::default()
                    }
                },
            ));
        }
        Ok(())
    }
}

fn parse_invocation_error(value: &str) -> anyhow::Result<InvocationError> {
    let (code, message) = value
        .strip_prefix('[')
        .and_then(|value| value.split_once("] "))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "invalid invocation failure {value:?}; expected `[numeric-code] message`"
            )
        })?;
    let code = code
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .parse::<u16>()?;
    Ok(InvocationError::new(
        InvocationErrorCode::new(code),
        message,
    ))
}

pub(super) struct SysVQueueMetaTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
}

impl SysVQueueMetaTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "id",
            "queue_is_paused",
            "service_name",
            "scope",
            "limit_key",
            "lock_name",
            "created_at",
            "num_inbox",
            "num_running",
            "num_suspended",
            "num_paused",
            "num_finished",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let vqueue_id = row.vqueue_id()?;
            self.transactions
                .validate_declared_partition(partition_id, &vqueue_id)?;

            let scope = row.get("scope").map(Scope::try_new).transpose()?;
            if let Some(scope) = &scope {
                ensure!(
                    scope.partition_key() == vqueue_id.partition_key(),
                    "text table row on line {} has scope {scope}, which belongs to partition key {}, but vqueue {vqueue_id} belongs to partition key {}",
                    row.line_number,
                    scope.partition_key(),
                    vqueue_id.partition_key()
                );
            }

            let service_name = row.required("service_name")?;
            let link = match row.get("lock_name") {
                Some(value) => {
                    let lock_name = LockName::parse(value)?;
                    ensure!(
                        lock_name.service_name().as_str() == service_name,
                        "text table row on line {} has service_name {service_name:?}, but lock_name belongs to service {:?}",
                        row.line_number,
                        lock_name.service_name().as_str()
                    );
                    VQueueLink::Lock(lock_name)
                }
                None => VQueueLink::Service(ServiceName::new(service_name)),
            };
            let created_at = unique_timestamp(&row, "created_at")?;
            let mut metadata =
                VQueueMeta::new(created_at, scope, row.required("limit_key")?.parse()?, link);

            for (stage, count) in [
                (Stage::Inbox, row.parse("num_inbox")?),
                (Stage::Running, row.parse("num_running")?),
                (Stage::Suspended, row.parse("num_suspended")?),
                (Stage::Paused, row.parse("num_paused")?),
                (Stage::Finished, row.parse("num_finished")?),
            ] {
                for _ in 0..count {
                    metadata.apply_update(&Update::new(
                        created_at,
                        Action::Move {
                            prev_stage: None,
                            next_stage: stage,
                            metrics: MoveMetrics {
                                last_transition_at: created_at,
                                has_started: false,
                                first_runnable_at: created_at.to_unix_millis(),
                                scheduler_wait_stats: None,
                            },
                        },
                    ));
                }
            }
            if row.parse("queue_is_paused")? {
                metadata.apply_update(&Update::new(created_at, Action::PauseVQueue {}));
            }

            self.transactions
                .for_key(&vqueue_id)?
                .create_vqueue(&vqueue_id, &metadata);
        }
        Ok(())
    }
}

pub(super) struct SysVQueuesTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
}

impl SysVQueuesTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "id",
            "stage",
            "status",
            "has_lock",
            "run_at",
            "sequence_number",
            "entry_id",
            "entry_kind",
            "created_at",
            "transitioned_at",
            "num_attempts",
            "num_errors",
            "num_pauses",
            "num_suspensions",
            "num_yields",
            "first_attempt_at",
            "latest_attempt_at",
            "first_runnable_at",
            "deployment",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let vqueue_id = row.vqueue_id()?;
            self.transactions
                .validate_declared_partition(partition_id, &vqueue_id)?;

            let displayed_entry_id: VQueueEntryId = row.parse("entry_id")?;
            ensure!(
                displayed_entry_id.partition_key() == vqueue_id.partition_key(),
                "text table row on line {} has entry_id {displayed_entry_id}, which belongs to partition key {}, but vqueue {vqueue_id} belongs to partition key {}",
                row.line_number,
                displayed_entry_id.partition_key(),
                vqueue_id.partition_key()
            );
            let entry_kind = match displayed_entry_id.kind() {
                EntryKind::Invocation => "invocation",
                EntryKind::StateMutation => "state-mutation",
                EntryKind::Unknown => unreachable!("parsed entry IDs have a known kind"),
            };
            ensure!(
                row.required("entry_kind")? == entry_kind,
                "text table row on line {} declares entry_kind {:?}, but entry_id {displayed_entry_id} has kind {entry_kind:?}",
                row.line_number,
                row.required("entry_kind")?
            );

            let entry_id: EntryId = displayed_entry_id.into();
            let run_at = MillisSinceEpoch::from(row.parse::<u64>("run_at")?);
            let entry_key = EntryKey::new(
                row.parse::<bool>("has_lock")?,
                run_at,
                row.parse::<u64>("sequence_number")?,
                entry_id,
            );
            let mut stats =
                EntryStatistics::new(unique_timestamp(&row, "created_at")?, run_at.into());
            stats.transitioned_at = unique_timestamp(&row, "transitioned_at")?;
            stats.num_attempts = row.parse("num_attempts")?;
            stats.num_errors = row.parse("num_errors")?;
            stats.num_paused = row.parse("num_pauses")?;
            stats.num_suspensions = row.parse("num_suspensions")?;
            stats.num_yields = row.parse("num_yields")?;
            stats.first_attempt_at = optional_unique_timestamp(&row, "first_attempt_at")?;
            stats.latest_attempt_at = optional_unique_timestamp(&row, "latest_attempt_at")?;
            stats.first_runnable_at =
                MillisSinceEpoch::from(row.parse::<u64>("first_runnable_at")?);
            let metadata = EntryMetadata {
                deployment: row.get("deployment").map(ReString::new),
                ..EntryMetadata::default()
            };
            let stage = vqueue_stage(&row)?;
            let status = vqueue_status(&row)?;
            let value = EntryValue {
                status,
                metadata,
                stats,
            };

            let transaction = self.transactions.for_key(&vqueue_id)?;
            transaction.put_vqueue_inbox(&vqueue_id, stage, &entry_key, &value);
            transaction.put_vqueue_entry_status(
                &vqueue_id,
                stage,
                &entry_key,
                &value.metadata,
                value.stats,
                status,
            );
        }
        Ok(())
    }
}

pub(super) struct SysJournalTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
}

impl SysJournalTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "id",
            "index",
            "version",
            "appended_at",
            "entry_type",
            "name",
            "value",
            "completion_id",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let invocation_id = row.invocation_id()?;
            self.transactions
                .validate_declared_partition(partition_id, &invocation_id)?;
            match row.parse::<u8>("version")? {
                1 => {
                    let entry = match row.required("entry_type")? {
                        "Input" => {
                            ensure!(
                                row.get("name").is_none()
                                    && row.get("completion_id").is_none()
                                    && row.get("appended_at").is_none(),
                                "text table row on line {} cannot set `name`, `completion_id`, or `appended_at` for a V1 Input entry",
                                row.line_number
                            );
                            JournalEntry::Entry(ProtobufRawEntryCodec::serialize_enriched(
                                EntryV1::Input(InputEntry {
                                    headers: Vec::new(),
                                    value: Bytes::copy_from_slice(
                                        row.required("value")?.as_bytes(),
                                    ),
                                }),
                            ))
                        }
                        "Run" => {
                            ensure!(
                                row.get("value").is_none()
                                    && row.get("completion_id").is_none()
                                    && row.get("appended_at").is_none(),
                                "text table row on line {} cannot set `value`, `completion_id`, or `appended_at` for a V1 Run entry",
                                row.line_number
                            );
                            JournalEntry::Entry(EnrichedRawEntry::new(
                                EnrichedEntryHeader::Run {},
                                service_protocol::RunEntryMessage {
                                    name: row.required("name")?.to_owned(),
                                    result: None,
                                }
                                .encode_to_vec()
                                .into(),
                            ))
                        }
                        value => anyhow::bail!(
                            "text table row on line {} has unsupported V1 `entry_type` value {value:?}",
                            row.line_number
                        ),
                    };
                    WriteJournalTableV1::put_journal_entry(
                        self.transactions.for_key(&invocation_id)?,
                        &invocation_id,
                        row.parse("index")?,
                        &entry,
                    )?;
                }
                2 => {
                    let (entry, related_completion_ids): (Entry, Vec<CompletionId>) = match row
                        .required("entry_type")?
                    {
                        "Input" => {
                            ensure!(
                                row.get("completion_id").is_none(),
                                "text table row on line {} cannot set `completion_id` for an Input entry",
                                row.line_number
                            );
                            (
                                InputCommand {
                                    headers: Vec::new(),
                                    payload: Bytes::copy_from_slice(
                                        row.required("value")?.as_bytes(),
                                    ),
                                    name: row.required("name")?.into(),
                                }
                                .into(),
                                Vec::new(),
                            )
                        }
                        "Run" => {
                            ensure!(
                                row.get("value").is_none(),
                                "text table row on line {} cannot set `value` for a Run entry",
                                row.line_number
                            );
                            let completion_id = row.parse("completion_id")?;
                            (
                                RunCommand {
                                    completion_id,
                                    name: row.required("name")?.into(),
                                }
                                .into(),
                                vec![completion_id],
                            )
                        }
                        value => anyhow::bail!(
                            "text table row on line {} has unsupported V2 `entry_type` value {value:?}",
                            row.line_number
                        ),
                    };
                    let entry = StoredRawEntry::new(
                        StoredRawEntryHeader::new(MillisSinceEpoch::from(
                            row.parse::<u64>("appended_at")?,
                        )),
                        ServiceProtocolV4Codec::encode_entry(entry),
                    );
                    WriteJournalTableV2::put_journal_entry(
                        self.transactions.for_key(&invocation_id)?,
                        &invocation_id,
                        row.parse("index")?,
                        &entry,
                        &related_completion_ids,
                    )?;
                }
                version => anyhow::bail!(
                    "text table row on line {} has unsupported journal version {version}; expected `1` or `2`",
                    row.line_number
                ),
            }
        }
        Ok(())
    }
}

pub(super) struct SysJournalEventsTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
}

impl SysJournalEventsTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "id",
            "after_journal_entry_index",
            "appended_at",
            "event_type",
            "event_json",
        ])?;

        for (lsn, row) in table.rows().enumerate() {
            let partition_id = row.partition_id()?;
            let invocation_id = row.invocation_id()?;
            self.transactions
                .validate_declared_partition(partition_id, &invocation_id)?;
            let event: Event =
                serde_json::from_str(row.required("event_json")?).with_context(|| {
                    format!(
                        "text table row on line {} has invalid `event_json`",
                        row.line_number
                    )
                })?;
            let event = EventView::new(
                MillisSinceEpoch::from(row.parse::<u64>("appended_at")?),
                row.parse("after_journal_entry_index")?,
                event,
            );
            ensure!(
                event.event.ty().to_string() == row.required("event_type")?,
                "text table row on line {} declares event_type {:?}, but event_json contains {:?}",
                row.line_number,
                row.required("event_type")?,
                event.event.ty().to_string()
            );
            self.transactions
                .for_key(&invocation_id)?
                .put_journal_event(&invocation_id, event, lsn as u64)?;
        }
        Ok(())
    }
}

pub(super) struct SysLocksTableFixture<'a, 'store> {
    transactions: &'a mut PartitionTransactions<'store>,
}

impl SysLocksTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "scope",
            "lock_name",
            "acquired_at",
            "acquired_by",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let partition_key: u64 = row.parse("partition_key")?;
            let scope = Scope::try_new(row.required("scope")?)?;
            ensure!(
                scope.partition_key() == partition_key,
                "text table row on line {} has partition_key {partition_key}, but scope {scope} belongs to partition key {}",
                row.line_number,
                scope.partition_key()
            );
            self.transactions
                .validate_declared_partition(partition_id, &scope)?;

            let state = LockState {
                acquired_at: unique_timestamp(&row, "acquired_at")?,
                acquired_by: AcquiredBy::InvocationId(row.parse("acquired_by")?),
            };
            self.transactions.for_key(&scope)?.acquire_lock(
                &Some(scope),
                &LockName::parse(row.required("lock_name")?)?,
                &state,
            );
        }
        Ok(())
    }
}

pub(super) struct SysSchedulerTableFixture<'a, 'store> {
    invocation_state: &'a MockStatusHandle,
    transactions: &'a PartitionTransactions<'store>,
}

pub(super) struct SysDeploymentTableFixture<'a> {
    schemas: &'a MockSchemas,
}

impl SysDeploymentTableFixture<'_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&["id", "endpoint", "created_at"])?;

        for row in table.rows() {
            let mut deployment = Deployment::mock_with_uri(row.required("endpoint")?);
            deployment.id = row.parse("id")?;
            deployment.created_at = required_millis(&row, "created_at")?;
            self.schemas.add_deployment(deployment);
        }
        Ok(())
    }
}

pub(super) struct SysServiceTableFixture<'a> {
    schemas: &'a MockSchemas,
}

impl SysServiceTableFixture<'_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&["name", "ty", "deployment_id"])?;

        for row in table.rows() {
            let mut service = ServiceMetadata::mock_service(row.required("name")?, ["run"]);
            service.ty = match row.required("ty")? {
                "service" => ServiceType::Service,
                "virtual_object" => ServiceType::VirtualObject,
                "workflow" => ServiceType::Workflow,
                value => anyhow::bail!(
                    "text table row on line {} has unsupported service type {value:?}",
                    row.line_number
                ),
            };
            service.deployment_id = row.parse("deployment_id")?;
            self.schemas.add_service(service);
        }
        Ok(())
    }
}

impl SysSchedulerTableFixture<'_, '_> {
    pub(super) fn populate_table(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        let table = TextTable::parse_fixture(lines)?;
        table.ensure_exact_columns(&[
            "partition_id",
            "partition_key",
            "id",
            "num_inbox",
            "status",
            "head_entry_id",
            "scheduled_at",
            "blocked_on",
            "blocked_on_json",
            "invoker_concurrency_block_duration",
            "throttling_rules_block_duration",
            "invoker_throttling_block_duration",
            "invoker_memory_block_duration",
            "concurrency_rules_block_duration",
            "lock_block_duration",
            "deployment_concurrency_block_duration",
        ])?;

        for row in table.rows() {
            let partition_id = row.partition_id()?;
            let vqueue_id = row.vqueue_id()?;
            self.transactions
                .validate_declared_partition(partition_id, &vqueue_id)?;

            let head_entry_id = row
                .get("head_entry_id")
                .map(str::parse::<VQueueEntryId>)
                .transpose()?;
            if let Some(head_entry_id) = &head_entry_id {
                ensure!(
                    head_entry_id.partition_key() == vqueue_id.partition_key(),
                    "text table row on line {} has head_entry_id {head_entry_id}, which belongs to partition key {}, but vqueue {vqueue_id} belongs to partition key {}",
                    row.line_number,
                    head_entry_id.partition_key(),
                    vqueue_id.partition_key()
                );
            }

            let status = match row.required("status")? {
                "dormant" => SchedulingStatus::Dormant,
                "empty" => SchedulingStatus::Empty,
                "ready" => SchedulingStatus::Ready,
                "scheduled" => SchedulingStatus::Scheduled {
                    at: RoughTimestamp::from_unix_millis_clamped(required_millis(
                        &row,
                        "scheduled_at",
                    )?),
                },
                "blocked" => {
                    let blocked_on = match row.required("blocked_on")? {
                        "invoker-concurrency" => BlockedResource::InvokerConcurrency,
                        "invoker-throttling" => BlockedResource::InvokerThrottling {
                            estimated_retry_at: None,
                        },
                        "invoker-memory" => BlockedResource::InvokerMemory,
                        "deployment-concurrency" => BlockedResource::DeploymentConcurrency,
                        value => anyhow::bail!(
                            "text table row on line {} has unsupported `blocked_on` value {value:?}",
                            row.line_number
                        ),
                    };
                    ensure!(
                        serde_json::to_string(&blocked_on)? == row.required("blocked_on_json")?,
                        "text table row on line {} has a `blocked_on_json` value that does not match `blocked_on`",
                        row.line_number
                    );
                    SchedulingStatus::BlockedOn(blocked_on)
                }
                value => anyhow::bail!(
                    "text table row on line {} has unsupported scheduler status {value:?}",
                    row.line_number
                ),
            };
            if !matches!(status, SchedulingStatus::Scheduled { .. }) {
                ensure!(
                    row.get("scheduled_at").is_none(),
                    "text table row on line {} can only set `scheduled_at` for a scheduled row",
                    row.line_number
                );
            }
            if !matches!(status, SchedulingStatus::BlockedOn(_)) {
                ensure!(
                    row.get("blocked_on").is_none() && row.get("blocked_on_json").is_none(),
                    "text table row on line {} can only set blocked-on columns for a blocked row",
                    row.line_number
                );
            }

            self.invocation_state.push_scheduler_status((
                vqueue_id,
                VQueueSchedulerStatus {
                    wait_stats: WaitStats {
                        blocked_on_invoker_concurrency_ms: row
                            .parse("invoker_concurrency_block_duration")?,
                        blocked_on_throttling_rules_ms: row
                            .parse("throttling_rules_block_duration")?,
                        blocked_on_invoker_throttling_ms: row
                            .parse("invoker_throttling_block_duration")?,
                        blocked_on_invoker_memory_ms: row.parse("invoker_memory_block_duration")?,
                        blocked_on_concurrency_rules_ms: row
                            .parse("concurrency_rules_block_duration")?,
                        blocked_on_lock_ms: row.parse("lock_block_duration")?,
                        blocked_on_deployment_concurrency_ms: row
                            .parse("deployment_concurrency_block_duration")?,
                    },
                    waiting_inbox: row.parse("num_inbox")?,
                    status,
                    head_entry_id: head_entry_id.map(Into::into),
                    ..VQueueSchedulerStatus::default()
                },
            ));
        }
        Ok(())
    }
}

#[test]
fn vertical_expected_table_respects_order_mode() {
    let expected = &[
        "+--------+---------+-----------+",
        "| column | row 1   | row 2     |",
        "+--------+---------+-----------+",
        "| id     | b       | a         |",
        "| status | running | completed |",
        "+--------+---------+-----------+",
    ];

    assert_eq!(
        expected_table(expected, false),
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
    assert_eq!(
        expected_table(expected, true),
        [
            "+----+-----------+",
            "| id | status    |",
            "+----+-----------+",
            "| b  | running   |",
            "| a  | completed |",
            "+----+-----------+",
        ]
        .join("\n")
    );
}

#[test]
fn header_only_expected_table_represents_an_empty_result() {
    assert!(is_empty_expected_table(&[
        "+----+--------+",
        "| id | status |",
        "+----+--------+",
        "+----+--------+",
    ]));
    assert!(!is_empty_expected_table(&[
        "+----+--------+",
        "| id | status |",
        "+----+--------+",
        "| 1  | ready  |",
        "+----+--------+",
    ]));
}

#[test]
fn partition_population_rejects_data_for_another_partition() {
    let transactions = PartitionTransactions {
        partition_table: PartitionTable::with_equally_sized_partitions(Version::MIN, 3),
        expected_partition: Some(PartitionId::new_unchecked(1)),
        transactions: BTreeMap::new(),
    };

    let error = transactions
        .validate_key(&Scope::try_from_static("scope-a").unwrap())
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "fixture with partition key 3169317165037139997 belongs to partition 0, but this population block targets partition 1"
    );
}

#[test]
fn text_table_partition_must_match_partition_key() {
    let transactions = PartitionTransactions {
        partition_table: PartitionTable::with_equally_sized_partitions(Version::MIN, 3),
        expected_partition: None,
        transactions: BTreeMap::new(),
    };

    let error = transactions
        .validate_declared_partition(
            PartitionId::new_unchecked(1),
            &Scope::try_from_static("scope-a").unwrap(),
        )
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "fixture declares partition 1, but partition key 3169317165037139997 belongs to partition 0"
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_report_includes_query_mismatch_and_plans() {
    let test = QueryTest::create_remote().await;
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
