// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::assert_batches_sorted_eq;
use futures::TryStreamExt;
use serde_json::Value;

use restate_partition_store::PartitionStoreTransaction;
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::WriteInvocationStatusTable;
use restate_storage_api::state_table::WriteStateTable;
use restate_worker_api::invoker::InvocationStatusReport;

use crate::mocks::{MockQueryEngine, MockSchemas, MockStatusHandle};

use super::data::{InvocationFixture, StateFixture};

pub(super) struct QueryFixture {
    engine: MockQueryEngine,
    invocation_state: MockStatusHandle,
}

pub(super) struct QueryExpectation<'a, S> {
    pub(super) name: &'a str,
    pub(super) sql: &'a str,
    pub(super) expected: &'a [S],
}

pub(super) type ExpectedRow = Vec<(&'static str, Value)>;

pub(super) fn format_expected_table(rows: &[ExpectedRow]) -> Vec<String> {
    let first = rows.first().expect("expected at least one row");
    let columns = first.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    let values = rows
        .iter()
        .map(|row| {
            assert_eq!(
                row.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                columns,
                "expected rows must have the same columns",
            );
            row.iter()
                .map(|(_, value)| match value {
                    Value::Null => String::new(),
                    Value::String(value) => value.clone(),
                    value => value.to_string(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let widths = columns
        .iter()
        .enumerate()
        .map(|(column, name)| {
            values
                .iter()
                .map(|row| row[column].len())
                .fold(name.len(), usize::max)
        })
        .collect::<Vec<_>>();

    let border = widths.iter().fold(String::from("+"), |mut line, width| {
        line.push_str(&"-".repeat(width + 2));
        line.push('+');
        line
    });
    let format_row = |row: &[&str]| {
        row.iter()
            .zip(&widths)
            .fold(String::from("|"), |mut line, (value, width)| {
                line.push_str(&format!(" {value:<width$} |"));
                line
            })
    };

    let mut table = Vec::with_capacity(values.len() + 4);
    table.push(border.clone());
    table.push(format_row(&columns));
    table.push(border.clone());
    table.extend(values.iter().map(|row| {
        let row = row.iter().map(String::as_str).collect::<Vec<_>>();
        format_row(&row)
    }));
    table.push(border);
    table
}

impl QueryFixture {
    pub(super) async fn create() -> Self {
        let invocation_state = MockStatusHandle::default();
        Self {
            engine: MockQueryEngine::create_with(invocation_state.clone(), MockSchemas::default())
                .await,
            invocation_state,
        }
    }

    pub(super) async fn populate(
        &mut self,
        populate: impl FnOnce(&mut QueryFixtureWriter<'_, '_>) -> anyhow::Result<()> + Send,
    ) {
        let mut tx = self.engine.partition_store().transaction();
        populate(&mut QueryFixtureWriter {
            transaction: &mut tx,
            invocation_state: &self.invocation_state,
        })
        .unwrap();
        tx.commit().await.unwrap();
    }

    pub(super) async fn assert_queries<S: AsRef<str>>(&self, queries: &[QueryExpectation<'_, S>]) {
        for query in queries {
            eprintln!("running query fixture: {}", query.name);
            let batches = self.execute(query.sql).await;
            let expected = query.expected.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            assert_batches_sorted_eq!(&expected, &batches);
        }
    }

    async fn execute(&self, sql: &str) -> Vec<datafusion::arrow::record_batch::RecordBatch> {
        self.engine
            .execute(sql)
            .await
            .unwrap()
            .stream
            .try_collect()
            .await
            .unwrap()
    }
}

pub(super) struct QueryFixtureWriter<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
    invocation_state: &'a MockStatusHandle,
}

impl<'a, 'store> QueryFixtureWriter<'a, 'store> {
    pub(super) fn state(&mut self) -> StateTableFixture<'_, 'store> {
        StateTableFixture {
            transaction: &mut *self.transaction,
        }
    }

    pub(super) fn sys_invocation_status(&mut self) -> SysInvocationStatusTableFixture<'_, 'store> {
        SysInvocationStatusTableFixture {
            transaction: &mut *self.transaction,
        }
    }

    pub(super) fn sys_invocation_state(&mut self) -> SysInvocationStateTableFixture<'_> {
        SysInvocationStateTableFixture {
            invocation_state: self.invocation_state,
        }
    }
}

pub(super) struct StateTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
}

impl StateTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, state: &StateFixture) -> anyhow::Result<()> {
        self.transaction
            .put_user_state(&state.service_id, &state.state_key, &state.state_value)?;
        Ok(())
    }
}

pub(super) struct SysInvocationStatusTableFixture<'a, 'store> {
    transaction: &'a mut PartitionStoreTransaction<'store>,
}

impl SysInvocationStatusTableFixture<'_, '_> {
    pub(super) fn populate(&mut self, invocation: &InvocationFixture) -> anyhow::Result<()> {
        let status = invocation.invocation_status();
        self.transaction
            .put_invocation_status(&invocation.id, &status)?;
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
}
