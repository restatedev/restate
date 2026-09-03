// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::json::writer::{JsonArray, WriterBuilder};
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

    pub(super) async fn assert_query_unordered(&self, sql: &str, mut expected: Vec<Value>) {
        let batches = self
            .engine
            .execute(sql)
            .await
            .unwrap()
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut json = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut json);
            for batch in &batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        }

        let mut actual: Vec<Value> = serde_json::from_slice(&json).unwrap();
        actual.sort_by_cached_key(|row| row.to_string());
        expected.sort_by_cached_key(|row| row.to_string());

        assert_eq!(actual, expected, "unexpected rows for query:\n{sql}");
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
