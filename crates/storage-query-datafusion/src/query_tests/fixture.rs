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

use restate_partition_store::PartitionStoreTransaction;
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::WriteInvocationStatusTable;
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table::WriteJournalTable;
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_api::vqueue_table::WriteVQueueTable;
use restate_worker_api::invoker::InvocationStatusReport;

use crate::mocks::{MockQueryEngine, MockSchemas, MockStatusHandle};

use super::data::{InvocationFixture, StateFixture, VQueueFixture};

pub(super) struct QueryFixture {
    engine: MockQueryEngine,
    invocation_state: MockStatusHandle,
}

pub(super) struct QueryExpectation<'a> {
    pub(super) name: &'a str,
    pub(super) sql: &'a str,
    pub(super) expected: &'a [&'a str],
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

    pub(super) async fn assert_queries(&self, queries: &[QueryExpectation<'_>]) {
        for query in queries {
            eprintln!("running query fixture: {}", query.name);
            let batches = self.execute(query.sql).await;
            assert_batches_sorted_eq!(query.expected, &batches);
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
