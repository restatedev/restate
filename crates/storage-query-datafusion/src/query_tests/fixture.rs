// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::test_util::{batches_to_sort_string, batches_to_string};
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
            let batches = match self.try_execute(query.sql).await {
                Ok(batches) => batches,
                Err(error) => {
                    let report = self
                        .failure_report(query, format!("query execution failed:\n{error:#}"))
                        .await;
                    panic!("{report}");
                }
            };
            let expected = sorted_expected_table(query.expected);
            let actual = batches_to_sort_string(&batches);

            if expected != actual {
                let report = self
                    .failure_report(query, format!("expected:\n{expected}\n\nactual:\n{actual}"))
                    .await;
                panic!("{report}");
            }
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
            "\nquery fixture `{}` failed\n\nQUERY\n{}\n\nMISMATCH\n{}\n\nEXPLAIN\n{}\n\nEXPLAIN ANALYZE FORMAT TREE\n{}\n",
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

fn sorted_expected_table(expected: &[&str]) -> String {
    let mut expected = expected.to_vec();
    let num_lines = expected.len();
    if num_lines > 3 {
        expected[2..num_lines - 1].sort_unstable();
    }
    expected.join("\n")
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

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_report_includes_query_mismatch_and_plans() {
    let fixture = QueryFixture::create().await;
    let query = QueryExpectation {
        name: "diagnostic fixture",
        sql: "SELECT 1 AS value",
        expected: &[],
    };

    let report = fixture
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
