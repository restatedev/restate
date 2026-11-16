// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Retires journal v1 when a partition applies
//! [`PartitionFeatureChange::RetireJournalV1`](restate_types::partitions::PartitionFeatureChange::RetireJournalV1).
//!
//! Every replica runs the retirement at the same log position, on the same state, so all of them
//! end up with the same state.

use bytes::BytesMut;
use tracing::debug;

use restate_partition_store::PartitionDb;
use restate_partition_store::journal_table::{InvocationsWithJournal, delete_journal_table};
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::identifiers::InvocationId;
use restate_types::logs::Lsn;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::time::MillisSinceEpoch;

use super::{ActionCollector, CommandHandler, Error, StateMachine, StateMachineApplyContext};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::lifecycle::OnPurgeJournalCommand;

/// The retirement commits whenever its transaction has grown to this size (1 MiB). Tests commit
/// after every invocation.
const MAX_TRANSACTION_SIZE: usize = if cfg!(test) { 1 } else { 1024 * 1024 };

/// What retiring journal v1 changed on a partition.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JournalV1Retirement {
    /// Invocations killed because their journal was stored in journal v1.
    pub killed: usize,
    /// Completed invocations whose journal v1 was dropped.
    pub journals_dropped: usize,
}

impl StateMachine {
    /// Retires journal v1. It visits every invocation that has entries in the journal v1 table:
    /// * invocations that haven't completed and whose journal is stored in journal v1 are killed,
    ///   and lose their journal even if they retain it;
    /// * completed invocations whose journal is stored in journal v1 lose their journal.
    ///
    /// It then deletes the journal v1 table of the partition, and with it the entries that no
    /// journal accounts for.
    ///
    /// The changes to the invocations are written to `txn`, which is committed in parts so that
    /// it doesn't grow without bound, and in full before the table is deleted. If the retirement
    /// stops before it has deleted the table, it runs again when the barrier is applied again.
    /// It then skips the invocations it has already retired: each one is retired within one
    /// commit, and afterwards it is free or completed without a journal.
    ///
    /// The actions it causes, such as responses to the ingress, are discarded: like every replica,
    /// the leader runs the retirement while it isn't leading yet.
    pub async fn retire_journal_v1<T>(
        processor: impl ProcessorContext,
        txn: &mut T,
        partition_db: &PartitionDb,
        record_created_at: MillisSinceEpoch,
        record_lsn: Lsn,
    ) -> Result<JournalV1Retirement, Error>
    where
        T: Transaction,
    {
        let mut action_collector = ActionCollector::default();
        let mut encoding_arena = BytesMut::new();
        let mut ctx = StateMachineApplyContext {
            processor,
            storage: txn,
            record_created_at,
            record_lsn,
            action_collector: &mut action_collector,
            leader: None,
            encoding_arena: &mut encoding_arena,
        };

        // The scan and the deletion of the table work on the partition, not on the transaction.
        ctx.storage.commit().await?;

        let mut retirement = JournalV1Retirement::default();
        for invocation_id in InvocationsWithJournal::new(partition_db)? {
            // Allow tokio to cancel this task if the processor is being cancelled.
            tokio::task::consume_budget().await;

            ctx.retire_journal_v1_of(invocation_id?, &mut retirement)
                .await?;
            ctx.action_collector.clear();

            if ctx.storage.estimated_size_in_bytes() >= MAX_TRANSACTION_SIZE {
                ctx.storage.commit().await?;
            }
        }
        ctx.storage.commit().await?;

        delete_journal_table(partition_db)?;
        Ok(retirement)
    }
}

impl<S, P> StateMachineApplyContext<'_, S, P>
where
    S: Transaction,
    P: ProcessorContext,
{
    async fn retire_journal_v1_of(
        &mut self,
        invocation_id: InvocationId,
        retirement: &mut JournalV1Retirement,
    ) -> Result<(), Error> {
        let status = self.get_invocation_status(&invocation_id).await?;
        if !stores_journal_in_v1(&status) {
            // Its journal v1 entries belong to no journal, such as completions that never found
            // their entry.
            return Ok(());
        }

        if matches!(status, InvocationStatus::Completed(_)) {
            retirement.journals_dropped += 1;
        } else {
            debug!(%invocation_id, "Killing invocation whose journal is stored in journal v1");
            self.on_kill_invocation(invocation_id, None).await?;
            retirement.killed += 1;
        }
        // A killed invocation that is retained keeps its journal, like a completed one. Purging it
        // is needed even though the table is deleted at the end: the purge clears the journal from
        // the invocation's status, and deletes its journal events and, for invocations with a
        // deterministic id such as workflows, the timers of its pending sleeps.
        OnPurgeJournalCommand {
            invocation_id: &invocation_id,
            response_sink: None,
            is_cleaner_purge: false,
        }
        .apply(&mut *self)
        .await
    }
}

/// Whether an invocation that has entries in the journal v1 table stores its journal there.
fn stores_journal_in_v1(status: &InvocationStatus) -> bool {
    let (journal_metadata, pinned_deployment) = match status {
        InvocationStatus::Invoked(metadata)
        | InvocationStatus::Suspended { metadata, .. }
        | InvocationStatus::Paused(metadata) => {
            (&metadata.journal_metadata, &metadata.pinned_deployment)
        }
        InvocationStatus::Completed(completed) => {
            (&completed.journal_metadata, &completed.pinned_deployment)
        }
        // Invocations that haven't started yet have no journal, or one in journal v2.
        InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_) | InvocationStatus::Free => {
            return false;
        }
    };
    // Deployments older than service protocol V4 store their journal in journal v1. An invocation
    // that isn't pinned to a deployment yet has its input there if it was started before journal
    // v2 became the default (see `init_journal`), and hasn't run on a deployment since. An
    // invocation without a journal, such as one whose journal was purged, has nothing to retire.
    journal_metadata.length > 0
        && pinned_deployment.as_ref().is_none_or(|pinned_deployment| {
            pinned_deployment.service_protocol_version < ServiceProtocolVersion::V4
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use googletest::prelude::{assert_that, pat};
    use test_log::test;

    use restate_partition_store::journal_table::InvocationsWithJournal;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InvocationStatus, ReadInvocationStatusTable,
        WriteInvocationStatusTable,
    };
    use restate_storage_api::journal_table::{JournalEntry, WriteJournalTable};
    use restate_storage_api::{Transaction, journal_table_v2};
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::{DeploymentId, InvocationId};
    use restate_types::invocation::{InvocationTarget, ServiceInvocation};
    use restate_types::journal::CompletionResult;
    use restate_types::logs::Lsn;
    use restate_types::partitions::PersistedFeatures;
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::v2::{Command, commands};

    use super::JournalV1Retirement;
    use crate::partition::state_machine::StateMachine;
    use crate::partition::state_machine::tests::TestEnv;
    use crate::partition::state_machine::tests::fixtures::{
        completed_invoke_entry, mock_start_invocation_with_invocation_target, pinned_deployment,
    };

    /// Starts an invocation that is retained with its journal once it ends, and pins it to a
    /// deployment that stores its journal in journal v1.
    async fn start_retained_on_journal_v1(env: &mut TestEnv) -> InvocationId {
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);
        env.apply_multiple([
            commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                invocation_target,
                completion_retention_duration: Duration::from_secs(60),
                journal_retention_duration: Duration::from_secs(60),
                ..ServiceInvocation::mock()
            }),
            pinned_deployment(invocation_id, ServiceProtocolVersion::V3),
        ])
        .await;
        invocation_id
    }

    async fn retire(env: &mut TestEnv) -> anyhow::Result<JournalV1Retirement> {
        let partition_db = env.storage.partition_db().clone();
        let mut txn = env.storage.transaction();
        let retirement = StateMachine::retire_journal_v1(
            &mut env.processor,
            &mut txn,
            &partition_db,
            MillisSinceEpoch::now(),
            Lsn::new(2),
        )
        .await?;
        Ok(retirement)
    }

    fn has_journal_v1_entries(env: &TestEnv) -> anyhow::Result<bool> {
        Ok(InvocationsWithJournal::new(env.storage.partition_db())?
            .next()
            .transpose()?
            .is_some())
    }

    #[test(restate_core::test)]
    async fn retires_journal_v1() -> anyhow::Result<()> {
        // Without journal v2 as default, new invocations store their input in journal v1, and
        // keep their journal there if they get pinned to a deployment older than V4.
        let mut env = TestEnv::create().await;
        let pinned_to_v3 = mock_start_invocation_with_invocation_target(
            &mut env,
            InvocationTarget::mock_service(),
        )
        .await;
        env.apply(pinned_deployment(pinned_to_v3, ServiceProtocolVersion::V3))
            .await;
        let unpinned = mock_start_invocation_with_invocation_target(
            &mut env,
            InvocationTarget::mock_service(),
        )
        .await;
        let retained = start_retained_on_journal_v1(&mut env).await;

        let completed = InvocationId::mock_random();
        let mut completed_status = CompletedInvocation::mock_neo();
        completed_status.journal_metadata.length = 1;
        completed_status.pinned_deployment = Some(PinnedDeployment {
            deployment_id: DeploymentId::default(),
            service_protocol_version: ServiceProtocolVersion::V3,
        });
        let mut txn = env.storage.transaction();
        txn.put_invocation_status(&completed, &InvocationStatus::Completed(completed_status))?;
        txn.put_journal_entry(&completed, 0, &completed_invoke_entry(completed))?;
        txn.commit().await?;

        // An invocation that stores its journal in journal v2 isn't touched. Journal v1 entries
        // of an invocation that no longer exists are deleted.
        env.set_enabled_features(PersistedFeatures {
            journal_v2: true,
            ..PersistedFeatures::default()
        });
        let on_journal_v2 = mock_start_invocation_with_invocation_target(
            &mut env,
            InvocationTarget::mock_service(),
        )
        .await;
        env.apply(pinned_deployment(on_journal_v2, ServiceProtocolVersion::V4))
            .await;
        let unknown = InvocationId::mock_random();
        let mut txn = env.storage.transaction();
        txn.put_journal_entry(
            &unknown,
            3,
            &JournalEntry::Completion(CompletionResult::Empty),
        )?;
        txn.commit().await?;

        assert_eq!(
            retire(&mut env).await?,
            JournalV1Retirement {
                killed: 3,
                journals_dropped: 1,
            }
        );
        // Without completion retention, killed invocations are removed.
        for killed in [pinned_to_v3, unpinned] {
            assert_that!(
                env.storage.get_invocation_status(&killed).await?,
                pat!(InvocationStatus::Free)
            );
        }
        for without_journal in [retained, completed] {
            let InvocationStatus::Completed(status) =
                env.storage.get_invocation_status(&without_journal).await?
            else {
                panic!("the invocation is retained");
            };
            assert_eq!(status.journal_metadata.length, 0);
        }
        assert_that!(
            env.storage.get_invocation_status(&on_journal_v2).await?,
            pat!(InvocationStatus::Invoked(_))
        );
        assert!(
            journal_table_v2::ReadJournalTable::get_journal_entry(
                &mut env.storage,
                on_journal_v2,
                0
            )
            .await?
            .is_some()
        );
        assert!(!has_journal_v1_entries(&env)?);

        // Retiring again changes nothing.
        assert_eq!(retire(&mut env).await?, JournalV1Retirement::default());

        // Once journal v1 is retired, new invocations store their journal in journal v2.
        env.set_enabled_features(PersistedFeatures {
            journal_v1_retired: true,
            ..PersistedFeatures::default()
        });
        let started_after = mock_start_invocation_with_invocation_target(
            &mut env,
            InvocationTarget::mock_service(),
        )
        .await;
        assert!(
            journal_table_v2::ReadJournalTable::get_journal_entry(
                &mut env.storage,
                started_after,
                0
            )
            .await?
            .is_some()
        );
        assert!(!has_journal_v1_entries(&env)?);

        env.shutdown().await;
        Ok(())
    }
}
