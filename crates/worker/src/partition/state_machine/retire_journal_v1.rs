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

use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::journal_table::{self as journal_table_v1, RetireJournalTable};
use restate_storage_api::{Transaction, journal_table_v2};
use restate_types::identifiers::InvocationId;
use restate_types::logs::Lsn;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::time::MillisSinceEpoch;

use super::{ActionCollector, CommandHandler, Error, StateMachine, StateMachineApplyContext};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::lifecycle::{
    OnPurgeJournalCommand, VerifyOrMigrateJournalTableToV2Command,
};

/// The retirement commits whenever its transaction has grown to this size (1 MiB). Tests commit
/// after every invocation.
const MAX_TRANSACTION_SIZE: usize = if cfg!(test) { 1 } else { 1024 * 1024 };

/// What retiring journal v1 changed on a partition.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JournalV1Retirement {
    /// Invocations whose input moved from journal v1 to journal v2.
    pub migrated: usize,
    /// Invocations killed because their journal was stored in journal v1.
    pub killed: usize,
    /// Completed invocations whose journal v1 was dropped.
    pub journals_dropped: usize,
}

impl StateMachine {
    /// Retires journal v1 by visiting every invocation that has entries in the journal v1 table:
    /// * invocations that aren't pinned to a deployment yet keep running: their input, the only
    ///   entry of their journal, moves to journal v2;
    /// * other invocations that haven't completed and whose journal is stored in journal v1 are
    ///   killed, and lose their journal even if they retain it;
    /// * completed invocations whose journal is stored in journal v1 lose their journal;
    /// * all other journal v1 entries, which no journal accounts for, are deleted.
    ///
    /// Afterwards, the journal v1 table of the partition is empty.
    ///
    /// The changes are written to `txn`, which is committed in parts so that it doesn't grow
    /// without bound. Committing early is safe: every invocation is retired within one commit, and
    /// a retired invocation has no journal v1 entries left. If the retirement doesn't finish, it
    /// retires the rest when it runs again.
    ///
    /// The actions it causes, such as responses to the ingress, are discarded: like every replica,
    /// the leader runs the retirement while it isn't leading yet.
    pub async fn retire_journal_v1<T>(
        processor: impl ProcessorContext,
        txn: &mut T,
        record_created_at: MillisSinceEpoch,
        record_lsn: Lsn,
    ) -> Result<JournalV1Retirement, Error>
    where
        T: Transaction + RetireJournalTable,
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

        let mut retirement = JournalV1Retirement::default();
        let mut last_retired = None;
        while let Some(invocation_id) = ctx
            .storage
            .next_invocation_with_journal(last_retired.as_ref())?
        {
            // Allow tokio to cancel this task if the processor is being cancelled.
            tokio::task::consume_budget().await;

            ctx.retire_journal_v1_of(invocation_id, &mut retirement)
                .await?;
            ctx.action_collector.clear();
            last_retired = Some(invocation_id);

            if ctx.storage.estimated_size_in_bytes() >= MAX_TRANSACTION_SIZE {
                ctx.storage.commit().await?;
            }
        }
        Ok(retirement)
    }
}

impl<S, P> StateMachineApplyContext<'_, S, P>
where
    S: Transaction + RetireJournalTable,
    P: ProcessorContext,
{
    async fn retire_journal_v1_of(
        &mut self,
        invocation_id: InvocationId,
        retirement: &mut JournalV1Retirement,
    ) -> Result<(), Error> {
        let mut status = self.get_invocation_status(&invocation_id).await?;
        if let Some(metadata) = status.get_invocation_metadata_mut()
            && metadata.pinned_deployment.is_none()
            && metadata.journal_metadata.length == 1
            && matches!(
                journal_table_v1::ReadJournalTable::get_journal_entry(
                    self.storage,
                    &invocation_id,
                    0
                )
                .await?,
                Some(journal_table_v1::JournalEntry::Entry(_))
            )
        {
            // The input moves to journal v2, as it would once the invocation is pinned to a
            // deployment.
            VerifyOrMigrateJournalTableToV2Command {
                invocation_id,
                metadata,
            }
            .apply(&mut *self)
            .await?;
            self.storage
                .put_invocation_status(&invocation_id, &status)?;
            retirement.migrated += 1;
        } else if self.stores_journal_in_v1(&invocation_id, &status).await? {
            if matches!(status, InvocationStatus::Completed(_)) {
                retirement.journals_dropped += 1;
            } else {
                debug!(%invocation_id, "Killing invocation whose journal is stored in journal v1");
                self.on_kill_invocation(invocation_id, None).await?;
                retirement.killed += 1;
            }
            // A killed invocation that is retained keeps its journal, like a completed one.
            OnPurgeJournalCommand {
                invocation_id: &invocation_id,
                response_sink: None,
                is_cleaner_purge: false,
            }
            .apply(&mut *self)
            .await?;
        }

        // What is left are entries that no journal accounts for, such as completions that never
        // found their entry.
        self.storage.delete_all_journal_entries(&invocation_id)?;
        Ok(())
    }

    /// Whether the invocation has a journal and stores it in journal v1.
    async fn stores_journal_in_v1(
        &mut self,
        invocation_id: &InvocationId,
        status: &InvocationStatus,
    ) -> Result<bool, Error> {
        let (journal_length, pinned_deployment) = match status {
            InvocationStatus::Invoked(metadata)
            | InvocationStatus::Suspended { metadata, .. }
            | InvocationStatus::Paused(metadata) => (
                metadata.journal_metadata.length,
                metadata.pinned_deployment.as_ref(),
            ),
            InvocationStatus::Completed(completed) => (
                completed.journal_metadata.length,
                completed.pinned_deployment.as_ref(),
            ),
            // Invocations that haven't started yet have no journal, or one in journal v2.
            InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_) => return Ok(false),
            InvocationStatus::Free => return Ok(false),
        };
        if journal_length == 0 {
            return Ok(false);
        }
        Ok(match pinned_deployment {
            // Deployments older than service protocol V4 store their journal in journal v1.
            Some(pinned_deployment) => {
                pinned_deployment.service_protocol_version < ServiceProtocolVersion::V4
            }
            // An unpinned invocation stores its input as the first entry, in journal v1 or v2.
            // The ones that haven't completed have been moved to journal v2 already.
            None => journal_table_v2::ReadJournalTable::get_journal_entry(
                self.storage,
                *invocation_id,
                0,
            )
            .await?
            .is_none(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use googletest::prelude::{assert_that, pat};
    use test_log::test;

    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InvocationStatus, ReadInvocationStatusTable,
        WriteInvocationStatusTable,
    };
    use restate_storage_api::journal_table::{JournalEntry, RetireJournalTable, WriteJournalTable};
    use restate_storage_api::{Transaction, journal_table_v2};
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::{DeploymentId, InvocationId};
    use restate_types::invocation::{
        InvocationResponse, InvocationTarget, JournalCompletionTarget, ResponseResult,
        ServiceInvocation,
    };
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
        let mut txn = env.storage.transaction();
        let retirement = StateMachine::retire_journal_v1(
            &mut env.processor,
            &mut txn,
            MillisSinceEpoch::now(),
            Lsn::new(2),
        )
        .await?;
        txn.commit().await?;
        Ok(retirement)
    }

    #[test(restate_core::test)]
    async fn retires_journal_v1() -> anyhow::Result<()> {
        // Without journal v2 as default, new invocations store their journal in journal v1.
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

        // An invocation started with journal v2 as default isn't touched, but loses the journal
        // v1 entries it has, like an invocation that no longer exists.
        env.set_enabled_features(PersistedFeatures {
            journal_v2: true,
            ..PersistedFeatures::default()
        });
        let on_journal_v2 = mock_start_invocation_with_invocation_target(
            &mut env,
            InvocationTarget::mock_service(),
        )
        .await;
        let unknown = InvocationId::mock_random();
        let mut txn = env.storage.transaction();
        for invocation_id in [on_journal_v2, unknown] {
            txn.put_journal_entry(
                &invocation_id,
                3,
                &JournalEntry::Completion(CompletionResult::Empty),
            )?;
        }
        txn.commit().await?;

        assert_eq!(
            retire(&mut env).await?,
            JournalV1Retirement {
                migrated: 1,
                killed: 2,
                journals_dropped: 1,
            }
        );
        assert_that!(
            env.storage.get_invocation_status(&pinned_to_v3).await?,
            pat!(InvocationStatus::Free)
        );
        for without_journal in [retained, completed] {
            let InvocationStatus::Completed(status) =
                env.storage.get_invocation_status(&without_journal).await?
            else {
                panic!("the invocation is retained");
            };
            assert_eq!(status.journal_metadata.length, 0);
        }
        // The unpinned invocation keeps running with its input in journal v2, and can be pinned
        // to a deployment.
        assert!(
            journal_table_v2::ReadJournalTable::get_journal_entry(&mut env.storage, unpinned, 0)
                .await?
                .is_some()
        );
        env.apply(pinned_deployment(unpinned, ServiceProtocolVersion::V4))
            .await;
        for running in [unpinned, on_journal_v2] {
            assert_that!(
                env.storage.get_invocation_status(&running).await?,
                pat!(InvocationStatus::Invoked(_))
            );
        }
        assert_eq!(
            env.storage
                .transaction()
                .next_invocation_with_journal(None)?,
            None
        );

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
        // A completion for an invocation that isn't pinned to a deployment yet has no entry to
        // complete. It is dropped rather than stored in journal v1.
        env.apply(commands::InvocationResponseCommand::test_envelope(
            InvocationResponse {
                target: JournalCompletionTarget::from_parts(started_after, 3),
                result: ResponseResult::Success(Bytes::new()),
            },
        ))
        .await;
        assert_that!(
            env.storage.get_invocation_status(&started_after).await?,
            pat!(InvocationStatus::Invoked(_))
        );
        assert_eq!(
            env.storage
                .transaction()
                .next_invocation_with_journal(None)?,
            None
        );

        env.shutdown().await;
        Ok(())
    }
}
