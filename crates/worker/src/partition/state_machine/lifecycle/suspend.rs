// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use tracing::{debug, trace};

use restate_clock::UniqueTimestamp;
use restate_storage_api::invocation_status_table::{InvocationStatus, WriteInvocationStatusTable};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table_v2::ReadJournalTable;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::vqueue_table::{EntryStatusHeader, ReadVQueueTable, WriteVQueueTable};
use restate_types::config::Configuration;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{Event, SuspendedEvent};
use restate_types::journal_v2::UnresolvedFuture;
use restate_types::vqueues::EntryId;
use restate_vqueues::VQueue;

use crate::partition::state_machine::lifecycle::event::ApplyEventCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnSuspendCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub awaiting_on: UnresolvedFuture,
    /// When true, append a [`SuspendedEvent`] to the journal events.
    pub emit_event: bool,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnSuspendCommand
where
    S: ReadJournalTable
        + WriteInvocationStatusTable
        + WriteJournalEventsTable
        + WriteVQueueTable
        + ReadVQueueTable
        + WriteLockTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        debug_assert!(
            !self.awaiting_on.is_empty(),
            "Expecting at least one entry on which the invocation {} is waiting.",
            self.invocation_id
        );

        // todo: Improve mechanical empathy by passing down the "waiting" set
        // down to the rocksdb iterator so that we can stop iterating as soon as we find
        // any match.

        let notifications: HashMap<_, _> = ctx
            .storage
            .get_notifications_index(self.invocation_id)
            .await?
            .into_iter()
            .map(|(notification_id, index)| (notification_id, index.result_variant))
            .collect();

        let mut invocation_status = self.invocation_status;
        debug!(
            "Try resolving future {:?} with all available notifications",
            self.awaiting_on
        );
        if self.awaiting_on.resolve(&notifications) {
            trace!(
                "Resuming instead of suspending service because a notification is already available."
            );
            super::ResumeInvocationCommand {
                invocation_id: self.invocation_id,
                invocation_status: &mut invocation_status,
            }
            .apply(ctx)
            .await?;
        } else {
            // --- Let's transition to suspended

            if self.emit_event {
                ApplyEventCommand {
                    invocation_id: self.invocation_id,
                    invocation_status: &invocation_status,
                    event: RawEvent::from(Event::Suspended(SuspendedEvent {
                        awaiting_on: self.awaiting_on.clone(),
                    })),
                }
                .apply(ctx)
                .await?;
            }

            let mut in_flight_invocation_metadata = invocation_status
                .into_invocation_metadata()
                .expect("Must be present unless status is killed or invoked");

            trace!(
                "Suspending invocation waiting future {:?}",
                self.awaiting_on
            );

            in_flight_invocation_metadata
                .timestamps
                .update(ctx.record_created_at);

            if Configuration::pinned().common.experimental_enable_vqueues {
                let now = UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);
                let entry_id = EntryId::from(&self.invocation_id);
                let Some(header) = ctx
                    .storage
                    .get_vqueue_entry_status(self.invocation_id.partition_key(), &entry_id)
                    .await?
                else {
                    // todo resolve once we decided on the actual migration strategy
                    panic!(
                        "Trying to suspend invocation {} which does not exist as a vqueue entry. Have you forgotten to migrate from the old inbox to vqueues?",
                        self.invocation_id
                    );
                };

                VQueue::get(
                    header.vqueue_id(),
                    ctx.storage,
                    ctx.vqueues_cache,
                    ctx.is_leader.then_some(ctx.action_collector),
                )
                .await?
                .expect("suspending in a non-existent vqueue")
                .suspend_entry(now, &header);
            }

            invocation_status = InvocationStatus::Suspended {
                metadata: in_flight_invocation_metadata,
                awaiting_on: self.awaiting_on,
            };
        }

        // Store invocation status
        ctx.storage
            .put_invocation_status(&self.invocation_id, &invocation_status)
            .map_err(Error::Storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::fixtures::{
        invoker_entry_effect, invoker_suspended,
    };
    use crate::partition::state_machine::tests::matchers::storage::{
        has_commands, has_journal_length, in_flight_metadata, is_variant,
    };
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use assert2::let_assert;
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_types::identifiers::InvocationId;
    use restate_types::invocation::NotifySignalRequest;
    use restate_types::journal_v2::{
        CommandType, Entry, EntryMetadata, EntryType, Failure, NotificationId, Signal, SignalId,
        SignalResult, SleepCommand, SleepCompletion, UnresolvedFuture, all_completed,
        all_succeeded_or_first_failed, first_completed, first_succeeded_or_all_failed, unknown,
    };
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{RESTATE_VERSION_1_6_0, SemanticRestateVersion};
    use restate_wal_protocol::Command;
    use restate_wal_protocol::timer::TimerKeyValue;
    use std::time::{Duration, SystemTime};
    use tracing::info;

    #[restate_core::test]
    async fn sleep_then_suspend_then_resume() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let completion_id = 1;
        let wake_up_time: MillisSinceEpoch = (SystemTime::now() + Duration::from_secs(60)).into();

        let sleep_command = SleepCommand {
            wake_up_time,
            name: Default::default(),
            completion_id,
        };
        let timer_key_value =
            TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, completion_id);
        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(invocation_id, sleep_command.clone()),
                invoker_suspended(invocation_id, NotificationId::for_completion(completion_id)),
            ])
            .await;
        assert_that!(
            actions,
            contains(pat!(Action::RegisterTimer {
                timer_value: eq(timer_key_value.clone())
            }))
        );

        let actions = test_env.apply(Command::Timer(timer_key_value)).await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id(invocation_id))
        );

        // Check journal
        let sleep_completion = SleepCompletion { completion_id };
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(sleep_command),
                matchers::entry_eq(sleep_completion),
            ]
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn suspend_with_already_completed_notifications() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let completion_id = 1;
        let wake_up_time: MillisSinceEpoch = (SystemTime::now() + Duration::from_secs(60)).into();

        let sleep_command = SleepCommand {
            wake_up_time,
            name: Default::default(),
            completion_id,
        };
        let sleep_completion = SleepCompletion { completion_id };
        let timer_key_value =
            TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, completion_id);
        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(invocation_id, sleep_command.clone()),
                Command::Timer(timer_key_value.clone()),
            ])
            .await;
        assert_that!(
            actions,
            all![
                contains(pat!(Action::RegisterTimer {
                    timer_value: eq(timer_key_value)
                })),
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    2,
                    NotificationId::CompletionId(completion_id),
                ))
            ]
        );

        let actions = test_env
            .apply(invoker_suspended(
                invocation_id,
                NotificationId::for_completion(completion_id),
            ))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id(invocation_id))
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(sleep_command),
                matchers::entry_eq(sleep_completion),
            ]
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn suspend_waiting_on_signal() {
        run_suspend_waiting_on_signal(SemanticRestateVersion::unknown()).await;
    }

    #[restate_core::test]
    async fn suspend_waiting_on_signal_journal_v2_enabled() {
        run_suspend_waiting_on_signal(RESTATE_VERSION_1_6_0.clone()).await;
    }

    async fn run_suspend_waiting_on_signal(min_restate_version: SemanticRestateVersion) {
        let mut test_env = TestEnv::create_with_min_restate_version(min_restate_version).await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        // We don't pin the deployment here, but this should work nevertheless.

        let _ = test_env
            .apply(invoker_suspended(
                invocation_id,
                NotificationId::for_signal(SignalId::for_index(17)),
            ))
            .await;

        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Suspended),
                has_journal_length(1),
                in_flight_metadata(pat!(InFlightInvocationMetadata {
                    pinned_deployment: none()
                })),
            ))
        );

        // Let's notify the signal
        let signal = Signal {
            id: SignalId::for_index(17),
            result: SignalResult::Void,
        };
        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: signal.clone(),
            }))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id(invocation_id))
        );

        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                has_journal_length(2),
                has_commands(1),
                in_flight_metadata(pat!(InFlightInvocationMetadata {
                    pinned_deployment: none()
                })),
            ))
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 2).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(signal),
            ]
        );

        test_env.shutdown().await;
    }

    struct SuspendTest {
        test_env: TestEnv,
        invocation_id: InvocationId,
    }

    impl SuspendTest {
        async fn given_suspended_on(fut: UnresolvedFuture) -> Self {
            let mut test_env = TestEnv::create().await;
            let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
            fixtures::mock_pinned_deployment_v7(&mut test_env, invocation_id).await;

            let _ = test_env
                .apply(invoker_suspended(invocation_id, fut.clone()))
                .await;

            Self {
                test_env,
                invocation_id,
            }
            .assert_suspended_on(fut)
            .await
        }

        async fn when_signal_notified_then_suspends(
            mut self,
            signal_id: SignalId,
            signal_result: SignalResult,
            remaining: UnresolvedFuture,
        ) -> Self {
            let actions = self
                .test_env
                .apply(Command::NotifySignal(NotifySignalRequest {
                    invocation_id: self.invocation_id,
                    signal: Signal {
                        id: signal_id,
                        result: signal_result,
                    },
                }))
                .await;
            self.log_invocation_status().await;

            assert_that!(
                actions,
                not(contains(matchers::actions::invoke_for_id(
                    self.invocation_id
                )))
            );
            assert_that!(
                self.test_env
                    .storage()
                    .get_invocation_status(&self.invocation_id)
                    .await,
                ok(is_variant(InvocationStatusDiscriminants::Suspended))
            );
            self.assert_suspended_on(remaining).await
        }

        async fn when_signal_notified_then_resumes(
            mut self,
            signal_id: SignalId,
            signal_result: SignalResult,
        ) -> Self {
            let actions = self
                .test_env
                .apply(Command::NotifySignal(NotifySignalRequest {
                    invocation_id: self.invocation_id,
                    signal: Signal {
                        id: signal_id,
                        result: signal_result,
                    },
                }))
                .await;
            self.log_invocation_status().await;

            assert_that!(
                actions,
                contains(matchers::actions::invoke_for_id(self.invocation_id))
            );
            assert_that!(
                self.test_env
                    .storage()
                    .get_invocation_status(&self.invocation_id)
                    .await,
                ok(is_variant(InvocationStatusDiscriminants::Invoked))
            );
            self
        }

        async fn end(self) {
            self.test_env.shutdown().await;
        }

        async fn log_invocation_status(&mut self) {
            let inv_status = self
                .test_env
                .storage()
                .get_invocation_status(&self.invocation_id)
                .await
                .unwrap();

            info!(
                "Invocation status for {}: {:#?}",
                self.invocation_id, inv_status
            )
        }

        async fn assert_suspended_on(mut self, expected_awaiting_on: UnresolvedFuture) -> Self {
            let invocation_status = self
                .test_env
                .storage
                .get_invocation_status(&self.invocation_id)
                .await
                .unwrap();

            let_assert!(InvocationStatus::Suspended { awaiting_on, .. } = invocation_status);
            assert_eq!(awaiting_on, expected_awaiting_on);

            self
        }
    }

    // Indices <= 16 are reserved for Built-in Signals.
    const A: SignalId = SignalId::for_index(17);
    const B: SignalId = SignalId::for_index(18);
    const C: SignalId = SignalId::for_index(19);
    const D: SignalId = SignalId::for_index(20);

    const SUCCESS: SignalResult = SignalResult::Void;
    const FAILURE: SignalResult = SignalResult::Failure(Failure::mock());

    #[restate_core::test]
    async fn asff_of_first_completed_resumes_when_all_succeed() {
        SuspendTest::given_suspended_on(all_succeeded_or_first_failed!(
            first_completed!(A, B),
            first_completed!(C, D)
        ))
        .await
        .when_signal_notified_then_suspends(
            B,
            SUCCESS,
            all_succeeded_or_first_failed!(first_completed!(C, D)),
        )
        .await
        .when_signal_notified_then_resumes(C, SUCCESS)
        .await
        .end()
        .await;
    }

    #[restate_core::test]
    async fn asff_of_first_completed_short_circuits_on_failure() {
        SuspendTest::given_suspended_on(all_succeeded_or_first_failed!(
            first_completed!(A, B),
            first_completed!(C, D)
        ))
        .await
        .when_signal_notified_then_resumes(B, FAILURE)
        .await
        .end()
        .await;
    }

    #[restate_core::test]
    async fn first_completed_of_asff_resumes_when_any_inner_asff_succeeds() {
        SuspendTest::given_suspended_on(first_completed!(
            all_succeeded_or_first_failed!(A, B),
            all_succeeded_or_first_failed!(C, D),
        ))
        .await
        .when_signal_notified_then_suspends(
            A,
            SUCCESS,
            first_completed!(
                all_succeeded_or_first_failed!(B),
                all_succeeded_or_first_failed!(C, D),
            ),
        )
        .await
        .when_signal_notified_then_suspends(
            C,
            SUCCESS,
            first_completed!(
                all_succeeded_or_first_failed!(B),
                all_succeeded_or_first_failed!(D),
            ),
        )
        .await
        .when_signal_notified_then_resumes(B, SUCCESS)
        .await
        .end()
        .await;
    }

    #[restate_core::test]
    async fn fsaf_of_asff_resumes_when_one_inner_asff_succeeds_after_other_failed() {
        SuspendTest::given_suspended_on(first_succeeded_or_all_failed!(
            all_succeeded_or_first_failed!(A, B),
            all_succeeded_or_first_failed!(C, D),
        ))
        .await
        // sig(A) fails
        //  -> inner all_succeeded_or_first_failed(A, B) short-circuits to failure
        //  -> outer first_succeeded_or_all_failed evicts it but still has all_succeeded_or_first_failed(C, D) pending.
        .when_signal_notified_then_suspends(
            A,
            FAILURE,
            first_succeeded_or_all_failed!(all_succeeded_or_first_failed!(C, D)),
        )
        .await
        .when_signal_notified_then_suspends(
            C,
            SUCCESS,
            first_succeeded_or_all_failed!(all_succeeded_or_first_failed!(D)),
        )
        .await
        .when_signal_notified_then_resumes(D, SUCCESS)
        .await
        .end()
        .await;
    }

    #[restate_core::test]
    async fn unknown_of_all_completed_resumes_when_first_inner_all_completes() {
        SuspendTest::given_suspended_on(unknown!(all_completed!(A, B), all_completed!(C, D),))
            .await
            // sig(A) completes
            //  -> we don't wake up yet
            .when_signal_notified_then_suspends(
                A,
                FAILURE,
                unknown!(all_completed!(B), all_completed!(C, D)),
            )
            .await
            .when_signal_notified_then_resumes(B, SUCCESS)
            .await
            .end()
            .await;
    }

    #[restate_core::test]
    async fn asff_of_all_completed_resumes_when_all_inner_complete_regardless_of_status() {
        SuspendTest::given_suspended_on(all_succeeded_or_first_failed!(
            all_completed!(A, B),
            all_completed!(C, D)
        ))
        .await
        .when_signal_notified_then_suspends(
            A,
            FAILURE,
            all_succeeded_or_first_failed!(all_completed!(B), all_completed!(C, D)),
        )
        .await
        .when_signal_notified_then_suspends(
            B,
            FAILURE,
            all_succeeded_or_first_failed!(all_completed!(C, D)),
        )
        .await
        .when_signal_notified_then_suspends(
            C,
            FAILURE,
            all_succeeded_or_first_failed!(all_completed!(D)),
        )
        .await
        .when_signal_notified_then_resumes(D, SUCCESS)
        .await
        .end()
        .await;
    }

    #[restate_core::test]
    async fn asff_short_circuits_on_unknown_child_completion() {
        SuspendTest::given_suspended_on(all_succeeded_or_first_failed!(unknown!(A), unknown!(B)))
            .await
            .when_signal_notified_then_resumes(A, SUCCESS)
            .await
            .end()
            .await;
    }

    #[restate_core::test]
    async fn all_completed_of_unknowns_resumes_when_all_inner_complete() {
        SuspendTest::given_suspended_on(all_completed!(unknown!(A), unknown!(B), unknown!(C)))
            .await
            .when_signal_notified_then_suspends(
                C,
                FAILURE,
                all_completed!(unknown!(A), unknown!(B)),
            )
            .await
            .when_signal_notified_then_suspends(A, SUCCESS, all_completed!(unknown!(B)))
            .await
            .when_signal_notified_then_resumes(B, SUCCESS)
            .await
            .end()
            .await;
    }

    #[restate_core::test]
    async fn suspend_v3_records_suspended_event() {
        use restate_types::journal_events::SuspendedEvent;

        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v7(&mut test_env, invocation_id).await;

        // V3 suspension on a non-trivial future records a SuspendedEvent that
        // mirrors the awaiting_on tree stored on the invocation status.
        let awaiting_on: UnresolvedFuture = first_completed!(A, B);
        let _ = test_env
            .apply(invoker_suspended(invocation_id, awaiting_on.clone()))
            .await;

        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(is_variant(InvocationStatusDiscriminants::Suspended))
        );
        assert_that!(
            test_env.read_journal_events(invocation_id).await,
            elements_are![eq(Event::Suspended(SuspendedEvent {
                awaiting_on: awaiting_on.clone()
            }))]
        );

        // Resuming via signal must not emit another SuspendedEvent — the resume
        // short-circuit branch in OnSuspendCommand skips the append. We provoke
        // this by making the invoker re-suspend on a notification that's
        // already available, so resolve() short-circuits.
        let _ = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: Signal {
                    id: A,
                    result: SUCCESS,
                },
            }))
            .await;
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(is_variant(InvocationStatusDiscriminants::Invoked))
        );
        let _ = test_env
            .apply(invoker_suspended(
                invocation_id,
                NotificationId::for_signal(A),
            ))
            .await;
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(is_variant(InvocationStatusDiscriminants::Invoked))
        );
        // Still exactly the one event from the first transition.
        assert_that!(
            test_env.read_journal_events(invocation_id).await,
            elements_are![eq(Event::Suspended(SuspendedEvent { awaiting_on }))]
        );

        test_env.shutdown().await;
    }
}
