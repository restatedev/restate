// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The partition processor side of the explicit recovery protocol, through the state machine.

use std::num::NonZeroU32;
use std::time::Duration;

use bytes::Bytes;
use googletest::prelude::*;

use restate_storage_api::explicit_recovery_table::ReadExplicitRecoveryTable;
use restate_storage_api::invocation_status_table::{
    ExplicitRecoveryStatus, InvocationStatus, ReadInvocationStatusTable,
};
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::state_table::ReadStateTable;
use restate_storage_api::vqueue_table::{EntryStatusHeader, ReadVQueueTable, Stage, Status};
use restate_types::errors::KILLED_INVOCATION_ERROR;
use restate_types::identifiers::{
    BaseEntryId, DeploymentId, InvocationId, PartitionProcessorRpcRequestId,
};
use restate_types::invocation::client::InvocationOutputResponse;
use restate_types::invocation::{
    InvocationResponse, InvocationTarget, InvocationTermination, JournalCompletionTarget,
    NotifySignalRequest, PurgeInvocationRequest, ResponseResult, ResumeInvocationRequest,
    ServiceInvocation, ServiceInvocationResponseSink, Source, TerminationFlavor,
};
use restate_types::journal_events::{AttemptEndedEvent, Event};
use restate_types::journal_v2::{BuiltInSignal, CallRequest, Signal, SignalId, SignalResult};
use restate_types::partitions::{PartitionFeatureChange, PersistedFeatures};
use restate_types::service_protocol::ProtocolFamily;
use restate_wal_protocol::v2::{Command, commands};
use restate_worker_api::invoker::explicit_recovery::{
    ErAction, ErEffect, ErEffectKind, ErFailureOutcome, ErStateScope, ErToInvoker, RestartRule,
};

use super::{TestEnv, run_decision};
use crate::partition::state_machine::{Action, RpcReply};

struct ErInvocation {
    id: InvocationId,
    target: InvocationTarget,
    request_id: PartitionProcessorRpcRequestId,
}

async fn test_env() -> TestEnv {
    TestEnv::create_with_features(PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableJournalV2,
        PartitionFeatureChange::EnableVqueues,
    ]))
    .await
}

/// Invokes an exclusive handler of a virtual object on the family, from the ingress.
async fn invoke(test_env: &mut TestEnv, journal_retention: Duration) -> ErInvocation {
    let target = InvocationTarget::mock_virtual_object();
    let id = InvocationId::mock_generate(&target);
    let request_id = PartitionProcessorRpcRequestId::new();
    test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            argument: Bytes::from_static(b"input"),
            response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
            completion_retention_duration: Duration::from_secs(3600),
            journal_retention_duration: journal_retention,
            protocol_family: ProtocolFamily::ExplicitRecovery,
            ..ServiceInvocation::initialize(id, target.clone(), Source::Ingress(request_id))
        }))
        .await;
    ErInvocation {
        id,
        target,
        request_id,
    }
}

/// Applies the scheduler's decision to run the invocation's entry.
async fn run(test_env: &mut TestEnv, id: InvocationId) -> Vec<Action> {
    let header = test_env
        .storage()
        .transaction()
        .get_vqueue_entry_status(&BaseEntryId::from(id))
        .await
        .unwrap()
        .expect("entry exists");
    assert_eq!(
        header.stage(),
        Stage::Inbox,
        "only an entry in the inbox runs"
    );
    test_env
        .apply(run_decision(header.vqueue_id(), *header.entry_key()))
        .await
}

fn effect(id: InvocationId, attempt: u32, seq: u64, kind: ErEffectKind) -> ErEffect {
    ErEffect {
        invocation_id: id,
        attempt,
        seq,
        kind,
    }
}

async fn apply_effect(test_env: &mut TestEnv, effect: ErEffect) -> Vec<Action> {
    test_env
        .apply(commands::ExplicitRecoveryEffectCommand::test_envelope(
            effect,
        ))
        .await
}

fn action(action: ErAction, highest_result_id: u32) -> ErEffectKind {
    ErEffectKind::Action {
        action,
        highest_result_id: NonZeroU32::new(highest_result_id),
    }
}

fn set_state(scope: ErStateScope, key: &'static str, value: &'static str) -> ErAction {
    ErAction::SetState {
        scope,
        key: Bytes::from_static(key.as_bytes()),
        value: Bytes::from_static(value.as_bytes()),
    }
}

fn retry_fallback() -> ErFailureOutcome {
    ErFailureOutcome::Retry {
        resume_at: None,
        retry_attempts: 1,
    }
}

fn suspend(received_notifications: u32) -> ErEffectKind {
    ErEffectKind::Suspend {
        received_notifications,
        idle_fallback: retry_fallback(),
    }
}

async fn status(test_env: &mut TestEnv, id: InvocationId) -> InvocationStatus {
    test_env.storage().get_invocation_status(&id).await.unwrap()
}

async fn er_status(test_env: &mut TestEnv, id: InvocationId) -> ExplicitRecoveryStatus {
    status(test_env, id)
        .await
        .explicit_recovery()
        .cloned()
        .expect("explicit recovery status")
}

async fn stage_and_status(test_env: &mut TestEnv, id: InvocationId) -> (Stage, Status, u32) {
    let header = test_env
        .storage()
        .transaction()
        .get_vqueue_entry_status(&BaseEntryId::from(id))
        .await
        .unwrap()
        .expect("entry exists");
    (
        header.stage(),
        header.status(),
        header.metadata().retry_attempts,
    )
}

fn forwards(actions: &[Action]) -> Vec<ErToInvoker> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::ForwardExplicitRecovery { message, .. } => Some(message.clone()),
            _ => None,
        })
        .collect()
}

fn response(
    id: InvocationId,
    result_id: u32,
) -> restate_wal_protocol::v2::Envelope<restate_wal_protocol::v2::Raw> {
    commands::InvocationResponseCommand::test_envelope(InvocationResponse {
        target: JournalCompletionTarget::from_parts(id, result_id),
        result: ResponseResult::Success(Bytes::from_static(b"answer")),
    })
}

fn named_signal(
    id: InvocationId,
    name: &str,
) -> restate_wal_protocol::v2::Envelope<restate_wal_protocol::v2::Raw> {
    commands::NotifySignalCommand::test_envelope(NotifySignalRequest {
        invocation_id: id,
        signal: Signal::new(
            SignalId::for_name(name.into()),
            SignalResult::Success(Bytes::from_static(b"payload")),
        ),
    })
}

fn cancel(id: InvocationId) -> restate_wal_protocol::v2::Envelope<restate_wal_protocol::v2::Raw> {
    commands::NotifySignalCommand::test_envelope(NotifySignalRequest {
        invocation_id: id,
        signal: Signal::new(
            SignalId::for_builtin_signal(BuiltInSignal::Cancel),
            SignalResult::Void,
        ),
    })
}

#[restate_core::test]
async fn attempt_actions_and_fencing() {
    let mut test_env = test_env().await;
    let inv = invoke(&mut test_env, Duration::ZERO).await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Inboxed(_))
    );
    assert_eq!(er_status(&mut test_env, inv.id).await.attempt, 0);

    // Run: the first attempt.
    let actions = run(&mut test_env, inv.id).await;
    assert_that!(
        actions,
        contains(pat!(Action::VQInvoke {
            explicit_recovery_attempt: eq(NonZeroU32::new(1))
        }))
    );
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Invoked(_))
    );

    // The attempt starts: the deployment is pinned, and the effect is acked.
    let deployment_id = DeploymentId::new();
    let actions = apply_effect(
        &mut test_env,
        effect(
            inv.id,
            1,
            1,
            ErEffectKind::AttemptStarted {
                deployment_id,
                protocol_version: 1,
            },
        ),
    )
    .await;
    assert_eq!(
        forwards(&actions),
        vec![ErToInvoker::Applied { attempt: 1, seq: 1 }]
    );
    assert_eq!(
        er_status(&mut test_env, inv.id).await.pinned_deployment_id,
        Some(deployment_id)
    );

    // An atomic group across both state scopes, with a call that wants its result.
    let callee_target = InvocationTarget::mock_service();
    let callee_id = InvocationId::mock_generate(&callee_target);
    let call = ErAction::Call {
        request: Box::new(CallRequest {
            protocol_family: ProtocolFamily::ExplicitRecovery,
            ..CallRequest::mock(callee_id, callee_target)
        }),
        execution_time: None,
        result_id: NonZeroU32::new(7),
        uninvokable: None,
    };
    let group = ErAction::Group(vec![
        set_state(ErStateScope::Object, "k", "object"),
        set_state(ErStateScope::Invocation, "k", "invocation"),
        call,
    ]);
    let actions = apply_effect(&mut test_env, effect(inv.id, 1, 2, action(group, 7))).await;
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::NewOutboxMessage {
                message: pat!(OutboxMessage::ServiceInvocation(pat!(ServiceInvocation {
                    invocation_id: eq(callee_id),
                    response_sink: some(eq(ServiceInvocationResponseSink::partition_processor(
                        inv.id, 7
                    ))),
                    protocol_family: eq(ProtocolFamily::ExplicitRecovery)
                })))
            })),
            contains(pat!(Action::ForwardExplicitRecovery {
                message: eq(ErToInvoker::Applied { attempt: 1, seq: 2 })
            }))
        )
    );
    let service_id = inv.target.as_keyed_service_id().unwrap();
    let key = Bytes::from_static(b"k");
    assert_eq!(
        test_env
            .storage()
            .get_user_state(&service_id, &key)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"object"))
    );
    assert_eq!(
        test_env
            .storage()
            .get_invocation_state(&inv.id, &key)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"invocation"))
    );
    let er = er_status(&mut test_env, inv.id).await;
    assert!(er.action_applied);
    assert_eq!(er.next_result_id.get(), 7 + 1 + 256);

    // An effect of another attempt is dropped.
    let actions = apply_effect(
        &mut test_env,
        effect(
            inv.id,
            2,
            3,
            action(set_state(ErStateScope::Invocation, "k", "stale"), 0),
        ),
    )
    .await;
    assert!(forwards(&actions).is_empty());
    assert_eq!(
        test_env
            .storage()
            .get_invocation_state(&inv.id, &key)
            .await
            .unwrap(),
        Some(Bytes::from_static(b"invocation"))
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn end_completes_and_records_the_attempt() {
    let mut test_env = test_env().await;
    let inv = invoke(&mut test_env, Duration::from_secs(3600)).await;
    run(&mut test_env, inv.id).await;
    apply_effect(
        &mut test_env,
        effect(
            inv.id,
            1,
            1,
            ErEffectKind::AttemptStarted {
                deployment_id: DeploymentId::new(),
                protocol_version: 1,
            },
        ),
    )
    .await;

    let output = Bytes::from_static(b"output");
    let actions = apply_effect(
        &mut test_env,
        effect(
            inv.id,
            1,
            2,
            ErEffectKind::End(ResponseResult::Success(output.clone())),
        ),
    )
    .await;
    assert_that!(
        actions,
        contains(pat!(Action::ReplyRpc {
            request_id: eq(inv.request_id),
            reply: pat!(RpcReply::Output(pat!(
                restate_types::invocation::client::InvocationOutput {
                    response: pat!(InvocationOutputResponse::Success(
                        anything(),
                        eq(output.clone())
                    ))
                }
            )))
        }))
    );
    let status = status(&mut test_env, inv.id).await;
    assert_that!(
        status,
        pat!(InvocationStatus::Completed(pat!(
            restate_storage_api::invocation_status_table::CompletedInvocation {
                response_result: eq(ResponseResult::Success(output)),
                explicit_recovery: some(anything())
            }
        )))
    );
    // The attempt record: started, then ended.
    let events = test_env.read_journal_events(&inv.id).await;
    assert_that!(
        events,
        elements_are![
            pat!(Event::AttemptStarted(anything())),
            eq(Event::AttemptEnded(AttemptEndedEvent {
                attempt: 1,
                reason: "end".to_owned()
            }))
        ]
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn suspend_and_wake_on_anything() {
    let mut test_env = test_env().await;
    let inv = invoke(&mut test_env, Duration::ZERO).await;

    // Suspends with matching counts, and the suspended status reads back (F3).
    run(&mut test_env, inv.id).await;
    apply_effect(&mut test_env, effect(inv.id, 1, 1, suspend(0))).await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Suspended { .. })
    );
    assert_eq!(
        stage_and_status(&mut test_env, inv.id).await.0,
        Stage::Suspended
    );

    // A result for an id nobody waits for wakes it, and is not written anywhere.
    test_env.apply(response(inv.id, 999)).await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Invoked(_))
    );
    assert_that!(
        status(&mut test_env, inv.id).await,
        crate::partition::state_machine::tests::matchers::storage::has_journal_length(1)
    );

    // A named signal is stored at the next index, and wakes it.
    for (attempt, index) in [(2, 0), (3, 1)] {
        run(&mut test_env, inv.id).await;
        apply_effect(&mut test_env, effect(inv.id, attempt, 1, suspend(0))).await;
        test_env.apply(named_signal(inv.id, "approval")).await;
        assert_that!(
            status(&mut test_env, inv.id).await,
            pat!(InvocationStatus::Invoked(_))
        );
        assert!(
            test_env
                .storage()
                .get_signal(&inv.id, "approval", index)
                .await
                .unwrap()
                .is_some()
        );
    }

    // The cancel signal wakes it once; a second one wakes nothing.
    run(&mut test_env, inv.id).await;
    apply_effect(&mut test_env, effect(inv.id, 4, 1, suspend(0))).await;
    test_env.apply(cancel(inv.id)).await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Invoked(_))
    );
    assert!(er_status(&mut test_env, inv.id).await.cancelled);
    run(&mut test_env, inv.id).await;
    apply_effect(&mut test_env, effect(inv.id, 5, 1, suspend(0))).await;
    test_env.apply(cancel(inv.id)).await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Suspended { .. })
    );

    // A signal by index, other than the cancel signal, is dropped (D61).
    test_env
        .apply(commands::NotifySignalCommand::test_envelope(
            NotifySignalRequest {
                invocation_id: inv.id,
                signal: Signal::new(SignalId::for_index(17), SignalResult::Void),
            },
        ))
        .await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Suspended { .. })
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn arrivals_while_invoked_are_forwarded_and_counted() {
    let mut test_env = test_env().await;
    let inv = invoke(&mut test_env, Duration::ZERO).await;
    run(&mut test_env, inv.id).await;

    let mut forwarded = forwards(&test_env.apply(response(inv.id, 5)).await);
    forwarded.extend(forwards(
        &test_env.apply(named_signal(inv.id, "approval")).await,
    ));
    forwarded.extend(forwards(&test_env.apply(cancel(inv.id)).await));
    assert_eq!(
        forwarded,
        vec![
            ErToInvoker::Result {
                attempt: 1,
                result_id: 5,
                result: ResponseResult::Success(Bytes::from_static(b"answer")),
            },
            ErToInvoker::Signal {
                attempt: 1,
                name: "approval".into(),
                index: 0,
                result: SignalResult::Success(Bytes::from_static(b"payload")),
            },
            ErToInvoker::Cancel { attempt: 1 },
        ]
    );
    let er = er_status(&mut test_env, inv.id).await;
    assert_eq!(er.forwarded_notifications, 3);
    assert!(er.cancelled);
    // Nothing was written to a journal.
    assert_that!(
        status(&mut test_env, inv.id).await,
        crate::partition::state_machine::tests::matchers::storage::has_journal_length(1)
    );

    // A suspend that did not receive them all restarts instead (D34).
    apply_effect(&mut test_env, effect(inv.id, 1, 1, suspend(2))).await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Invoked(_))
    );
    assert_eq!(
        stage_and_status(&mut test_env, inv.id).await.0,
        Stage::Inbox
    );

    // The next attempt starts counting from zero, and suspends when the counts match.
    run(&mut test_env, inv.id).await;
    assert_eq!(
        er_status(&mut test_env, inv.id)
            .await
            .forwarded_notifications,
        0
    );
    test_env.apply(response(inv.id, 6)).await;
    apply_effect(&mut test_env, effect(inv.id, 2, 1, suspend(1))).await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Suspended { .. })
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn idle_restarts_are_bounded() {
    let mut test_env = test_env().await;
    let inv = invoke(&mut test_env, Duration::ZERO).await;
    let restart = |rule| ErEffectKind::Restart {
        rule,
        idle_fallback: ErFailureOutcome::Retry {
            resume_at: None,
            retry_attempts: 2,
        },
    };

    // Three idle restarts in a row restart.
    for attempt in 1..=3 {
        run(&mut test_env, inv.id).await;
        apply_effect(
            &mut test_env,
            effect(inv.id, attempt, 1, restart(RestartRule::NothingOutstanding)),
        )
        .await;
        assert_eq!(
            stage_and_status(&mut test_env, inv.id).await,
            (Stage::Inbox, Status::Yielded, 0)
        );
    }
    // The fourth is a failure: the retry policy applies.
    run(&mut test_env, inv.id).await;
    apply_effect(
        &mut test_env,
        effect(inv.id, 4, 1, restart(RestartRule::NoAnswer)),
    )
    .await;
    assert_eq!(
        stage_and_status(&mut test_env, inv.id).await,
        (Stage::Inbox, Status::BackingOff, 2)
    );
    assert_eq!(
        er_status(&mut test_env, inv.id).await.idle_restart_streak,
        4
    );

    // A restart for a reason of the runtime's own never counts, and resets the retry counters.
    run(&mut test_env, inv.id).await;
    apply_effect(
        &mut test_env,
        effect(inv.id, 5, 1, restart(RestartRule::OwnReason)),
    )
    .await;
    assert_eq!(
        stage_and_status(&mut test_env, inv.id).await,
        (Stage::Inbox, Status::Yielded, 0)
    );
    assert_eq!(
        er_status(&mut test_env, inv.id).await.idle_restart_streak,
        4
    );

    // An applied action ends the streak: the restart after it is not idle.
    run(&mut test_env, inv.id).await;
    apply_effect(
        &mut test_env,
        effect(
            inv.id,
            6,
            1,
            action(set_state(ErStateScope::Invocation, "progress", "1"), 0),
        ),
    )
    .await;
    apply_effect(
        &mut test_env,
        effect(inv.id, 6, 2, restart(RestartRule::NothingOutstanding)),
    )
    .await;
    assert_eq!(
        stage_and_status(&mut test_env, inv.id).await,
        (Stage::Inbox, Status::Yielded, 0)
    );
    assert_eq!(
        er_status(&mut test_env, inv.id).await.idle_restart_streak,
        0
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn kill_pause_resume_and_purge() {
    let mut test_env = test_env().await;

    // Pause a running attempt, resume it: a new attempt.
    let inv = invoke(&mut test_env, Duration::from_secs(3600)).await;
    run(&mut test_env, inv.id).await;
    let actions = test_env
        .apply(commands::PauseInvocationCommand::test_envelope(
            commands::PauseInvocationCommand {
                invocation_id: inv.id,
                request_id: None,
            },
        ))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::AbortInvocation {
            invocation_id: eq(inv.id)
        }))
    );
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Paused(_))
    );
    test_env
        .apply(commands::ResumeInvocationCommand::test_envelope(
            ResumeInvocationRequest {
                invocation_id: inv.id,
                update_deployment_id: None,
                update_pinned_deployment_id: None,
                run_at: None,
                response_sink: None,
            },
        ))
        .await;
    let actions = run(&mut test_env, inv.id).await;
    assert_that!(
        actions,
        contains(pat!(Action::VQInvoke {
            explicit_recovery_attempt: eq(NonZeroU32::new(2))
        }))
    );

    // Write invocation state, send to a callee, suspend; then kill.
    apply_effect(
        &mut test_env,
        effect(
            inv.id,
            2,
            1,
            action(set_state(ErStateScope::Invocation, "k", "v"), 0),
        ),
    )
    .await;
    apply_effect(&mut test_env, effect(inv.id, 2, 2, suspend(0))).await;
    let actions = test_env
        .apply(commands::TerminateInvocationCommand::test_envelope(
            InvocationTermination {
                invocation_id: inv.id,
                flavor: TerminationFlavor::Kill,
                response_sink: None,
            },
        ))
        .await;
    // Completed with the killed error; nothing is enqueued for the invocations it called.
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::ReplyRpc {
                request_id: eq(inv.request_id),
                reply: pat!(RpcReply::Output(pat!(
                    restate_types::invocation::client::InvocationOutput {
                        response: eq(InvocationOutputResponse::Failure(KILLED_INVOCATION_ERROR))
                    }
                )))
            })),
            not(contains(pat!(Action::NewOutboxMessage { .. })))
        )
    );
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Completed(pat!(
            restate_storage_api::invocation_status_table::CompletedInvocation {
                response_result: eq(ResponseResult::Failure(KILLED_INVOCATION_ERROR))
            }
        )))
    );
    // The journal is retained, and so is the invocation state, until the purge.
    let key = Bytes::from_static(b"k");
    assert!(
        test_env
            .storage()
            .get_invocation_state(&inv.id, &key)
            .await
            .unwrap()
            .is_some()
    );
    test_env
        .apply(commands::PurgeInvocationCommand::test_envelope(
            PurgeInvocationRequest {
                invocation_id: inv.id,
                response_sink: None,
            },
        ))
        .await;
    assert_that!(
        status(&mut test_env, inv.id).await,
        pat!(InvocationStatus::Free)
    );
    assert!(
        test_env
            .storage()
            .get_all_invocation_state(&inv.id)
            .await
            .unwrap()
            .is_empty()
    );
    test_env.shutdown().await;
}
