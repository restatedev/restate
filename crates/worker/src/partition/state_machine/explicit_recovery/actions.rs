// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use tracing::warn;

use restate_storage_api::invocation_status_table::{InFlightInvocationMetadata, InvocationStatus};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{
    AttachInvocationRequest, NotifySignalRequest, ServiceInvocation, ServiceInvocationResponseSink,
    Source,
};
use restate_types::journal_events::{
    ActionRecordedEvent, Event, RecordedAction, ResultDeliveredEvent, SpanEndedEvent,
    SpanStartedEvent,
};
use restate_types::journal_v2::{BuiltInSignal, CallRequest, Signal, SignalId, SignalResult};
use restate_wal_protocol::v2::commands;
use restate_worker_api::invoker::explicit_recovery::{ErAction, ErRecord, ErStateScope};

use super::{ErStorage, RESULT_ID_BLOCK, er_of, lsn, put_event};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{Error, StateMachineApplyContext};

/// Applies an action of the current attempt: a single one, or a group in order. The whole
/// action is applied in this record, so it is applied all or none (G3).
pub(super) async fn on_action<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    mut metadata: InFlightInvocationMetadata,
    action: ErAction,
    highest_result_id: Option<NonZeroU32>,
    record: Option<Box<RecordedAction>>,
) -> Result<(), Error>
where
    S: ErStorage,
    P: ProcessorContext,
{
    apply_action(ctx, &invocation_id, &metadata, action).await?;
    if let Some(record) = record {
        on_record(ctx, &invocation_id, &metadata, ErRecord::Action(record))?;
    }

    let er = er_of(&mut metadata);
    let mut changed = false;
    if !er.action_applied {
        er.action_applied = true;
        changed = true;
    }
    if er.idle_restart_streak != 0 {
        // D63: an applied action ends the streak of idle restarts.
        er.idle_restart_streak = 0;
        changed = true;
    }
    if let Some(highest) = highest_result_id
        && highest >= er.next_result_id
    {
        // No result id at or above the mark was ever applied.
        er.next_result_id = highest.saturating_add(1).saturating_add(RESULT_ID_BLOCK);
        changed = true;
    }
    if changed {
        ctx.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))?;
    }
    Ok(())
}

/// Writes what the record of the attempt keeps of an action, a span or a delivered result: one
/// event, also for an atomic group (its members are in it).
pub(super) fn on_record<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: &InvocationId,
    metadata: &InFlightInvocationMetadata,
    record: ErRecord,
) -> Result<(), Error>
where
    S: WriteJournalEventsTable,
{
    let attempt = metadata
        .explicit_recovery
        .as_ref()
        .expect("explicit recovery invocation")
        .attempt;
    let lsn = lsn(ctx);
    let event = match record {
        ErRecord::Action(action) => Event::ActionRecorded(ActionRecordedEvent {
            attempt,
            action,
            lsn,
        }),
        ErRecord::SpanStart {
            span_id,
            parent_span_id,
            name,
        } => Event::SpanStarted(SpanStartedEvent {
            attempt,
            span_id,
            parent_span_id,
            name,
            lsn,
        }),
        ErRecord::SpanEnd {
            span_id,
            outcome,
            value,
            failure,
        } => Event::SpanEnded(SpanEndedEvent {
            attempt,
            span_id,
            outcome,
            value,
            failure,
            lsn,
        }),
        ErRecord::ResultDelivered { result_id, variant } => {
            Event::ResultDelivered(ResultDeliveredEvent {
                attempt,
                result_id,
                variant,
                lsn,
            })
        }
    };
    put_event(ctx, invocation_id, metadata, event)
}

async fn apply_action<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: &InvocationId,
    metadata: &InFlightInvocationMetadata,
    action: ErAction,
) -> Result<(), Error>
where
    S: ErStorage,
    P: ProcessorContext,
{
    match action {
        ErAction::SetState { scope, key, value } => match scope {
            ErStateScope::Object => {
                let Some(service_id) = metadata.invocation_target.as_keyed_service_id() else {
                    warn!("Ignoring an object state write of a target without a key");
                    return Ok(());
                };
                ctx.storage.put_user_state(&service_id, &key, &value)?;
            }
            ErStateScope::Invocation => {
                ctx.storage
                    .put_invocation_state(invocation_id, &key, &value)?;
            }
        },
        ErAction::ClearState { scope, key } => match scope {
            ErStateScope::Object => {
                let Some(service_id) = metadata.invocation_target.as_keyed_service_id() else {
                    warn!("Ignoring an object state write of a target without a key");
                    return Ok(());
                };
                ctx.storage.delete_user_state(&service_id, &key)?;
            }
            ErStateScope::Invocation => {
                ctx.storage.delete_invocation_state(invocation_id, &key)?;
            }
        },
        ErAction::ClearAllState { scope } => match scope {
            ErStateScope::Object => {
                let Some(service_id) = metadata.invocation_target.as_keyed_service_id() else {
                    warn!("Ignoring an object state write of a target without a key");
                    return Ok(());
                };
                ctx.storage.delete_all_user_state(&service_id)?;
            }
            ErStateScope::Invocation => {
                ctx.storage.delete_all_invocation_state(invocation_id)?;
            }
        },
        ErAction::Call {
            request,
            execution_time,
            result_id,
            uninvokable,
        } => {
            if uninvokable.is_some() {
                // A target that cannot be invoked is a failed invocation: the attempt answers the
                // call itself, and nothing is enqueued (D22, step 1).
                return Ok(());
            }
            let CallRequest {
                invocation_id: callee_id,
                invocation_target,
                span_context,
                parameter,
                headers,
                idempotency_key,
                completion_retention_duration,
                journal_retention_duration,
                limit_key,
                protocol_family,
            } = *request;
            let service_invocation = ServiceInvocation {
                argument: parameter,
                headers,
                response_sink: result_id.map(|result_id| {
                    ServiceInvocationResponseSink::partition_processor(
                        *invocation_id,
                        result_id.get(),
                    )
                }),
                span_context,
                execution_time,
                completion_retention_duration,
                journal_retention_duration,
                idempotency_key,
                limit_key,
                protocol_family,
                ..ServiceInvocation::initialize(
                    callee_id,
                    invocation_target,
                    Source::Service(*invocation_id, metadata.invocation_target.clone()),
                )
            };
            ctx.do_enqueue_into_outbox(commands::InvokeCommand::from(service_invocation))?;
        }
        ErAction::Attach { query, result_id } => {
            ctx.do_enqueue_into_outbox(commands::AttachInvocationCommand::from(
                AttachInvocationRequest {
                    invocation_query: query,
                    block_on_inflight: true,
                    response_sink: ServiceInvocationResponseSink::partition_processor(
                        *invocation_id,
                        result_id.get(),
                    ),
                },
            ))?;
        }
        ErAction::CancelInvocation { target } => {
            ctx.do_enqueue_into_outbox(commands::NotifySignalCommand::from(NotifySignalRequest {
                invocation_id: target,
                signal: Signal::new(
                    SignalId::for_builtin_signal(BuiltInSignal::Cancel),
                    SignalResult::Void,
                ),
            }))?;
        }
        ErAction::SendSignal {
            target,
            name,
            result,
        } => {
            ctx.do_enqueue_into_outbox(commands::NotifySignalCommand::from(NotifySignalRequest {
                invocation_id: target,
                signal: Signal::new(SignalId::for_name(name), result),
            }))?;
        }
        ErAction::Group(members) => {
            for member in members {
                Box::pin(apply_action(ctx, invocation_id, metadata, member)).await?;
            }
        }
    }
    Ok(())
}
