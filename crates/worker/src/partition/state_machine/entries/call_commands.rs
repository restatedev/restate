// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{ServiceInvocation, ServiceInvocationResponseSink, Source};
use restate_types::journal_v2::command::{CallCommand, CallRequest, OneWayCallCommand};
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{CallInvocationIdCompletion, CompletionId, Entry};
use restate_types::time::MillisSinceEpoch;
use std::collections::VecDeque;

pub(super) type ApplyCallCommand<'e> = ApplyJournalCommandEffect<'e, CallCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyJournalCommandEffect<'e, CallCommand>
where
    S: OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        _ApplyCallCommand {
            caller_invocation_id: self.invocation_id,
            caller_invocation_status: self.invocation_status,
            request: self.entry.request,
            invocation_id_notification_idx: self.entry.invocation_id_completion_id,
            execution_time: None,
            result_notification_idx: Some(self.entry.result_completion_id),
            completions_to_process: self.completions_to_process,
        }
        .apply(ctx)
        .await
    }
}

pub(super) type ApplyOneWayCallCommand<'e> = ApplyJournalCommandEffect<'e, OneWayCallCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyJournalCommandEffect<'e, OneWayCallCommand>
where
    S: OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let execution_time = if self.entry.invoke_time.as_u64() == 0 {
            None
        } else {
            Some(self.entry.invoke_time)
        };
        _ApplyCallCommand {
            caller_invocation_id: self.invocation_id,
            caller_invocation_status: self.invocation_status,
            request: self.entry.request,
            invocation_id_notification_idx: self.entry.invocation_id_completion_id,
            execution_time,
            result_notification_idx: None,
            completions_to_process: self.completions_to_process,
        }
        .apply(ctx)
        .await
    }
}

struct _ApplyCallCommand<'e> {
    caller_invocation_id: InvocationId,
    caller_invocation_status: &'e InvocationStatus,
    request: CallRequest,
    invocation_id_notification_idx: CompletionId,
    execution_time: Option<MillisSinceEpoch>,
    result_notification_idx: Option<CompletionId>,
    completions_to_process: &'e mut VecDeque<RawEntry>,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for _ApplyCallCommand<'e>
where
    S: OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let caller_invocation_metadata = self
            .caller_invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let CallRequest {
            invocation_id,
            invocation_target,
            span_context,
            parameter,
            headers,
            idempotency_key,
            completion_retention_duration,
        } = self.request;

        // Prepare the service invocation to propose
        let service_invocation = ServiceInvocation {
            invocation_id,
            invocation_target,
            argument: parameter,
            source: Source::Service(
                self.caller_invocation_id,
                caller_invocation_metadata.invocation_target.clone(),
            ),
            response_sink: self.result_notification_idx.map(|notification_idx| {
                ServiceInvocationResponseSink::partition_processor(
                    self.caller_invocation_id,
                    notification_idx,
                )
            }),
            span_context: span_context.clone(),
            headers,
            execution_time: self.execution_time,
            completion_retention_duration,
            idempotency_key,
            submit_notification_sink: None,
        };

        ctx.handle_outgoing_message(OutboxMessage::ServiceInvocation(service_invocation))
            .await?;

        // Notify the invocation id back
        self.completions_to_process.push_back(
            Entry::from(CallInvocationIdCompletion {
                completion_id: self.invocation_id_notification_idx,
                invocation_id,
            })
            .encode::<ServiceProtocolV4Codec>(),
        );

        Ok(())
    }
}
