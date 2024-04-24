// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::identifiers::{EntryIndex, IdempotencyId, InvocationId};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    InvocationResponse, InvocationTarget, ResponseResult, ServiceInvocationResponseSink,
};
use restate_wal_protocol::Command;

pub(crate) type InvokerEffect = restate_invoker_api::Effect;
pub(crate) type InvokerEffectKind = restate_invoker_api::EffectKind;

/// This type carries together invocation id and target.
/// Use this type only when you need to group together id and target,
/// and generally use only InvocationId for identifying an invocation.
pub(crate) type InvocationIdAndTarget = (InvocationId, InvocationTarget);

// Extension methods to the OutboxMessage type
pub(crate) trait OutboxMessageExt {
    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage;

    fn to_command(self) -> Command;
}

impl OutboxMessageExt for OutboxMessage {
    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage {
        OutboxMessage::ServiceResponse(InvocationResponse {
            entry_index,
            result,
            id: invocation_id,
        })
    }

    fn to_command(self) -> Command {
        match self {
            OutboxMessage::ServiceInvocation(si) => Command::Invoke(si),
            OutboxMessage::ServiceResponse(sr) => Command::InvocationResponse(sr),
            OutboxMessage::InvocationTermination(it) => Command::TerminateInvocation(it),
        }
    }
}

pub fn create_response_message(
    callee: &InvocationId,
    idempotency_id: Option<IdempotencyId>,
    response_sink: ServiceInvocationResponseSink,
    result: ResponseResult,
) -> ResponseMessage {
    match response_sink {
        ServiceInvocationResponseSink::PartitionProcessor {
            entry_index,
            caller,
        } => ResponseMessage::Outbox(OutboxMessage::ServiceResponse(InvocationResponse {
            id: caller,
            entry_index,
            result,
        })),
        ServiceInvocationResponseSink::Ingress(ingress_dispatcher_id) => {
            ResponseMessage::Ingress(IngressResponse {
                target_node: ingress_dispatcher_id,
                invocation_id: *callee,
                idempotency_id,
                response: result,
            })
        }
    }
}

pub enum ResponseMessage {
    Outbox(OutboxMessage),
    Ingress(IngressResponse),
}
