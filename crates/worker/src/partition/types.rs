// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prost::Message;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    InvocationResponse, MaybeFullInvocationId, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, Source, SpanRelation,
};
use restate_wal_protocol::Command;

pub(crate) type InvokerEffect = restate_invoker_api::Effect;
pub(crate) type InvokerEffectKind = restate_invoker_api::EffectKind;

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
            id: MaybeFullInvocationId::Partial(invocation_id),
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
    response_sink: ServiceInvocationResponseSink,
    result: ResponseResult,
) -> ResponseMessage {
    match response_sink {
        ServiceInvocationResponseSink::PartitionProcessor {
            entry_index,
            caller,
        } => ResponseMessage::Outbox(OutboxMessage::ServiceResponse(InvocationResponse {
            id: MaybeFullInvocationId::Full(caller),
            entry_index,
            result,
        })),
        ServiceInvocationResponseSink::Ingress(ingress_dispatcher_id) => {
            ResponseMessage::Ingress(IngressResponse {
                target_node: ingress_dispatcher_id,
                invocation_id: callee.clone(),
                response: result,
            })
        }
        ServiceInvocationResponseSink::NewInvocation {
            target,
            method,
            caller_context,
        } => {
            ResponseMessage::Outbox(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
                target,
                method,
                // Methods receiving responses MUST accept this input type
                restate_pb::restate::internal::ServiceInvocationSinkRequest {
                    response: Some(result.into()),
                    caller_context,
                }
                .encode_to_vec(),
                Source::Internal,
                None,
                SpanRelation::None,
                vec![],
                None,
            )))
        }
    }
}

pub enum ResponseMessage {
    Outbox(OutboxMessage),
    Ingress(IngressResponse),
}
