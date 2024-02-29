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
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    InvocationResponse, MaybeFullInvocationId, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, Source, SpanRelation,
};

pub(crate) type InvokerEffect = restate_invoker_api::Effect;
pub(crate) type InvokerEffectKind = restate_invoker_api::EffectKind;

// Extension methods to the OutboxMessage type
pub(crate) trait OutboxMessageExt {
    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage;
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
}

pub fn create_response_message(
    callee: &FullInvocationId,
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
                full_invocation_id: callee.clone(),
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
                Source::Service(callee.clone()),
                None,
                SpanRelation::None,
            )))
        }
    }
}

pub enum ResponseMessage {
    Outbox(OutboxMessage),
    Ingress(IngressResponse),
}
