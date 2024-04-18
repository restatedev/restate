// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::types::{create_response_message, OutboxMessageExt, ResponseMessage};
use bytes::Bytes;
use restate_pb::builtin_service::BuiltInService;
use restate_pb::restate::internal::AwakeablesInvoker;
use restate_pb::restate::internal::ProxyInvoker;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    InvocationTarget, ServiceInvocationResponseSink, ServiceInvocationSpanContext,
};
use std::ops::Deref;

mod awakeables;
mod proxy;

#[derive(Debug)]
pub(crate) enum Effect {
    OutboxMessage(OutboxMessage),
    IngressResponse(IngressResponse),
}

/// Deterministic built-in services are executed by both leaders and followers, hence they must generate the same output.
pub(crate) struct ServiceInvoker<'a> {
    invocation_target: &'a InvocationTarget,
    span_context: &'a ServiceInvocationSpanContext,

    effects: Vec<Effect>,
}

impl<'a> ServiceInvoker<'a> {
    pub(crate) fn is_supported(service_name: &str) -> bool {
        service_name == restate_pb::AWAKEABLES_SERVICE_NAME
            || service_name == restate_pb::PROXY_SERVICE_NAME
    }

    pub(crate) async fn invoke(
        invocation_id: &'a InvocationId,
        invocation_target: &'a InvocationTarget,
        span_context: &'a ServiceInvocationSpanContext,
        response_sink: Option<&'a ServiceInvocationResponseSink>,
        argument: Bytes,
    ) -> Vec<Effect> {
        let mut this: ServiceInvoker<'a> = Self {
            invocation_target,
            span_context,
            effects: vec![],
        };

        let res = this
            ._invoke(invocation_target.handler_name(), argument)
            .await;

        if let Some(response_sink) = response_sink {
            match create_response_message(invocation_id, None, response_sink.clone(), res.into()) {
                ResponseMessage::Outbox(outbox) => this.outbox_message(outbox),
                ResponseMessage::Ingress(ingress) => this.ingress_response(ingress),
            }
        }

        this.effects
    }

    fn outbox_message(&mut self, msg: OutboxMessage) {
        self.effects.push(Effect::OutboxMessage(msg));
    }

    fn ingress_response(&mut self, response: IngressResponse) {
        self.effects.push(Effect::IngressResponse(response));
    }
}

impl ServiceInvoker<'_> {
    // Function that routes through the available built-in services
    async fn _invoke(&mut self, method: &str, argument: Bytes) -> Result<Bytes, InvocationError> {
        match self.invocation_target.service_name().deref() {
            restate_pb::AWAKEABLES_SERVICE_NAME => {
                AwakeablesInvoker(self)
                    .invoke_builtin(method, argument)
                    .await
            }
            restate_pb::PROXY_SERVICE_NAME => {
                ProxyInvoker(self).invoke_builtin(method, argument).await
            }
            _ => Err(InvocationError::component_not_found(
                self.invocation_target.service_name(),
            )),
        }
    }
}
