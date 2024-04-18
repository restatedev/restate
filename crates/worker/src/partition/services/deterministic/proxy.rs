// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use bytestring::ByteString;

use restate_pb::restate::internal::*;
use restate_types::identifiers::{InvocationUuid, WithPartitionKey};
use restate_types::invocation::{HandlerType, InvocationTarget, ServiceInvocation, Source};
use tracing::{instrument, trace};

impl ProxyBuiltInService for &mut ServiceInvoker<'_> {
    #[instrument(level = "trace", skip(self))]
    async fn proxy_through(&mut self, req: ProxyThroughRequest) -> Result<(), InvocationError> {
        let invocation_target = InvocationTarget::VirtualObject {
            name: req.target_service.into(),
            key: ByteString::try_from(req.target_key)
                .expect("Key should be UTF-8. This should have been checked before"),
            handler: req.target_method.into(),
            handler_ty: HandlerType::Exclusive,
        };
        let mut invocation_id = InvocationId::generate(&invocation_target);
        invocation_id = InvocationId::from_parts(
            invocation_id.partition_key(),
            InvocationUuid::from_slice(&req.target_invocation_uuid)
                .map_err(InvocationError::internal)?,
        );
        trace!(restate.invocation.id = %invocation_id, "Proxying");

        self.outbox_message(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
            invocation_id,
            invocation_target,
            req.input,
            // Proxy service is only used by the ingress dispatcher to for deduplication purposes.
            Source::Ingress,
            None,
            self.span_context.as_parent(),
            vec![],
            None,
            None,
        )));

        Ok(())
    }
}
