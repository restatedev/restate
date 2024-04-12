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
use restate_types::identifiers::InvocationUuid;
use restate_types::invocation::{HandlerType, InvocationTarget, ServiceInvocation, Source};
use tracing::{instrument, trace};

impl ProxyBuiltInService for &mut ServiceInvoker<'_> {
    #[instrument(level = "trace", skip(self))]
    async fn proxy_through(&mut self, req: ProxyThroughRequest) -> Result<(), InvocationError> {
        let target_fid = FullInvocationId::new(
            req.target_service,
            req.target_key,
            InvocationUuid::from_slice(&req.target_invocation_uuid)
                .map_err(InvocationError::internal)?,
        );
        trace!(restate.invocation.id = %target_fid, "Proxying");

        let invocation_target = InvocationTarget::VirtualObject {
            name: target_fid.service_id.service_name.clone(),
            key: ByteString::try_from(target_fid.service_id.key.clone())
                .expect("Key should be UTF-8. This should have been checked before"),
            handler: req.target_method.clone().into(),
            handler_ty: HandlerType::Exclusive,
        };
        self.outbox_message(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
            InvocationId::from(&target_fid),
            invocation_target,
            target_fid,
            req.target_method,
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
