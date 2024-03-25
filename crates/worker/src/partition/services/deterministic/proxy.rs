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

use restate_pb::restate::internal::*;
use restate_types::identifiers::InvocationUuid;
use restate_types::invocation::{ServiceInvocation, Source};
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

        self.outbox_message(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
            target_fid,
            req.target_method,
            req.input,
            // Proxy service is only used by the ingress dispatcher to for deduplication purposes.
            Source::Ingress,
            None,
            self.span_context.as_parent(),
            vec![],
            None,
        )));

        Ok(())
    }
}
