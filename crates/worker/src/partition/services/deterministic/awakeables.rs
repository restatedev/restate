// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use crate::partition::services::deterministic::*;

use restate_pb::restate::internal::AwakeablesBuiltInService;
use restate_pb::restate::internal::*;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::errors::codes;
use restate_types::invocation::ResponseResult;

impl AwakeablesBuiltInService for &mut ServiceInvoker<'_> {
    async fn resolve(&mut self, req: ResolveAwakeableRequest) -> Result<(), InvocationError> {
        let (invocation_id, entry_index) = AwakeableIdentifier::from_str(&req.id)
            .map_err(|e| InvocationError::new(codes::BAD_REQUEST, e.to_string()))?
            .into_inner();

        self.outbox_message(OutboxMessage::from_awakeable_completion(
            invocation_id,
            entry_index,
            ResponseResult::from(Ok(req.result)),
        ));
        Ok(())
    }

    async fn reject(&mut self, req: RejectAwakeableRequest) -> Result<(), InvocationError> {
        let (invocation_id, entry_index) = AwakeableIdentifier::from_str(&req.id)
            .map_err(|e| InvocationError::new(codes::BAD_REQUEST, e.to_string()))?
            .into_inner();

        self.outbox_message(OutboxMessage::from_awakeable_completion(
            invocation_id,
            entry_index,
            ResponseResult::from(Err(InvocationError::new(codes::UNKNOWN, req.reason))),
        ));

        Ok(())
    }
}
