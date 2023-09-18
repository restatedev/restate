// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::services::deterministic::*;

use prost_reflect::ReflectMessage;
use restate_pb::restate::AwakeablesBuiltInService;
use restate_pb::restate::*;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::invocation::ResponseResult;

#[async_trait::async_trait]
impl AwakeablesBuiltInService for &mut ServiceInvoker<'_> {
    async fn resolve(&mut self, req: ResolveAwakeableRequest) -> Result<(), InvocationError> {
        let (invocation_id, entry_index) = AwakeableIdentifier::decode(req.id)
            .map_err(|e| InvocationError::new(UserErrorCode::InvalidArgument, e.to_string()))?
            .into_inner();

        let result = match req.result {
            None => {
                return Err(InvocationError::new_static(
                    UserErrorCode::InvalidArgument.into(),
                    "result must be non-empty",
                ));
            }
            Some(resolve_awakeable_request::Result::BytesResult(bytes)) => bytes,
            Some(resolve_awakeable_request::Result::JsonResult(value)) => Bytes::from(
                serde_json::to_vec(&value.transcode_to_dynamic()).map_err(|e| {
                    InvocationError::new(UserErrorCode::InvalidArgument, e.to_string())
                })?,
            ),
        };

        self.send_message(OutboxMessage::from_awakeable_completion(
            invocation_id,
            entry_index,
            ResponseResult::from(Ok(result)),
        ));
        Ok(())
    }

    async fn reject(&mut self, req: RejectAwakeableRequest) -> Result<(), InvocationError> {
        let (invocation_id, entry_index) = AwakeableIdentifier::decode(req.id)
            .map_err(|e| InvocationError::new(UserErrorCode::InvalidArgument, e.to_string()))?
            .into_inner();

        self.send_message(OutboxMessage::from_awakeable_completion(
            invocation_id,
            entry_index,
            ResponseResult::from(Err(InvocationError::new(
                UserErrorCode::Unknown,
                req.reason,
            ))),
        ));

        Ok(())
    }
}
