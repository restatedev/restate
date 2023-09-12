// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::types::OutboxMessageExt;
use bytes::Bytes;
use restate_pb::builtin_service::BuiltInService;
use restate_pb::restate::AwakeablesInvoker;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::FullInvocationId;
use restate_types::invocation::ServiceInvocationResponseSink;
use std::ops::Deref;

#[derive(Debug)]
pub(crate) enum Effect {
    OutboxMessage(OutboxMessage),
}

/// Deterministic built-in services are executed by both leaders and followers, hence they must generate the same output.
pub(crate) struct ServiceInvoker<'a> {
    fid: &'a FullInvocationId,
    effects: Vec<Effect>,
}

impl<'a> ServiceInvoker<'a> {
    pub(crate) fn is_supported(service_name: &str) -> bool {
        service_name == restate_pb::AWAKEABLES_SERVICE_NAME
    }

    pub(crate) async fn invoke(
        fid: &'a FullInvocationId,
        method: &'a str,
        argument: Bytes,
        response_sink: Option<&ServiceInvocationResponseSink>,
    ) -> Vec<Effect> {
        let mut this: ServiceInvoker<'a> = Self {
            fid,
            effects: vec![],
        };

        let res = this._invoke(method, argument).await;

        if let Some(response_sink) = response_sink {
            this.send_message(OutboxMessage::from_response_sink(
                fid,
                response_sink.clone(),
                res.into(),
            ));
        }

        this.effects
    }

    fn send_message(&mut self, msg: OutboxMessage) {
        self.effects.push(Effect::OutboxMessage(msg));
    }
}

impl ServiceInvoker<'_> {
    // Function that routes through the available built-in services
    async fn _invoke(&mut self, method: &str, argument: Bytes) -> Result<Bytes, InvocationError> {
        match self.fid.service_id.service_name.deref() {
            restate_pb::AWAKEABLES_SERVICE_NAME => {
                AwakeablesInvoker(self)
                    .invoke_builtin(method, argument)
                    .await
            }
            _ => Err(InvocationError::new(
                UserErrorCode::NotFound,
                format!("{} not found", self.fid.service_id.service_name),
            )),
        }
    }
}

mod awakeables {
    use super::*;

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
}
