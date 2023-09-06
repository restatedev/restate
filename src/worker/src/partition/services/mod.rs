// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::effects::Effects;
use bytes::Bytes;
use restate_pb::builtin_service::BuiltInService;
use restate_schema_api::key::KeyExtractor;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::FullInvocationId;
use std::ops::Deref;

// -- Deterministic built-in services infra

/// Deterministic built-in services are executed by both leaders and followers, hence they must generate the same output.
pub(super) struct DeterministicBuiltInServiceInvoker<'a> {
    fid: &'a FullInvocationId,
    effects: &'a mut Effects,
}

impl<'a> DeterministicBuiltInServiceInvoker<'a> {
    pub(super) fn is_supported(service_name: &str) -> bool {
        service_name == restate_pb::AWAKEABLES_SERVICE_NAME
    }

    pub(super) async fn invoke(
        fid: &'a FullInvocationId,
        effects: &'a mut Effects,
        method: &'a str,
        argument: Bytes,
    ) -> Result<Bytes, InvocationError> {
        let this: DeterministicBuiltInServiceInvoker<'a> = Self { fid, effects };

        this._invoke(method, argument).await
    }
}

impl DeterministicBuiltInServiceInvoker<'_> {
    // Function that routes through the available built-in services
    async fn _invoke(self, method: &str, argument: Bytes) -> Result<Bytes, InvocationError> {
        match self.fid.service_id.service_name.deref() {
            restate_pb::AWAKEABLES_SERVICE_NAME => {
                restate_pb::restate::services::AwakeablesInvoker(self)
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

// Non-deterministic built-in services infra

#[allow(dead_code)]
pub(super) struct NonDeterministicBuiltInServiceInvoker<'a, State, Schemas> {
    fid: &'a FullInvocationId,
    effects: &'a mut Effects,
    state: &'a mut State,
    schemas: &'a Schemas,
}

impl<'a, State, Schemas> NonDeterministicBuiltInServiceInvoker<'a, State, Schemas>
where
    State: Send,
    Schemas: KeyExtractor + Send + Sync,
{
    pub(super) fn is_supported(service_name: &str) -> bool {
        // The reason we just check for the prefix is the following:
        //
        // * No user can register services starting with dev.restate
        // * We already checked in the previous step of the state machine whether the service is a deterministic built-in service
        // * Hence with this assertion we can 404 sooner in case the user inputs a bad built-in service name, avoiding to get it stuck in the invoker
        service_name.starts_with("dev.restate")
    }

    pub(super) async fn invoke(
        fid: &'a FullInvocationId,
        effects: &'a mut Effects,
        state: &'a mut State,
        schemas: &'a Schemas,
        method: &'a str,
        argument: Bytes,
    ) -> Result<Bytes, InvocationError> {
        let this: NonDeterministicBuiltInServiceInvoker<'a, _, _> = Self {
            fid,
            effects,
            state,
            schemas,
        };

        this._invoke(method, argument).await
    }
}

impl<State, Schemas> NonDeterministicBuiltInServiceInvoker<'_, State, Schemas>
where
    State: Send,
    Schemas: KeyExtractor + Send + Sync,
{
    // Function that routes through the available built-in services
    async fn _invoke(self, method: &str, argument: Bytes) -> Result<Bytes, InvocationError> {
        match self.fid.service_id.service_name.deref() {
            restate_pb::INGRESS_SERVICE_NAME => {
                restate_pb::restate::services::IngressInvoker(self)
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
    use restate_pb::restate::services::{
        resolve_awakeable_request, AwakeablesBuiltInService, RejectAwakeableRequest,
        ResolveAwakeableRequest,
    };
    use restate_service_protocol::awakeable_id::AwakeableIdentifier;

    #[async_trait::async_trait]
    impl AwakeablesBuiltInService for DeterministicBuiltInServiceInvoker<'_> {
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

            self.effects
                .send_awakeable_completion(invocation_id, entry_index, Ok(result));
            Ok(())
        }

        async fn reject(&mut self, req: RejectAwakeableRequest) -> Result<(), InvocationError> {
            let (invocation_id, entry_index) = AwakeableIdentifier::decode(req.id)
                .map_err(|e| InvocationError::new(UserErrorCode::InvalidArgument, e.to_string()))?
                .into_inner();

            self.effects.send_awakeable_completion(
                invocation_id,
                entry_index,
                Err(InvocationError::new(UserErrorCode::Unknown, req.reason)),
            );
            Ok(())
        }
    }
}

mod ingress {
    use super::*;

    use restate_pb::restate::services::{IngressBuiltInService, InvokeRequest, InvokeResponse};
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext};

    #[async_trait::async_trait]
    impl<State, Schemas> IngressBuiltInService
        for NonDeterministicBuiltInServiceInvoker<'_, State, Schemas>
    where
        State: Send,
        Schemas: KeyExtractor + Send + Sync,
    {
        async fn invoke(
            &mut self,
            input: InvokeRequest,
        ) -> Result<InvokeResponse, InvocationError> {
            // Extract the key
            let key = self
                .schemas
                .extract(&input.service, &input.method, input.argument.clone())
                .map_err(|err| match err {
                    restate_schema_api::key::KeyExtractorError::NotFound => InvocationError::new(
                        UserErrorCode::NotFound,
                        format!(
                            "Service method {}/{} not found",
                            input.service, input.method
                        ),
                    ),
                    err => InvocationError::new(UserErrorCode::InvalidArgument, err.to_string()),
                })?;

            let fid = FullInvocationId::new(input.service, key, InvocationUuid::now_v7());

            self.effects
                .enqueue_into_outbox(OutboxMessage::ServiceInvocation(ServiceInvocation {
                    fid: fid.clone(),
                    method_name: input.method.into(),
                    argument: input.argument,
                    response_sink: None,
                    span_context: ServiceInvocationSpanContext::empty(),
                }));

            Ok(InvokeResponse {
                id: InvocationId::from(fid).to_string(),
            })
        }
    }
}
