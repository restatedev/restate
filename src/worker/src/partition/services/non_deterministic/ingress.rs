// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::services::non_deterministic::*;

use restate_pb::builtin_service::ResponseSerializer;
use restate_pb::restate::{IngressBuiltInService, InvokeRequest, InvokeResponse};
use restate_schema_api::key::KeyExtractor;
use restate_types::identifiers::InvocationUuid;
use restate_types::invocation::{ServiceInvocation, SpanRelation};

impl<'a, State> InvocationContext<'a, State> {
    fn generate_fid(
        &self,
        service_name: String,
        method_name: &str,
        argument: &Bytes,
    ) -> Result<FullInvocationId, InvocationError> {
        let key = self
            .schemas
            .extract(&service_name, method_name, argument.clone())
            .map_err(|err| match err {
                restate_schema_api::key::KeyExtractorError::NotFound => InvocationError::new(
                    UserErrorCode::NotFound,
                    format!("Service method {}/{} not found", service_name, method_name),
                ),
                err => InvocationError::new(UserErrorCode::InvalidArgument, err.to_string()),
            })?;

        Ok(FullInvocationId::new(
            service_name,
            key,
            InvocationUuid::now_v7(),
        ))
    }
}

#[async_trait::async_trait]
impl<'a, State: StateReader + Send + Sync> IngressBuiltInService for InvocationContext<'a, State> {
    async fn invoke(
        &mut self,
        request: InvokeRequest,
        response_serializer: ResponseSerializer<InvokeResponse>,
    ) -> Result<(), InvocationError> {
        // Extract the fid
        let fid = self.generate_fid(request.service, &request.method, &request.argument)?;

        // Respond to caller
        self.reply_to_caller(response_serializer.serialize_success(InvokeResponse {
            id: fid.to_string(),
        }));

        // Invoke service
        self.send_message(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
            fid,
            request.method,
            request.argument,
            None,
            self.span_context.as_linked(),
        )));

        Ok(())
    }
}
