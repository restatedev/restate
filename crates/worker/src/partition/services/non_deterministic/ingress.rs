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

use prost_reflect::{DeserializeOptions, ReflectMessage};
use restate_pb::builtin_service::ResponseSerializer;
use restate_pb::restate::*;
use restate_schema_api::json::{JsonMapperResolver, JsonToProtobufMapper};
use restate_schema_api::key::KeyExtractor;
use restate_types::identifiers::InvocationUuid;
use restate_types::invocation::ServiceInvocation;
use serde::Serialize;
use tracing::instrument;

impl<'a, State> InvocationContext<'a, State> {
    fn pb_struct_to_pb(
        &mut self,
        service_name: &str,
        method_name: &str,
        json: prost_reflect::prost_types::Struct,
    ) -> Result<Bytes, InvocationError> {
        let serde_json_value = json
            .transcode_to_dynamic()
            .serialize(serde_json::value::Serializer)
            .map_err(InvocationError::internal)?;

        let (json_to_proto, _) = self
            .schemas
            .resolve_json_mapper_for_service(service_name, method_name)
            .ok_or_else(|| InvocationError::service_method_not_found(service_name, method_name))?;

        json_to_proto
            .json_value_to_protobuf(serde_json_value, &DeserializeOptions::default())
            .map_err(|e| InvocationError::new(UserErrorCode::FailedPrecondition, e))
    }

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
                restate_schema_api::key::KeyExtractorError::NotFound => {
                    InvocationError::service_method_not_found(&service_name, method_name)
                }
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
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %self.full_invocation_id,
            restate.ingress_invoke.target_service = request.service,
            restate.ingress_invoke.target_method = request.method
        )
    )]
    async fn invoke(
        &mut self,
        request: InvokeRequest,
        response_serializer: ResponseSerializer<InvokeResponse>,
    ) -> Result<(), InvocationError> {
        use restate_pb::restate::invoke_request::Argument;

        // Extract the argument
        let argument = match request.argument {
            None => Bytes::new(),
            Some(Argument::Pb(bytes)) => bytes,
            Some(Argument::Json(json)) => {
                self.pb_struct_to_pb(&request.service, &request.method, json)?
            }
        };

        // Extract the fid
        let fid = self.generate_fid(request.service, &request.method, &argument)?;

        // Respond to caller
        self.reply_to_caller(response_serializer.serialize_success(InvokeResponse {
            id: fid.to_string(),
        }));

        // Invoke service
        self.send_message(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
            fid,
            request.method,
            argument,
            None,
            self.span_context.as_linked(),
        )));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::services::non_deterministic::tests::TestInvocationContext;
    use googletest::assert_that;
    use googletest::{all, pat};
    use prost::Message;
    use prost_reflect::DynamicMessage;
    use restate_schema_api::deployment::DeploymentMetadata;
    use restate_test_util::matchers::*;
    use restate_test_util::test;
    use restate_types::invocation::ServiceInvocation;

    fn mock_schemas() -> Schemas {
        let schemas = Schemas::default();

        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        DeploymentMetadata::mock(),
                        vec![restate_pb::mocks::GREETER_SERVICE_NAME.to_owned()],
                        restate_pb::mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas
    }

    #[test(tokio::test)]
    async fn invoke_with_pb_argument() {
        invoke_test(|expected_req| {
            invoke_request::Argument::Pb(expected_req.encode_to_vec().into())
        })
        .await
    }

    #[test(tokio::test)]
    async fn invoke_with_json_argument() {
        invoke_test(|expected_req| {
            let mut expected_req_dynamic = DynamicMessage::new(
                restate_pb::mocks::DESCRIPTOR_POOL
                    .get_message_by_name("greeter.GreetingRequest")
                    .unwrap(),
            );
            expected_req_dynamic.transcode_from(expected_req).unwrap();

            let actual_req_argument = DynamicMessage::deserialize(
                prost_reflect::prost_types::Struct::default().descriptor(),
                expected_req_dynamic
                    .serialize(serde_json::value::Serializer)
                    .unwrap(),
            )
            .unwrap();

            invoke_request::Argument::Json(actual_req_argument.transcode_to().unwrap())
        })
        .await
    }

    async fn invoke_test(
        serialize_pb_request: impl FnOnce(
            &restate_pb::mocks::greeter::GreetingRequest,
        ) -> invoke_request::Argument,
    ) {
        let mut ctx = TestInvocationContext::new(restate_pb::INGRESS_SERVICE_NAME)
            .with_schemas(mock_schemas());

        let expected_req = restate_pb::mocks::greeter::GreetingRequest {
            person: "Francesco".to_string(),
        };
        let (fid, effects) = ctx
            .invoke(|ctx| {
                ctx.invoke(
                    InvokeRequest {
                        service: restate_pb::mocks::GREETER_SERVICE_NAME.to_string(),
                        method: "Greet".to_string(),
                        argument: Some(serialize_pb_request(&expected_req)),
                    },
                    ResponseSerializer::default(),
                )
            })
            .await
            .unwrap();

        assert_that!(
            effects,
            all!(
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::IngressResponse {
                        full_invocation_id: eq(fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            restate_pb::restate::InvokeResponse { id: anything() }
                        ))))
                    }
                )))),
                contains(pat!(Effect::OutboxMessage(pat!(
                    OutboxMessage::ServiceInvocation(pat!(ServiceInvocation {
                        fid: pat!(FullInvocationId {
                            service_id: pat!(ServiceId {
                                service_name: displays_as(eq(
                                    restate_pb::mocks::GREETER_SERVICE_NAME
                                ))
                            })
                        }),
                        method_name: displays_as(eq("Greet")),
                        argument: protobuf_decoded(eq(expected_req)),
                        response_sink: none()
                    }))
                ))))
            )
        );
    }
}
