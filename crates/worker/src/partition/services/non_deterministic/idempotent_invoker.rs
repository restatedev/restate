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

use crate::partition::types::create_response_message;
use prost::Message;
use restate_pb::builtin_service::ResponseSerializer;
use restate_pb::restate::internal::*;
use restate_types::identifiers::InvocationUuid;
use restate_types::invocation::{ServiceInvocation, SpanRelation};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use tracing::{instrument, trace};

// -- State entries of this service
#[derive(Serialize, Deserialize, Debug)]
struct RequestMetaState {
    service_name: String,
    method_name: String,
    retention_period_sec: u32,
}
const REQUEST_META: StateKey<Bincode<RequestMetaState>> = StateKey::new_bincode("request_meta");

type ResultState = IdempotentInvokeResponse;
const RESPONSE: StateKey<Protobuf<ResultState>> = StateKey::new_pb("response");

type SinksState = Vec<(FullInvocationId, ServiceInvocationResponseSink)>;
const SINKS: StateKey<Bincode<SinksState>> = StateKey::new_bincode("sinks");

const DEFAULT_RETENTION_PERIOD: u32 = 30 * 60;

impl<'a, State: StateReader + Send + Sync> IdempotentInvokerBuiltInService
    for InvocationContext<'a, State>
{
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %self.full_invocation_id,
            restate.ingress_idempotent_invoke.id = ?request.idempotency_id,
            restate.ingress_idempotent_invoke.target_service = request.service_name,
            restate.ingress_idempotent_invoke.target_method = request.method
        )
    )]
    async fn invoke(
        &mut self,
        request: IdempotentInvokeRequest,
        _response_serializer: ResponseSerializer<IdempotentInvokeResponse>,
    ) -> Result<(), InvocationError> {
        if let Some(res) = self.load_state_raw(&RESPONSE).await? {
            trace!("Already got the response");
            self.reply_to_caller(ResponseResult::Success(res));
            return Ok(());
        }

        // Store this sink
        if let Some(response_sink) = self.response_sink.take() {
            // In this case we just store callee id and sink, to make sure it will be invoked
            #[allow(clippy::mutable_key_type)]
            let mut sinks = self.load_state(&SINKS).await?.unwrap_or_default();
            sinks.push((self.full_invocation_id.clone(), response_sink.clone()));
            self.set_state(&SINKS, &sinks)?;
        }

        if self.load_state(&REQUEST_META).await?.is_some() {
            trace!("Target service already invoked");
            // --- Case when we already received the request, but we don't have a result yet. No need to re-invoke
            return Ok(());
        }

        // --- Case when we see this request here for the first time

        // Extract the fid
        let fid = FullInvocationId::new(
            request.service_name.clone(),
            request.service_key,
            InvocationUuid::from_slice(&request.invocation_uuid)
                .map_err(InvocationError::internal)?,
        );

        // Store request meta
        self.set_state(
            &REQUEST_META,
            &RequestMetaState {
                service_name: request.service_name.to_string(),
                method_name: request.method.to_string(),
                retention_period_sec: if request.retention_period_sec == 0 {
                    DEFAULT_RETENTION_PERIOD
                } else {
                    request.retention_period_sec
                },
            },
        )?;

        trace!(restate.invocation.id = %fid, "Invoking target service");

        // Invoke service
        self.outbox_message(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
            fid,
            request.method,
            request.argument,
            Source::Service(self.full_invocation_id.clone()),
            Some(ServiceInvocationResponseSink::NewInvocation {
                target: FullInvocationId::generate(self.full_invocation_id.service_id.clone()),
                method: restate_pb::IDEMPOTENT_INVOKER_INTERNAL_ON_RESPONSE_METHOD_NAME.to_string(),
                caller_context: Default::default(),
            }),
            SpanRelation::None,
        )));

        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %self.full_invocation_id
        )
    )]
    async fn internal_on_response(
        &mut self,
        result: ServiceInvocationSinkRequest,
        _: ResponseSerializer<()>,
    ) -> Result<(), InvocationError> {
        let request_meta: RequestMetaState = self
            .load_state(&REQUEST_META)
            .await?
            .ok_or_else(|| InvocationError::internal("request meta should be non empty"))?;
        let expiry_time =
            SystemTime::now() + Duration::from_secs(request_meta.retention_period_sec as u64);

        trace!("Got response for {:?}: {:?}", request_meta, result);

        // Generate response
        let response = IdempotentInvokeResponse {
            expiry_time: humantime::format_rfc3339(expiry_time).to_string(),
            response: Some(
                result
                    .response
                    .map(|r| match r {
                        service_invocation_sink_request::Response::Success(v) => {
                            idempotent_invoke_response::Response::Success(v)
                        }
                        service_invocation_sink_request::Response::Failure(f) => {
                            idempotent_invoke_response::Response::Failure(f)
                        }
                    })
                    .ok_or_else(|| {
                        InvocationError::internal(
                            "ServiceInvocationSinkRequest.response should not be empty",
                        )
                    })?,
            ),
        };

        // Store expiry time and response
        self.set_state(&RESPONSE, &response)?;

        // Set response timer
        self.delay_invoke(
            FullInvocationId::generate(self.full_invocation_id.service_id.clone()),
            restate_pb::IDEMPOTENT_INVOKER_INTERNAL_ON_TIMER_METHOD_NAME.to_string(),
            Bytes::new(),
            Source::Service(self.full_invocation_id.clone()),
            None,
            expiry_time.into(),
            0,
        );

        // Send response to registered sinks
        let encoded_response = Bytes::from(response.encode_to_vec());
        for (callee_fid, sink) in self
            .pop_state(&SINKS)
            .await?
            .unwrap_or_default()
            .into_iter()
        {
            self.send_response(create_response_message(
                &callee_fid,
                sink,
                ResponseResult::Success(encoded_response.clone()),
            ))
        }

        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %self.full_invocation_id
        )
    )]
    async fn internal_on_timer(
        &mut self,
        _: (),
        _: ResponseSerializer<()>,
    ) -> Result<(), InvocationError> {
        trace!("Cleaning up state");

        self.clear_state(&SINKS);
        self.clear_state(&REQUEST_META);
        self.clear_state(&RESPONSE);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::FutureExt;
    use googletest::{all, pat};
    use googletest::{assert_that, elements_are};
    use test_log::test;

    use restate_test_util::matchers::*;
    use restate_types::invocation::ServiceInvocation;

    use crate::partition::services::non_deterministic::tests::TestInvocationContext;

    #[test(tokio::test)]
    async fn idempotent_invoke_test() {
        let mut ctx = TestInvocationContext::from_service_id(ServiceId::new(
            restate_pb::INGRESS_SERVICE_NAME,
            Bytes::copy_from_slice(b"123456"),
        ));

        let expected_greeter_invocation_fid = FullInvocationId::generate(ServiceId::new(
            restate_pb::mocks::GREETER_SERVICE_NAME,
            Bytes::copy_from_slice(b"654321"),
        ));
        let expected_req = restate_pb::mocks::greeter::GreetingRequest {
            person: "Francesco".to_string(),
        };
        let expected_res = restate_pb::mocks::greeter::GreetingResponse {
            greeting: "Hello Francesco!".to_string(),
        };

        let (expected_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.invoke(
                    IdempotentInvokeRequest {
                        service_name: expected_greeter_invocation_fid
                            .service_id
                            .service_name
                            .to_string(),
                        service_key: expected_greeter_invocation_fid.service_id.key.clone(),
                        invocation_uuid: expected_greeter_invocation_fid.invocation_uuid.into(),
                        method: "Greet".to_string(),
                        argument: expected_req.encode_to_vec().into(),
                        ..IdempotentInvokeRequest::default()
                    },
                    ResponseSerializer::default(),
                )
                .boxed_local()
            })
            .await
            .unwrap();

        // Assert state
        assert_that!(
            ctx.state().assert_has_state(&REQUEST_META),
            pat!(RequestMetaState {
                service_name: eq(restate_pb::mocks::GREETER_SERVICE_NAME),
                method_name: eq("Greet"),
                retention_period_sec: eq(DEFAULT_RETENTION_PERIOD)
            })
        );
        assert_that!(
            ctx.state().assert_has_state(&SINKS),
            elements_are![eq((
                expected_fid.clone(),
                ctx.response_sink().unwrap().clone()
            ))]
        );
        ctx.state().assert_has_not_state(&RESPONSE);

        // Assert contains outbox message effect
        assert_that!(
            effects,
            contains(pat!(BuiltinServiceEffect::OutboxMessage(pat!(
                OutboxMessage::ServiceInvocation(pat!(ServiceInvocation {
                    fid: eq(expected_greeter_invocation_fid),
                    method_name: displays_as(eq("Greet")),
                    argument: protobuf_decoded(eq(expected_req.clone())),
                    response_sink: some(pat!(ServiceInvocationResponseSink::NewInvocation {
                        target: pat!(FullInvocationId {
                            service_id: eq(expected_fid.service_id.clone())
                        }),
                        method: eq(restate_pb::IDEMPOTENT_INVOKER_INTERNAL_ON_RESPONSE_METHOD_NAME)
                    }))
                }))
            ))))
        );
        // Assert doesn't contain ingress response
        assert_that!(
            effects,
            not(contains(pat!(BuiltinServiceEffect::IngressResponse(pat!(
                IngressResponse { .. }
            )))))
        );

        // Now let's complete the execution
        let (_, effects) = ctx
            .invoke(|ctx| {
                IdempotentInvokerBuiltInService::internal_on_response(
                    ctx,
                    ServiceInvocationSinkRequest {
                        caller_context: Default::default(),
                        response: Some(
                            ResponseResult::Success(expected_res.encode_to_vec().into()).into(),
                        ),
                    },
                    ResponseSerializer::default(),
                )
                .boxed_local()
            })
            .await
            .unwrap();

        // There should be no pending sinks
        ctx.state().assert_has_not_state(&SINKS);
        // There should be a result
        ctx.state().assert_has_state(&RESPONSE);

        assert_that!(
            effects,
            all!(
                contains(pat!(BuiltinServiceEffect::IngressResponse(pat!(
                    IngressResponse {
                        full_invocation_id: eq(expected_fid.clone()),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            IdempotentInvokeResponse {
                                response: some(pat!(
                                    idempotent_invoke_response::Response::Success(
                                        protobuf_decoded(eq(expected_res.clone()))
                                    )
                                ))
                            }
                        ))))
                    }
                )))),
                contains(pat!(BuiltinServiceEffect::DelayedInvoke {
                    target_fid: pat!(FullInvocationId {
                        service_id: eq(expected_fid.service_id.clone())
                    }),
                    target_method: eq(restate_pb::IDEMPOTENT_INVOKER_INTERNAL_ON_TIMER_METHOD_NAME)
                }))
            )
        );

        // Next invocation returns the same value and doesn't generate new invocation
        let (expected_fid, effects) = ctx
            .invoke(|ctx| {
                ctx.invoke(
                    IdempotentInvokeRequest {
                        service_name: restate_pb::mocks::GREETER_SERVICE_NAME.to_string(),
                        method: "Greet".to_string(),
                        argument: expected_req.encode_to_vec().into(),
                        ..IdempotentInvokeRequest::default()
                    },
                    ResponseSerializer::default(),
                )
                .boxed_local()
            })
            .await
            .unwrap();

        // Contains the response, but not a new invocation
        assert_that!(
            effects,
            all!(
                contains(pat!(BuiltinServiceEffect::IngressResponse(pat!(
                    IngressResponse {
                        full_invocation_id: eq(expected_fid),
                        response: pat!(ResponseResult::Success(protobuf_decoded(pat!(
                            IdempotentInvokeResponse {
                                response: some(pat!(
                                    idempotent_invoke_response::Response::Success(
                                        protobuf_decoded(eq(expected_res.clone()))
                                    )
                                ))
                            }
                        ))))
                    }
                )))),
                not(contains(pat!(BuiltinServiceEffect::OutboxMessage(pat!(
                    OutboxMessage::ServiceInvocation(_)
                )))))
            )
        );

        // Cleanup
        let (_, _) = ctx
            .invoke(|ctx| {
                ctx.internal_on_timer((), ResponseSerializer::default())
                    .boxed_local()
            })
            .await
            .unwrap();

        // Nothing remaining in the state storage
        ctx.state().assert_is_empty();
    }
}
