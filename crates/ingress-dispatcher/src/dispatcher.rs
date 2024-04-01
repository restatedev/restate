// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use prost::Message;
use restate_bifrost::Bifrost;
use restate_wal_protocol::append_envelope_to_bifrost;
use tracing::{debug, info, trace};

use restate_core::metadata;
use restate_core::network::MessageHandler;
use restate_node_protocol::codec::Targeted;
use restate_node_protocol::ingress::IngressMessage;
use restate_pb::restate::internal::{
    idempotent_invoke_response, IdempotentInvokeRequest, IdempotentInvokeResponse,
};
use restate_types::errors::InvocationError;
use restate_types::identifiers::{FullInvocationId, InvocationId, ServiceId};
use restate_types::invocation::{self, ServiceInvocation, ServiceInvocationResponseSink};
use restate_types::message::MessageIndex;

use crate::error::IngressDispatchError;
use crate::{
    wrap_service_invocation_in_envelope, ExpiringIngressResponse, IdempotencyMode, IngressRequest,
    IngressRequestMode, IngressResponseSender,
};

/// Dispatches a request from ingress to bifrost
pub trait DispatchIngressRequest {
    fn evict_pending_response(&self, invocation_id: &InvocationId);
    fn dispatch_ingress_request(
        &self,
        ingress_request: IngressRequest,
    ) -> impl std::future::Future<Output = Result<(), IngressDispatchError>> + Send;
}

#[derive(Default)]
struct IngressDispatcherState {
    msg_index: AtomicU64,
    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_responses: DashMap<InvocationId, (MapResponseAction, IngressResponseSender)>,
}

impl IngressDispatcherState {
    pub fn get_and_increment_msg_index(&self) -> MessageIndex {
        self.msg_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct IngressDispatcher {
    bifrost: Bifrost,
    state: Arc<IngressDispatcherState>,
}
impl IngressDispatcher {
    pub fn new(bifrost: Bifrost) -> Self {
        Self {
            bifrost,
            state: Arc::new(IngressDispatcherState::default()),
        }
    }
}

impl DispatchIngressRequest for IngressDispatcher {
    fn evict_pending_response(&self, invocation_id: &InvocationId) {
        self.state.waiting_responses.remove(invocation_id);
    }

    async fn dispatch_ingress_request(
        &self,
        ingress_request: IngressRequest,
    ) -> Result<(), IngressDispatchError> {
        let mut bifrost = self.bifrost.clone();
        let my_node_id = metadata().my_node_id();
        let IngressRequest {
            fid,
            method_name,
            argument,
            span_context,
            request_mode,
            idempotency,
            headers,
        } = ingress_request;

        let invocation_id: InvocationId = fid.clone().into();
        let response_sink = if matches!(request_mode, IngressRequestMode::RequestResponse(_)) {
            Some(ServiceInvocationResponseSink::Ingress(my_node_id))
        } else {
            None
        };

        let (service_invocation, map_response_action) =
            if let IdempotencyMode::Key(idempotency_key, retention_period) = idempotency {
                // Use service name + user provided idempotency key for the actual idempotency key
                let mut idempotency_fid_key = BytesMut::with_capacity(
                    fid.service_id.service_name.len() + idempotency_key.len(),
                );
                idempotency_fid_key.put(fid.service_id.service_name.clone().into_bytes());
                idempotency_fid_key.put(idempotency_key.clone());

                (
                    ServiceInvocation {
                        fid: FullInvocationId::generate(ServiceId::new(
                            restate_pb::IDEMPOTENT_INVOKER_SERVICE_NAME,
                            idempotency_fid_key.freeze(),
                        )),
                        method_name: restate_pb::IDEMPOTENT_INVOKER_INVOKE_METHOD_NAME
                            .to_string()
                            .into(),
                        argument: IdempotentInvokeRequest {
                            idempotency_id: idempotency_key,
                            service_name: fid.service_id.service_name.into(),
                            service_key: fid.service_id.key,
                            invocation_uuid: fid.invocation_uuid.into(),
                            method: method_name.into(),
                            argument,
                            retention_period_sec: retention_period.unwrap_or_default().as_secs()
                                as u32,
                        }
                        .encode_to_vec()
                        .into(),
                        source: invocation::Source::Ingress,
                        response_sink,
                        span_context,
                        headers,
                        execution_time: None,
                    },
                    MapResponseAction::IdempotentInvokerResponse,
                )
            } else {
                (
                    ServiceInvocation {
                        fid,
                        method_name,
                        argument,
                        source: invocation::Source::Ingress,
                        response_sink,
                        span_context,
                        headers,
                        execution_time: None,
                    },
                    MapResponseAction::None,
                )
            };

        let (dedup_source, msg_index) = match request_mode {
            IngressRequestMode::RequestResponse(response_sender) => {
                self.state.waiting_responses.insert(
                    service_invocation.fid.clone().into(),
                    (map_response_action, response_sender),
                );
                (None, self.state.get_and_increment_msg_index())
            }
            IngressRequestMode::FireAndForget => {
                let msg_index = self.state.get_and_increment_msg_index();
                (None, msg_index)
            }
            IngressRequestMode::DedupFireAndForget(dedup_id) => (Some(dedup_id.0), dedup_id.1),
        };

        let envelope = wrap_service_invocation_in_envelope(
            service_invocation,
            my_node_id,
            dedup_source,
            msg_index,
        );
        let (log_id, lsn) = append_envelope_to_bifrost(&mut bifrost, envelope).await?;

        info!(
            restate.invocation.id = %invocation_id,
            log_id = %log_id,
            lsn = %lsn,
            "Ingress request written to bifrost"
        );
        Ok(())
    }
}

impl MessageHandler for IngressDispatcher {
    type MessageType = IngressMessage;

    async fn on_message(&self, msg: restate_node_protocol::MessageEnvelope<Self::MessageType>) {
        let (peer, msg) = msg.split();
        trace!("Processing message '{}' from '{}'", msg.kind(), peer);
        match msg {
            IngressMessage::InvocationResponse(response) => {
                if let Some((_, (map_response_action, sender))) =
                    self.state.waiting_responses.remove(&response.id)
                {
                    let mapped_response = match response.response.into() {
                        Ok(v) => map_response_action.map(v),
                        Err(e) => ExpiringIngressResponse {
                            idempotency_expiry_time: None,
                            result: Err(e),
                        },
                    };
                    if let Err(response) = sender.send(mapped_response) {
                        debug!(
                            "Failed to send response '{:?}' because the handler has been \
                                closed, probably caused by the client connection that went away",
                            response
                        );
                    } else {
                        debug!(
                            restate.invocation.id = %response.id,
                            partition_processor_peer = %peer,
                            "Sent response of invocation out");
                    }
                } else {
                    debug!("Failed to handle response '{:?}' because no handler was found locally waiting for its invocation Id", &response);
                }
            }
        }
    }
}

enum MapResponseAction {
    // We need to map the output type from IdempotentInvokeResponse
    IdempotentInvokerResponse,
    // No need to map the output type
    None,
}

impl MapResponseAction {
    fn map(&self, buf: Bytes) -> ExpiringIngressResponse {
        match self {
            MapResponseAction::IdempotentInvokerResponse => {
                use idempotent_invoke_response::Response;
                let idempotent_invoke_response = match IdempotentInvokeResponse::decode(buf) {
                    Ok(v) => v,
                    Err(_) => {
                        return ExpiringIngressResponse {
                            idempotency_expiry_time: None,
                            result: Err(InvocationError::internal(
                                "Unexpected response from IdempotentInvoker",
                            )),
                        }
                    }
                };

                let result = match idempotent_invoke_response.response {
                    None => Err(InvocationError::internal(
                        "Unexpected response from IdempotentInvoker",
                    )),
                    Some(Response::Success(v)) => Ok(v),
                    Some(Response::Failure(v)) => Err(v.into()),
                };

                ExpiringIngressResponse {
                    idempotency_expiry_time: Some(idempotent_invoke_response.expiry_time),
                    result,
                }
            }
            MapResponseAction::None => ExpiringIngressResponse {
                idempotency_expiry_time: None,
                result: Ok(buf),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::{assert_that, pat};
    use restate_core::network::NetworkSender;
    use restate_core::TestCoreEnvBuilder;
    use restate_node_protocol::ingress::InvocationResponse;
    use restate_types::identifiers::WithPartitionKey;
    use restate_wal_protocol::Command;
    use restate_wal_protocol::Envelope;
    use test_log::test;

    use restate_test_util::{let_assert, matchers::*};
    use restate_types::identifiers::ServiceId;
    use restate_types::invocation::{ResponseResult, SpanRelation};
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::partition_table::{FindPartition, FixedPartitionTable};
    use restate_types::Version;

    #[test(tokio::test)]
    async fn idempotent_invoke() -> anyhow::Result<()> {
        // set it to 1 partition so that we know where the invocation for the IdempotentInvoker goes to
        let mut env_builder = TestCoreEnvBuilder::new_with_mock_network()
            .add_mock_nodes_config()
            .with_partition_table(FixedPartitionTable::new(Version::MIN, 1));

        let bifrost_svc =
            restate_bifrost::Options::memory().build(env_builder.metadata_store_client.clone());
        let bifrost = bifrost_svc.handle();
        let dispatcher = IngressDispatcher::new(bifrost.clone());

        env_builder = env_builder.add_message_handler(dispatcher.clone());
        let node_env = env_builder.build().await;

        node_env
            .tc
            .run_in_scope("test", None, async {
                bifrost_svc.start().await?;

                // Ask for a response, then drop the receiver
                let fid = FullInvocationId::generate(ServiceId::new("MySvc", "MyKey"));
                let argument = Bytes::from_static(b"nbfjksdfs");
                let idempotency_key = Bytes::copy_from_slice(b"123");
                let (invocation, res) = IngressRequest::invocation(
                    fid.clone(),
                    "pippo",
                    argument.clone(),
                    SpanRelation::None,
                    IdempotencyMode::key(idempotency_key.clone(), None),
                    vec![],
                );
                dispatcher.dispatch_ingress_request(invocation).await?;

                // Let's check we correct have generated a bifrost write
                let partition_id = node_env
                    .metadata
                    .partition_table()
                    .unwrap()
                    .find_partition_id(fid.partition_key())?;
                let log_id = LogId::from(partition_id);
                let log_record = bifrost.read_next_single(log_id, Lsn::INVALID).await?;

                let output_message =
                    Envelope::decode_with_bincode(log_record.record.payload().unwrap().as_ref())?;

                let_assert!(
                    Envelope {
                        command: Command::Invoke(service_invocation),
                        ..
                    } = output_message
                );
                assert_that!(
                    service_invocation,
                    pat!(ServiceInvocation {
                        fid: pat!(FullInvocationId {
                            service_id: pat!(ServiceId {
                                service_name: displays_as(eq(
                                    restate_pb::IDEMPOTENT_INVOKER_SERVICE_NAME
                                )),
                                key: eq(Bytes::copy_from_slice(b"MySvc123")),
                            }),
                        }),
                        method_name: displays_as(eq(
                            restate_pb::IDEMPOTENT_INVOKER_INVOKE_METHOD_NAME
                        )),
                        argument: protobuf_decoded(pat!(IdempotentInvokeRequest {
                            idempotency_id: eq(idempotency_key),
                            service_name: eq("MySvc"),
                            service_key: eq("MyKey"),
                            method: eq("pippo"),
                            argument: eq(argument),
                            retention_period_sec: eq(0)
                        }))
                    })
                );

                // Now check we get the response is routed back to the handler correctly
                let response = Bytes::from_static(b"vmoaifnuei");
                let expiry_time = "2023-09-25T07:47:58.661309Z".to_string();
                node_env
                    .network_sender
                    .send(
                        metadata().my_node_id().into(),
                        &IngressMessage::InvocationResponse(InvocationResponse {
                            id: service_invocation.fid.into(),
                            response: ResponseResult::Success(
                                IdempotentInvokeResponse {
                                    expiry_time: expiry_time.clone(),
                                    response: Some(idempotent_invoke_response::Response::Success(
                                        response.clone(),
                                    )),
                                }
                                .encode_to_vec()
                                .into(),
                            ),
                        }),
                    )
                    .await?;

                assert_that!(
                    res.await?,
                    pat!(ExpiringIngressResponse {
                        idempotency_expiry_time: some(eq(expiry_time)),
                        result: ok(eq(response))
                    })
                );

                Ok(())
            })
            .await
    }
}
