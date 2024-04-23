// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::error::IngressDispatchError;
use crate::{
    wrap_service_invocation_in_envelope, IngressCorrelationId, IngressDispatcherRequest,
    IngressDispatcherResponse, IngressRequestMode, IngressResponseSender,
};
use dashmap::DashMap;
use restate_bifrost::Bifrost;
use restate_core::metadata;
use restate_core::network::MessageHandler;
use restate_node_protocol::codec::Targeted;
use restate_node_protocol::ingress::IngressMessage;
use restate_types::invocation::{self, ServiceInvocation, ServiceInvocationResponseSink};
use restate_types::message::MessageIndex;
use restate_wal_protocol::append_envelope_to_bifrost;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::{debug, info, trace};

/// Dispatches a request from ingress to bifrost
pub trait DispatchIngressRequest {
    fn evict_pending_response(&self, correlation_id: &IngressCorrelationId);
    fn dispatch_ingress_request(
        &self,
        ingress_request: IngressDispatcherRequest,
    ) -> impl std::future::Future<Output = Result<(), IngressDispatchError>> + Send;
}

#[derive(Default)]
struct IngressDispatcherState {
    msg_index: AtomicU64,
    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_responses: DashMap<IngressCorrelationId, IngressResponseSender>,
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
    fn evict_pending_response(&self, ingress_correlation_id: &IngressCorrelationId) {
        self.state.waiting_responses.remove(ingress_correlation_id);
    }

    async fn dispatch_ingress_request(
        &self,
        ingress_request: IngressDispatcherRequest,
    ) -> Result<(), IngressDispatchError> {
        let mut bifrost = self.bifrost.clone();
        let my_node_id = metadata().my_node_id();
        let IngressDispatcherRequest {
            correlation_id,
            invocation_id,
            invocation_target,
            argument,
            span_context,
            request_mode,
            idempotency,
            headers,
            execution_time,
        } = ingress_request;

        let response_sink = if matches!(request_mode, IngressRequestMode::RequestResponse(_)) {
            Some(ServiceInvocationResponseSink::Ingress(my_node_id))
        } else {
            None
        };

        let service_invocation = ServiceInvocation {
            invocation_id,
            invocation_target,
            argument,
            source: invocation::Source::Ingress,
            response_sink,
            span_context,
            headers,
            execution_time,
            idempotency,
        };

        let (dedup_source, msg_index) = match request_mode {
            IngressRequestMode::RequestResponse(response_sender) => {
                self.state
                    .waiting_responses
                    .insert(correlation_id, response_sender);
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
            IngressMessage::InvocationResponse(invocation_response) => {
                let correlation_id = invocation_response
                    .idempotency_id
                    .as_ref()
                    .map(|idempotency_id| {
                        IngressCorrelationId::IdempotencyId(idempotency_id.clone())
                    })
                    .unwrap_or_else(|| {
                        IngressCorrelationId::InvocationId(invocation_response.invocation_id)
                    });
                if let Some((_, sender)) = self.state.waiting_responses.remove(&correlation_id) {
                    let dispatcher_response = IngressDispatcherResponse {
                        // TODO we need to add back the expiration time for idempotent results
                        idempotency_expiry_time: None,
                        result: invocation_response.response,
                    };
                    if let Err(response) = sender.send(dispatcher_response) {
                        debug!(
                            "Failed to send response '{:?}' because the handler has been \
                                closed, probably caused by the client connection that went away",
                            response
                        );
                    } else {
                        debug!(
                            restate.invocation.id = %invocation_response.invocation_id,
                            partition_processor_peer = %peer,
                            "Sent response of invocation out"
                        );
                    }
                } else {
                    debug!("Failed to handle response '{:?}' because no handler was found locally waiting for its invocation Id", &invocation_response);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use bytes::Bytes;
    use bytestring::ByteString;
    use googletest::{assert_that, pat};
    use restate_core::network::NetworkSender;
    use restate_core::TestCoreEnvBuilder;
    use restate_node_protocol::ingress::InvocationResponse;
    use restate_test_util::{let_assert, matchers::*};
    use restate_types::identifiers::{IdempotencyId, InvocationId, WithPartitionKey};
    use restate_types::invocation::{
        HandlerType, Idempotency, InvocationTarget, ResponseResult, SpanRelation,
    };
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::partition_table::{FindPartition, FixedPartitionTable};
    use restate_types::Version;
    use restate_wal_protocol::Command;
    use restate_wal_protocol::Envelope;
    use test_log::test;

    #[test(tokio::test)]
    async fn idempotent_invoke() -> anyhow::Result<()> {
        // set it to 1 partition so that we know where the invocation for the IdempotentInvoker goes to
        let mut env_builder = TestCoreEnvBuilder::new_with_mock_network()
            .add_mock_nodes_config()
            .with_partition_table(FixedPartitionTable::new(Version::MIN, 1));

        let bifrost_svc = restate_bifrost::BifrostService::new(env_builder.metadata.clone());
        let bifrost = bifrost_svc.handle();
        let dispatcher = IngressDispatcher::new(bifrost.clone());

        env_builder = env_builder.add_message_handler(dispatcher.clone());
        let node_env = env_builder.build().await;

        node_env
            .tc
            .run_in_scope("test", None, async {
                bifrost_svc.start().await?;

                // Ask for a response, then drop the receiver
                let invocation_target = InvocationTarget::virtual_object(
                    "MySvc",
                    "MyKey",
                    "pippo",
                    HandlerType::Exclusive,
                );
                let argument = Bytes::from_static(b"nbfjksdfs");
                let idempotency_key = ByteString::from_static("123");
                let invocation_id = InvocationId::generate_with_idempotency_key(
                    &invocation_target,
                    Some(idempotency_key.clone()),
                );
                let idempotency_id = IdempotencyId::combine(
                    invocation_id,
                    &invocation_target,
                    idempotency_key.clone(),
                );

                let (invocation, res) = IngressDispatcherRequest::invocation(
                    invocation_id,
                    invocation_target.clone(),
                    argument.clone(),
                    SpanRelation::None,
                    Some(Idempotency {
                        key: idempotency_key.clone(),
                        retention: Duration::from_secs(60),
                    }),
                    vec![],
                );
                dispatcher.dispatch_ingress_request(invocation).await?;

                // Let's check we correct have generated a bifrost write
                let partition_id = node_env
                    .metadata
                    .partition_table()
                    .unwrap()
                    .find_partition_id(invocation_id.partition_key())?;
                let log_id = LogId::from(partition_id);
                let log_record = bifrost.read_next_single(log_id, Lsn::INVALID).await?;

                let output_message =
                    Envelope::from_bytes(log_record.record.payload().unwrap().as_ref())?;

                let_assert!(
                    Envelope {
                        command: Command::Invoke(service_invocation),
                        ..
                    } = output_message
                );
                assert_that!(
                    service_invocation,
                    pat!(ServiceInvocation {
                        invocation_id: eq(invocation_id),
                        invocation_target: eq(invocation_target.clone()),
                        argument: eq(argument.clone()),
                        idempotency: some(eq(Idempotency {
                            key: idempotency_key.clone(),
                            retention: Duration::from_secs(60),
                        }))
                    })
                );

                // Now check we get the response is routed back to the handler correctly
                let response = Bytes::from_static(b"vmoaifnuei");
                node_env
                    .network_sender
                    .send(
                        metadata().my_node_id().into(),
                        &IngressMessage::InvocationResponse(InvocationResponse {
                            invocation_id: service_invocation.invocation_id,
                            idempotency_id: Some(idempotency_id),
                            response: ResponseResult::Success(response.clone()),
                        }),
                    )
                    .await?;

                assert_that!(
                    res.await?,
                    pat!(IngressDispatcherResponse {
                        result: eq(ResponseResult::Success(response))
                    })
                );

                Ok(())
            })
            .await
    }
}
