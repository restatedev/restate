// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{
    error::IngressDispatchError, IngressDispatcherRequest, IngressDispatcherRequestInner,
    IngressInvocationResponse, IngressInvocationResponseSender, IngressRequestMode,
    IngressSubmittedInvocationNotificationSender, SubmittedInvocationNotification,
};

use dashmap::DashMap;
use restate_bifrost::Bifrost;
use restate_core::metadata;
use restate_core::network::MessageHandler;
use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{IngressRequestId, PartitionKey, WithPartitionKey};
use restate_types::message::MessageIndex;
use restate_types::net::codec::Targeted;
use restate_types::net::ingress::IngressMessage;
use restate_types::net::MessageEnvelope;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::{
    append_envelope_to_bifrost, Command, Destination, Envelope, Header, Source,
};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::{debug, trace};

/// Dispatches a request from ingress to bifrost
pub trait DispatchIngressRequest {
    fn evict_pending_response(&self, req_id: IngressRequestId);
    fn evict_pending_submit_notification(&self, req_id: IngressRequestId);
    fn dispatch_ingress_request(
        &self,
        ingress_request: IngressDispatcherRequest,
    ) -> impl std::future::Future<Output = Result<(), IngressDispatchError>> + Send;
}

#[derive(Default)]
struct IngressDispatcherState {
    msg_index: AtomicU64,

    // TODO those two maps below can be replaced with the ResponseTracker from the network module

    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_responses: DashMap<IngressRequestId, IngressInvocationResponseSender>,

    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_submit_notification:
        DashMap<IngressRequestId, IngressSubmittedInvocationNotificationSender>,
}

impl IngressDispatcherState {
    fn get_and_increment_msg_index(&self) -> MessageIndex {
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
    fn evict_pending_response(&self, req_id: IngressRequestId) {
        self.state.waiting_responses.remove(&req_id);
    }

    fn evict_pending_submit_notification(&self, req_id: IngressRequestId) {
        self.state.waiting_submit_notification.remove(&req_id);
    }

    async fn dispatch_ingress_request(
        &self,
        ingress_request: IngressDispatcherRequest,
    ) -> Result<(), IngressDispatchError> {
        let mut bifrost = self.bifrost.clone();
        let IngressDispatcherRequest {
            inner,
            request_mode,
        } = ingress_request;

        let (dedup_source, msg_index, proxying_partition_key) = match request_mode {
            IngressRequestMode::RequestResponse(ingress_response_key, response_sender) => {
                self.state
                    .waiting_responses
                    .insert(ingress_response_key, response_sender);
                (None, self.state.get_and_increment_msg_index(), None)
            }
            IngressRequestMode::WaitSubmitNotification(id, tx) => {
                self.state.waiting_submit_notification.insert(id, tx);
                let msg_index = self.state.get_and_increment_msg_index();
                (None, msg_index, None)
            }
            IngressRequestMode::FireAndForget => {
                let msg_index = self.state.get_and_increment_msg_index();
                (None, msg_index, None)
            }
            IngressRequestMode::DedupFireAndForget {
                deduplication_id,
                proxying_partition_key,
            } => (
                Some(deduplication_id.0),
                deduplication_id.1,
                proxying_partition_key,
            ),
        };

        let partition_key = proxying_partition_key.unwrap_or_else(|| inner.partition_key());

        let envelope = wrap_service_invocation_in_envelope(
            partition_key,
            inner,
            metadata().my_node_id(),
            dedup_source,
            msg_index,
        );
        let (log_id, lsn) = append_envelope_to_bifrost(&mut bifrost, envelope).await?;

        debug!(
            log_id = %log_id,
            lsn = %lsn,
            "Ingress request written to bifrost"
        );
        Ok(())
    }
}

impl MessageHandler for IngressDispatcher {
    type MessageType = IngressMessage;

    async fn on_message(&self, msg: MessageEnvelope<Self::MessageType>) {
        let (peer, msg) = msg.split();
        trace!("Processing message '{}' from '{}'", msg.kind(), peer);

        match msg {
            IngressMessage::InvocationResponse(invocation_response) => {
                if let Some((_, tx)) = self
                    .state
                    .waiting_responses
                    .remove(&invocation_response.request_id)
                {
                    let dispatcher_response = IngressInvocationResponse {
                        // TODO we need to add back the expiration time for idempotent results
                        idempotency_expiry_time: None,
                        result: invocation_response.response.clone(),
                        invocation_id: invocation_response.invocation_id,
                    };
                    if let Err(response) = tx.send(dispatcher_response) {
                        debug!(
                            "Ignoring response '{:?}' because the handler has been \
                                closed, probably caused by the client connection that went away",
                            response
                        );
                    } else {
                        trace!(
                            partition_processor_peer = %peer,
                            "Sent response of invocation {:?} out",
                            invocation_response.invocation_id
                        );
                    }
                } else {
                    debug!(
                        "Ignoring response to request id '{}' and invocation id '{:?}' because no handler was found locally waiting",
                        &invocation_response.request_id, invocation_response.invocation_id
                    );
                }
            }
            IngressMessage::SubmittedInvocationNotification(attach_idempotent_invocation) => {
                if let Some((_, sender)) = self
                    .state
                    .waiting_submit_notification
                    .remove(&attach_idempotent_invocation.request_id)
                {
                    if let Err(response) = sender.send(SubmittedInvocationNotification {
                        invocation_id: attach_idempotent_invocation.attached_invocation_id,
                    }) {
                        trace!(
                            "Ignoring submit notification '{:?}' because the handler has been \
                                closed, probably caused by the client connection that went away",
                            response
                        );
                    } else {
                        trace!(
                            restate.invocation.id = %attach_idempotent_invocation.original_invocation_id,
                            partition_processor_peer = %peer,
                            "Sent response of invocation out"
                        );
                    }
                } else {
                    trace!("Ignoring submit notification '{:?}' because no handler was found locally waiting for its invocation Id", &attach_idempotent_invocation.original_invocation_id);
                }
            }
        }
    }
}

fn wrap_service_invocation_in_envelope(
    partition_key: PartitionKey,
    inner: IngressDispatcherRequestInner,
    from_node_id: GenerationalNodeId,
    deduplication_source: Option<String>,
    msg_index: MessageIndex,
) -> Envelope {
    let header = Header {
        source: Source::Ingress {
            node_id: from_node_id,
            nodes_config_version: metadata().nodes_config_version(),
        },
        dest: Destination::Processor {
            partition_key,
            dedup: deduplication_source.map(|src| DedupInformation::ingress(src, msg_index)),
        },
    };

    Envelope::new(
        header,
        match inner {
            IngressDispatcherRequestInner::Invoke(si) => Command::Invoke(si),
            IngressDispatcherRequestInner::ProxyThrough(si) => Command::ProxyThrough(si),
            IngressDispatcherRequestInner::InvocationResponse(ir) => {
                Command::InvocationResponse(ir)
            }
            IngressDispatcherRequestInner::Attach(attach_invocation_req) => {
                Command::AttachInvocation(attach_invocation_req)
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use bytestring::ByteString;
    use googletest::{assert_that, pat};
    use restate_core::network::NetworkSender;
    use restate_core::TestCoreEnvBuilder;
    use restate_test_util::{let_assert, matchers::*};
    use restate_types::identifiers::{InvocationId, WithPartitionKey};
    use restate_types::ingress::{IngressResponseResult, InvocationResponse};
    use restate_types::invocation::{
        AttachInvocationRequest, InvocationQuery, InvocationTarget, ServiceInvocation,
        ServiceInvocationResponseSink, VirtualObjectHandlerType,
    };
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::partition_table::{FindPartition, FixedPartitionTable};
    use restate_types::Version;
    use restate_wal_protocol::Command;
    use restate_wal_protocol::Envelope;
    use std::time::Duration;
    use test_log::test;

    #[test(tokio::test)]
    async fn idempotent_invoke() -> anyhow::Result<()> {
        // set it to 1 partition so that we know where the invocation for the IdempotentInvoker goes to
        let mut env_builder = TestCoreEnvBuilder::new_with_mock_network()
            .add_mock_nodes_config()
            .with_partition_table(FixedPartitionTable::new(Version::MIN, 1));

        let bifrost_svc = restate_bifrost::BifrostService::new(
            env_builder.tc.clone(),
            env_builder.metadata.clone(),
        )
        .enable_in_memory_loglet();
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
                    VirtualObjectHandlerType::Exclusive,
                );
                let argument = Bytes::from_static(b"nbfjksdfs");
                let idempotency_key = ByteString::from_static("123");
                let invocation_id = InvocationId::generate_with_idempotency_key(
                    &invocation_target,
                    Some(idempotency_key.clone()),
                );

                let mut invocation = ServiceInvocation::initialize(
                    invocation_id,
                    invocation_target.clone(),
                    restate_types::invocation::Source::Ingress,
                );
                invocation.argument = argument.clone();
                invocation.idempotency_key = Some(idempotency_key.clone());
                invocation.completion_retention_time = Some(Duration::from_secs(60));
                let (ingress_req, _, res) = IngressDispatcherRequest::invocation(invocation);
                dispatcher.dispatch_ingress_request(ingress_req).await?;

                // Let's check we correct have generated a bifrost write
                let partition_id = node_env
                    .metadata
                    .partition_table()
                    .unwrap()
                    .find_partition_id(invocation_id.partition_key())?;
                let log_id = LogId::from(partition_id);
                let log_record = bifrost.read(log_id, Lsn::OLDEST).await?.unwrap();

                let output_message =
                    Envelope::from_bytes(log_record.record.into_payload_unchecked().into_body())?;

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
                        idempotency_key: some(eq(idempotency_key.clone())),
                        completion_retention_time: some(eq(Duration::from_secs(60)))
                    })
                );
                let_assert!(
                    Some(ServiceInvocationResponseSink::Ingress { request_id, .. }) =
                        service_invocation.response_sink
                );

                // Now check we get the response is routed back to the handler correctly
                let response = Bytes::from_static(b"vmoaifnuei");
                node_env
                    .network_sender
                    .send(
                        metadata().my_node_id().into(),
                        &IngressMessage::InvocationResponse(InvocationResponse {
                            request_id,
                            response: IngressResponseResult::Success(
                                invocation_target.clone(),
                                response.clone(),
                            ),
                            invocation_id: Some(service_invocation.invocation_id),
                        }),
                    )
                    .await?;

                assert_that!(
                    res.await?,
                    pat!(IngressInvocationResponse {
                        result: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response
                        ))
                    })
                );

                Ok(())
            })
            .await
    }

    #[test(tokio::test)]
    async fn attach_invocation() {
        // set it to 1 partition so that we know where the invocation for the IdempotentInvoker goes to
        let mut env_builder = TestCoreEnvBuilder::new_with_mock_network()
            .add_mock_nodes_config()
            .with_partition_table(FixedPartitionTable::new(Version::MIN, 1));

        let bifrost_svc = restate_bifrost::BifrostService::new(
            env_builder.tc.clone(),
            env_builder.metadata.clone(),
        )
        .enable_in_memory_loglet();
        let bifrost = bifrost_svc.handle();
        let dispatcher = IngressDispatcher::new(bifrost.clone());

        env_builder = env_builder.add_message_handler(dispatcher.clone());
        let node_env = env_builder.build().await;

        node_env
            .tc
            .run_in_scope("test", None, async {
                bifrost_svc.start().await?;

                let invocation_id = InvocationId::mock_random();

                let (attach_req, _, attach_res) =
                    IngressDispatcherRequest::attach(InvocationQuery::Invocation(invocation_id));
                dispatcher.dispatch_ingress_request(attach_req).await?;

                // Let's check the command was written to bifrost
                let partition_id = node_env
                    .metadata
                    .partition_table()
                    .unwrap()
                    .find_partition_id(invocation_id.partition_key())?;
                let bifrost_messages = bifrost.read_all(LogId::from(partition_id)).await?;

                let output_message_1 =
                    Envelope::from_bytes(bifrost_messages[0].record.payload().unwrap().body())?;

                let_assert!(
                    Command::AttachInvocation(attach_invocation_req) = output_message_1.command
                );
                assert_that!(
                    attach_invocation_req,
                    pat!(AttachInvocationRequest {
                        invocation_query: eq(InvocationQuery::Invocation(invocation_id)),
                    })
                );
                let_assert!(
                    ServiceInvocationResponseSink::Ingress { request_id, .. } =
                        attach_invocation_req.response_sink
                );

                // Now send the attach response
                let response = Bytes::from_static(b"vmoaifnuei");
                node_env
                    .network_sender
                    .send(
                        metadata().my_node_id().into(),
                        &IngressMessage::InvocationResponse(InvocationResponse {
                            request_id,
                            response: IngressResponseResult::Success(
                                InvocationTarget::mock_service(),
                                response.clone(),
                            ),
                            invocation_id: Some(invocation_id),
                        }),
                    )
                    .await?;

                assert_that!(
                    attach_res.await?,
                    pat!(IngressInvocationResponse {
                        result: pat!(IngressResponseResult::Success(anything(), eq(response)))
                    })
                );

                Ok::<(), anyhow::Error>(())
            })
            .await
            .unwrap()
    }
}
