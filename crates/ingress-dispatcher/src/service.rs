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

use assert2::let_assert;
use bytes::{BufMut, BytesMut};
use prost::Message;
use restate_bifrost::Bifrost;
use restate_core::cancellation_watcher;
use restate_futures_util::pipe::{
    new_pipe_target, Either, EitherPipeInput, Pipe, PipeError, ReceiverPipeInput,
    UnboundedReceiverPipeInput,
};
use restate_pb::restate::internal::{
    idempotent_invoke_response, IdempotentInvokeRequest, IdempotentInvokeResponse,
};
use restate_types::dedup::{DedupSequenceNumber, ProducerId};
use restate_types::identifiers::FullInvocationId;
use restate_types::invocation::{ServiceInvocationResponseSink, Source};
use restate_types::GenerationalNodeId;
use restate_wal_protocol::append_envelope_to_bifrost;
use std::collections::HashMap;
use std::future::poll_fn;
use tokio::select;
use tokio::sync::mpsc;
use tracing::{debug, info, trace};

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] PipeError);

/// This loop is taking care of dispatching responses back to [super::RequestResponseHandler].
///
/// The reason to have a separate loop, rather than a simple channel to communicate back the response,
/// is that you need multiplexing between different processes, in case the request came to an handler
/// which lives in a separate process of the partition processor leader.
///
/// To interact with the loop use [IngressDispatcherInputSender] and [ResponseRequester].
pub struct Service {
    // This channel can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    server_rx: IngressRequestReceiver,

    input_rx: IngressDispatcherInputReceiver,

    // For constructing the sender sides
    input_tx: IngressDispatcherInputSender,
    server_tx: IngressRequestSender,
}

impl Service {
    pub fn new(channel_size: usize) -> Service {
        let (input_tx, input_rx) = mpsc::channel(channel_size);
        let (server_tx, server_rx) = mpsc::unbounded_channel();

        Service {
            input_rx,
            server_rx,
            input_tx,
            server_tx,
        }
    }

    pub async fn run(self, bifrost: Bifrost) -> anyhow::Result<()> {
        debug!("Running the ResponseDispatcher");
        let my_node_id = metadata().my_node_id();

        let Service {
            server_rx,
            input_rx,
            ..
        } = self;

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        let pipe = Pipe::new(
            EitherPipeInput::new(
                ReceiverPipeInput::new(input_rx, "network input rx"),
                UnboundedReceiverPipeInput::new(server_rx, "ingress rx"),
            ),
            new_pipe_target(
                (),
                |_, envelope| async {
                    append_envelope_to_bifrost(&mut bifrost.clone(), envelope).await
                },
                "bifrost output",
            ),
        );

        tokio::pin!(pipe);

        let mut handler = DispatcherLoopHandler::new(my_node_id);

        loop {
            select! {
                _ = &mut shutdown => {
                    info!("Shut down of ResponseDispatcher requested. Shutting down now.");
                    break;
                },
                pipe_input = poll_fn(|cx| pipe.as_mut().poll_next_input(cx)) => {
                    match pipe_input? {
                        Either::Left(ingress_input) => {
                            handler.handle_network_input(ingress_input);
                        },
                        Either::Right(invocation_or_response) => {
                            let (envelope, message_index) = handler.handle_ingress_command(invocation_or_response);
                            let_assert!(Destination::Processor { dedup, .. } = &envelope.header.dest);

                            let ack = if let Some(dedup) = dedup {
                                let_assert!(ProducerId::Other(producer_id) = &dedup.producer_id);
                                let_assert!(DedupSequenceNumber::Sn(sn) = dedup.sequence_number);
                                IngressDispatcherInput::DedupMessageAck(producer_id.clone().into(), sn)
                            } else {
                                IngressDispatcherInput::MessageAck(message_index)
                            };

                            pipe.as_mut().write(envelope)?;

                            handler.handle_network_input(ack);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn create_ingress_dispatcher_input_sender(&self) -> IngressDispatcherInputSender {
        self.input_tx.clone()
    }

    pub fn create_ingress_request_sender(&self) -> IngressRequestSender {
        self.server_tx.clone()
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

struct DispatcherLoopHandler {
    my_node_id: GenerationalNodeId,
    msg_index: MessageIndex,

    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_responses: HashMap<FullInvocationId, (MapResponseAction, IngressResponseSender)>,
    waiting_for_acks: HashMap<MessageIndex, AckSender>,
    waiting_for_acks_with_custom_id: HashMap<IngressDeduplicationId, AckSender>,
}

impl DispatcherLoopHandler {
    fn new(my_node_id: GenerationalNodeId) -> Self {
        Self {
            my_node_id,
            msg_index: 0,
            waiting_responses: HashMap::new(),
            waiting_for_acks: HashMap::default(),
            waiting_for_acks_with_custom_id: Default::default(),
        }
    }

    fn handle_network_input(&mut self, input: IngressDispatcherInput) {
        match input {
            IngressDispatcherInput::Response(response) => {
                if let Some((map_response_action, sender)) =
                    self.waiting_responses.remove(&response.full_invocation_id)
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
                            "Failed to send response '{:?}' because the handler has been closed, \
                    probably caused by the client connection that went away",
                            response
                        );
                    }
                } else {
                    debug!("Failed to handle response '{:?}' because no handler was found locally waiting for its invocation key", &response);
                }
            }
            IngressDispatcherInput::MessageAck(acked_index) => {
                trace!("Received message ack: {acked_index:?}.");

                if let Some(ack_sender) = self.waiting_for_acks.remove(&acked_index) {
                    // Receivers might be gone if they are not longer interested in the ack notification
                    let _ = ack_sender.send(());
                }
            }
            IngressDispatcherInput::DedupMessageAck(dedup_name, dedup_seq_number) => {
                trace!("Received dedup message ack: {dedup_name} {dedup_seq_number:?}.");

                if let Some(ack_sender) = self
                    .waiting_for_acks_with_custom_id
                    .remove(&(dedup_name, dedup_seq_number))
                {
                    // Receivers might be gone if they are not longer interested in the ack notification
                    let _ = ack_sender.send(());
                }
            }
        }
    }

    fn handle_ingress_command(
        &mut self,
        ingress_request: IngressRequest,
    ) -> (Envelope, MessageIndex) {
        let IngressRequest {
            fid,
            method_name,
            argument,
            span_context,
            request_mode,
            idempotency,
        } = ingress_request;

        let response_sink = if matches!(request_mode, IngressRequestMode::RequestResponse(_)) {
            Some(ServiceInvocationResponseSink::Ingress(self.my_node_id))
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
                        source: Source::Ingress,
                        response_sink,
                        span_context,
                    },
                    MapResponseAction::IdempotentInvokerResponse,
                )
            } else {
                (
                    ServiceInvocation {
                        fid,
                        method_name,
                        argument,
                        source: Source::Ingress,
                        response_sink,
                        span_context,
                    },
                    MapResponseAction::None,
                )
            };

        let (dedup_source, msg_index) = match request_mode {
            IngressRequestMode::RequestResponse(response_sender) => {
                self.waiting_responses.insert(
                    service_invocation.fid.clone(),
                    (map_response_action, response_sender),
                );
                (None, self.get_and_increment_msg_index())
            }
            IngressRequestMode::FireAndForget(ack_sender) => {
                let msg_index = self.get_and_increment_msg_index();
                self.waiting_for_acks.insert(msg_index, ack_sender);
                (None, msg_index)
            }
            IngressRequestMode::DedupFireAndForget(dedup_id, ack_sender) => {
                self.waiting_for_acks_with_custom_id
                    .insert(dedup_id.clone(), ack_sender);
                (Some(dedup_id.0), dedup_id.1)
            }
        };

        (
            wrap_service_invocation_in_envelope(
                service_invocation,
                self.my_node_id,
                dedup_source,
                msg_index,
            ),
            msg_index,
        )
    }

    fn get_and_increment_msg_index(&mut self) -> MessageIndex {
        let current_msg_index = self.msg_index;
        self.msg_index += 1;
        current_msg_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::{assert_that, pat};
    use restate_bifrost::Bifrost;
    use restate_core::TaskKind;
    use restate_core::TestCoreEnv;
    use test_log::test;

    use restate_test_util::{let_assert, matchers::*};
    use restate_types::identifiers::ServiceId;
    use restate_types::invocation::{ResponseResult, SpanRelation};
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::partition_table::{FindPartition, FixedPartitionTable};
    use restate_types::Version;

    #[test(tokio::test)]
    async fn test_closed_handler() -> anyhow::Result<()> {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let tc = node_env.tc;

        let ingress_dispatcher = Service::new(1);
        let input_sender = ingress_dispatcher.create_ingress_dispatcher_input_sender();
        let command_sender = ingress_dispatcher.create_ingress_request_sender();

        let num_partitions = 64;
        node_env
            .metadata_writer
            .update(FixedPartitionTable::new(Version::MIN, num_partitions))
            .await?;

        let bifrost = tc
            .run_in_scope("init bifrost", None, Bifrost::new_in_memory(num_partitions))
            .await;

        // Start the dispatcher loop
        let dispatcher_task = tc.spawn(
            TaskKind::SystemService,
            "ingress-dispatcher",
            None,
            ingress_dispatcher.run(bifrost),
        )?;

        // Ask for a response, then drop the receiver
        let fid = FullInvocationId::generate(ServiceId::new("MySvc", "MyKey"));
        let (invocation, response_rx) = IngressRequest::invocation(
            fid.clone(),
            "pippo",
            Bytes::default(),
            SpanRelation::None,
            IdempotencyMode::None,
        );
        command_sender.send(invocation)?;
        drop(response_rx);

        // Now let's send the response
        input_sender
            .send(IngressDispatcherInput::Response(IngressResponse {
                full_invocation_id: fid.clone(),
                response: ResponseResult::Success(Bytes::new()),
                target_node: GenerationalNodeId::new(0, 0),
            }))
            .await?;

        // Close and check it did not panic
        tc.cancel_task(dispatcher_task).unwrap().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn idempotent_invoke() -> anyhow::Result<()> {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let tc = node_env.tc;

        let ingress_dispatcher = Service::new(1);
        let handler_tx = ingress_dispatcher.create_ingress_request_sender();
        let network_tx = ingress_dispatcher.create_ingress_dispatcher_input_sender();

        // set it to 1 partition so that we know where the invocation for the IdempotentInvoker goes to
        let num_partitions = 1;
        node_env
            .metadata_writer
            .update(FixedPartitionTable::new(Version::MIN, num_partitions))
            .await?;

        let bifrost = tc
            .run_in_scope("init bifrost", None, Bifrost::new_in_memory(num_partitions))
            .await;

        // Start the dispatcher loop
        tc.spawn(
            TaskKind::SystemService,
            "ingress-dispatcher",
            None,
            ingress_dispatcher.run(bifrost.clone()),
        )?;

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
        );
        handler_tx.send(invocation)?;

        // Let's check we correct have a response
        let partition_id = node_env
            .metadata
            .partition_table()
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
                        service_name: displays_as(eq(restate_pb::IDEMPOTENT_INVOKER_SERVICE_NAME)),
                        key: eq(Bytes::copy_from_slice(b"MySvc123")),
                    }),
                }),
                method_name: displays_as(eq(restate_pb::IDEMPOTENT_INVOKER_INVOKE_METHOD_NAME)),
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
        network_tx
            .send(IngressDispatcherInput::Response(IngressResponse {
                full_invocation_id: service_invocation.fid,
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
                target_node: GenerationalNodeId::new(0, 0),
            }))
            .await?;

        assert_that!(
            res.await?,
            pat!(ExpiringIngressResponse {
                idempotency_expiry_time: some(eq(expiry_time)),
                result: ok(eq(response))
            })
        );

        Ok(())
    }
}
