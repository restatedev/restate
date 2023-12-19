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

use bytes::{BufMut, BytesMut};
use prost::Message;
use restate_futures_util::pipe::{
    new_sender_pipe_target, Either, EitherPipeInput, Pipe, PipeError, ReceiverPipeInput,
    UnboundedReceiverPipeInput,
};
use restate_pb::restate::internal::{
    idempotent_invoke_response, IdempotentInvokeRequest, IdempotentInvokeResponse,
};
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::IngressDispatcherId;
use restate_types::invocation::{ServiceInvocationResponseSink, Source};
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
    ingress_dispatcher_id: IngressDispatcherId,

    // This channel can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    server_rx: IngressRequestReceiver,

    input_rx: IngressDispatcherInputReceiver,

    // For constructing the sender sides
    input_tx: IngressDispatcherInputSender,
    server_tx: IngressRequestSender,
}

impl Service {
    pub fn new(ingress_dispatcher_id: IngressDispatcherId, channel_size: usize) -> Service {
        let (input_tx, input_rx) = mpsc::channel(channel_size);
        let (server_tx, server_rx) = mpsc::unbounded_channel();

        Service {
            ingress_dispatcher_id,
            input_rx,
            server_rx,
            input_tx,
            server_tx,
        }
    }

    pub async fn run(
        self,
        output_tx: mpsc::Sender<IngressDispatcherOutput>,
        drain: drain::Watch,
    ) -> Result<(), Error> {
        debug!("Running the ResponseDispatcher");

        let Service {
            ingress_dispatcher_id,
            server_rx,
            input_rx,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        let pipe = Pipe::new(
            EitherPipeInput::new(
                ReceiverPipeInput::new(input_rx, "network input rx"),
                UnboundedReceiverPipeInput::new(server_rx, "ingress rx"),
            ),
            new_sender_pipe_target(output_tx.clone(), "network output tx"),
        );

        tokio::pin!(pipe);

        let mut handler = DispatcherLoopHandler::new(ingress_dispatcher_id);

        loop {
            select! {
                _ = &mut shutdown => {
                    info!("Shut down of ResponseDispatcher requested. Shutting down now.");
                    break;
                },
                pipe_input = poll_fn(|cx| pipe.as_mut().poll_next_input(cx)) => {
                    match pipe_input? {
                        Either::Left(ingress_input) => {
                            if let Some(output) = handler.handle_network_input(ingress_input) {
                                pipe.as_mut().write(output)?;
                            }
                        },
                        Either::Right(invocation_or_response) => pipe.as_mut().write(
                            handler.handle_ingress_command(invocation_or_response)
                        )?
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
    fn map(&self, buf: Bytes) -> IngressResponse {
        match self {
            MapResponseAction::IdempotentInvokerResponse => {
                use idempotent_invoke_response::Response;
                let idempotent_invoke_response = match IdempotentInvokeResponse::decode(buf) {
                    Ok(v) => v,
                    Err(_) => {
                        return IngressResponse {
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

                IngressResponse {
                    idempotency_expiry_time: Some(idempotent_invoke_response.expiry_time),
                    result,
                }
            }
            MapResponseAction::None => IngressResponse {
                idempotency_expiry_time: None,
                result: Ok(buf),
            },
        }
    }
}

struct DispatcherLoopHandler {
    ingress_dispatcher_id: IngressDispatcherId,
    msg_index: MessageIndex,

    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_responses: HashMap<FullInvocationId, (MapResponseAction, IngressResponseSender)>,
    waiting_for_acks: HashMap<MessageIndex, AckSender>,
    waiting_for_acks_with_custom_id: HashMap<IngressDeduplicationId, AckSender>,
}

impl DispatcherLoopHandler {
    fn new(ingress_dispatcher_id: IngressDispatcherId) -> Self {
        Self {
            ingress_dispatcher_id,
            msg_index: 0,
            waiting_responses: HashMap::new(),
            waiting_for_acks: HashMap::default(),
            waiting_for_acks_with_custom_id: Default::default(),
        }
    }

    fn handle_network_input(
        &mut self,
        input: IngressDispatcherInput,
    ) -> Option<IngressDispatcherOutput> {
        match input {
            IngressDispatcherInput::Response(response) => {
                if let Some((map_response_action, sender)) =
                    self.waiting_responses.remove(&response.full_invocation_id)
                {
                    let mapped_response = match response.result {
                        Ok(v) => map_response_action.map(v),
                        Err(e) => IngressResponse {
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

                Some(IngressDispatcherOutput::Ack(
                    response.ack_target.acknowledge(),
                ))
            }
            IngressDispatcherInput::MessageAck(acked_index) => {
                trace!("Received message ack: {acked_index:?}.");

                if let Some(ack_sender) = self.waiting_for_acks.remove(&acked_index) {
                    // Receivers might be gone if they are not longer interested in the ack notification
                    let _ = ack_sender.send(());
                }
                None
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
                None
            }
        }
    }

    fn handle_ingress_command(
        &mut self,
        ingress_request: IngressRequest,
    ) -> IngressDispatcherOutput {
        let IngressRequest {
            fid,
            method_name,
            argument,
            span_context,
            request_mode,
            idempotency,
        } = ingress_request;

        let response_sink = if matches!(request_mode, IngressRequestMode::RequestResponse(_)) {
            Some(ServiceInvocationResponseSink::Ingress(
                self.ingress_dispatcher_id,
            ))
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
                        fid: FullInvocationId::generate(
                            restate_pb::IDEMPOTENT_INVOKER_SERVICE_NAME,
                            idempotency_fid_key.freeze(),
                        ),
                        method_name: restate_pb::IDEMPOTENT_INVOKER_INVOKE_METHOD_NAME
                            .to_string()
                            .into(),
                        argument: IdempotentInvokeRequest {
                            idempotency_id: idempotency_key,
                            service_name: fid.service_id.service_name.into(),
                            service_key: fid.service_id.key,
                            invocation_uuid: Bytes::copy_from_slice(fid.invocation_uuid.as_bytes()),
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
                self.msg_index += 1;
                self.waiting_for_acks.insert(msg_index, ack_sender);
                (None, msg_index)
            }
            IngressRequestMode::DedupFireAndForget(dedup_id, ack_sender) => {
                self.waiting_for_acks_with_custom_id
                    .insert(dedup_id.clone(), ack_sender);
                (Some(dedup_id.0), dedup_id.1)
            }
        };

        IngressDispatcherOutput::service_invocation(
            service_invocation,
            self.ingress_dispatcher_id,
            dedup_source,
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
    use restate_test_util::{let_assert, matchers::*, test};
    use restate_types::identifiers::IngressDispatcherId;
    use restate_types::identifiers::ServiceId;
    use restate_types::invocation::SpanRelation;

    #[test(tokio::test)]
    async fn test_closed_handler() {
        let (output_tx, _output_rx) = mpsc::channel(2);

        let ingress_dispatcher =
            Service::new(IngressDispatcherId("127.0.0.1:0".parse().unwrap()), 1);
        let input_sender = ingress_dispatcher.create_ingress_dispatcher_input_sender();
        let command_sender = ingress_dispatcher.create_ingress_request_sender();

        // Start the dispatcher loop
        let (drain_signal, watch) = drain::channel();
        let loop_handle = tokio::spawn(ingress_dispatcher.run(output_tx, watch));

        // Ask for a response, then drop the receiver
        let fid = FullInvocationId::generate("MySvc", "MyKey");
        let (invocation, response_rx) = IngressRequest::invocation(
            fid.clone(),
            "pippo",
            Bytes::default(),
            SpanRelation::None,
            IdempotencyMode::None,
        );
        command_sender.send(invocation).unwrap();
        drop(response_rx);

        // Now let's send the response
        input_sender
            .send(IngressDispatcherInput::Response(IngressResponseMessage {
                full_invocation_id: fid.clone(),
                result: Ok(Bytes::new()),
                ack_target: AckTarget::new(0, 0),
            }))
            .await
            .unwrap();

        // Close and check it did not panic
        drain_signal.drain().await;
        loop_handle.await.unwrap().unwrap()
    }

    #[test(tokio::test)]
    async fn idempotent_invoke() {
        let (output_tx, mut output_rx) = mpsc::channel(2);

        let ingress_dispatcher =
            Service::new(IngressDispatcherId("127.0.0.1:0".parse().unwrap()), 1);
        let handler_tx = ingress_dispatcher.create_ingress_request_sender();
        let network_tx = ingress_dispatcher.create_ingress_dispatcher_input_sender();

        // Start the dispatcher loop
        let (drain_signal, watch) = drain::channel();
        tokio::spawn(ingress_dispatcher.run(output_tx, watch));

        // Ask for a response, then drop the receiver
        let fid = FullInvocationId::generate("MySvc", "MyKey");
        let argument = Bytes::from_static(b"nbfjksdfs");
        let idempotency_key = Bytes::copy_from_slice(b"123");
        let (invocation, res) = IngressRequest::invocation(
            fid.clone(),
            "pippo",
            argument.clone(),
            SpanRelation::None,
            IdempotencyMode::key(idempotency_key.clone(), None),
        );
        handler_tx.send(invocation).unwrap();

        // Let's check we correct have a response
        let output_message = output_rx.recv().await.unwrap();

        let_assert!(
            IngressDispatcherOutput::Invocation {
                service_invocation,
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
            .send(IngressDispatcherInput::Response(IngressResponseMessage {
                full_invocation_id: service_invocation.fid,
                result: Ok(IdempotentInvokeResponse {
                    expiry_time: expiry_time.clone(),
                    response: Some(idempotent_invoke_response::Response::Success(
                        response.clone(),
                    )),
                }
                .encode_to_vec()
                .into()),
                ack_target: AckTarget::new(0, 0),
            }))
            .await
            .unwrap();

        assert_that!(
            res.await.unwrap(),
            pat!(IngressResponse {
                idempotency_expiry_time: some(eq(expiry_time)),
                result: ok(eq(response))
            })
        );

        drain_signal.drain().await;
    }
}
