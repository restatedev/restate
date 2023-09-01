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

use restate_futures_util::pipe::{
    new_sender_pipe_target, Either, EitherPipeInput, Pipe, PipeError, ReceiverPipeInput,
    UnboundedReceiverPipeInput,
};
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::IngressId;
use restate_types::invocation::ServiceInvocationResponseSink;
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
    ingress_id: IngressId,

    // This channel can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    server_rx: IngressRequestReceiver,

    input_rx: IngressDispatcherInputReceiver,

    // For constructing the sender sides
    input_tx: IngressDispatcherInputSender,
    server_tx: IngressRequestSender,
}

impl Service {
    pub fn new(ingress_id: IngressId, channel_size: usize) -> Service {
        let (input_tx, input_rx) = mpsc::channel(channel_size);
        let (server_tx, server_rx) = mpsc::unbounded_channel();

        Service {
            ingress_id,
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
            ingress_id,
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

        let mut handler = DispatcherLoopHandler::new(ingress_id);

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

struct DispatcherLoopHandler {
    // TODO perhaps we should rename this type to IngressDispatcherId, because that's what it is.
    ingress_id: IngressId,
    msg_index: MessageIndex,

    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_responses: HashMap<FullInvocationId, IngressResponseSender>,
    waiting_for_acks: HashMap<MessageIndex, AckSender>,
}

impl DispatcherLoopHandler {
    fn new(ingress_id: IngressId) -> Self {
        Self {
            ingress_id,
            msg_index: 0,
            waiting_responses: HashMap::new(),
            waiting_for_acks: HashMap::default(),
        }
    }

    fn handle_network_input(
        &mut self,
        input: IngressDispatcherInput,
    ) -> Option<IngressDispatcherOutput> {
        match input {
            IngressDispatcherInput::Response(response) => {
                if let Some(sender) = self.waiting_responses.remove(&response.full_invocation_id) {
                    if let Err(Ok(response)) = sender.send(response.result.map_err(Into::into)) {
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
        }
    }

    fn handle_ingress_command(
        &mut self,
        ingress_request: IngressRequest,
    ) -> IngressDispatcherOutput {
        let current_msg_index = self.msg_index;
        self.msg_index += 1;

        match ingress_request.0 {
            IngressRequestInner::Invocation(service_invocation, result_or_ack) => {
                let response_sink = match result_or_ack {
                    ResponseOrAckSender::Response(response_sender) => {
                        self.waiting_responses
                            .insert(service_invocation.fid.clone(), response_sender);
                        Some(ServiceInvocationResponseSink::Ingress(self.ingress_id))
                    }
                    ResponseOrAckSender::Ack(ack_sender) => {
                        self.waiting_for_acks.insert(current_msg_index, ack_sender);
                        None
                    }
                };

                IngressDispatcherOutput::service_invocation(
                    ServiceInvocation {
                        fid: service_invocation.fid,
                        method_name: service_invocation.method_name,
                        argument: service_invocation.argument,
                        response_sink,
                        span_context: service_invocation.span_context,
                    },
                    self.ingress_id,
                    current_msg_index,
                )
            }
            IngressRequestInner::Response(response, ack_listener) => {
                self.waiting_for_acks
                    .insert(current_msg_index, ack_listener);
                IngressDispatcherOutput::awakeable_completion(
                    response,
                    self.ingress_id,
                    current_msg_index,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_test_util::test;
    use restate_types::identifiers::IngressId;
    use restate_types::invocation::SpanRelation;

    #[test(tokio::test)]
    async fn test_closed_handler() {
        let (output_tx, _output_rx) = mpsc::channel(2);

        let ingress_dispatcher = Service::new(IngressId("127.0.0.1:0".parse().unwrap()), 1);
        let input_sender = ingress_dispatcher.create_ingress_dispatcher_input_sender();
        let command_sender = ingress_dispatcher.create_ingress_request_sender();

        // Start the dispatcher loop
        let (drain_signal, watch) = drain::channel();
        let loop_handle = tokio::spawn(ingress_dispatcher.run(output_tx, watch));

        // Ask for a response, then drop the receiver
        let fid = FullInvocationId::generate("MySvc", "MyKey");
        let (invocation, response_rx) =
            IngressRequest::invocation(fid.clone(), "pippo", Bytes::default(), SpanRelation::None);
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
}
