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

use std::collections::HashMap;
use std::future::poll_fn;

use restate_futures_util::pipe::{
    new_sender_pipe_target, Either, EitherPipeInput, Pipe, PipeError, ReceiverPipeInput,
    UnboundedReceiverPipeInput,
};
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::IngressId;
use tokio::select;
use tokio::sync::mpsc;
use tracing::{debug, info, trace};

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct IngressDispatcherLoopError(#[from] PipeError);

/// This loop is taking care of dispatching responses back to [super::RequestResponseHandler].
///
/// The reason to have a separate loop, rather than a simple channel to communicate back the response,
/// is that you need multiplexing between different processes, in case the request came to an handler
/// which lives in a separate process of the partition processor leader.
///
/// To interact with the loop use [IngressInputSender] and [ResponseRequester].
pub struct IngressDispatcherLoop {
    ingress_id: IngressId,

    // This channel can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    server_rx: UnboundedCommandReceiver<InvocationOrResponse, IngressResult>,

    input_rx: IngressInputReceiver,

    // For constructing the sender sides
    input_tx: IngressInputSender,
    server_tx: DispatcherCommandSender,
}

impl IngressDispatcherLoop {
    pub fn new(ingress_id: IngressId, channel_size: usize) -> IngressDispatcherLoop {
        let (input_tx, input_rx) = mpsc::channel(channel_size);
        let (server_tx, server_rx) = mpsc::unbounded_channel();

        IngressDispatcherLoop {
            ingress_id,
            input_rx,
            server_rx,
            input_tx,
            server_tx,
        }
    }

    pub async fn run(
        self,
        output_tx: mpsc::Sender<IngressOutput>,
        drain: drain::Watch,
    ) -> Result<(), IngressDispatcherLoopError> {
        debug!("Running the ResponseDispatcher");

        let IngressDispatcherLoop {
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
                        Either::Right(cmd) => pipe.as_mut().write(
                            handler.handle_ingress_command(cmd)
                        )?
                    }
                }
            }
        }

        Ok(())
    }

    pub fn create_response_sender(&self) -> IngressInputSender {
        self.input_tx.clone()
    }

    pub fn create_command_sender(&self) -> DispatcherCommandSender {
        self.server_tx.clone()
    }
}

struct DispatcherLoopHandler {
    ingress_id: IngressId,
    msg_index: MessageIndex,

    // This map can be unbounded, because we enforce concurrency limits in the ingress
    // services using the global semaphore
    waiting_responses: HashMap<FullInvocationId, CommandResponseSender<IngressResult>>,
}

impl DispatcherLoopHandler {
    fn new(ingress_id: IngressId) -> Self {
        Self {
            ingress_id,
            msg_index: 0,
            waiting_responses: HashMap::new(),
        }
    }

    fn handle_network_input(&mut self, input: IngressInput) -> Option<IngressOutput> {
        match input {
            IngressInput::Response(response) => {
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

                Some(IngressOutput::Ack(response.ack_target.acknowledge()))
            }
            IngressInput::MessageAck(ack_kind) => {
                trace!("Received message ack: {ack_kind:?}.");
                None
            }
        }
    }

    fn handle_ingress_command(
        &mut self,
        cmd: Command<InvocationOrResponse, IngressResult>,
    ) -> IngressOutput {
        let current_msg_index = self.msg_index;
        self.msg_index += 1;

        let (invocation_or_response, reply_channel) = cmd.into_inner();
        match invocation_or_response {
            InvocationOrResponse::Invocation(service_invocation) => {
                self.waiting_responses
                    .insert(service_invocation.id.clone(), reply_channel);
                IngressOutput::service_invocation(
                    service_invocation,
                    self.ingress_id,
                    current_msg_index,
                )
            }
            InvocationOrResponse::Response(response) => {
                IngressOutput::awakeable_completion(response, self.ingress_id, current_msg_index)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytestring::ByteString;
    use restate_test_util::test;
    use restate_types::identifiers::IngressId;
    use restate_types::invocation::{ServiceInvocationResponseSink, SpanRelation};

    #[test(tokio::test)]
    async fn test_closed_handler() {
        let (output_tx, _output_rx) = mpsc::channel(2);

        let ingress_dispatcher =
            IngressDispatcherLoop::new(IngressId("127.0.0.1:0".parse().unwrap()), 1);
        let input_sender = ingress_dispatcher.create_response_sender();
        let command_sender = ingress_dispatcher.create_command_sender();

        // Start the dispatcher loop
        let (drain_signal, watch) = drain::channel();
        let loop_handle = tokio::spawn(ingress_dispatcher.run(output_tx, watch));

        // Ask for a response, then drop the receiver=
        let method_name = ByteString::from_static("pippo");
        let service_invocation = ServiceInvocation::new(
            FullInvocationId::new("MySvc", "MyMethod", uuid::Uuid::now_v7()),
            method_name,
            Default::default(),
            Some(ServiceInvocationResponseSink::Ingress(IngressId(
                "0.0.0.0:0".parse().unwrap(),
            ))),
            SpanRelation::None,
        );
        let (cmd, cmd_rx) =
            Command::prepare(InvocationOrResponse::Invocation(service_invocation.clone()));
        command_sender.send(cmd).unwrap();
        drop(cmd_rx);

        // Now let's send the response
        input_sender
            .send(IngressInput::Response(IngressResponseMessage {
                full_invocation_id: service_invocation.id.clone(),
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
