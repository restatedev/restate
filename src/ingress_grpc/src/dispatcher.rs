use super::*;

use std::collections::HashMap;
use std::future::poll_fn;

use common::types::ServiceInvocationId;
use futures_util::pipe::{new_sender_pipe_target, Pipe, UnboundedReceiverPipeInput};
use tokio::select;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// This loop is taking care of dispatching responses back to [super::RequestResponseHandler].
///
/// The reason to have a separate loop, rather than a simple channel to communicate back the response,
/// is that you need multiplexing between different processes, in case the request came to an handler
/// which lives in a separate process of the partition processor leader.
///
/// To interact with the loop use [IngressInputSender] and [ResponseRequester].
pub struct IngressDispatcherLoop {
    // This channel and map can be unbounded,
    // because we enforce concurrency limits in the ingress services using the global semaphore
    local_waiting_responses: HashMap<ServiceInvocationId, CommandResponseSender<IngressResult>>,
    server_rx: UnboundedCommandReceiver<ServiceInvocation, IngressResult>,

    input_rx: IngressInputReceiver,

    // For constructing the sender sides
    input_tx: IngressInputSender,
    server_tx: DispatcherCommandSender,
}

impl Default for IngressDispatcherLoop {
    fn default() -> Self {
        IngressDispatcherLoop::new()
    }
}

impl IngressDispatcherLoop {
    pub fn new() -> IngressDispatcherLoop {
        let (input_tx, input_rx) = mpsc::channel(64);
        let (server_tx, server_rx) = mpsc::unbounded_channel();

        IngressDispatcherLoop {
            local_waiting_responses: HashMap::new(),
            input_rx,
            server_rx,
            input_tx,
            server_tx,
        }
    }

    pub async fn run(self, output_tx: mpsc::Sender<IngressOutput>, drain: drain::Watch) {
        debug!("Running the ResponseDispatcher.");

        let IngressDispatcherLoop {
            mut local_waiting_responses,
            server_rx,
            mut input_rx,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        let server_commands_to_network_pipe = Pipe::new(
            UnboundedReceiverPipeInput::new(server_rx),
            new_sender_pipe_target(output_tx),
        );
        tokio::pin!(server_commands_to_network_pipe);

        loop {
            select! {
                _ = &mut shutdown => {
                    info!("Shut down of ResponseDispatcher requested. Shutting down now.");
                    break;
                },
                response = input_rx.recv() => {
                    Self::handle_input(&mut local_waiting_responses, response.unwrap());
                },
                res_cmd = poll_fn(|cx| server_commands_to_network_pipe.as_mut().poll_next_input(cx)) => {
                    server_commands_to_network_pipe.as_mut().write(
                        Self::map_command(&mut local_waiting_responses, res_cmd.unwrap())
                    ).unwrap()
                }
            }
        }
    }

    pub fn create_response_sender(&self) -> IngressInputSender {
        self.input_tx.clone()
    }

    pub fn create_command_sender(&self) -> DispatcherCommandSender {
        self.server_tx.clone()
    }

    #[allow(clippy::mutable_key_type)]
    fn map_command(
        local_waiting_responses: &mut HashMap<
            ServiceInvocationId,
            CommandResponseSender<IngressResult>,
        >,
        cmd: Command<ServiceInvocation, IngressResult>,
    ) -> IngressOutput {
        let (service_invocation, reply_channel) = cmd.into_inner();
        local_waiting_responses.insert(service_invocation.id.clone(), reply_channel);
        IngressOutput::Invocation(service_invocation)
    }

    #[allow(clippy::mutable_key_type)]
    fn handle_input(
        local_waiting_responses: &mut HashMap<
            ServiceInvocationId,
            CommandResponseSender<IngressResult>,
        >,
        input: IngressInput,
    ) {
        match input {
            IngressInput::Response(response) => {
                if let Some(sender) =
                    local_waiting_responses.remove(&response.service_invocation_id)
                {
                    if let Err(Ok(response)) = sender.send(response.result.map_err(Into::into)) {
                        warn!(
                            "Failed to send response '{:?}' because the handler has been closed, \
                    probably caused by the client connection that went away",
                            response
                        );
                    }
                } else {
                    warn!("Failed to handle response '{:?}' because no handler was found locally waiting for its invocation key", &response);
                }
            }
            IngressInput::MessageAck { .. } => {
                todo!("https://github.com/restatedev/restate/issues/132");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use common::types::{IngressId, ServiceInvocationResponseSink, SpanRelation};
    use test_utils::test;

    #[test(tokio::test)]
    async fn test_closed_handler() {
        let (output_tx, _output_rx) = mpsc::channel(1);

        let ingress_dispatcher = IngressDispatcherLoop::default();
        let input_sender = ingress_dispatcher.create_response_sender();
        let command_sender = ingress_dispatcher.create_command_sender();

        // Start the dispatcher loop
        let (drain_signal, watch) = drain::channel();
        let loop_handle = tokio::spawn(ingress_dispatcher.run(output_tx, watch));

        // Ask for a response, then drop the receiver
        let service_invocation = ServiceInvocation {
            id: ServiceInvocationId::new("MySvc", "MyMethod", uuid::Uuid::now_v7()),
            method_name: Default::default(),
            argument: Default::default(),
            response_sink: ServiceInvocationResponseSink::Ingress(IngressId(
                "0.0.0.0:0".parse().unwrap(),
            )),
            span_relation: SpanRelation::None,
        };
        let (cmd, cmd_rx) = Command::prepare(service_invocation.clone());
        command_sender.send(cmd).unwrap();
        drop(cmd_rx);

        // Now let's send the response
        input_sender
            .send(IngressInput::Response(IngressResponseMessage {
                service_invocation_id: service_invocation.id.clone(),
                result: Ok(Bytes::new()),
                ack_target: AckTarget::new(0, 0),
            }))
            .await
            .unwrap();

        // Close and check it did not panic
        drain_signal.drain().await;
        loop_handle.await.unwrap()
    }
}
