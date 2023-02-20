use super::*;

use std::collections::HashMap;

use common::types::ServiceInvocationId;
use tokio::select;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// This loop is taking care of dispatching responses back to [super::RequestResponseHandler].
///
/// The reason to have a separate loop, rather than a simple channel to communicate back the response,
/// is that you need multiplexing between different processes, in case the request came to an handler
/// which lives in a separate process of the partition processor leader.
///
/// To interact with the loop use [IngressResponseSender] and [ResponseRequester].
pub struct ResponseDispatcherLoop {
    local_waiting_responses: HashMap<ServiceInvocationId, CommandResponseSender<IngressResult>>,

    // Channels
    response_rx: mpsc::UnboundedReceiver<IngressResponseMessage>,
    waiting_response_registration_rx: UnboundedCommandReceiver<ServiceInvocationId, IngressResult>,

    // For constructing the sender sides
    response_tx: mpsc::UnboundedSender<IngressResponseMessage>,
    waiting_response_registration_tx: UnboundedCommandSender<ServiceInvocationId, IngressResult>,
}

impl Default for ResponseDispatcherLoop {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseDispatcherLoop {
    pub fn new() -> ResponseDispatcherLoop {
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        let (waiting_response_registration_tx, waiting_response_registration_rx) =
            mpsc::unbounded_channel();

        ResponseDispatcherLoop {
            local_waiting_responses: HashMap::new(),
            response_rx,
            waiting_response_registration_rx,
            response_tx,
            waiting_response_registration_tx,
        }
    }

    pub async fn run(mut self, drain: drain::Watch) {
        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        loop {
            select! {
                _ = &mut shutdown => {
                    info!("Shut down of ResponseDispatcher requested. Shutting down now.");
                    break;
                },
                response = self.response_rx.recv() => {
                    self.handle_response(response.unwrap()).await;
                },
                registration = self.waiting_response_registration_rx.recv() => {
                    self.handle_awaiter_registration(registration.unwrap());
                }
            }
        }
    }

    pub fn create_response_sender(&self) -> IngressResponseSender {
        IngressResponseSender {
            response_tx: self.response_tx.clone(),
        }
    }

    pub fn create_response_requester(&self) -> IngressResponseRequester {
        self.waiting_response_registration_tx.clone()
    }

    fn handle_awaiter_registration(
        &mut self,
        registration: Command<ServiceInvocationId, IngressResult>,
    ) {
        let (fn_key, reply_channel) = registration.into_inner();
        self.local_waiting_responses.insert(fn_key, reply_channel);
    }

    async fn handle_response(&mut self, res: IngressResponseMessage) {
        if let Some(sender) = self
            .local_waiting_responses
            .remove(&res.service_invocation_id)
        {
            if let Err(Ok(res)) = sender.send(res.result) {
                warn!(
                    "Failed to send response '{:?}' because the handler has been closed, \
                    probably caused by the client connection that went away",
                    res
                );
            }
        } else {
            warn!("Failed to handle response '{:?}' because no handler was found locally waiting for its invocation key", &res);
        }
    }
}

#[derive(Clone)]
pub struct IngressResponseSender {
    response_tx: mpsc::UnboundedSender<IngressResponseMessage>,
}

impl IngressResponseSender {
    /// Dispatches the response to the ResponseDispatcherLoop, to route it back to the request handler
    pub fn dispatch_response(
        &self,
        res: IngressResponseMessage,
    ) -> Result<(), mpsc::error::SendError<IngressResponseMessage>> {
        self.response_tx.send(res)
    }
}

pub type IngressResponseRequester = UnboundedCommandSender<ServiceInvocationId, IngressResult>;

#[cfg(test)]
mod tests {
    use super::*;

    use test_utils::test;
    use tokio::sync::mpsc;

    impl IngressResponseSender {
        pub(crate) fn mock() -> (
            IngressResponseSender,
            mpsc::UnboundedReceiver<IngressResponseMessage>,
        ) {
            let (response_tx, response_rx) = mpsc::unbounded_channel();
            (IngressResponseSender { response_tx }, response_rx)
        }
    }

    #[test(tokio::test)]
    async fn test_closed_handler() {
        let response_dispatcher = ResponseDispatcherLoop::default();
        let response_requester = response_dispatcher.create_response_requester();
        let response_sender = response_dispatcher.create_response_sender();

        // Start the dispatcher loop
        let (drain_signal, watch) = drain::channel();
        let loop_handle = tokio::spawn(response_dispatcher.run(watch));

        // Ask for a response, then drop the receiver
        let service_invocation_id =
            ServiceInvocationId::new("MySvc", "MyMethod", uuid::Uuid::now_v7());
        let (cmd, cmd_rx) = Command::prepare(service_invocation_id.clone());
        response_requester.send(cmd).unwrap();
        drop(cmd_rx);

        // Now let's send the response
        response_sender
            .dispatch_response(IngressResponseMessage {
                service_invocation_id,
                result: Ok(Bytes::new()),
            })
            .unwrap();

        // Close and check it did not panic
        drain_signal.drain().await;
        loop_handle.await.unwrap()
    }
}
