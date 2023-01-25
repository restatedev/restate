use futures::{Sink, SinkExt};
use std::fmt::Debug;
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
use tracing::debug;

pub type ConsensusSender<T> = PollSender<T>;

/// Component which is responsible for routing messages from different components.
#[derive(Debug)]
pub struct Network<ConMsg, ConOut> {
    /// Receiver for messages from the consensus module
    consensus_in_rx: mpsc::Receiver<ConMsg>,

    /// Sender for messages to the consensus module
    consensus_out: ConOut,

    // used for creating the ConsensusSender
    consensus_in_tx: mpsc::Sender<ConMsg>,
}

impl<ConMsg, ConOut> Network<ConMsg, ConOut>
where
    ConMsg: Send + 'static,
    ConOut: Sink<ConMsg>,
    <ConOut as Sink<ConMsg>>::Error: Debug,
{
    pub fn new(consensus_out: ConOut) -> Self {
        let (consensus_in_tx, consensus_in_rx) = mpsc::channel(64);

        Self {
            consensus_out,
            consensus_in_rx,
            consensus_in_tx,
        }
    }

    pub fn create_consensus_sender(&self) -> ConsensusSender<ConMsg> {
        PollSender::new(self.consensus_in_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        let Network {
            mut consensus_in_rx,
            consensus_out,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);
        tokio::pin!(consensus_out);

        loop {
            tokio::select! {
                message = consensus_in_rx.recv() => {
                    let message = message.expect("Network owns the consensus sender, that's why the receiver will never be closed.");
                    consensus_out.send(message).await.expect("Consensus component must be running.");
                },
                _ = &mut shutdown => {
                    debug!("Shutting network down.");
                    break;
                }
            }
        }
    }
}
