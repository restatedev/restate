use futures::{Sink, SinkExt};
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
use tracing::debug;

pub type ConsensusSender<T> = PollSender<T>;

#[derive(Debug)]
pub struct Network<ConMsg, ConOut> {
    consensus_rx: mpsc::Receiver<ConMsg>,
    consensus_out: ConOut,

    // used for creating the ConsensusSender
    consensus_tx: mpsc::Sender<ConMsg>,

    _phantom_data: PhantomData<ConMsg>,
}

impl<ConMsg, ConOut> Network<ConMsg, ConOut>
where
    ConMsg: Send + 'static,
    ConOut: Sink<ConMsg>,
    <ConOut as Sink<ConMsg>>::Error: Debug,
{
    pub fn build(consensus_out: ConOut) -> Self {
        let (consensus_tx, consensus_rx) = mpsc::channel(64);

        Self {
            consensus_out,
            consensus_rx,
            consensus_tx,
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn create_consensus_sender(&self) -> ConsensusSender<ConMsg> {
        PollSender::new(self.consensus_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        let Network {
            mut consensus_rx,
            consensus_out,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);
        tokio::pin!(consensus_out);

        loop {
            tokio::select! {
                message = consensus_rx.recv() => {
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
