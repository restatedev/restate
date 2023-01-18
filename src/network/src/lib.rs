use futures::{Sink, SinkExt, Stream, StreamExt};
use std::fmt::Debug;
use std::marker::PhantomData;
use tracing::debug;

#[derive(Debug)]
pub struct Network<ConIn, ConOut, ConMsg> {
    consensus_in: ConIn,
    consensus_out: ConOut,
    _phantom_data: PhantomData<ConMsg>,
}

impl<ConIn, ConOut, ConMsg> Network<ConIn, ConOut, ConMsg>
where
    ConIn: Stream<Item = ConMsg>,
    ConOut: Sink<ConMsg>,
    <ConOut as Sink<ConMsg>>::Error: Debug,
{
    pub fn build(consensus_in: ConIn, consensus_out: ConOut) -> Self {
        Self {
            consensus_out,
            consensus_in,
            _phantom_data: PhantomData::default(),
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let Network {
            consensus_in,
            consensus_out,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);
        tokio::pin!(consensus_in);
        tokio::pin!(consensus_out);

        loop {
            tokio::select! {
                message = consensus_in.next() => {
                    if let Some(message) = message {
                        consensus_out.send(message).await.expect("Consensus component must be running.");
                    }
                },
                _ = &mut shutdown => {
                    debug!("Shutting network down.");
                }
            }
        }
    }
}
