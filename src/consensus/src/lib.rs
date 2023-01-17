use futures::{Stream, StreamExt};
use futures_sink::Sink;
use std::fmt::Debug;
use std::marker::PhantomData;
use tracing::{debug, info};

#[derive(Debug)]
pub enum Command<T> {
    Commit(T),
    CreateSnapshot,
    ApplySnapshot,
    Leader,
    Follower,
}

#[derive(Debug)]
pub struct Consensus<S, C, P>
where
    S: Sink<Command<C>>,
{
    _command_sink: S,
    proposal_stream: P,
    phantom_data: PhantomData<C>,
}

impl<S, C, P> Consensus<S, C, P>
where
    S: Sink<Command<C>>,
    P: Stream<Item = C>,
    C: Debug,
{
    pub fn build(command_sink: S, proposal_stream: P) -> Self {
        Self {
            _command_sink: command_sink,
            proposal_stream,
            phantom_data: PhantomData::default(),
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let Consensus {
            proposal_stream, ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);
        tokio::pin!(proposal_stream);

        loop {
            tokio::select! {
                proposal = proposal_stream.next() => {
                    info!(?proposal, "Received proposal");
                },
                _ = &mut shutdown => {
                    debug!("Shutting down Consensus.");
                    break;
                }
            }
        }
    }
}
