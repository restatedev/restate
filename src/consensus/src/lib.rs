use futures_sink::Sink;
use std::marker::PhantomData;
use tracing::debug;

#[derive(Debug)]
pub enum Command<T> {
    Commit(T),
    CreateSnapshot,
    ApplySnapshot,
    Leader,
    Follower,
}

#[derive(Debug)]
pub struct Consensus<S, C>
where
    S: Sink<Command<C>>,
{
    _command_sink: S,
    phantom_data: PhantomData<C>,
}

impl<S, C> Consensus<S, C>
where
    S: Sink<Command<C>>,
{
    pub fn build(command_sink: S) -> Self {
        Self {
            _command_sink: command_sink,
            phantom_data: PhantomData::default(),
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let shutdown = drain.signaled();
        tokio::select! {
            _ = shutdown => {
                debug!("Shutting down Consensus.");
            }
        }
    }
}
