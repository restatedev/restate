use futures_sink::Sink;
use std::marker::PhantomData;
use tracing::debug;

#[derive(Debug)]
pub struct Consensus<S, T>
where
    S: Sink<T>,
{
    _command_sink: S,
    phantom_data: PhantomData<T>,
}

impl<S, T> Consensus<S, T>
where
    S: Sink<T>,
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
