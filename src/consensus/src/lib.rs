use tokio::sync::mpsc;
use tracing::debug;

#[derive(Debug)]
pub struct Consensus<T> {
    command_writer: mpsc::Sender<T>
}

impl<T> Consensus<T> {
    pub fn build(command_writer: mpsc::Sender<T>) -> Self {
        Self {
            command_writer,
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