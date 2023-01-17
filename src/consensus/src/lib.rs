use tracing::debug;

#[derive(Debug, Default)]
pub struct Consensus {}

impl Consensus {
    pub async fn run(self, drain: drain::Watch) {
        let shutdown = drain.signaled();
        tokio::select! {
            _ = shutdown => {
                debug!("Shutting down Consensus.");
            }
        }
    }
}