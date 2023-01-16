use tracing::debug;

mod error;

type WorkerResult = Result<(), error::Error>;

#[derive(Default)]
pub struct Worker;

impl Worker {
    pub async fn run(self, drain: drain::Watch) -> WorkerResult {

        let shutdown = drain.signaled();

        tokio::select! {
            _ = shutdown => {
                debug!("Shutting down the worker component.")
            }
        }

        Ok(())
    }
}
