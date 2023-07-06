use codederror::CodedError;
use restate_meta::Meta;
use restate_worker::Worker;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ApplicationError {
    #[error("meta failed: {0}")]
    Meta(
        #[from]
        #[code]
        restate_meta::Error,
    ),
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::Error,
    ),
    #[error("meta panicked: {0}")]
    #[code(unknown)]
    MetaPanic(tokio::task::JoinError),
    #[error("worker panicked: {0}")]
    #[code(unknown)]
    WorkerPanic(tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed creating restate application: {cause}")]
pub struct BuildError {
    #[from]
    #[code]
    cause: restate_worker::BuildError,
}

pub struct Application {
    meta: Meta,
    worker: Worker,
}

impl Application {
    pub fn new(
        meta: restate_meta::Options,
        worker: restate_worker::Options,
    ) -> Result<Self, BuildError> {
        let meta = meta.build();
        let worker = worker.build(meta.schemas())?;

        Ok(Self { meta, worker })
    }

    pub async fn run(mut self, drain: drain::Watch) -> Result<(), ApplicationError> {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        // TODO This is a super terrible hack to enforce meta reloads from disk before
        //  a partition processor starts processing enqueued requests.
        //  Will be replaced with https://github.com/restatedev/restate/issues/91
        self.meta.init().await?;

        let worker_command_tx = self.worker.worker_command_tx();
        let mut meta_handle =
            tokio::spawn(self.meta.run(shutdown_watch.clone(), worker_command_tx));
        let mut worker_handle = tokio::spawn(self.worker.run(shutdown_watch));

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        tokio::select! {
            _ = shutdown => {
                let _ = tokio::join!(shutdown_signal.drain(), meta_handle, worker_handle);
            },
            result = &mut meta_handle => {
                result.map_err(ApplicationError::MetaPanic)??;
                panic!("Unexpected termination of meta.");
            },
            result = &mut worker_handle => {
                result.map_err(ApplicationError::WorkerPanic)??;
                panic!("Unexpected termination of worker.");
            }
        }

        Ok(())
    }
}
