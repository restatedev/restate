use crate::types::ServiceInvocationId;

/// Commands that can be sent to a worker.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WorkerCommand {
    KillInvocation(ServiceInvocationId),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("worker is unreachable")]
    Unreachable,
}

#[derive(Debug, Clone)]
pub struct WorkerCommandSender(tokio::sync::mpsc::Sender<WorkerCommand>);

impl WorkerCommandSender {
    pub fn new(command_tx: tokio::sync::mpsc::Sender<WorkerCommand>) -> Self {
        Self(command_tx)
    }

    pub async fn kill_invocation(
        &self,
        service_invocation_id: ServiceInvocationId,
    ) -> Result<(), Error> {
        self.0
            .send(WorkerCommand::KillInvocation(service_invocation_id))
            .await
            .map_err(|_| Error::Unreachable)
    }
}
