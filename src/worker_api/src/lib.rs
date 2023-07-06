use restate_types::identifiers::ServiceInvocationId;
use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("worker is unreachable")]
    Unreachable,
}

pub trait Handle {
    type Future: Future<Output = Result<(), Error>> + Send;

    /// Send a command to kill an invocation. This command is best-effort.
    fn kill_invocation(&self, service_invocation_id: ServiceInvocationId) -> Self::Future;
}
