use common::types::{AckKind, InvocationResponse, ServiceInvocation};

#[derive(Debug)]
pub(crate) struct IngressOutput(pub(crate) ServiceInvocation);

#[derive(Debug)]
pub(crate) enum IngressInput {
    InvocationResult(InvocationResponse),
    MessageAck(AckKind),
}
