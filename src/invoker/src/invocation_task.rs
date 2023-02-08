use std::error::Error;

use crate::JournalReader;
use common::types::{PartitionLeaderEpoch, ServiceInvocationId};
use futures::stream::Stream;
use hyper::http;
use journal::raw::RawEntry;
use journal::{Completion, EntryIndex};
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvocationTaskResultKind {
    #[error("request channel was closed")]
    RequestTxClosed,
    #[error("response channel was closed")]
    ResponseRxClosed,
    #[error("Output tx was closed")]
    OutputTxClosed,
    #[error("input rx was closed")]
    InputRxClosed,

    #[error("suspended: {0:?}")]
    Suspended(Vec<u32>),
    #[error("unexpected http status code: {0}")]
    UnexpectedResponse(http::StatusCode),
    #[error("other hyper error: {0}")]
    Network(hyper::Error),
    #[error(transparent)]
    Other(#[from] Box<dyn Error + Send + Sync + 'static>),
}

// Copy pasted from hyper::Error
// TODO hopefully this code is not needed anymore with hyper 1.0,
//  as we'll have more control on the h2 frames themselves.
//  Revisit when upgrading to hyper 1.0.
fn find_source<E: Error + 'static>(err: &hyper::Error) -> Option<&E> {
    let mut cause = err.source();
    while let Some(err) = cause {
        if let Some(typed) = err.downcast_ref() {
            return Some(typed);
        }
        cause = err.source();
    }

    // else
    None
}
fn h2_reason(err: &hyper::Error) -> h2::Reason {
    // Find an h2::Reason somewhere in the cause stack, if it exists,
    // otherwise assume an INTERNAL_ERROR.
    find_source::<h2::Error>(err)
        .and_then(|h2_err| h2_err.reason())
        .unwrap_or(h2::Reason::INTERNAL_ERROR)
}

impl From<hyper::Error> for InvocationTaskResultKind {
    fn from(err: hyper::Error) -> Self {
        if h2_reason(&err) == h2::Reason::NO_ERROR {
            InvocationTaskResultKind::ResponseRxClosed
        } else {
            InvocationTaskResultKind::Network(err)
        }
    }
}

#[derive(Debug)]
pub(crate) struct InvocationTaskResult {
    // Info to propagate to upper layer
    pub(crate) partition: PartitionLeaderEpoch,
    pub(crate) service_invocation_id: ServiceInvocationId,
    #[allow(unused)]
    pub(crate) used_protocol_version: u16,

    pub(crate) kind: InvocationTaskResultKind,
}

#[derive(Debug)]
pub(crate) struct InvocationEntry {
    // Info to propagate to upper layer
    pub(crate) partition: PartitionLeaderEpoch,
    pub(crate) service_invocation_id: ServiceInvocationId,

    pub(crate) entry_index: EntryIndex,
    pub(crate) raw_entry: RawEntry,
}

/// Represents an open invocation stream
pub(crate) struct InvocationTask<JR> {
    // Connection params
    partition: PartitionLeaderEpoch,
    sid: ServiceInvocationId,
    protocol_version: u16,

    // Invoker tx/rx
    journal_reader: JR,
    invoker_tx: mpsc::UnboundedSender<InvocationEntry>,
    invoker_rx: Option<mpsc::UnboundedReceiver<Completion>>,
}

impl<JR, JS> InvocationTask<JR>
where
    JR: JournalReader<JournalStream = JS>,
    JS: Stream<Item = RawEntry> + Unpin,
{
    pub fn new(
        partition: PartitionLeaderEpoch,
        sid: ServiceInvocationId,
        protocol_version: u16,
        journal_reader: JR,
        invoker_tx: mpsc::UnboundedSender<InvocationEntry>,
        invoker_rx: Option<mpsc::UnboundedReceiver<Completion>>,
    ) -> Self {
        Self {
            partition,
            sid,
            protocol_version,
            journal_reader,
            invoker_tx,
            invoker_rx,
        }
    }

    /// Loop opening the request to service endpoint and consuming the stream
    #[tracing::instrument(name = "run", level = "trace", skip_all, fields(restate.sid = % self.sid))]
    pub async fn run(self) -> InvocationTaskResult {
        unimplemented!("Implement this")
    }
}
