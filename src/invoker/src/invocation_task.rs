use std::error::Error;

use crate::{EndpointMetadata, JournalReader};
use common::types::{PartitionLeaderEpoch, ServiceInvocationId};
use futures::stream::Stream;
use hyper::http;
use journal::raw::RawEntry;
use journal::{Completion, EntryIndex, JournalRevision};
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvocationTaskResultKind {
    #[error("no error")]
    Ok,

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
            InvocationTaskResultKind::Ok
        } else {
            InvocationTaskResultKind::Network(err)
        }
    }
}

pub(crate) struct InvocationTaskOutput {
    pub(crate) partition: PartitionLeaderEpoch,
    pub(crate) service_invocation_id: ServiceInvocationId,
    pub(crate) inner: InvocationTaskOutputInner,
}

pub(crate) enum InvocationTaskOutputInner {
    Result {
        last_journal_index: EntryIndex,
        last_journal_revision: JournalRevision,

        kind: InvocationTaskResultKind,
    },
    NewEntry {
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    },
}

/// Represents an open invocation stream
pub(crate) struct InvocationTask<JR> {
    // Connection params
    partition: PartitionLeaderEpoch,
    service_invocation_id: ServiceInvocationId,
    protocol_version: u16,
    service_metadata: EndpointMetadata,

    journal_index: EntryIndex,
    // Last revision received from the partition processor.
    // It is updated every time a completion is sent on the wire
    last_journal_revision: JournalRevision,

    // Invoker tx/rx
    journal_reader: JR,
    invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invoker_rx: Option<mpsc::UnboundedReceiver<(JournalRevision, Completion)>>,
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
        service_metadata: EndpointMetadata,
        journal_reader: JR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: Option<mpsc::UnboundedReceiver<(JournalRevision, Completion)>>,
    ) -> Self {
        Self {
            partition,
            service_invocation_id: sid,
            protocol_version,
            service_metadata,
            journal_index: 0,
            last_journal_revision: 0,
            journal_reader,
            invoker_tx,
            invoker_rx,
        }
    }

    /// Loop opening the request to service endpoint and consuming the stream
    #[tracing::instrument(name = "run", level = "trace", skip_all, fields(restate.sid = %self.service_invocation_id))]
    pub async fn run(self) {
        unimplemented!("Implement this")
    }
}
