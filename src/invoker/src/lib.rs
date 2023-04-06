use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use common::retry_policy::RetryPolicy;
use common::types::{EntryIndex, JournalMetadata, PartitionLeaderEpoch, ServiceInvocationId};
use futures::Stream;
use journal::raw::PlainRawEntry;
use journal::Completion;
use opentelemetry::trace::SpanContext;
use tokio::sync::mpsc;

mod invoker;
pub use crate::invoker::*;

mod invocation_task;

// --- Journal Reader

pub trait JournalReader {
    type JournalStream: Stream<Item = PlainRawEntry>;
    type Error: std::error::Error + Send + Sync + 'static;
    type Future<'a>: Future<Output = Result<(JournalMetadata, Self::JournalStream), Self::Error>>
        + Send
    where
        Self: 'a;

    fn read_journal<'a>(&'a self, sid: &'a ServiceInvocationId) -> Self::Future<'_>;
}

// --- Invoker input sender

#[derive(Debug)]
pub enum InvokeInputJournal {
    NoCachedJournal,
    CachedJournal(JournalMetadata, Vec<PlainRawEntry>),
}

#[derive(Debug, thiserror::Error)]
#[error("invoker is not running")]
pub struct InvokerNotRunning;

pub trait InvokerInputSender {
    type Future: Future<Output = Result<(), InvokerNotRunning>>;

    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future;
    fn resume(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future;

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) -> Self::Future;
    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future;

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future;
    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<OutputEffect>,
    ) -> Self::Future;
}

// --- Output messages

pub type InvokerError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
pub struct OutputEffect {
    pub service_invocation_id: ServiceInvocationId,
    pub kind: Kind,
}

#[derive(Debug)]
pub enum Kind {
    JournalEntry {
        entry_index: EntryIndex,
        entry: PlainRawEntry,
        /// If this entry generates new spans, use this SpanContext as parent span
        parent_span_context: Arc<SpanContext>,
    },
    Suspended {
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed {
        error_code: i32,
        error: InvokerError,
    },
}
