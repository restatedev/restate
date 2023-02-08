use common::types::{EntryIndex, PartitionLeaderEpoch, ServiceInvocationId};
use futures::Stream;
use journal::raw::RawEntry;
use journal::{Completion, JournalRevision};
use opentelemetry::Context;
use std::future::Future;
use tokio::sync::mpsc;

mod invoker;
pub use crate::invoker::*;

mod message;

// --- Journal Reader

#[allow(dead_code)]
#[derive(Debug)]
pub struct JournalMetadata {
    method: String,

    /// Span attached to this invocation.
    tracing_context: Context,

    journal_size: EntryIndex,
    journal_revision: JournalRevision,
}

pub trait JournalReader {
    type JournalStream: Stream<Item = RawEntry>;
    type Error;
    type Future: Future<Output = Result<(JournalMetadata, Self::JournalStream), Self::Error>>;

    fn read_journal(&self, sid: &ServiceInvocationId) -> Self::Future;
}

// --- Invoker input sender

#[derive(Debug)]
pub enum InvokeInputJournal {
    NoCachedJournal,
    CachedJournal(JournalMetadata, Vec<RawEntry>),
}

pub trait InvokerInputSender {
    type Error;
    type Future: Future<Output = Result<(), Self::Error>>;

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
        journal_revision: JournalRevision,
        completion: Completion,
    ) -> Self::Future;
    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal_revision: JournalRevision,
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
        entry: RawEntry,
    },
    Suspended {
        journal_revision: JournalRevision,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed {
        error: InvokerError,
    },
}
