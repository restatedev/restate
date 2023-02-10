use common::types::{EntryIndex, PartitionLeaderEpoch, ServiceInvocationId};
use futures::Stream;
use hyper::Uri;
use journal::raw::RawEntry;
use journal::Completion;
use opentelemetry::Context;
use std::collections::HashSet;
use std::future::Future;
use tokio::sync::mpsc;

mod message;

mod invoker;
pub use crate::invoker::*;

mod invocation_task;

// --- Service Endpoint Registry

#[derive(Debug, Copy, Clone)]
pub enum ProtocolType {
    RequestResponse,
    BidiStream,
}

#[derive(Debug, Clone)]
pub struct EndpointMetadata {
    address: Uri,
    protocol_type: ProtocolType,
}

impl EndpointMetadata {
    pub fn new(address: Uri, protocol_type: ProtocolType) -> Self {
        Self {
            address,
            protocol_type,
        }
    }
}

pub trait ServiceEndpointRegistry {
    fn resolve_endpoint(&self, service_name: &str) -> Option<EndpointMetadata>;
}

// --- Journal Reader

#[allow(dead_code)]
#[derive(Debug)]
pub struct JournalMetadata {
    method: String,

    /// Span attached to this invocation.
    tracing_context: Context,

    journal_size: EntryIndex,
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
        entry: RawEntry,
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
