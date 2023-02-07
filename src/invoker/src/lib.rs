use common::types::{PartitionLeaderEpoch, ServiceInvocationId};
use futures::Stream;
use journal::raw::RawEntry;
use journal::{Completion, EntryIndex};
use opentelemetry::Context;
use std::future::Future;
use tokio::sync::mpsc;

mod invoker;
pub use crate::invoker::*;

// --- Journal

pub type JournalRevision = u32;

#[allow(dead_code)]
#[derive(Debug)]
pub struct JournalMetadata {
    method: String,

    /// Span attached to this invocation.
    tracing_context: Context,

    journal_size: EntryIndex,
    journal_revision: JournalRevision,
}

// --- Input messages

#[derive(Debug)]
pub struct Input<I> {
    partition: PartitionLeaderEpoch,
    inner: I,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct InvokeInputCommand {
    service_invocation_id: ServiceInvocationId,

    journal: InvokeInputJournal,
}

#[derive(Debug)]
pub enum InvokeInputJournal {
    NoCachedJournal,
    CachedJournal(JournalMetadata, Vec<RawEntry>),
}

#[derive(Debug)]
pub enum OtherInputCommand {
    Completion {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
        journal_revision: JournalRevision,
    },
    StoredEntryAck {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        journal_revision: JournalRevision,
    },

    /// Command used to clean up internal state when a partition leader is going away
    AbortAllPartition,
    AbortInvocation {
        service_invocation_id: ServiceInvocationId,
    },

    // needed for dynamic registration at Invoker
    RegisterPartition(mpsc::Sender<Output>),
}

impl Input<InvokeInputCommand> {
    pub fn new_invoke(
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) -> Self {
        Self {
            partition,
            inner: InvokeInputCommand {
                service_invocation_id,
                journal: InvokeInputJournal::NoCachedJournal,
            },
        }
    }

    pub fn new_cached_invoke(
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        journal: Vec<RawEntry>,
    ) -> Self {
        Self {
            partition,
            inner: InvokeInputCommand {
                service_invocation_id,
                journal: InvokeInputJournal::CachedJournal(journal_metadata, journal),
            },
        }
    }
}

impl Input<OtherInputCommand> {
    pub fn new_completion(
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
        journal_revision: JournalRevision,
    ) -> Self {
        Self {
            partition,
            inner: OtherInputCommand::Completion {
                service_invocation_id,
                completion,
                journal_revision,
            },
        }
    }

    pub fn new_stored_entry_ack(
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        journal_revision: JournalRevision,
    ) -> Self {
        Self {
            partition,
            inner: OtherInputCommand::StoredEntryAck {
                service_invocation_id,
                entry_index,
                journal_revision,
            },
        }
    }

    pub fn new_abort_all_partition(partition: PartitionLeaderEpoch) -> Self {
        Self {
            partition,
            inner: OtherInputCommand::AbortAllPartition,
        }
    }

    pub fn new_abort_invocation(
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) -> Self {
        Self {
            partition,
            inner: OtherInputCommand::AbortInvocation {
                service_invocation_id,
            },
        }
    }

    pub fn new_register_partition(
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Output>,
    ) -> Self {
        Self {
            partition,
            inner: OtherInputCommand::RegisterPartition(sender),
        }
    }
}

// --- Output messages

pub type InvokerError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
pub enum Output {
    JournalEntry {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        entry: RawEntry,
    },
    Suspended {
        service_invocation_id: ServiceInvocationId,
    },
    End {
        service_invocation_id: ServiceInvocationId,
    },
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed {
        service_invocation_id: ServiceInvocationId,
        error: InvokerError,
    },
}

// --- Journal Reader

pub trait JournalReader {
    type JournalStream: Stream<Item = RawEntry>;
    type Error;
    type Future: Future<Output = Result<(JournalMetadata, Self::JournalStream), Self::Error>>;

    fn read_journal(&self, sid: &ServiceInvocationId) -> Self::Future;
}
