use bytes::Bytes;
use futures::Stream;
use restate_common::errors::{InvocationError, InvocationErrorCode, UserErrorCode};
use restate_common::retry_policy::RetryPolicy;
use restate_common::types::{
    EnrichedRawEntry, EntryIndex, JournalMetadata, PartitionLeaderEpoch, ServiceId,
    ServiceInvocationId,
};
use restate_journal::raw::PlainRawEntry;
use restate_journal::Completion;
use std::collections::HashSet;
use std::future::Future;
use std::time::SystemTime;
use tokio::sync::mpsc;

mod invoker;
pub use crate::invoker::*;

mod invocation_task;

mod options;
pub use options::Options;

mod status;

pub use status::{InvocationErrorReport, InvocationStatusReport, InvokerStatusReader};

// --- Error trait used to figure out whether errors are transient or not

trait InvokerError: std::error::Error {
    fn is_transient(&self) -> bool;
    fn to_invocation_error(&self) -> InvocationError;

    fn as_invocation_error_code(&self) -> InvocationErrorCode {
        UserErrorCode::Internal.into()
    }
}

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

/// Container for the eager state returned by [`StateReader`]
pub struct EagerState<I> {
    iterator: I,
    partial: bool,
}

impl<I: Default> Default for EagerState<I> {
    fn default() -> Self {
        Self {
            iterator: I::default(),
            partial: true,
        }
    }
}

impl<I> EagerState<I> {
    /// Create an [`EagerState`] where the provided iterator contains only a subset of entries of the given service instance.
    pub fn new_partial(iterator: I) -> Self {
        Self {
            iterator,
            partial: true,
        }
    }

    /// Create an [`EagerState`] where the provided iterator contains all the set of entries of the given service instance.
    pub fn new_complete(iterator: I) -> Self {
        Self {
            iterator,
            partial: false,
        }
    }

    /// If true, it is not guaranteed the iterator will return all the entries for the given service instance.
    pub fn is_partial(&self) -> bool {
        self.partial
    }

    pub fn map<U, F: FnOnce(I) -> U>(self, f: F) -> EagerState<U> {
        EagerState {
            iterator: f(self.iterator),
            partial: self.partial,
        }
    }
}

impl<I: Iterator<Item = (Bytes, Bytes)>> IntoIterator for EagerState<I> {
    type Item = (Bytes, Bytes);
    type IntoIter = I;

    fn into_iter(self) -> Self::IntoIter {
        self.iterator
    }
}

pub trait StateReader {
    type StateIter: Iterator<Item = (Bytes, Bytes)>;
    type Error: std::error::Error + Send + Sync + 'static;
    type Future<'a>: Future<Output = Result<EagerState<Self::StateIter>, Self::Error>> + Send
    where
        Self: 'a;

    fn read_state<'a>(&'a self, service_id: &'a ServiceId) -> Self::Future<'_>;
}

// --- Invoker input sender

#[derive(Debug, Default)]
pub enum InvokeInputJournal {
    #[default]
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

    fn abort_invocation(
        &mut self,
        partition_leader_epoch: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) -> Self::Future;

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Effect>,
    ) -> Self::Future;
}

// --- Invoker effects

#[derive(Debug)]
pub struct Effect {
    pub service_invocation_id: ServiceInvocationId,
    pub kind: EffectKind,
}

#[derive(Debug)]
pub enum EffectKind {
    JournalEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    },
    Suspended {
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed(InvocationError),
}
