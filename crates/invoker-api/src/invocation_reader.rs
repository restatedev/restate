// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use futures::Stream;
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{EntryIndex, InvocationId, ServiceId};
use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::CompletionResult;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::storage::StoredRawEntry;
use restate_types::time::MillisSinceEpoch;
use std::future::Future;

/// Metadata associated with a journal
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JournalMetadata {
    pub length: EntryIndex,
    pub span_context: ServiceInvocationSpanContext,
    pub pinned_deployment: Option<PinnedDeployment>,
    /// This value is not agreed among Partition processor replicas right now.
    ///
    /// The upper bound for the total clock skew is the clock skew of the different machines
    /// and the max time difference between two replicas applying the journal append command.
    pub last_modification_date: MillisSinceEpoch,
    pub random_seed: u64,
    /// If true, the entries are stored in journal table v2
    pub using_journal_table_v2: bool,
}

impl JournalMetadata {
    pub fn new(
        length: EntryIndex,
        span_context: ServiceInvocationSpanContext,
        pinned_deployment: Option<PinnedDeployment>,
        last_modification_date: MillisSinceEpoch,
        random_seed: u64,
        using_journal_table_v2: bool,
    ) -> Self {
        Self {
            pinned_deployment,
            span_context,
            length,
            last_modification_date,
            random_seed,
            using_journal_table_v2,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum JournalEntry {
    JournalV1(PlainRawEntry),
    /// V1 journal entry where only the completion result is stored (completion arrived before entry).
    JournalV1Completion(CompletionResult),
    JournalV2(StoredRawEntry),
}

/// Read information about invocations from the underlying storage.
pub trait InvocationReader {
    type Transaction<'a>: InvocationReaderTransaction + Send
    where
        Self: 'a;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Create a read transaction to read information about invocations from the underlying storage.
    fn transaction(&mut self) -> Self::Transaction<'_>;

    /// Non-transactional point read of a single journal entry by index.
    ///
    /// Used during the bidi-stream phase (after the replay transaction is dropped)
    /// to read notification/completion data on-demand when a signal-only notification arrives.
    /// Non-transactional reads are appropriate here because entries are immutable once written.
    fn read_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        using_journal_table_v2: bool,
    ) -> impl Future<Output = Result<Option<JournalEntry>, Self::Error>> + Send;
}

/// Read transaction to read information about invocations from the underlying storage.
///
/// Important: Implementations must ensure that all read methods return consistent results.
pub trait InvocationReaderTransaction {
    type JournalStream<'a>: Stream<Item = Result<JournalEntry, Self::Error>> + Unpin + Send
    where
        Self: 'a;
    type StateStream<'a>: Stream<Item = Result<(Bytes, Bytes), Self::Error>> + Send
    where
        Self: 'a;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Read only the journal metadata for the given invocation id.
    ///
    /// Returns `None` when either the invocation was not found, or the invocation
    /// is not in `Invoked` state.
    fn read_journal_metadata(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<Option<JournalMetadata>, Self::Error>> + Send;

    /// Read the journal stream for replay.
    ///
    /// The returned journal **MUST** not return events.
    /// This method returns a lazy stream that reads entries on-demand from storage.
    fn read_journal(
        &self,
        invocation_id: &InvocationId,
        length: EntryIndex,
        using_journal_table_v2: bool,
    ) -> Result<Self::JournalStream<'_>, Self::Error>;

    /// Read the state for the given service id.
    /// Returns a stream of state entries that can be collected by the caller.
    fn read_state(
        &self,
        service_id: &ServiceId,
    ) -> Result<EagerState<Self::StateStream<'_>>, Self::Error>;
}

/// Container for the state returned by [`InvocationReaderTransaction::read_state`].
/// Contains a stream of state entries and a flag indicating if the state is partial.
pub struct EagerState<S> {
    stream: S,
    partial: bool,
}

impl<S> EagerState<S> {
    /// Create an [`EagerState`] where the provided stream contains only a subset of entries of the given service instance.
    pub fn new_partial(stream: S) -> Self {
        Self {
            stream,
            partial: true,
        }
    }

    /// Create an [`EagerState`] where the provided stream contains all the set of entries of the given service instance.
    pub fn new_complete(stream: S) -> Self {
        Self {
            stream,
            partial: false,
        }
    }

    /// If true, it is not guaranteed the stream will return all the entries for the given service instance.
    pub fn is_partial(&self) -> bool {
        self.partial
    }

    /// Consume the container and return the inner stream.
    pub fn into_inner(self) -> S {
        self.stream
    }
}
