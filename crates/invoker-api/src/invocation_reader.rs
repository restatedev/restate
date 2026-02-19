// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use bytes::Bytes;
use futures::Stream;

use restate_memory::{BudgetLease, DirectionalBudget};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{EntryIndex, InvocationId, ServiceId};
use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::CompletionResult;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::storage::StoredRawEntry;
use restate_types::time::MillisSinceEpoch;

/// Which journal storage table an invocation's entries are stored in.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum JournalKind {
    /// Legacy journal table (service protocol v1-v3).
    V1,
    /// New journal table (service protocol v4+).
    V2,
}

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
    pub journal_kind: JournalKind,
}

impl JournalMetadata {
    pub fn new(
        length: EntryIndex,
        span_context: ServiceInvocationSpanContext,
        pinned_deployment: Option<PinnedDeployment>,
        last_modification_date: MillisSinceEpoch,
        random_seed: u64,
        journal_kind: JournalKind,
    ) -> Self {
        Self {
            pinned_deployment,
            span_context,
            length,
            last_modification_date,
            random_seed,
            journal_kind,
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

/// Error trait for invocation reader operations.
///
/// Implementations should indicate when a failure is due to memory budget
/// exhaustion so that callers can yield the invocation instead of treating
/// it as a transient storage error.
pub trait InvocationReaderError: std::error::Error + Send + Sync + 'static {
    /// If this error represents a memory budget exhaustion, returns the number
    /// of bytes that were requested but could not be allocated.
    fn budget_exhaustion(&self) -> Option<usize>;
}

impl InvocationReaderError for std::convert::Infallible {
    fn budget_exhaustion(&self) -> Option<usize> {
        match *self {}
    }
}

/// Read information about invocations from the underlying storage.
pub trait InvocationReader {
    type Transaction<'a>: InvocationReaderTransaction + Send
    where
        Self: 'a;
    type Error: InvocationReaderError;

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
        journal_kind: JournalKind,
    ) -> impl Future<Output = Result<Option<JournalEntry>, Self::Error>> + Send;

    /// Budget-aware point read of a single journal entry.
    ///
    /// Reads the entry and acquires a [`BudgetLease`] for its serialized size from
    /// the given outbound budget. The lease tracks the memory until the caller
    /// releases it (typically when hyper consumes the corresponding frame).
    ///
    // TODO: Push the budget into the lower storage layers so that the lease is
    //  acquired *before* the entry is decoded into memory.
    fn read_journal_entry_budgeted(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        journal_kind: JournalKind,
        budget: &mut DirectionalBudget,
    ) -> impl Future<Output = Result<Option<(JournalEntry, BudgetLease)>, Self::Error>> + Send;
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
    type BudgetedJournalStream<'a>: Stream<Item = Result<(JournalEntry, BudgetLease), Self::Error>>
        + Unpin
        + Send
    where
        Self: 'a;
    type BudgetedStateStream<'a>: Stream<Item = Result<((Bytes, Bytes), BudgetLease), Self::Error>>
        + Send
    where
        Self: 'a;
    type Error: InvocationReaderError;

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
        journal_kind: JournalKind,
    ) -> Result<Self::JournalStream<'_>, Self::Error>;

    /// Read the state for the given service id.
    /// Returns a stream of state entries that can be collected by the caller.
    fn read_state(
        &self,
        service_id: &ServiceId,
    ) -> Result<EagerState<Self::StateStream<'_>>, Self::Error>;

    /// Budget-gated journal replay stream.
    ///
    /// Each entry's raw byte size is peeked from RocksDB first. A [`BudgetLease`]
    /// is acquired from `budget` for that size *before* the entry is decoded.
    /// The lease flows through to the caller and must be held until the
    /// corresponding data is no longer needed (e.g., until hyper consumes the frame).
    fn read_journal_budgeted<'a>(
        &'a self,
        invocation_id: &InvocationId,
        length: EntryIndex,
        journal_kind: JournalKind,
        budget: &'a mut DirectionalBudget,
    ) -> Result<Self::BudgetedJournalStream<'a>, Self::Error>;

    /// Budget-gated state loading stream.
    ///
    /// Each state entry's raw byte size is peeked first. A [`BudgetLease`]
    /// is acquired from `budget` for that size *before* the entry is decoded.
    fn read_state_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        budget: &'a mut DirectionalBudget,
    ) -> Result<EagerState<Self::BudgetedStateStream<'a>>, Self::Error>;
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
