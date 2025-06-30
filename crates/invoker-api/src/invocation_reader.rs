// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_types::identifiers::{InvocationId, ServiceId};
use restate_types::invocation::{InvocationEpoch, ServiceInvocationSpanContext};
use restate_types::journal::EntryIndex;
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
    pub invocation_epoch: InvocationEpoch,
    /// This value is not agreed among Partition processor replicas right now.
    ///
    /// The upper bound for the total clock skew is the clock skew of the different machines
    /// and the max time difference between two replicas applying the journal append command.
    pub last_modification_date: MillisSinceEpoch,
}

impl JournalMetadata {
    pub fn new(
        length: EntryIndex,
        span_context: ServiceInvocationSpanContext,
        pinned_deployment: Option<PinnedDeployment>,
        invocation_epoch: InvocationEpoch,
        last_modification_date: MillisSinceEpoch,
    ) -> Self {
        Self {
            pinned_deployment,
            span_context,
            length,
            last_modification_date,
            invocation_epoch,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum JournalEntry {
    JournalV1(PlainRawEntry),
    JournalV2(StoredRawEntry),
}

/// Read information about invocations from the underlying storage.
pub trait InvocationReader {
    type Transaction<'a>: InvocationReaderTransaction + Send
    where
        Self: 'a;

    /// Create a read transaction to read information about invocations from the underlying storage.
    fn transaction(&mut self) -> Self::Transaction<'_>;
}

/// Read transaction to read information about invocations from the underlying storage.
///
/// Important: Implementations must ensure that all read methods return consistent results.
pub trait InvocationReaderTransaction {
    type JournalStream: Stream<Item = JournalEntry> + Unpin + Send + 'static;
    type StateIter: Iterator<Item = (Bytes, Bytes)> + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Read the journal for the given invocation id.
    ///
    /// The returned journal **MUST** not return events.
    ///
    /// Returns `None` when either the invocation was not found, or the invocation is not in `Invoked` state.
    fn read_journal<'a>(
        &'a mut self,
        fid: &'a InvocationId,
    ) -> impl Future<Output = Result<Option<(JournalMetadata, Self::JournalStream)>, Self::Error>> + Send;

    /// Read the state for the given service id.
    fn read_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
    ) -> impl Future<Output = Result<EagerState<Self::StateIter>, Self::Error>> + Send;
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
