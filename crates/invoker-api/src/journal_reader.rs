// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::EntryIndex;
use restate_types::time::MillisSinceEpoch;
use std::future::Future;

/// Metadata associated with a journal
#[derive(Debug, Clone, PartialEq)]
pub struct JournalMetadata {
    pub length: EntryIndex,
    pub span_context: ServiceInvocationSpanContext,
    pub pinned_deployment: Option<PinnedDeployment>,
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
        last_modification_date: MillisSinceEpoch,
    ) -> Self {
        Self {
            pinned_deployment,
            span_context,
            length,
            last_modification_date,
        }
    }
}

pub trait JournalReader {
    type JournalStream: Stream<Item = PlainRawEntry>;
    type Error: std::error::Error + Send + Sync + 'static;

    fn read_journal<'a>(
        &'a mut self,
        fid: &'a InvocationId,
    ) -> impl Future<Output = Result<(JournalMetadata, Self::JournalStream), Self::Error>> + Send;
}
