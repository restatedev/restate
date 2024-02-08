// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Effect;
use super::JournalMetadata;

use restate_errors::NotRunningError;
use restate_types::identifiers::{EntryIndex, PartitionLeaderEpoch};
use restate_types::identifiers::{FullInvocationId, PartitionKey};
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::Completion;
use std::future::Future;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;

#[derive(Debug, Default)]
pub enum InvokeInputJournal {
    #[default]
    NoCachedJournal,
    CachedJournal(JournalMetadata, Vec<PlainRawEntry>),
}

// TODO We can remove FullInvocationId awareness from the invoker completely

pub trait ServiceHandle {
    type Future: Future<Output = Result<(), NotRunningError>>;

    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future;

    fn resume(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future;

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        completion: Completion,
    ) -> Self::Future;

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future;

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future;

    fn abort_invocation(
        &mut self,
        partition_leader_epoch: PartitionLeaderEpoch,
        full_invocation_id: FullInvocationId,
    ) -> Self::Future;

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        sender: mpsc::Sender<Effect>,
    ) -> Self::Future;
}
