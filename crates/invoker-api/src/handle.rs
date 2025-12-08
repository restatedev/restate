// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_types::vqueue::VQueueId;
use tokio::sync::mpsc;

use restate_errors::NotRunningError;
use restate_futures_util::concurrency::Permit;
use restate_types::identifiers::PartitionKey;
use restate_types::identifiers::{InvocationId, PartitionLeaderEpoch};
use restate_types::invocation::{InvocationEpoch, InvocationTarget};
use restate_types::journal::Completion;
use restate_types::journal_v2::CommandIndex;
use restate_types::journal_v2::raw::RawNotification;

use super::Effect;
use super::JournalMetadata;
use crate::invocation_reader::JournalEntry;

#[derive(Debug, Eq, PartialEq, Default)]
pub enum InvokeInputJournal {
    #[default]
    NoCachedJournal,
    CachedJournal(JournalMetadata, Vec<JournalEntry>),
}

pub trait InvokerHandle<SR> {
    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        invocation_target: InvocationTarget,
        journal: InvokeInputJournal,
    ) -> Result<(), NotRunningError>;

    fn vqueue_invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        qid: VQueueId,
        permit: Permit<()>,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        journal: InvokeInputJournal,
    ) -> Result<(), NotRunningError>;

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Result<(), NotRunningError>;

    fn notify_notification(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        entry: RawNotification,
    ) -> Result<(), NotRunningError>;

    fn retry_invocation_now(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) -> Result<(), NotRunningError>;

    fn pause_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) -> Result<(), NotRunningError>;

    fn notify_stored_command_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        command_index: CommandIndex,
    ) -> Result<(), NotRunningError>;

    fn abort_all_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Result<(), NotRunningError>;

    /// *Note*: When aborting an invocation, and restarting it, the `invocation_epoch` MUST be bumped.
    fn abort_invocation(
        &mut self,
        partition_leader_epoch: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) -> Result<(), NotRunningError>;

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        storage_reader: SR,
        sender: mpsc::Sender<Box<Effect>>,
    ) -> Result<(), NotRunningError>;
}
