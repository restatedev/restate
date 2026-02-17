// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::identifiers::{EntryIndex, InvocationId, PartitionKey, PartitionLeaderEpoch};
use restate_types::invocation::InvocationTarget;
use restate_types::journal_v2::{CommandIndex, NotificationId};

use super::Effect;

pub trait InvokerHandle<SR> {
    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError>;

    fn vqueue_invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        qid: VQueueId,
        permit: Permit,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError>;

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) -> Result<(), NotRunningError>;

    fn notify_notification(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    ) -> Result<(), NotRunningError>;

    fn retry_invocation_now(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) -> Result<(), NotRunningError>;

    fn pause_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) -> Result<(), NotRunningError>;

    fn notify_stored_command_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
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
    ) -> Result<(), NotRunningError>;

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        storage_reader: SR,
        sender: mpsc::Sender<Box<Effect>>,
    ) -> Result<(), NotRunningError>;
}
