// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_errors::NotRunningError;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::invocation::InvocationTarget;
use restate_types::journal_v2::{CommandIndex, NotificationId};
use restate_types::vqueues::VQueueId;

use crate::resources::ReservedResources;

pub trait InvokerHandle {
    fn invoke(
        &mut self,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError>;

    fn vqueue_invoke(
        &mut self,
        qid: VQueueId,
        permit: ReservedResources,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError>;

    fn notify_completion(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) -> Result<(), NotRunningError>;

    fn notify_notification(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    ) -> Result<(), NotRunningError>;

    fn retry_invocation_now(&mut self, invocation_id: InvocationId) -> Result<(), NotRunningError>;

    fn pause_invocation(&mut self, invocation_id: InvocationId) -> Result<(), NotRunningError>;

    fn notify_stored_command_ack(
        &mut self,
        invocation_id: InvocationId,
        command_index: CommandIndex,
    ) -> Result<(), NotRunningError>;

    fn abort_all(&mut self) -> Result<(), NotRunningError>;

    /// *Note*: When aborting an invocation, and restarting it, the `invocation_epoch` MUST be bumped.
    fn abort_invocation(&mut self, invocation_id: InvocationId) -> Result<(), NotRunningError>;
}
