// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::ReservedResources;
use restate_errors::NotRunningError;
use restate_types::LimitKey;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::invocation::{FencingToken, InvocationTarget};
use restate_types::journal_v2::{CommandIndex, NotificationId};
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;

pub trait InvokerHandle {
    fn invoke(
        &mut self,
        invocation_id: InvocationId,
        fencing_token: FencingToken,
        invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError>;

    #[allow(clippy::too_many_arguments)]
    fn vqueue_invoke(
        &mut self,
        qid: VQueueId,
        permit: ReservedResources,
        invocation_id: InvocationId,
        fencing_token: FencingToken,
        invocation_target: InvocationTarget,
        limit_key: LimitKey<ReString>,
        idempotency_key: Option<ReString>,
    ) -> Result<(), NotRunningError>;

    // The `notify_*` forwards below don't carry a `fencing_token` as the invoke calls establish
    // the current fencing token which is used to stamp all outgoing invoker effects.
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

    /// Aborts the running task for `invocation_id`.
    ///
    /// Stale effects from a previous attempt are fenced at write time by the leader's in-memory
    /// fencing-token map: it is used to terminate an invocation, pause it, or clean up an orphaned
    /// task.
    fn abort_invocation(&mut self, invocation_id: InvocationId) -> Result<(), NotRunningError>;
}
