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
use restate_limiter::Level;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::invocation::{FencingToken, InvocationTarget};
use restate_types::journal_v2::{CommandIndex, NotificationId};
use restate_types::vqueues::VQueueId;
use restate_types::{LimitKey, Scope};
use restate_util_string::ReString;

use crate::ResourceKind;
use crate::resources::ReservedResources;

/// The resource a blocked vqueue is waiting for, used to ask the invoker to eagerly suspend
/// one waiting invocation that holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuspendIdleTarget {
    /// The node-wide invoker concurrency limit. Any invocation of this partition frees it.
    InvokerConcurrency,
    /// A scope/limit-key concurrency rule. Only invocations whose limit-key permit lies
    /// under `limit_key` (in `scope`) free it. `limit_key` is already truncated to the
    /// level at which the waiter is blocked.
    LimitKey {
        scope: Scope,
        limit_key: LimitKey<ReString>,
    },
}

impl SuspendIdleTarget {
    /// Maps the resource a queue is blocked on to a suspension target. Returns `None` for
    /// resources that suspending an invocation does not release (locks are held across
    /// suspensions) or that are not tied to an invocation (throttling, memory).
    pub fn from_blocked(resource: &ResourceKind) -> Option<Self> {
        match resource {
            ResourceKind::InvokerConcurrency => Some(Self::InvokerConcurrency),
            ResourceKind::LimitKeyConcurrency {
                scope,
                limit_key,
                blocked_level,
                ..
            } => {
                let limit_key = match blocked_level {
                    Level::Scope => LimitKey::None,
                    Level::Level1 => LimitKey::L1(limit_key.level1()?.clone()),
                    Level::Level2 => limit_key.clone(),
                };
                Some(Self::LimitKey {
                    scope: scope.clone(),
                    limit_key,
                })
            }
            ResourceKind::Lock { .. }
            | ResourceKind::InvokerThrottling { .. }
            | ResourceKind::InvokerMemory
            | ResourceKind::DeploymentConcurrency => None,
        }
    }
}

pub trait InvokerHandle {
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

    fn notify_notification(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    ) -> Result<(), NotRunningError>;

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

    /// Asks the invoker to suspend one in-flight invocation that is waiting on an await point
    /// and holds `target`, as if its inactivity timeout had fired. Best effort: nothing
    /// happens if no such invocation exists.
    fn suspend_idle(&mut self, target: SuspendIdleTarget) -> Result<(), NotRunningError>;
}
