// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_limiter::RuleUpdate;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::timer_table::TimerKey;
use restate_storage_api::vqueue_table::EntryKey;
use restate_types::identifiers::{EntryIndex, InvocationId, PartitionProcessorRpcRequestId};
use restate_types::invocation::InvocationTarget;
use restate_types::invocation::client::{
    CancelInvocationResponse, InvocationOutput, KillInvocationResponse, PauseInvocationResponse,
    PurgeInvocationResponse, RestartAsNewInvocationResponse, ResumeInvocationResponse,
    SubmittedInvocationNotification,
};
use restate_types::journal_v2::{CommandIndex, NotificationId};
use restate_types::message::MessageIndex;
use restate_util_string::ReString;
use restate_vqueues::{VQueueEvent, VQueueHandle};
use restate_wal_protocol::timer::TimerKeyValue;

pub type ActionCollector = Vec<Action>;

#[derive(derive_more::Debug)]
pub enum RpcReply {
    Output(InvocationOutput),
    Submitted(SubmittedInvocationNotification),
    KillInvocation(KillInvocationResponse),
    CancelInvocation(CancelInvocationResponse),
    PurgeInvocation(PurgeInvocationResponse),
    PurgeJournal(PurgeInvocationResponse),
    ResumeInvocation(ResumeInvocationResponse),
    PauseInvocation(PauseInvocationResponse),
    RestartAsNewInvocation(RestartAsNewInvocationResponse),
}

#[derive(derive_more::Debug, strum::IntoStaticStr)]
pub enum Action {
    /// Notifies the scheduler about a vqueue inbox event (e.g, enqueue, run permitted, etc.)
    VQEvent(VQueueEvent),
    /// Tells invoker to run this invocation (similar to Invoke) but carries more information
    VQInvoke {
        vq_handle: VQueueHandle,
        key: EntryKey,
        invocation_target: InvocationTarget,
        idempotency_key: Option<ReString>,
    },
    NewOutboxMessage {
        seq_number: MessageIndex,
        message: OutboxMessage,
    },
    RegisterTimer {
        timer_value: TimerKeyValue,
    },
    DeleteTimer {
        timer_key: TimerKey,
    },
    AckStoredCommand {
        invocation_id: InvocationId,
        command_index: CommandIndex,
    },
    ForwardNotification {
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    },
    AbortInvocation {
        invocation_id: InvocationId,
    },
    ReplyRpc {
        request_id: PartitionProcessorRpcRequestId,
        reply: RpcReply,
    },

    /// Forward a batch of rule-book diff entries to the leader's
    /// `UserLimiter` via the resource-manager mpsc. Emitted by
    /// `Command::UpsertRuleBook` apply when the rule book version
    /// advances. Followers ignore this action (no live UserLimiter to
    /// notify); only the leader's `leader_state` dispatches it onward.
    RulesUpdated(Box<[RuleUpdate]>),
}

impl From<VQueueEvent> for Action {
    #[inline(always)]
    fn from(value: VQueueEvent) -> Self {
        Self::VQEvent(value)
    }
}

impl Action {
    pub fn name(&self) -> &'static str {
        self.into()
    }
}
