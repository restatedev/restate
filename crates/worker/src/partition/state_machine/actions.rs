// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::timer_table::TimerKey;
use restate_storage_api::vqueue_table::EntryCard;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use restate_types::invocation::InvocationTarget;
use restate_types::invocation::client::{
    CancelInvocationResponse, InvocationOutputResponse, KillInvocationResponse,
    PurgeInvocationResponse, RestartAsNewInvocationResponse, ResumeInvocationResponse,
};
use restate_types::journal::Completion;
use restate_types::journal_v2::CommandIndex;
use restate_types::journal_v2::raw::RawNotification;
use restate_types::message::MessageIndex;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueue::VQueueId;
use restate_vqueues::VQueueEvent;
use restate_wal_protocol::timer::TimerKeyValue;

pub type ActionCollector = Vec<Action>;

#[derive(derive_more::Debug, Eq, PartialEq, strum::IntoStaticStr)]
pub enum Action {
    /// Notifies the scheduler about a vqueue inbox event (e.g, enqueue, run permitted, etc.)
    VQEvent(VQueueEvent<EntryCard>),
    /// Tells invoker to run this invocation (similar to Invoke) but carries more information
    VQInvoke {
        qid: VQueueId,
        item_hash: u64,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        invoke_input_journal: InvokeInputJournal,
    },
    Invoke {
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        invoke_input_journal: InvokeInputJournal,
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
    ForwardCompletion {
        invocation_id: InvocationId,
        completion: Completion,
    },
    ForwardNotification {
        invocation_id: InvocationId,
        notification: RawNotification,
    },
    AbortInvocation {
        invocation_id: InvocationId,
    },
    IngressResponse {
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: Option<InvocationId>,
        completion_expiry_time: Option<MillisSinceEpoch>,
        response: InvocationOutputResponse,
    },
    IngressSubmitNotification {
        request_id: PartitionProcessorRpcRequestId,
        execution_time: Option<MillisSinceEpoch>,
        /// If true, this request_id created a "fresh invocation",
        /// otherwise the invocation was previously submitted.
        is_new_invocation: bool,
    },
    ScheduleInvocationStatusCleanup {
        invocation_id: InvocationId,
        retention: Duration,
    },
    ForwardKillResponse {
        request_id: PartitionProcessorRpcRequestId,
        response: KillInvocationResponse,
    },
    ForwardCancelResponse {
        request_id: PartitionProcessorRpcRequestId,
        response: CancelInvocationResponse,
    },
    ForwardPurgeInvocationResponse {
        request_id: PartitionProcessorRpcRequestId,
        response: PurgeInvocationResponse,
    },
    ForwardPurgeJournalResponse {
        request_id: PartitionProcessorRpcRequestId,
        response: PurgeInvocationResponse,
    },
    ForwardResumeInvocationResponse {
        request_id: PartitionProcessorRpcRequestId,
        response: ResumeInvocationResponse,
    },
    ForwardRestartAsNewInvocationResponse {
        request_id: PartitionProcessorRpcRequestId,
        response: RestartAsNewInvocationResponse,
    },
}

impl From<VQueueEvent<EntryCard>> for Action {
    #[inline(always)]
    fn from(value: VQueueEvent<EntryCard>) -> Self {
        Self::VQEvent(value)
    }
}

impl Action {
    pub fn name(&self) -> &'static str {
        self.into()
    }
}
