// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::timer_table::TimerKey;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use restate_types::invocation::client::{
    CancelInvocationResponse, InvocationOutputResponse, KillInvocationResponse,
    PurgeInvocationResponse, ResumeInvocationResponse,
};
use restate_types::invocation::{InvocationEpoch, InvocationTarget};
use restate_types::journal::Completion;
use restate_types::journal_v2::CommandIndex;
use restate_types::journal_v2::raw::RawNotification;
use restate_types::message::MessageIndex;
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::timer::TimerKeyValue;
use std::time::Duration;

pub type ActionCollector = Vec<Action>;

#[derive(Debug, Eq, PartialEq, strum::IntoStaticStr)]
pub enum Action {
    Invoke {
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
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
        invocation_epoch: InvocationEpoch,
        command_index: CommandIndex,
    },
    ForwardCompletion {
        invocation_id: InvocationId,
        completion: Completion,
    },
    ForwardNotification {
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        notification: RawNotification,
    },
    AbortInvocation {
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
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
}

impl Action {
    pub fn name(&self) -> &'static str {
        self.into()
    }
}
