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
use restate_types::identifiers::{EntryIndex, InvocationId, PartitionProcessorRpcRequestId};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::Completion;
use restate_types::message::MessageIndex;
use restate_types::net::partition_processor::IngressResponseResult;
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::timer::TimerKeyValue;
use std::time::Duration;

pub type ActionCollector = Vec<Action>;

#[derive(Debug, Eq, PartialEq, strum::IntoStaticStr)]
pub enum Action {
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
    AckStoredEntry {
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    },
    ForwardCompletion {
        invocation_id: InvocationId,
        completion: Completion,
    },
    AbortInvocation {
        invocation_id: InvocationId,
        acknowledge: bool,
    },
    IngressResponse {
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: Option<InvocationId>,
        completion_expiry_time: Option<MillisSinceEpoch>,
        response: IngressResponseResult,
    },
    IngressSubmitNotification {
        request_id: PartitionProcessorRpcRequestId,
        /// If true, this request_id created a "fresh invocation",
        /// otherwise the invocation was previously submitted.
        is_new_invocation: bool,
    },
    ScheduleInvocationStatusCleanup {
        invocation_id: InvocationId,
        retention: Duration,
    },
}

impl Action {
    pub fn name(&self) -> &'static str {
        self.into()
    }
}
