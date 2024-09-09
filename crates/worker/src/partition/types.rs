// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::invocation::{InvocationResponse, ResponseResult};
use restate_wal_protocol::Command;

pub(crate) type InvokerEffect = restate_invoker_api::Effect;
pub(crate) type InvokerEffectKind = restate_invoker_api::EffectKind;

// Extension methods to the OutboxMessage type
pub(crate) trait OutboxMessageExt {
    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage;

    fn to_command(self) -> Command;
}

impl OutboxMessageExt for OutboxMessage {
    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage {
        OutboxMessage::ServiceResponse(InvocationResponse {
            entry_index,
            result,
            id: invocation_id,
        })
    }

    fn to_command(self) -> Command {
        match self {
            OutboxMessage::ServiceInvocation(si) => Command::Invoke(si),
            OutboxMessage::ServiceResponse(sr) => Command::InvocationResponse(sr),
            OutboxMessage::InvocationTermination(it) => Command::TerminateInvocation(it),
        }
    }
}
