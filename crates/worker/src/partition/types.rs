// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::invocation::{InvocationResponse, JournalCompletionTarget, ResponseResult};

pub(crate) type InvokerEffect = restate_worker_api::invoker::FencedEffect;
pub(crate) type InvokerEffectKind = restate_worker_api::invoker::EffectKind;

// Extension methods to the OutboxMessage type
pub(crate) trait OutboxMessageExt {
    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage;
}

impl OutboxMessageExt for OutboxMessage {
    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage {
        OutboxMessage::ServiceResponse(InvocationResponse {
            target: JournalCompletionTarget {
                caller_id: invocation_id,
                caller_completion_id: entry_index,
            },
            result,
        })
    }
}
