// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::debug_if_leader;
use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use assert2::let_assert;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::promise_table::{Promise, PromiseState};
use restate_storage_api::state_table::ReadOnlyStateTable;
use restate_types::errors::ALREADY_COMPLETED_INVOCATION_ERROR;
use restate_types::invocation::InvocationResponse;
use restate_types::journal::{Completion, CompletionResult};
use restate_types::journal_v2::{
    CompletePromiseCommand, CompletePromiseCompletion, CompletePromiseResult, EntryMetadata,
    GetLazyStateCompletion, GetStateResult,
};
use tracing::warn;

pub(super) type ApplyCompletePromiseCommand<'e> =
    ApplyJournalCommandEffect<'e, CompletePromiseCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyCompletePromiseCommand<'e>
where
    S: ReadOnlyStateTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let complete_result =
            if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
                // Load state and write completion
                let promise_metadata = ctx
                    .storage
                    .get_promise(&service_id, &self.entry.key)
                    .await?;

                match promise_metadata {
                    None => {
                        debug_if_leader!(
                            ctx.is_leader,
                            rpc.service = %service_id.service_name,
                            "Complete promise {} without listeners",
                            self.entry.key
                        );

                        ctx.storage
                            .put_promise(
                                &service_id,
                                &self.entry.key,
                                &Promise {
                                    state: PromiseState::Completed(self.entry.value.into()),
                                },
                            )
                            .await;
                        CompletePromiseResult::Void
                    }
                    Some(Promise {
                        state: PromiseState::NotCompleted(listeners),
                    }) => {
                        // Send response to listeners
                        for listener in listeners {
                            ctx.handle_outgoing_message(OutboxMessage::ServiceResponse(
                                InvocationResponse {
                                    id: listener.invocation_id(),
                                    entry_index: listener.journal_index(),
                                    result: completion.clone().into(),
                                },
                            ))
                            .await?;
                        }

                        debug_if_leader!(
                            ctx.is_leader,
                            rpc.service = %service_id.service_name,
                            "Complete promise {} with listeners waiting on it",
                            self.entry.key
                        );

                        ctx.storage
                            .put_promise(
                                &service_id,
                                &self.entry.key,
                                &Promise {
                                    state: PromiseState::Completed(self.entry.value.into()),
                                },
                            )
                            .await;
                        CompletePromiseResult::Void
                    }
                    Some(Promise {
                        state: PromiseState::Completed(_),
                    }) => {
                        // Conflict!
                        CompletePromiseResult::Failure(&ALREADY_COMPLETED_INVOCATION_ERROR.into())
                    }
                }
            } else {
                warn!(
                    "Trying to process entry {} for a target that has no promises",
                    self.entry.ty()
                );
                CompletePromiseResult::Void
            };

        self.then_apply(CompletePromiseCompletion {
            completion_id: self.entry.completion_id,
            result: complete_result,
        });

        Ok(())
    }
}
