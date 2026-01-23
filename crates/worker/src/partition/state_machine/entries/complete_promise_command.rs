// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};
use restate_storage_api::promise_table::{
    Promise, PromiseState, ReadPromiseTable, WritePromiseTable,
};
use restate_storage_api::state_table::ReadStateTable;
use restate_types::errors::ALREADY_COMPLETED_INVOCATION_ERROR;
use restate_types::invocation::{InvocationResponse, ResponseResult};
use restate_types::journal_v2::{
    CompletePromiseCommand, CompletePromiseCompletion, CompletePromiseResult, CompletePromiseValue,
    EntryMetadata,
};
use tracing::warn;

pub(super) type ApplyCompletePromiseCommand<'e> =
    ApplyJournalCommandEffect<'e, CompletePromiseCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyCompletePromiseCommand<'e>
where
    S: ReadStateTable + ReadPromiseTable + WritePromiseTable + WriteFsmTable + WriteOutboxTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let complete_result = if let Some(service_id) =
            invocation_metadata.invocation_target.as_keyed_service_id()
        {
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
                                state: PromiseState::Completed(self.entry.value.clone().into()),
                            },
                        )
                        .map_err(Error::Storage)?;
                    CompletePromiseResult::Void
                }
                Some(Promise {
                    state: PromiseState::NotCompleted(listeners),
                }) => {
                    // Send response to listeners
                    // TODO there's no point here to send an outbox message,
                    //  because we have the guarantee the the listener has the same partition key, so we could just process the command now.
                    //  Because we still miss the API for doing that, for now we use the outbox.
                    for listener in listeners {
                        ctx.handle_outgoing_message(OutboxMessage::ServiceResponse(
                            InvocationResponse {
                                target: listener,
                                result: match self.entry.value.clone() {
                                    CompletePromiseValue::Success(s) => ResponseResult::Success(s),
                                    CompletePromiseValue::Failure(f) => {
                                        ResponseResult::Failure(f.into())
                                    }
                                },
                            },
                        ))?;
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
                                state: PromiseState::Completed(self.entry.value.clone().into()),
                            },
                        )
                        .map_err(Error::Storage)?;
                    CompletePromiseResult::Void
                }
                Some(Promise {
                    state: PromiseState::Completed(_),
                }) => {
                    // Conflict!
                    CompletePromiseResult::Failure(ALREADY_COMPLETED_INVOCATION_ERROR.into())
                }
            }
        } else {
            warn!(
                "Trying to process entry {} for a target that has no promises",
                self.entry.ty()
            );
            CompletePromiseResult::Void
        };

        self.then_apply_completion(CompletePromiseCompletion {
            completion_id: self.entry.completion_id,
            result: complete_result,
        });

        Ok(())
    }
}
