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
use bytes::Bytes;
use restate_storage_api::promise_table::{
    Promise, PromiseState, ReadPromiseTable, WritePromiseTable,
};
use restate_types::invocation::JournalCompletionTarget;
use restate_types::journal_v2::{
    EntryMetadata, GetPromiseCommand, GetPromiseCompletion, GetPromiseResult,
};
use tracing::warn;

pub(super) type ApplyGetPromiseCommand<'e> = ApplyJournalCommandEffect<'e, GetPromiseCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyGetPromiseCommand<'e>
where
    S: ReadPromiseTable + WritePromiseTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
            // Load state and write completion
            let promise_metadata = ctx
                .storage
                .get_promise(&service_id, &self.entry.key)
                .await?;

            let uncompleted_promise_state = match promise_metadata {
                Some(Promise {
                    state: PromiseState::Completed(result),
                }) => {
                    // Result is already available
                    self.then_apply_completion(GetPromiseCompletion {
                        completion_id: self.entry.completion_id,
                        result: result.into(),
                    });
                    return Ok(());
                }
                Some(Promise {
                    state: PromiseState::NotCompleted(mut v),
                }) => {
                    v.push(JournalCompletionTarget::from_parts(
                        self.invocation_id,
                        self.entry.completion_id,
                    ));
                    PromiseState::NotCompleted(v)
                }
                None => PromiseState::NotCompleted(vec![JournalCompletionTarget::from_parts(
                    self.invocation_id,
                    self.entry.completion_id,
                )]),
            };

            debug_if_leader!(
                ctx.is_leader,
                rpc.service = %service_id.service_name,
                "Append listener to promise {} in non completed state",
                self.entry.key
            );

            ctx.storage
                .put_promise(
                    &service_id,
                    &self.entry.key,
                    &Promise {
                        state: uncompleted_promise_state,
                    },
                )
                .map_err(Error::Storage)?;
        } else {
            warn!(
                "Trying to process entry {} for a target that has no promises",
                self.entry.ty()
            );
            self.then_apply_completion(GetPromiseCompletion {
                completion_id: self.entry.completion_id,
                result: GetPromiseResult::Success(Bytes::new()),
            });
        }

        Ok(())
    }
}
