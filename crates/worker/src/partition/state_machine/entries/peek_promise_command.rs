// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::promise_table::{Promise, PromiseState, ReadPromiseTable};
use restate_types::journal_v2::{
    EntryMetadata, PeekPromiseCommand, PeekPromiseCompletion, PeekPromiseResult,
};
use tracing::warn;

pub(super) type ApplyPeekPromiseCommand<'e> = ApplyJournalCommandEffect<'e, PeekPromiseCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyPeekPromiseCommand<'e>
where
    S: ReadPromiseTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let peek_promise_result =
            if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
                // Load state and write completion
                let promise_metadata = ctx
                    .storage
                    .get_promise(&service_id, &self.entry.key)
                    .await?;
                match promise_metadata {
                    Some(Promise {
                        state: PromiseState::Completed(result),
                    }) => result.into(),
                    _ => PeekPromiseResult::Void,
                }
            } else {
                warn!(
                    "Trying to process entry {} for a target that has no promises",
                    self.entry.ty()
                );
                PeekPromiseResult::Void
            };

        self.then_apply_completion(PeekPromiseCompletion {
            completion_id: self.entry.completion_id,
            result: peek_promise_result,
        });

        Ok(())
    }
}
