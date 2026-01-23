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
use restate_storage_api::state_table::ReadStateTable;
use restate_types::journal_v2::{
    EntryMetadata, GetLazyStateCommand, GetLazyStateCompletion, GetStateResult,
};
use tracing::warn;

pub(super) type ApplyGetLazyStateCommand<'e> = ApplyJournalCommandEffect<'e, GetLazyStateCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyGetLazyStateCommand<'e>
where
    S: ReadStateTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let result =
            if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
                ctx.storage
                    .get_user_state(&service_id, &self.entry.key)
                    .await?
                    .map(GetStateResult::Success)
                    .unwrap_or(GetStateResult::Void)
            } else {
                warn!(
                    "Trying to process entry {} for a target that has no state",
                    self.entry.ty()
                );
                GetStateResult::Void
            };

        self.then_apply_completion(GetLazyStateCompletion {
            completion_id: self.entry.completion_id,
            result,
        });

        Ok(())
    }
}
