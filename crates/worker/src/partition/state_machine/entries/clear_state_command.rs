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
use restate_storage_api::state_table::WriteStateTable;
use restate_tracing_instrumentation as instrumentation;
use restate_types::journal_v2::{ClearStateCommand, EntryMetadata};
use tracing::warn;

pub(super) type ApplyClearStateCommand<'e> = ApplyJournalCommandEffect<'e, ClearStateCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyClearStateCommand<'e>
where
    S: WriteStateTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        if ctx.is_leader {
            let _span = instrumentation::info_invocation_span!(
                relation = invocation_metadata
                    .journal_metadata
                    .span_context
                    .as_parent(),
                id = self.invocation_id,
                name = "clear-state",
                tags = (rpc.service = invocation_metadata
                    .invocation_target
                    .service_name()
                    .to_string())
            );
        }

        if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
            debug_if_leader!(
                ctx.is_leader,
                restate.state.key = ?self.entry.key,
                "Clear state"
            );

            ctx.storage
                .delete_user_state(&service_id, &self.entry.key)
                .map_err(Error::Storage)?;
        } else {
            warn!(
                "Trying to process entry {} for a target that has no state",
                self.entry.ty()
            );
        }

        Ok(())
    }
}
