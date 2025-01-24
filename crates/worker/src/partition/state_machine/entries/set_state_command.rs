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
use restate_storage_api::state_table::StateTable;
use restate_tracing_instrumentation as instrumentation;
use restate_types::journal_v2::{EntryMetadata, SetStateCommand};
use tracing::warn;

pub(super) type ApplySetStateCommand<'e> = ApplyJournalCommandEffect<'e, SetStateCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplySetStateCommand<'e>
where
    S: StateTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let _span = instrumentation::info_invocation_span!(
            relation = invocation_metadata
                .journal_metadata
                .span_context
                .as_parent(),
            id = self.invocation_id,
            name = format!("set-state {:?}", self.entry.key),
            tags = (rpc.service = invocation_metadata
                .invocation_target
                .service_name()
                .to_string())
        );

        if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
            debug_if_leader!(
                ctx.is_leader,
                restate.state.key = ?self.entry.key,
                "Set state"
            );

            ctx.storage
                .put_user_state(&service_id, self.entry.key, self.entry.value)
                .await;
        } else {
            warn!(
                "Trying to process entry {} for a target that has no state",
                self.entry.ty()
            );
        }

        Ok(())
    }
}
