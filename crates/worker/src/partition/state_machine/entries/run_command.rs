// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_tracing_instrumentation as instrumentation;
use restate_types::journal_v2::RunCommand;

pub(super) type ApplyRunCommand<'e> = ApplyJournalCommandEffect<'e, RunCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyRunCommand<'e>
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        // todo(azmy): avoid "format!" if tracing is not enabled?

        // todo(azmy): completable commands like Run and Sleep should have their span
        // created when the completion is received.

        if ctx.is_leader {
            let _span = instrumentation::info_invocation_span!(
                relation = invocation_metadata
                    .journal_metadata
                    .span_context
                    .as_parent(),
                id = self.invocation_id,
                name = format!("run {}", self.entry.name),
                tags = (rpc.service = invocation_metadata
                    .invocation_target
                    .service_name()
                    .to_string())
            );
        }

        Ok(())
    }
}
