// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::{InvocationStatus, WriteInvocationStatusTable};
use restate_storage_api::journal_events::{EventView, WriteJournalEventsTable};
use restate_types::identifiers::InvocationId;
use restate_types::journal_events::raw::RawEvent;

pub struct OnInvokerEventCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub event: RawEvent,
}

impl<'ctx, 's: 'ctx, S: WriteJournalEventsTable + WriteInvocationStatusTable>
    CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for OnInvokerEventCommand
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let Self {
            invocation_id,
            mut invocation_status,
            event,
        } = self;
        ApplyEventCommand {
            invocation_id,
            invocation_status: &mut invocation_status,
            event,
        }
        .apply(ctx)
        .await?;

        // Store invocation status
        ctx.storage
            .put_invocation_status(&invocation_id, &invocation_status)
            .map_err(Error::Storage)?;

        Ok(())
    }
}

pub(super) struct ApplyEventCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e InvocationStatus,
    pub(super) event: RawEvent,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S: WriteJournalEventsTable>
    CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for ApplyEventCommand<'e>
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let Some(journal_metadata) = self.invocation_status.get_journal_metadata() else {
            // No journal, no events
            return Ok(());
        };

        // To store the event, we need to give it a total order wrt journal.
        let after_journal_entry_index = journal_metadata.length.checked_sub(1).unwrap_or_default();

        // Store event
        ctx.storage.put_journal_event(
            self.invocation_id,
            EventView {
                append_time: ctx.record_created_at,
                after_journal_entry_index,
                event: self.event,
            },
            ctx.record_lsn.as_u64(),
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::{TestEnv, fixtures};
    use crate::partition::types::InvokerEffectKind;
    use googletest::prelude::*;
    use restate_invoker_api::Effect;
    use restate_types::journal_events::raw::RawEvent;
    use restate_types::journal_events::{Event, TransientErrorEvent};
    use restate_wal_protocol::Command;

    #[restate_core::test]
    async fn store_event() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let transient_error_event = TransientErrorEvent {
            error_code: 501u16.into(),
            error_message: "my bad".to_string(),
            error_stacktrace: Some("something something".to_string()),
            restate_doc_error_code: Some("RT0001".to_string()),
            related_command_index: None,
            related_command_name: Some("my command".to_string()),
            related_command_type: None,
        };

        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::JournalEvent {
                    event: RawEvent::from(Event::TransientError(transient_error_event.clone()))
                        .clone(),
                },
            })))
            .await;

        assert_that!(
            test_env.read_journal_events(invocation_id).await,
            elements_are![eq(Event::TransientError(transient_error_event.clone()))]
        );

        test_env.shutdown().await;
    }
}
