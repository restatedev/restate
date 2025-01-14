// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::lifecycle::ResumeInvocationCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::raw::RawNotification;
use restate_types::journal_v2::NotificationId;

pub(super) struct ApplyNotificationCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: &'e RawNotification,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyNotificationCommand<'e>
where
    S: ReadOnlyJournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        if cfg!(debug_assertions) {
            if let NotificationId::CompletionId(completion_id) = self.entry.id() {
                assert!(
                    ctx.storage.get_command_by_completion_id(self.invocation_id, completion_id).await?.is_some(),
                    "For given completion id {completion_id}, the corresponding command must be present already in the journal"
                )
            }
        }

        // If we're suspended, let's figure out if we need to resume
        if let InvocationStatus::Suspended {
            waiting_for_notifications,
            ..
        } = self.invocation_status
        {
            if waiting_for_notifications.remove(&self.entry.id()) {
                ResumeInvocationCommand {
                    invocation_id: self.invocation_id,
                    invocation_status: self.invocation_status,
                }
                .apply(ctx)
                .await?;
            }
        } else if let InvocationStatus::Invoked(_) = self.invocation_status {
            // Just forward the notification if we're invoked
            ctx.forward_notification(self.invocation_id, self.entry.clone());
        }

        // In all the other cases, just move on.

        Ok(())
    }
}
