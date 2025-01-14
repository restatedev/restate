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
use futures::{StreamExt, TryStreamExt};
use restate_storage_api::state_table::ReadOnlyStateTable;
use restate_types::journal_v2::{
    EntryMetadata, GetLazyStateKeysCommand, GetLazyStateKeysCompletion,
};
use tracing::warn;

pub(super) type ApplyGetLazyStateKeysCommand<'e> =
    ApplyJournalCommandEffect<'e, GetLazyStateKeysCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyGetLazyStateKeysCommand<'e>
where
    S: ReadOnlyStateTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let state_keys =
            if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
                ctx.storage
                    .get_all_user_states_for_service(&service_id)
                    .map(|res| {
                        let key_bytes = res?.0;
                        let key_string = String::from_utf8(key_bytes.to_vec())
                            .map_err(|e| Error::ApplyCommandEffect(self.entry.ty(), e.into()))?;
                        Ok::<_, Error>(key_string)
                    })
                    .try_collect()
                    .await?
            } else {
                warn!(
                    "Trying to process entry {} for a target that has no state",
                    self.entry.ty()
                );
                vec![]
            };

        self.then_apply_completion(GetLazyStateKeysCompletion {
            completion_id: self.entry.completion_id,
            state_keys,
        });

        Ok(())
    }
}
