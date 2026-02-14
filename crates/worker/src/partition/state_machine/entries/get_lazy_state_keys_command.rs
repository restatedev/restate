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
use futures::{StreamExt, TryStreamExt};
use restate_storage_api::state_table::ReadStateTable;
use restate_types::journal_v2::{
    EntryMetadata, GetLazyStateKeysCommand, GetLazyStateKeysCompletion,
};
use tracing::warn;

pub(super) type ApplyGetLazyStateKeysCommand<'e> =
    ApplyJournalCommandEffect<'e, GetLazyStateKeysCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyGetLazyStateKeysCommand<'e>
where
    S: ReadStateTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let state_keys =
            if let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() {
                ctx.storage
                    .get_all_user_states_for_service(&service_id)?
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

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::fixtures::invoker_entry_effect;
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::matchers::contains;
    use googletest::prelude::{assert_that, eq};
    use restate_storage_api::Transaction;
    use restate_storage_api::state_table::WriteStateTable;
    use restate_types::identifiers::ServiceId;
    use restate_types::journal_v2::{GetLazyStateKeysCommand, GetLazyStateKeysCompletion};

    #[restate_core::test]
    async fn get_lazy_state_keys() {
        let mut test_env = TestEnv::create().await;
        let service_id = ServiceId::mock_random();
        let invocation_id =
            fixtures::mock_start_invocation_with_service_id(&mut test_env, service_id.clone())
                .await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Mock some state
        let mut txn = test_env.storage.transaction();
        txn.put_user_state(&service_id, b"key1", b"value1").unwrap();
        txn.put_user_state(&service_id, b"key2", b"value2").unwrap();
        txn.commit().await.unwrap();

        let completion_id = 1;
        let actions = test_env
            .apply(invoker_entry_effect(
                invocation_id,
                GetLazyStateKeysCommand {
                    completion_id,
                    name: Default::default(),
                },
            ))
            .await;

        // At this point we expect the completion to be forwarded to the invoker
        assert_that!(
            actions,
            contains(matchers::actions::forward_notification(
                invocation_id,
                GetLazyStateKeysCompletion {
                    completion_id,
                    state_keys: vec!["key1".to_string(), "key2".to_string()]
                }
            ))
        );

        let state_keys_command = test_env
            .read_journal_entry::<GetLazyStateKeysCommand>(invocation_id, 1)
            .await;
        assert_that!(
            state_keys_command,
            eq(GetLazyStateKeysCommand {
                completion_id: 1,
                name: Default::default()
            })
        );

        test_env.shutdown().await;
    }
}
