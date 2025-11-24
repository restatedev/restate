// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::journal_v2::{
    CallCommand, CallCompletion, CallRequest, SleepCommand, SleepCompletion, raw::TryFromEntry,
};

use super::*;

#[restate_core::test]
async fn fence_old_calls_and_completions() {
    let mut test_env = TestEnv::create().await;

    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
    fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

    test_env
        .modify_invocation_status(invocation_id, |is| {
            let im = is.get_invocation_metadata_mut().unwrap();
            im.current_invocation_epoch += 1;
            im.completion_range_epoch_map.add_trim_point(2, 1);
        })
        .await;

    // Call with epoch 0 won't get accepted
    let actions = test_env
        .apply(fixtures::invoker_entry_effect_for_epoch(
            invocation_id,
            0,
            CallCommand {
                request: CallRequest {
                    headers: vec![Header::new("foo", "bar")],
                    ..CallRequest::mock(
                        InvocationId::mock_random(),
                        InvocationTarget::mock_service(),
                    )
                },
                invocation_id_completion_id: 1,
                result_completion_id: 2,
                name: Default::default(),
            }
            .clone(),
        ))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::AbortInvocation {
            invocation_id: eq(invocation_id),
            invocation_epoch: eq(0),
        }))
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // Only Input entry
        ok(matchers::storage::has_journal_length(1))
    );

    // Create a call with epoch 1
    let actions = test_env
        .apply(fixtures::invoker_entry_effect_for_epoch(
            invocation_id,
            1,
            CallCommand {
                request: CallRequest {
                    headers: vec![Header::new("foo", "bar")],
                    ..CallRequest::mock(
                        InvocationId::mock_random(),
                        InvocationTarget::mock_service(),
                    )
                },
                invocation_id_completion_id: 1,
                result_completion_id: 2,
                name: Default::default(),
            }
            .clone(),
        ))
        .await;
    assert_that!(
        actions,
        not(contains(pat!(Action::AbortInvocation {
            invocation_id: eq(invocation_id)
        })))
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input, CallCommand and CallInvocationIdCompletion
        ok(matchers::storage::has_journal_length(3))
    );

    // Call with epoch 0 again won't get accepted
    let actions = test_env
        .apply(fixtures::invoker_entry_effect_for_epoch(
            invocation_id,
            0,
            CallCommand {
                request: CallRequest {
                    headers: vec![Header::new("foo", "bar")],
                    ..CallRequest::mock(
                        InvocationId::mock_random(),
                        InvocationTarget::mock_service(),
                    )
                },
                invocation_id_completion_id: 1,
                result_completion_id: 2,
                name: Default::default(),
            }
            .clone(),
        ))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::AbortInvocation {
            invocation_id: eq(invocation_id),
            invocation_epoch: eq(0),
        }))
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input, CallCommand and CallInvocationIdCompletion
        ok(matchers::storage::has_journal_length(3))
    );

    // Completion with epoch 0 gets ignored
    let actions = test_env
        .apply(records::InvocationResponse::new_test(InvocationResponse {
            target: JournalCompletionTarget::from_parts(invocation_id, 2, 0),
            result: ResponseResult::Success(Bytes::default()),
        }))
        .await;
    assert_that!(actions, empty());
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input, CallCommand and CallInvocationIdCompletion
        ok(matchers::storage::has_journal_length(3))
    );

    // Completion with epoch 1 gets accepted
    let actions = test_env
        .apply(records::InvocationResponse::new_test(InvocationResponse {
            target: JournalCompletionTarget::from_parts(invocation_id, 2, 1),
            result: ResponseResult::Success(Bytes::default()),
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::ForwardNotification {
            invocation_id: eq(invocation_id),
            invocation_epoch: eq(1),
        }))
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input, CallCommand, CallInvocationIdCompletion, CallCompletion
        ok(matchers::storage::has_journal_length(4))
    );
    assert_that!(
        TryFromEntry::try_from(
            test_env
                .read_journal_to_vec(invocation_id, 4)
                .await
                .remove(3)
        ),
        ok(pat!(CallCompletion {
            completion_id: eq(2)
        }))
    );

    test_env.shutdown().await;
}

#[restate_core::test]
async fn fence_old_sleep_and_completions() {
    let mut test_env = TestEnv::create().await;

    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
    fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

    test_env
        .modify_invocation_status(invocation_id, |is| {
            let im = is.get_invocation_metadata_mut().unwrap();
            im.current_invocation_epoch += 1;
            im.completion_range_epoch_map.add_trim_point(1, 1);
        })
        .await;

    let wake_up_time = MillisSinceEpoch::now();

    // Sleep with epoch 0 won't get accepted
    let actions = test_env
        .apply(fixtures::invoker_entry_effect_for_epoch(
            invocation_id,
            0,
            SleepCommand {
                wake_up_time,
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::AbortInvocation {
            invocation_id: eq(invocation_id),
            invocation_epoch: eq(0),
        }))
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // Only Input entry
        ok(matchers::storage::has_journal_length(1))
    );

    // Create a Sleep with epoch 1
    let actions = test_env
        .apply(fixtures::invoker_entry_effect_for_epoch(
            invocation_id,
            1,
            SleepCommand {
                wake_up_time,
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    assert_that!(
        actions,
        all![
            not(contains(pat!(Action::AbortInvocation {
                invocation_id: eq(invocation_id)
            }))),
            contains(pat!(Action::RegisterTimer {
                timer_value: eq(TimerKeyValue::complete_journal_entry(
                    wake_up_time,
                    invocation_id,
                    1,
                    1
                ))
            }))
        ]
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input and SleepCommand
        ok(matchers::storage::has_journal_length(2))
    );

    // Sleep with epoch 0 again won't get accepted
    let actions = test_env
        .apply(fixtures::invoker_entry_effect_for_epoch(
            invocation_id,
            0,
            SleepCommand {
                wake_up_time,
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    assert_that!(
        actions,
        all![
            contains(pat!(Action::AbortInvocation {
                invocation_id: eq(invocation_id),
                invocation_epoch: eq(0),
            })),
            not(contains(pat!(Action::RegisterTimer {
                timer_value: eq(TimerKeyValue::complete_journal_entry(
                    wake_up_time,
                    invocation_id,
                    1,
                    0
                ))
            })))
        ]
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input and SleepCommand
        ok(matchers::storage::has_journal_length(2))
    );

    // Completion with epoch 0 gets ignored
    let actions = test_env
        .apply(records::Timer::new_test(
            TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, 1, 0),
        ))
        .await;
    assert_that!(
        actions,
        not(contains(pat!(Action::ForwardNotification {
            invocation_id: eq(invocation_id)
        })))
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input and SleepCommand
        ok(matchers::storage::has_journal_length(2))
    );

    // Completion with epoch 1 gets accepted
    let actions = test_env
        .apply(records::Timer::new_test(
            TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, 1, 1),
        ))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::ForwardNotification {
            invocation_id: eq(invocation_id),
            invocation_epoch: eq(1),
        }))
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        // This has Input, SleepCommand and SleepCompletion
        ok(matchers::storage::has_journal_length(3))
    );
    assert_that!(
        TryFromEntry::try_from(
            test_env
                .read_journal_to_vec(invocation_id, 3)
                .await
                .remove(2)
        ),
        ok(pat!(SleepCompletion {
            completion_id: eq(1)
        }))
    );

    test_env.shutdown().await;
}
