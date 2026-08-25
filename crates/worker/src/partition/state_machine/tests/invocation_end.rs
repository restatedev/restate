// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use bytes::Bytes;
use googletest::prelude::{all, assert_that, eq, none, ok, pat};

use restate_storage_api::invocation_status_table::{
    InvocationStatus, InvocationStatusDiscriminants, ReadInvocationStatusTable,
};
use restate_storage_api::journal_table_v2::ReadJournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{InvocationTarget, ServiceInvocation};
use restate_types::journal_v2::{ClearAllStateCommand, CommandType, OutputCommand, OutputResult};
use restate_types::partitions::PersistedFeatures;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_wal_protocol::v2::{Command, commands};

use crate::partition::state_machine::tests::matchers::storage::has_result_reference;

use super::TestEnv;
use super::fixtures::{invoker_end_effect, invoker_entry_effect, pinned_deployment};
use super::matchers::storage::{has_commands, has_journal_length, is_variant};

const RESULT: Bytes = Bytes::from_static(b"result");

#[restate_core::test]
async fn zero_completion_retention_drops_invocation_and_journal_with_result_reference_enabled() {
    let (mut test_env, invocation_id) =
        end_invocation(true, Duration::ZERO, Duration::ZERO, false).await;

    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );
    assert_journal_entries_exists(&mut test_env, invocation_id, &[false, false]).await;

    test_env.shutdown().await;
}

#[restate_core::test]
async fn zero_journal_retention_keeps_referenced_output() {
    let (mut test_env, invocation_id) =
        end_invocation(true, Duration::from_secs(60), Duration::ZERO, false).await;

    assert_completed_with_reference(&mut test_env, invocation_id, Some(1)).await;
    assert_journal_entries_exists(&mut test_env, invocation_id, &[false, true]).await;

    test_env.shutdown().await;
}

// todo(azmy): Remove once write_result_reference becomes a default feature.
#[restate_core::test]
async fn zero_journal_retention_drops_all_journals_with_result_reference_disabled() {
    let (mut test_env, invocation_id) =
        end_invocation(false, Duration::from_secs(60), Duration::ZERO, false).await;

    assert_completed_with_reference(&mut test_env, invocation_id, None).await;
    assert_journal_entries_exists(&mut test_env, invocation_id, &[false, false]).await;

    test_env.shutdown().await;
}

#[restate_core::test]
async fn referenced_output_does_not_need_to_be_last_journal_entry() {
    let (mut test_env, invocation_id) =
        end_invocation(true, Duration::from_secs(60), Duration::ZERO, true).await;

    assert_completed_with_reference(&mut test_env, invocation_id, Some(1)).await;
    assert_journal_entries_exists(&mut test_env, invocation_id, &[false, true, false]).await;
    assert_that!(
        test_env
            .read_journal_entry::<OutputCommand>(invocation_id, 1)
            .await,
        pat!(OutputCommand {
            result: eq(OutputResult::Success(RESULT)),
        })
    );

    test_env.shutdown().await;
}

async fn end_invocation(
    write_result_reference: bool,
    completion_retention_duration: Duration,
    journal_retention_duration: Duration,
    append_after_output: bool,
) -> (TestEnv, InvocationId) {
    let mut test_env = TestEnv::create_with_features(PersistedFeatures {
        write_result_reference,
        ..PersistedFeatures::default()
    })
    .await;

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::mock_generate(&invocation_target);
    test_env
        .apply_multiple([
            commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                invocation_target,
                completion_retention_duration,
                journal_retention_duration,
                ..ServiceInvocation::mock()
            }),
            pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
            invoker_entry_effect(
                invocation_id,
                OutputCommand {
                    result: OutputResult::Success(RESULT),
                    name: Default::default(),
                },
            ),
        ])
        .await;

    if append_after_output {
        test_env
            .apply(invoker_entry_effect(
                invocation_id,
                ClearAllStateCommand {
                    name: Default::default(),
                },
            ))
            .await;
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::Output.into(),
                    CommandType::ClearAllState.into(),
                ],
            )
            .await;
    } else {
        test_env
            .verify_journal_components(
                invocation_id,
                [CommandType::Input.into(), CommandType::Output.into()],
            )
            .await;
    }

    test_env.apply(invoker_end_effect(invocation_id)).await;
    (test_env, invocation_id)
}

async fn assert_completed_with_reference(
    test_env: &mut TestEnv,
    invocation_id: InvocationId,
    expected_output_index: Option<u32>,
) {
    let status = test_env
        .storage()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    assert_that!(
        status,
        all!(
            is_variant(InvocationStatusDiscriminants::Completed),
            has_commands(0),
            has_journal_length(0),
            has_result_reference(expected_output_index)
        )
    );
}

async fn assert_journal_entries_exists(
    test_env: &mut TestEnv,
    invocation_id: InvocationId,
    expected_entries: &[bool],
) {
    for (entry_index, expected) in expected_entries.iter().cloned().enumerate() {
        assert_eq!(
            test_env
                .storage()
                .get_journal_entry(invocation_id, entry_index as u32)
                .await
                .unwrap()
                .is_some(),
            expected,
            "unexpected journal presence at index {entry_index}"
        );
    }
    assert_that!(
        test_env
            .storage()
            .get_journal_entry(invocation_id, expected_entries.len() as u32)
            .await,
        ok(none())
    );
}
