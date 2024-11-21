// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{fixtures, matchers, *};

use assert2::assert;
use assert2::let_assert;
use googletest::any;
use prost::Message;
use restate_storage_api::journal_table::JournalTable;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerKeyKind, TimerTable};
use restate_types::identifiers::EntryIndex;
use restate_types::invocation::TerminationFlavor;
use restate_types::journal::enriched::EnrichedEntryHeader;
use restate_types::service_protocol;
use test_log::test;

#[test(tokio::test)]
async fn kill_inboxed_invocation() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    let inboxed_target = invocation_target.clone();
    let inboxed_id = InvocationId::mock_generate(&inboxed_target);

    let caller_id = InvocationId::mock_random();

    let _ = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    let _ = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: inboxed_id,
            invocation_target: inboxed_target,
            response_sink: Some(ServiceInvocationResponseSink::PartitionProcessor {
                caller: caller_id,
                entry_index: 0,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;

    let current_invocation_status = test_env
        .storage()
        .get_invocation_status(&inboxed_id)
        .await?;

    // assert that inboxed invocation is in invocation_status
    assert!(let InvocationStatus::Inboxed(_) = current_invocation_status);

    let actions = test_env
        .apply(Command::TerminateInvocation(InvocationTermination::kill(
            inboxed_id,
        )))
        .await;

    let current_invocation_status = test_env
        .storage()
        .get_invocation_status(&inboxed_id)
        .await?;

    // assert that invocation status was removed
    assert!(let InvocationStatus::Free = current_invocation_status);

    fn outbox_message_matcher(
        caller_id: InvocationId,
    ) -> impl Matcher<ActualT = restate_storage_api::outbox_table::OutboxMessage> {
        pat!(
            restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                restate_types::invocation::InvocationResponse {
                    id: eq(caller_id),
                    entry_index: eq(0),
                    result: eq(ResponseResult::Failure(KILLED_INVOCATION_ERROR))
                }
            ))
        )
    }

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: outbox_message_matcher(caller_id)
        }))
    );

    let outbox_message = test_env.storage().get_next_outbox_message(0).await?;

    assert_that!(
        outbox_message,
        some((ge(0), outbox_message_matcher(caller_id)))
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(tokio::test)]
async fn kill_call_tree() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::mock_generate(&invocation_target);
    let enqueued_invocation_id_on_same_target = InvocationId::mock_generate(&invocation_target);

    let _ = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    // Let's enqueue an invocation afterward
    let _ = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: enqueued_invocation_id_on_same_target,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    // Let's add some journal entries
    let mut tx = test_env.storage.transaction();
    tx.put_journal_entry(
        &invocation_id,
        1,
        &fixtures::incomplete_invoke_entry(call_invocation_id),
    )
    .await;
    tx.put_journal_entry(
        &invocation_id,
        2,
        &fixtures::background_invoke_entry(background_call_invocation_id),
    )
    .await;
    tx.put_journal_entry(
        &invocation_id,
        3,
        &fixtures::completed_invoke_entry(finished_call_invocation_id),
    )
    .await;
    let mut invocation_status = tx.get_invocation_status(&invocation_id).await?;
    invocation_status.get_journal_metadata_mut().unwrap().length = 4;
    tx.put_invocation_status(&invocation_id, &invocation_status)
        .await;
    tx.commit().await?;

    // Now let's send the termination command
    let actions = test_env
        .apply(Command::TerminateInvocation(InvocationTermination::kill(
            invocation_id,
        )))
        .await;

    // Invocation should be gone
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await?,
        pat!(InvocationStatus::Free)
    );
    assert_that!(
        test_env
            .storage
            .get_journal(&invocation_id, 4)
            .try_collect::<Vec<_>>()
            .await?,
        empty()
    );

    assert_that!(
        actions,
        all!(
            contains(pat!(Action::AbortInvocation(eq(invocation_id)))),
            contains(pat!(Action::Invoke {
                invocation_id: eq(enqueued_invocation_id_on_same_target),
                invocation_target: eq(invocation_target)
            })),
            contains(matchers::actions::terminate_invocation(
                call_invocation_id,
                TerminationFlavor::Kill
            )),
            not(contains(pat!(Action::NewOutboxMessage {
                message: pat!(
                    restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(pat!(
                        InvocationTermination {
                            invocation_id: any!(
                                eq(background_call_invocation_id),
                                eq(finished_call_invocation_id)
                            )
                        }
                    ))
                )
            })))
        )
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(tokio::test)]
async fn cancel_invoked_invocation() -> Result<(), Error> {
    let mut test_env = TestEnv::create().await;

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_target = InvocationTarget::mock_workflow();
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    let _ = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    // Let's add some journal entries
    let mut tx = test_env.storage.transaction();
    let journal = create_termination_journal(
        call_invocation_id,
        background_call_invocation_id,
        finished_call_invocation_id,
    );
    let journal_length = journal.len();
    let (sleep_entry_idx, _) = journal
        .iter()
        .enumerate()
        .find(|(_, j)| {
            if let JournalEntry::Entry(e) = j {
                e.header().as_entry_type() == EntryType::Sleep
            } else {
                false
            }
        })
        .unwrap();
    for (idx, entry) in journal.into_iter().enumerate() {
        tx.put_journal_entry(&invocation_id, (idx + 1) as u32, &entry)
            .await;
    }
    // Update journal length
    let mut invocation_status = tx.get_invocation_status(&invocation_id).await?;
    invocation_status.get_journal_metadata_mut().unwrap().length =
        (journal_length + 1) as EntryIndex;
    tx.put_invocation_status(&invocation_id, &invocation_status)
        .await;
    // Add timer
    tx.put_timer(
        &TimerKey {
            timestamp: 1337,
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: invocation_id.invocation_uuid(),
                journal_index: (sleep_entry_idx + 1) as u32,
            },
        },
        &Timer::CompleteJournalEntry(invocation_id, (sleep_entry_idx + 1) as u32),
    )
    .await;
    tx.commit().await?;

    let actions = test_env
        .apply(Command::TerminateInvocation(InvocationTermination::cancel(
            invocation_id,
        )))
        .await;

    // Invocation shouldn't be gone
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await?,
        pat!(InvocationStatus::Invoked { .. })
    );

    // Timer is gone
    assert_that!(
        test_env
            .storage
            .next_timers_greater_than(None, usize::MAX)
            .try_collect::<Vec<_>>()
            .await?,
        empty()
    );

    // Entries are completed
    for idx in 4..=9 {
        assert_entry_completed(&mut test_env, invocation_id, idx).await;
    }

    assert_that!(
        actions,
        all!(
            contains(matchers::actions::terminate_invocation(
                call_invocation_id,
                TerminationFlavor::Cancel
            )),
            contains(matchers::actions::forward_canceled_completion(4)),
            contains(matchers::actions::forward_canceled_completion(5)),
            contains(matchers::actions::forward_canceled_completion(6)),
            contains(matchers::actions::forward_canceled_completion(7)),
            contains(matchers::actions::forward_canceled_completion(8)),
            contains(matchers::actions::forward_canceled_completion(9)),
            contains(matchers::actions::delete_sleep_timer(5)),
        )
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(tokio::test)]
async fn cancel_suspended_invocation() -> Result<(), Error> {
    let mut test_env = TestEnv::create().await;

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_target = InvocationTarget::mock_workflow();
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    let _ = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    // Let's add some journal entries
    let mut tx = test_env.storage.transaction();
    let journal = create_termination_journal(
        call_invocation_id,
        background_call_invocation_id,
        finished_call_invocation_id,
    );
    let journal_length = journal.len();
    let (sleep_entry_idx, _) = journal
        .iter()
        .enumerate()
        .find(|(_, j)| {
            if let JournalEntry::Entry(e) = j {
                e.header().as_entry_type() == EntryType::Sleep
            } else {
                false
            }
        })
        .unwrap();
    for (idx, entry) in journal.into_iter().enumerate() {
        tx.put_journal_entry(&invocation_id, (idx + 1) as u32, &entry)
            .await;
    }
    // Update journal length and suspend invocation
    let invocation_status = tx.get_invocation_status(&invocation_id).await?;
    let_assert!(InvocationStatus::Invoked(mut in_flight_meta) = invocation_status);
    in_flight_meta.journal_metadata.length = (journal_length + 1) as EntryIndex;
    tx.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Suspended {
            metadata: in_flight_meta,
            waiting_for_completed_entries: HashSet::from([3, 4, 5, 6, 7, 8, 9]),
        },
    )
    .await;
    // Add timer
    tx.put_timer(
        &TimerKey {
            timestamp: 1337,
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: invocation_id.invocation_uuid(),
                journal_index: (sleep_entry_idx + 1) as u32,
            },
        },
        &Timer::CompleteJournalEntry(invocation_id, (sleep_entry_idx + 1) as u32),
    )
    .await;
    tx.commit().await?;

    let actions = test_env
        .apply(Command::TerminateInvocation(InvocationTermination::cancel(
            invocation_id,
        )))
        .await;

    // Invocation shouldn't be gone
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await?,
        pat!(InvocationStatus::Invoked { .. })
    );

    // Timer is gone
    assert_that!(
        test_env
            .storage
            .next_timers_greater_than(None, usize::MAX)
            .try_collect::<Vec<_>>()
            .await?,
        empty()
    );

    // Entries are completed
    for idx in 4..=9 {
        assert_entry_completed(&mut test_env, invocation_id, idx).await;
    }

    assert_that!(
        actions,
        all!(
            contains(matchers::actions::terminate_invocation(
                call_invocation_id,
                TerminationFlavor::Cancel
            )),
            contains(matchers::actions::delete_sleep_timer(5)),
            contains(pat!(Action::Invoke {
                invocation_id: eq(invocation_id),
                invocation_target: eq(invocation_target)
            }))
        )
    );
    test_env.shutdown().await;

    Ok(())
}

#[test(tokio::test)]
async fn cancel_invocation_entry_referring_to_previous_entry() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_service();
    let invocation_id = InvocationId::mock_random();

    let callee_1 = InvocationId::mock_random();
    let callee_2 = InvocationId::mock_random();

    let _ = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    // Add call and one way call journal entry
    let mut tx = test_env.storage.transaction();
    tx.put_journal_entry(
        &invocation_id,
        1,
        &fixtures::background_invoke_entry(callee_1),
    )
    .await;
    tx.put_journal_entry(
        &invocation_id,
        2,
        &fixtures::incomplete_invoke_entry(callee_2),
    )
    .await;
    let mut invocation_status = tx.get_invocation_status(&invocation_id).await.unwrap();
    invocation_status.get_journal_metadata_mut().unwrap().length = 3;
    tx.put_invocation_status(&invocation_id, &invocation_status)
        .await;
    tx.commit().await.unwrap();

    // Now create cancel invocation entry
    let actions = test_env
        .apply_multiple(vec![
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 3,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::cancel_invocation(
                        CancelInvocationTarget::InvocationId(callee_1.to_string().into()),
                    )),
                },
            }),
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 4,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::cancel_invocation(
                        CancelInvocationTarget::CallEntryIndex(2),
                    )),
                },
            }),
        ])
        .await;

    assert_that!(
        actions,
        all!(
            contains(matchers::actions::terminate_invocation(
                callee_1,
                TerminationFlavor::Cancel
            )),
            contains(matchers::actions::terminate_invocation(
                callee_2,
                TerminationFlavor::Cancel
            )),
        )
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        ok(pat!(InvocationStatus::Invoked { .. }))
    );
    test_env.shutdown().await;
}

async fn assert_entry_completed(
    test_env: &mut TestEnv,
    invocation_id: InvocationId,
    idx: EntryIndex,
) {
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, idx)
            .await
            .unwrap(),
        some(pat!(JournalEntry::Entry(matchers::completed_entry())))
    );
}

fn create_termination_journal(
    call_invocation_id: InvocationId,
    background_invocation_id: InvocationId,
    finished_call_invocation_id: InvocationId,
) -> Vec<JournalEntry> {
    vec![
        fixtures::incomplete_invoke_entry(call_invocation_id),
        fixtures::completed_invoke_entry(finished_call_invocation_id),
        fixtures::background_invoke_entry(background_invocation_id),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::GetState {
                is_completed: false,
            },
            Bytes::default(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Sleep {
                is_completed: false,
            },
            service_protocol::SleepEntryMessage {
                wake_up_time: 1337,
                result: None,
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Awakeable {
                is_completed: false,
            },
            Bytes::default(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::GetPromise {
                is_completed: false,
            },
            service_protocol::GetPromiseEntryMessage {
                key: "my-promise".to_string(),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::PeekPromise {
                is_completed: false,
            },
            service_protocol::PeekPromiseEntryMessage {
                key: "my-promise".to_string(),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::CompletePromise {
                is_completed: false,
            },
            service_protocol::CompletePromiseEntryMessage {
                key: "my-promise".to_string(),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        )),
    ]
}
