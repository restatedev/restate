// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use assert2::assert;
use assert2::let_assert;
use googletest::any;
use prost::Message;
use restate_storage_api::journal_table::JournalTable;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerKeyKind, TimerTable};
use restate_types::identifiers::EntryIndex;
use restate_types::invocation::{ServiceInvocationSpanContext, TerminationFlavor};
use restate_types::journal::enriched::{CallEnrichmentResult, EnrichedEntryHeader};
use restate_types::service_protocol;
use test_log::test;

#[test(tokio::test)]
async fn kill_inboxed_invocation() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;

    let (invocation_id, invocation_target) =
        InvocationId::mock_with(InvocationTarget::mock_virtual_object());
    let (inboxed_id, inboxed_target) = InvocationId::mock_with(invocation_target.clone());
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

    Ok(())
}

#[test(tokio::test)]
async fn kill_call_tree() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target);
    let enqueued_invocation_id_on_same_target = InvocationId::generate(&invocation_target);

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
        &uncompleted_invoke_entry(call_invocation_id),
    )
    .await;
    tx.put_journal_entry(
        &invocation_id,
        2,
        &background_invoke_entry(background_call_invocation_id),
    )
    .await;
    tx.put_journal_entry(
        &invocation_id,
        3,
        &completed_invoke_entry(finished_call_invocation_id),
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
            contains(terminate_invocation_outbox_message_matcher(
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

    Ok(())
}

#[test(tokio::test)]
async fn cancel_invoked_invocation() -> Result<(), Error> {
    let mut test_env = TestEnv::create().await;

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target);

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
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 4)
            .await?,
        some(pat!(JournalEntry::Entry(entry_completed_matcher())))
    );
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 5)
            .await?,
        some(pat!(JournalEntry::Entry(entry_completed_matcher())))
    );
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 6)
            .await?,
        some(pat!(JournalEntry::Entry(entry_completed_matcher())))
    );

    assert_that!(
        actions,
        all!(
            contains(terminate_invocation_outbox_message_matcher(
                call_invocation_id,
                TerminationFlavor::Cancel
            )),
            contains(forward_canceled_completion_matcher(4)),
            contains(forward_canceled_completion_matcher(5)),
            contains(forward_canceled_completion_matcher(6)),
            contains(delete_timer_matcher(5)),
        )
    );

    Ok(())
}

#[test(tokio::test)]
async fn cancel_suspended_invocation() -> Result<(), Error> {
    let mut test_env = TestEnv::create().await;

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target);

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
            waiting_for_completed_entries: HashSet::from([3, 4, 5, 6]),
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
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 4)
            .await?,
        some(pat!(JournalEntry::Entry(entry_completed_matcher())))
    );
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 5)
            .await?,
        some(pat!(JournalEntry::Entry(entry_completed_matcher())))
    );
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 6)
            .await?,
        some(pat!(JournalEntry::Entry(entry_completed_matcher())))
    );

    assert_that!(
        actions,
        all!(
            contains(terminate_invocation_outbox_message_matcher(
                call_invocation_id,
                TerminationFlavor::Cancel
            )),
            contains(delete_timer_matcher(5)),
            contains(pat!(Action::Invoke {
                invocation_id: eq(invocation_id),
                invocation_target: eq(invocation_target)
            }))
        )
    );

    Ok(())
}

fn completed_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::Call {
            is_completed: true,
            enrichment_result: Some(CallEnrichmentResult {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                completion_retention_time: None,
                span_context: ServiceInvocationSpanContext::empty(),
            }),
        },
        Bytes::default(),
    ))
}

fn background_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::OneWayCall {
            enrichment_result: CallEnrichmentResult {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                completion_retention_time: None,
                span_context: ServiceInvocationSpanContext::empty(),
            },
        },
        Bytes::default(),
    ))
}

fn uncompleted_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::Call {
            is_completed: false,
            enrichment_result: Some(CallEnrichmentResult {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                completion_retention_time: None,
                span_context: ServiceInvocationSpanContext::empty(),
            }),
        },
        Bytes::default(),
    ))
}

fn create_termination_journal(
    call_invocation_id: InvocationId,
    background_invocation_id: InvocationId,
    finished_call_invocation_id: InvocationId,
) -> Vec<JournalEntry> {
    vec![
        uncompleted_invoke_entry(call_invocation_id),
        completed_invoke_entry(finished_call_invocation_id),
        background_invoke_entry(background_invocation_id),
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
    ]
}

fn canceled_completion_matcher(entry_index: EntryIndex) -> impl Matcher<ActualT = Completion> {
    pat!(Completion {
        entry_index: eq(entry_index),
        result: pat!(CompletionResult::Failure(
            eq(codes::ABORTED),
            eq(ByteString::from_static("canceled"))
        ))
    })
}

fn entry_completed_matcher() -> impl Matcher<ActualT = EnrichedRawEntry> {
    predicate(|e: &EnrichedRawEntry| e.header().is_completed().unwrap_or(false))
        .with_description("completed entry", "uncompleted entry")
}

fn forward_canceled_completion_matcher(entry_index: EntryIndex) -> impl Matcher<ActualT = Action> {
    pat!(Action::ForwardCompletion {
        completion: canceled_completion_matcher(entry_index),
    })
}

fn delete_timer_matcher(entry_index: EntryIndex) -> impl Matcher<ActualT = Action> {
    pat!(Action::DeleteTimer {
        timer_key: pat!(TimerKey {
            kind: pat!(TimerKeyKind::CompleteJournalEntry {
                journal_index: eq(entry_index),
            }),
            timestamp: eq(1337),
        })
    })
}

fn terminate_invocation_outbox_message_matcher(
    target_invocation_id: InvocationId,
    termination_flavor: TerminationFlavor,
) -> impl Matcher<ActualT = Action> {
    pat!(Action::NewOutboxMessage {
        message: pat!(
            restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(pat!(
                InvocationTermination {
                    invocation_id: eq(target_invocation_id),
                    flavor: eq(termination_flavor)
                }
            ))
        )
    })
}
