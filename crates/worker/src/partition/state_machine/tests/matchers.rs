// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;
use googletest::prelude::*;
use restate_storage_api::timer_table::{TimerKey, TimerKeyKind};
use restate_types::errors::codes;
use restate_types::identifiers::EntryIndex;
use restate_types::invocation::{InvocationTermination, TerminationFlavor};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{Completion, CompletionResult};

pub mod storage {
    use super::*;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;

    use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
    use restate_storage_api::journal_table::JournalEntry;
    use restate_types::identifiers::InvocationId;
    use restate_types::invocation::InvocationTarget;
    use restate_types::journal::Entry;

    pub fn invocation_inbox_entry(
        invocation_id: InvocationId,
        invocation_target: &InvocationTarget,
    ) -> impl Matcher<ActualT = SequenceNumberInboxEntry> {
        pat!(SequenceNumberInboxEntry {
            inbox_entry: pat!(InboxEntry::Invocation(
                eq(invocation_target.as_keyed_service_id().unwrap()),
                eq(invocation_id)
            ))
        })
    }

    pub fn is_entry(entry: Entry) -> impl Matcher<ActualT = JournalEntry> {
        pat!(JournalEntry::Entry(eq(
            ProtobufRawEntryCodec::serialize_enriched(entry)
        )))
    }
}

pub mod actions {
    use super::*;

    use crate::partition::state_machine::Action;
    use restate_types::identifiers::InvocationId;
    use restate_types::invocation::{InvocationResponse, ResponseResult};

    pub fn invoke_for_id(invocation_id: InvocationId) -> impl Matcher<ActualT = Action> {
        pat!(Action::Invoke {
            invocation_id: eq(invocation_id)
        })
    }

    pub fn delete_sleep_timer(entry_index: EntryIndex) -> impl Matcher<ActualT = Action> {
        pat!(Action::DeleteTimer {
            timer_key: pat!(TimerKey {
                kind: pat!(TimerKeyKind::CompleteJournalEntry {
                    journal_index: eq(entry_index),
                }),
                timestamp: eq(1337),
            })
        })
    }

    pub fn terminate_invocation(
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

    pub fn forward_canceled_completion(entry_index: EntryIndex) -> impl Matcher<ActualT = Action> {
        pat!(Action::ForwardCompletion {
            completion: canceled_completion(entry_index),
        })
    }

    pub fn forward_completion(
        invocation_id: InvocationId,
        inner: impl Matcher<ActualT = Completion> + 'static,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::ForwardCompletion {
            invocation_id: eq(invocation_id),
            completion: inner,
        })
    }

    pub fn invocation_response_to_partition_processor(
        caller_invocation_id: InvocationId,
        caller_entry_index: EntryIndex,
        response_result_matcher: impl Matcher<ActualT = ResponseResult> + 'static,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    InvocationResponse {
                        id: eq(caller_invocation_id),
                        entry_index: eq(caller_entry_index),
                        result: response_result_matcher
                    }
                ))
            )
        })
    }
}

pub fn completion(
    entry_index: EntryIndex,
    completion_result: CompletionResult,
) -> impl Matcher<ActualT = Completion> {
    pat!(Completion {
        entry_index: eq(entry_index),
        result: eq(completion_result)
    })
}

pub fn success_completion(
    entry_index: EntryIndex,
    bytes: impl Into<Bytes>,
) -> impl Matcher<ActualT = Completion> {
    completion(entry_index, CompletionResult::Success(bytes.into()))
}

pub fn canceled_completion(entry_index: EntryIndex) -> impl Matcher<ActualT = Completion> {
    completion(
        entry_index,
        CompletionResult::Failure(codes::ABORTED, ByteString::from_static("canceled")),
    )
}

pub fn completed_entry() -> impl Matcher<ActualT = EnrichedRawEntry> {
    predicate(|e: &EnrichedRawEntry| e.header().is_completed().unwrap_or(false))
        .with_description("completed entry", "uncompleted entry")
}
