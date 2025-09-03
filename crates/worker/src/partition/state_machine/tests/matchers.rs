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
use restate_types::invocation::{InvocationTermination, TerminationFlavor};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{Completion, CompletionResult};
use restate_types::journal_v2::{Entry, EntryIndex};

pub mod storage {
    use super::*;

    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatus, InvocationStatusDiscriminants,
    };
    use restate_storage_api::journal_table::JournalEntry;
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::{DeploymentId, InvocationId};
    use restate_types::invocation::InvocationTarget;
    use restate_types::journal::Entry;

    pub fn has_journal_length(
        journal_length: EntryIndex,
    ) -> impl Matcher<ActualT = InvocationStatus> {
        predicate(move |is: &InvocationStatus| {
            is.get_journal_metadata()
                .is_some_and(|jm| jm.length == journal_length)
        })
        .with_description(
            format!("has journal length {journal_length}"),
            format!("hasn't journal length {journal_length}"),
        )
    }

    pub fn has_events(events: EntryIndex) -> impl Matcher<ActualT = InvocationStatus> {
        predicate(move |is: &InvocationStatus| {
            is.get_journal_metadata()
                .is_some_and(|jm| jm.events == events)
        })
        .with_description(
            format!("has journal events {events}"),
            format!("hasn't journal events {events}"),
        )
    }

    pub fn has_commands(commands: EntryIndex) -> impl Matcher<ActualT = InvocationStatus> {
        predicate(move |is: &InvocationStatus| {
            is.get_journal_metadata()
                .is_some_and(|jm| jm.commands == commands)
        })
        .with_description(
            format!("has commands {commands}"),
            format!("hasn't commands {commands}"),
        )
    }

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

    pub fn is_variant(
        discriminant: InvocationStatusDiscriminants,
    ) -> impl Matcher<ActualT = InvocationStatus> {
        property_matcher::internal::property_matcher(
            |o: &InvocationStatus| o.discriminant(),
            "discriminant()",
            some(eq(discriminant)),
        )
    }

    pub fn in_flight_metadata(
        inner: impl Matcher<ActualT = InFlightInvocationMetadata> + 'static,
    ) -> impl Matcher<ActualT = InvocationStatus> {
        // Guilty!
        property_matcher::internal::property_matcher(
            |o: &InvocationStatus| o.get_invocation_metadata().cloned(),
            "get_invocation_metadata()",
            some(inner),
        )
    }

    pub fn pinned_deployment_id_eq(
        deployment_id: DeploymentId,
    ) -> impl Matcher<ActualT = InvocationStatus> {
        in_flight_metadata(field!(
            InFlightInvocationMetadata.pinned_deployment,
            some(pat!(PinnedDeployment {
                deployment_id: eq(deployment_id)
            }))
        ))
    }
}

pub mod actions {
    use super::*;

    use crate::partition::state_machine::Action;
    use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
    use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
    use restate_types::invocation::client::{
        CancelInvocationResponse, KillInvocationResponse, PurgeInvocationResponse,
    };
    use restate_types::invocation::{InvocationTarget, ResponseResult};
    use restate_types::journal_v2::{Notification, Signal};

    pub fn invoke_for_id(invocation_id: InvocationId) -> impl Matcher<ActualT = Action> {
        pat!(Action::Invoke {
            invocation_id: eq(invocation_id)
        })
    }

    pub fn invoke_for_id_and_target(
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invocation_target: eq(invocation_target)
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

    pub fn forward_cancel_invocation_response(
        request_id: PartitionProcessorRpcRequestId,
        cancel_invocation_response: CancelInvocationResponse,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::ForwardCancelResponse {
            request_id: eq(request_id),
            response: eq(cancel_invocation_response)
        })
    }

    pub fn forward_kill_invocation_response(
        request_id: PartitionProcessorRpcRequestId,
        kill_invocation_response: KillInvocationResponse,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::ForwardKillResponse {
            request_id: eq(request_id),
            response: eq(kill_invocation_response)
        })
    }

    pub fn forward_purge_invocation_response(
        request_id: PartitionProcessorRpcRequestId,
        purge_invocation_response: PurgeInvocationResponse,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::ForwardPurgeInvocationResponse {
            request_id: eq(request_id),
            response: eq(purge_invocation_response)
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

    pub fn forward_notification(
        invocation_id: InvocationId,
        notif: impl Into<Notification>,
    ) -> impl Matcher<ActualT = Action> {
        let notification = notif
            .into()
            .encode::<ServiceProtocolV4Codec>()
            .try_as_notification()
            .unwrap();
        pat!(Action::ForwardNotification {
            invocation_id: eq(invocation_id),
            notification: eq(notification),
        })
    }

    pub fn invocation_response_to_partition_processor(
        caller_invocation_id: InvocationId,
        caller_entry_index: EntryIndex,
        response_result_matcher: impl Matcher<ActualT = ResponseResult> + 'static,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::NewOutboxMessage {
            message: outbox::invocation_response_to_partition_processor(
                caller_invocation_id,
                caller_entry_index,
                response_result_matcher
            )
        })
    }

    pub fn notify_signal(
        caller_invocation_id: InvocationId,
        signal: Signal,
    ) -> impl Matcher<ActualT = Action> {
        pat!(Action::NewOutboxMessage {
            message: outbox::notify_signal(caller_invocation_id, signal)
        })
    }
}

pub mod outbox {
    use super::*;

    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::identifiers::InvocationId;
    use restate_types::invocation::{
        InvocationResponse, JournalCompletionTarget, NotifySignalRequest, ResponseResult,
    };
    use restate_types::journal_v2::Signal;

    pub fn invocation_response_to_partition_processor(
        caller_invocation_id: InvocationId,
        caller_entry_index: EntryIndex,
        response_result_matcher: impl Matcher<ActualT = ResponseResult> + 'static,
    ) -> impl Matcher<ActualT = OutboxMessage> {
        pat!(
            restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                InvocationResponse {
                    target: pat!(JournalCompletionTarget {
                        caller_id: eq(caller_invocation_id),
                        caller_completion_id: eq(caller_entry_index),
                    }),
                    result: response_result_matcher
                }
            ))
        )
    }

    pub fn notify_signal(
        caller_invocation_id: InvocationId,
        signal: Signal,
    ) -> impl Matcher<ActualT = OutboxMessage> {
        pat!(
            restate_storage_api::outbox_table::OutboxMessage::NotifySignal(pat!(
                NotifySignalRequest {
                    invocation_id: eq(caller_invocation_id),
                    signal: eq(signal),
                }
            ))
        )
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

pub fn entry_eq(entry: impl Into<Entry>) -> impl Matcher<ActualT = Entry> {
    eq(entry.into())
}
