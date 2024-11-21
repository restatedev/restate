// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::tests::TestEnv;
use crate::partition::state_machine::Action;
use bytes::Bytes;
use googletest::prelude::*;
use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::journal_table::JournalEntry;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, ServiceId};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocation, ServiceInvocationSpanContext, Source,
};
use restate_types::journal::enriched::{
    CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};
use restate_wal_protocol::Command;

pub fn completed_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
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

pub fn background_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
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

pub fn incomplete_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
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

pub async fn mock_start_invocation_with_service_id(
    state_machine: &mut TestEnv,
    service_id: ServiceId,
) -> InvocationId {
    mock_start_invocation_with_invocation_target(
        state_machine,
        InvocationTarget::mock_from_service_id(service_id),
    )
    .await
}

pub async fn mock_start_invocation_with_invocation_target(
    state_machine: &mut TestEnv,
    invocation_target: InvocationTarget,
) -> InvocationId {
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            argument: Default::default(),
            source: Source::Ingress(PartitionProcessorRpcRequestId::new()),
            response_sink: None,
            span_context: Default::default(),
            headers: vec![],
            execution_time: None,
            completion_retention_duration: None,
            idempotency_key: None,
            submit_notification_sink: None,
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invocation_target: eq(invocation_target),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    invocation_id
}

pub async fn mock_start_invocation(state_machine: &mut TestEnv) -> InvocationId {
    mock_start_invocation_with_invocation_target(
        state_machine,
        InvocationTarget::mock_virtual_object(),
    )
    .await
}
