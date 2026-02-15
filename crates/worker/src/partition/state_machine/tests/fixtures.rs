// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::Action;
use crate::partition::state_machine::tests::TestEnv;
use crate::partition::types::InvokerEffectKind;
use bytes::Bytes;
use googletest::prelude::*;
use restate_invoker_api::Effect;
use restate_memory::MemoryLease;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::journal_table::JournalEntry;
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{
    DeploymentId, InvocationId, PartitionProcessorRpcRequestId, ServiceId,
};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocation, ServiceInvocationSpanContext, Source,
};
use restate_types::journal::enriched::{
    CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};
use restate_types::journal_v2;
use restate_types::journal_v2::Entry;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_wal_protocol::Command;
use std::collections::HashSet;

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

pub fn invoker_entry_effect(invocation_id: InvocationId, entry: impl Into<Entry>) -> Command {
    invoker_entry_effect_for_epoch(invocation_id, entry)
}

pub fn invoker_entry_effect_for_epoch(
    invocation_id: InvocationId,
    entry: impl Into<Entry>,
) -> Command {
    Command::InvokerEffect(Box::new(Effect {
        invocation_id,
        kind: InvokerEffectKind::journal_entry(
            entry.into().encode::<ServiceProtocolV4Codec>(),
            None,
        ),
        memory_lease: MemoryLease::unlinked(),
    }))
}

pub fn invoker_end_effect(invocation_id: InvocationId) -> Command {
    invoker_end_effect_for_epoch(invocation_id)
}

pub fn invoker_end_effect_for_epoch(invocation_id: InvocationId) -> Command {
    Command::InvokerEffect(Box::new(Effect {
        invocation_id,
        kind: InvokerEffectKind::End,
        memory_lease: MemoryLease::unlinked(),
    }))
}

pub fn pinned_deployment(
    invocation_id: InvocationId,
    service_protocol_version: ServiceProtocolVersion,
) -> Command {
    Command::InvokerEffect(Box::new(Effect {
        invocation_id,
        kind: InvokerEffectKind::PinnedDeployment(PinnedDeployment {
            deployment_id: DeploymentId::default(),
            service_protocol_version,
        }),
        memory_lease: MemoryLease::unlinked(),
    }))
}

pub fn invoker_suspended(
    invocation_id: InvocationId,
    waiting_for_notifications: impl Into<HashSet<journal_v2::NotificationId>>,
) -> Command {
    Command::InvokerEffect(Box::new(Effect {
        invocation_id,
        kind: InvokerEffectKind::SuspendedV2 {
            waiting_for_notifications: waiting_for_notifications.into(),
        },
        memory_lease: MemoryLease::unlinked(),
    }))
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
        .apply(Command::Invoke(Box::new(ServiceInvocation::initialize(
            invocation_id,
            invocation_target.clone(),
            Source::Ingress(PartitionProcessorRpcRequestId::new()),
        ))))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invocation_target: eq(invocation_target),
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

pub async fn mock_pinned_deployment_v5(state_machine: &mut TestEnv, invocation_id: InvocationId) {
    let _ = state_machine
        .apply(pinned_deployment(invocation_id, ServiceProtocolVersion::V5))
        .await;
}
