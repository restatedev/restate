// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::invocation_status::schema::{SysInvocationStatusBuilder, SysInvocationStatusRowBuilder};
use crate::table_util::format_using;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, JournalMetadata, StatusTimestamps,
};
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{
    ResponseResult, ServiceInvocationSpanContext, ServiceType, Source, TraceId,
};

#[inline]
pub(crate) fn append_invocation_status_row(
    builder: &mut SysInvocationStatusBuilder,
    output: &mut String,
    invocation_id: InvocationId,
    invocation_status: InvocationStatus,
) {
    let mut row = builder.row();

    row.partition_key(invocation_id.partition_key());
    if let Some(invocation_target) = invocation_status.invocation_target() {
        row.target_service_name(invocation_target.service_name());
        if let Some(key) = invocation_target.key() {
            row.target_service_key(key);
        }
        row.target_handler_name(invocation_target.handler_name());
        if row.is_target_defined() {
            row.target(format_using(output, &invocation_target));
        }
        row.target_service_ty(match invocation_target.service_ty() {
            ServiceType::Service => "service",
            ServiceType::VirtualObject => "virtual_object",
            ServiceType::Workflow => "workflow",
        });
    }

    // Invocation id
    if row.is_id_defined() {
        row.id(format_using(output, &invocation_id));
    }

    if row.is_idempotency_key_defined() {
        if let Some(key) = invocation_status.idempotency_key() {
            row.idempotency_key(format_using(output, &key))
        }
    }

    // Journal metadata
    if let Some(journal_metadata) = invocation_status.get_journal_metadata() {
        fill_journal_metadata(&mut row, output, journal_metadata)
    }

    // Stat
    if let Some(timestamps) = invocation_status.get_timestamps() {
        fill_timestamps(&mut row, timestamps);
    }

    // Additional invocation metadata
    match invocation_status {
        InvocationStatus::Scheduled(scheduled) => {
            row.status("scheduled");
            row.created_using_restate_version(
                scheduled.metadata.created_using_restate_version.as_str(),
            );
            fill_invoked_by(&mut row, output, scheduled.metadata.source);
            if let Some(execution_time) = scheduled.metadata.execution_time {
                row.scheduled_start_at(execution_time.as_u64() as i64)
            }
            row.completion_retention(
                scheduled.metadata.completion_retention_duration.as_millis() as i64
            );
            row.journal_retention(scheduled.metadata.journal_retention_duration.as_millis() as i64);
        }
        InvocationStatus::Inboxed(inboxed) => {
            row.status("inboxed");
            row.created_using_restate_version(
                inboxed.metadata.created_using_restate_version.as_str(),
            );
            fill_invoked_by(&mut row, output, inboxed.metadata.source);
            if let Some(execution_time) = inboxed.metadata.execution_time {
                row.scheduled_start_at(execution_time.as_u64() as i64)
            }
            row.completion_retention(
                inboxed.metadata.completion_retention_duration.as_millis() as i64
            );
            row.journal_retention(inboxed.metadata.journal_retention_duration.as_millis() as i64);
        }
        InvocationStatus::Invoked(metadata) => {
            row.status("invoked");
            fill_in_flight_invocation_metadata(&mut row, output, metadata);
        }
        InvocationStatus::Suspended { metadata, .. } => {
            row.status("suspended");
            fill_in_flight_invocation_metadata(&mut row, output, metadata);
        }
        InvocationStatus::Completed(completed) => {
            row.status("completed");
            row.created_using_restate_version(completed.created_using_restate_version.as_str());
            fill_invoked_by(&mut row, output, completed.source);
            if let Some(execution_time) = completed.execution_time {
                row.scheduled_start_at(execution_time.as_u64() as i64)
            }
            row.completion_retention(completed.completion_retention_duration.as_millis() as i64);
            row.journal_retention(completed.journal_retention_duration.as_millis() as i64);

            match completed.response_result {
                ResponseResult::Success(_) => {
                    row.completion_result("success");
                }
                ResponseResult::Failure(failure) => {
                    row.completion_result("failure");
                    if row.is_completion_failure_defined() {
                        row.completion_failure(format_using(output, &failure));
                    }
                }
            }
        }
        InvocationStatus::Free => {
            row.status("free");
        }
    };
}

fn fill_in_flight_invocation_metadata(
    row: &mut SysInvocationStatusRowBuilder,
    output: &mut String,
    meta: InFlightInvocationMetadata,
) {
    row.created_using_restate_version(meta.created_using_restate_version.as_str());
    // journal_metadata and stats are filled by other functions
    if let Some(pinned_deployment) = meta.pinned_deployment {
        if row.is_pinned_deployment_id_defined() {
            row.pinned_deployment_id(format_using(output, &pinned_deployment.deployment_id));
        }
        row.pinned_service_protocol_version(
            pinned_deployment
                .service_protocol_version
                .as_repr()
                .unsigned_abs(),
        );
    }
    fill_invoked_by(row, output, meta.source);
    if let Some(execution_time) = meta.execution_time {
        row.scheduled_start_at(execution_time.as_u64() as i64)
    }
    row.completion_retention(meta.completion_retention_duration.as_millis() as i64);
    row.journal_retention(meta.journal_retention_duration.as_millis() as i64);
}

#[inline]
fn fill_invoked_by(row: &mut SysInvocationStatusRowBuilder, output: &mut String, source: Source) {
    match source {
        Source::Service(invocation_id, invocation_target) => {
            row.invoked_by("service");
            row.invoked_by_service_name(invocation_target.service_name());
            if row.is_invoked_by_id_defined() {
                row.invoked_by_id(format_using(output, &invocation_id));
            }
            if row.is_invoked_by_target_defined() {
                row.invoked_by_target(format_using(output, &invocation_target));
            }
        }
        Source::Ingress(_) => {
            row.invoked_by("ingress");
        }
        Source::Internal => {
            row.invoked_by("restate");
        }
        Source::Subscription(sub_id) => {
            row.invoked_by("subscription");
            if row.is_invoked_by_subscription_id_defined() {
                row.invoked_by_subscription_id(format_using(output, &sub_id))
            }
        }
        Source::RestartAsNew(invocation_id) => {
            row.invoked_by("restart_as_new");
            if row.is_restarted_from_defined() {
                row.restarted_from(format_using(output, &invocation_id))
            }
        }
    }
}

#[inline]
fn fill_timestamps(row: &mut SysInvocationStatusRowBuilder, stat: &StatusTimestamps) {
    row.created_at(stat.creation_time().as_u64() as i64);
    row.modified_at(stat.modification_time().as_u64() as i64);
    if let Some(inboxed_at) = stat.inboxed_transition_time() {
        row.inboxed_at(inboxed_at.as_u64() as i64);
    }
    if let Some(scheduled_at) = stat.scheduled_transition_time() {
        row.scheduled_at(scheduled_at.as_u64() as i64);
    }
    if let Some(running_at) = stat.running_transition_time() {
        row.running_at(running_at.as_u64() as i64);
    }
    if let Some(completed_at) = stat.completed_transition_time() {
        row.completed_at(completed_at.as_u64() as i64);
    }
}

#[inline]
fn fill_journal_metadata(
    row: &mut SysInvocationStatusRowBuilder,
    output: &mut String,
    journal_metadata: &JournalMetadata,
) {
    fill_span_context(row, output, &journal_metadata.span_context);
    row.journal_size(journal_metadata.length);
    row.journal_commands_size(journal_metadata.commands);
}

#[inline]
fn fill_span_context(
    row: &mut SysInvocationStatusRowBuilder,
    output: &mut String,
    span_context: &ServiceInvocationSpanContext,
) {
    if row.is_trace_id_defined() {
        let tid = span_context.trace_id();
        if tid != TraceId::INVALID {
            row.trace_id(format_using(output, &tid));
        }
    }
}
