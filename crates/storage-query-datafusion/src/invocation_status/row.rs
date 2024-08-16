// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
    InFlightInvocationMetadata, InvocationStatus, JournalMetadata, SourceTable, StatusTimestamps,
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
            fill_invoked_by(&mut row, output, scheduled.metadata.source);
        }
        InvocationStatus::Inboxed(inboxed) => {
            row.status("inboxed");
            fill_invoked_by(&mut row, output, inboxed.metadata.source);
        }
        InvocationStatus::Invoked(metadata) => {
            row.status("invoked");
            fill_in_flight_invocation_metadata(&mut row, output, metadata);
        }
        InvocationStatus::Suspended { metadata, .. } => {
            row.status("suspended");
            fill_in_flight_invocation_metadata(&mut row, output, metadata);
        }
        InvocationStatus::Free => {
            row.status("free");
        }
        InvocationStatus::Completed(completed) => {
            row.status("completed");
            fill_invoked_by(&mut row, output, completed.source);

            // We fill the span context only for the new table, as the old table will contain always the empty value
            if completed.source_table == SourceTable::New {
                fill_span_context(&mut row, output, &completed.span_context);
            }

            match completed.response_result {
                ResponseResult::Success(_) => {
                    row.completion_result("success");
                }
                ResponseResult::Failure(failure) => {
                    row.completion_result("failure");
                    row.completion_failure(format_using(output, &failure));
                }
            }
        }
    };
}

fn fill_in_flight_invocation_metadata(
    row: &mut SysInvocationStatusRowBuilder,
    output: &mut String,
    meta: InFlightInvocationMetadata,
) {
    // journal_metadata and stats are filled by other functions
    if let Some(pinned_deployment) = meta.pinned_deployment {
        row.pinned_deployment_id(pinned_deployment.deployment_id.to_string());
    }
    fill_invoked_by(row, output, meta.source)
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
        Source::Ingress => {
            row.invoked_by("ingress");
        }
        Source::Internal => {
            row.invoked_by("restate");
        }
    }
}

#[inline]
fn fill_timestamps(row: &mut SysInvocationStatusRowBuilder, stat: &StatusTimestamps) {
    row.created_at(unsafe { stat.creation_time() }.as_u64() as i64);
    row.modified_at(unsafe { stat.modification_time() }.as_u64() as i64);
}

#[inline]
fn fill_journal_metadata(
    row: &mut SysInvocationStatusRowBuilder,
    output: &mut String,
    journal_metadata: &JournalMetadata,
) {
    fill_span_context(row, output, &journal_metadata.span_context);
    row.journal_size(journal_metadata.length);
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
