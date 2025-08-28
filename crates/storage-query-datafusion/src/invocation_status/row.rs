// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::invocation_status_table::{
    CompletedInvocationMetadataAccessor, InFlightInvocationMetadataAccessor,
    InvocationMetadataAccessor, InvocationStatusAccessor, JournalMetadataAccessor,
    PreFlightInvocationMetadataAccessor, ResponseResultRef, SourceRef,
};
use restate_types::errors::ConversionError;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{ServiceType, TraceId};

use crate::invocation_status::schema::{SysInvocationStatusBuilder, SysInvocationStatusRowBuilder};

#[inline]
pub(crate) fn append_invocation_status_row<
    P: PreFlightInvocationMetadataAccessor,
    I: InFlightInvocationMetadataAccessor,
    C: CompletedInvocationMetadataAccessor,
>(
    builder: &mut SysInvocationStatusBuilder,
    invocation_id: InvocationId,
    invocation_status: InvocationStatusAccessor<P, I, C>,
) -> Result<(), ConversionError> {
    let mut row = builder.row();

    if row.is_partition_key_defined() {
        row.partition_key(invocation_id.partition_key());
    }

    if (row.is_target_service_name_defined()
        || row.is_target_service_key_defined()
        || row.is_target_handler_name_defined()
        || row.is_target_defined()
        || row.is_target_service_ty_defined())
        && let Some(invocation_target) = invocation_status.invocation_target()?
    {
        if row.is_target_service_name_defined() {
            row.target_service_name(invocation_target.service_name()?);
        }
        if row.is_target_service_key_defined()
            && let Some(key) = invocation_target.key()?
        {
            row.target_service_key(key);
        }
        if row.is_target_handler_name_defined() {
            row.target_handler_name(invocation_target.handler_name()?);
        }
        if row.is_target_defined() {
            row.fmt_target(invocation_target.target_fmt()?);
        }
        if row.is_target_service_ty_defined() {
            row.target_service_ty(match invocation_target.service_ty() {
                ServiceType::Service => "service",
                ServiceType::VirtualObject => "virtual_object",
                ServiceType::Workflow => "workflow",
            });
        }
    }

    // Invocation id
    if row.is_id_defined() {
        row.fmt_id(invocation_id);
    }

    if row.is_idempotency_key_defined()
        && let Some(key) = invocation_status.idempotency_key()?
    {
        row.idempotency_key(key)
    }

    // Additional invocation metadata
    match invocation_status {
        InvocationStatusAccessor::Scheduled(scheduled) => {
            fill_timestamps(&mut row, &scheduled);

            row.status("scheduled");
            if row.is_created_using_restate_version_defined() {
                row.created_using_restate_version(scheduled.created_using_restate_version()?);
            }
            if needs_invoked_by(&row) {
                fill_invoked_by(&mut row, scheduled.source()?)?;
            }
            if row.is_scheduled_at_defined()
                && let Some(execution_time) = scheduled.execution_time()
            {
                row.scheduled_start_at(execution_time.as_u64() as i64)
            }
            if row.is_completion_retention_defined() {
                row.completion_retention(
                    scheduled.completion_retention_duration()?.as_millis() as i64
                );
            }
            if row.is_journal_retention_defined() {
                row.journal_retention(scheduled.journal_retention_duration()?.as_millis() as i64);
            }
        }
        InvocationStatusAccessor::Inboxed(inboxed) => {
            fill_timestamps(&mut row, &inboxed);

            row.status("inboxed");
            if row.is_created_using_restate_version_defined() {
                row.created_using_restate_version(inboxed.created_using_restate_version()?);
            }
            if needs_invoked_by(&row) {
                fill_invoked_by(&mut row, inboxed.source()?)?;
            }
            if row.is_scheduled_at_defined()
                && let Some(execution_time) = inboxed.execution_time()
            {
                row.scheduled_start_at(execution_time.as_u64() as i64)
            }
            if row.is_completion_retention_defined() {
                row.completion_retention(
                    inboxed.completion_retention_duration()?.as_millis() as i64
                );
            }
            if row.is_journal_retention_defined() {
                row.journal_retention(inboxed.journal_retention_duration()?.as_millis() as i64);
            }
        }
        InvocationStatusAccessor::Invoked(metadata) => {
            fill_journal_metadata(&mut row, &metadata)?;
            fill_timestamps(&mut row, &metadata);

            row.status("invoked");
            fill_in_flight_invocation_metadata(&mut row, metadata)?;
        }
        InvocationStatusAccessor::Suspended(metadata) => {
            fill_journal_metadata(&mut row, &metadata)?;
            fill_timestamps(&mut row, &metadata);

            row.status("suspended");
            fill_in_flight_invocation_metadata(&mut row, metadata)?;
        }
        InvocationStatusAccessor::Completed(completed) => {
            fill_journal_metadata(&mut row, &completed)?;
            fill_timestamps(&mut row, &completed);

            row.status("completed");
            if row.is_created_using_restate_version_defined() {
                row.created_using_restate_version(completed.created_using_restate_version()?);
            }
            if needs_invoked_by(&row) {
                fill_invoked_by(&mut row, completed.source()?)?;
            }
            if row.is_scheduled_at_defined()
                && let Some(execution_time) = completed.execution_time()
            {
                row.scheduled_start_at(execution_time.as_u64() as i64)
            }
            if row.is_completion_retention_defined() {
                row.completion_retention(
                    completed.completion_retention_duration()?.as_millis() as i64
                );
            }
            if row.is_journal_retention_defined() {
                row.journal_retention(completed.journal_retention_duration()?.as_millis() as i64);
            }

            if row.is_completion_result_defined() || row.is_completion_failure_defined() {
                match completed.response_result()? {
                    ResponseResultRef::Success => {
                        row.completion_result("success");
                    }
                    ResponseResultRef::Failure { code, message } => {
                        row.completion_result("failure");
                        if row.is_completion_failure_defined() {
                            row.fmt_completion_failure(format_args!(
                                "[{}] {}",
                                code,
                                message.as_str("result")?
                            ));
                        }
                    }
                }
            }
        }
        InvocationStatusAccessor::Free => {
            row.status("free");
        }
    };

    Ok(())
}

fn fill_in_flight_invocation_metadata(
    row: &mut SysInvocationStatusRowBuilder,
    meta: impl InFlightInvocationMetadataAccessor,
) -> Result<(), ConversionError> {
    if row.is_created_using_restate_version_defined() {
        row.created_using_restate_version(meta.created_using_restate_version()?);
    }
    // journal_metadata and stats are filled by other functions
    if row.is_pinned_deployment_id_defined()
        && let Some(deployment_id) = meta.deployment_id()?
    {
        row.fmt_pinned_deployment_id(deployment_id);
    }
    if row.is_pinned_service_protocol_version_defined()
        && let Some(service_protocol_version) = meta.service_protocol_version()?
    {
        row.pinned_service_protocol_version(service_protocol_version.as_repr().unsigned_abs());
    }
    if needs_invoked_by(row) {
        fill_invoked_by(row, meta.source()?)?;
    }
    if row.is_scheduled_at_defined()
        && let Some(execution_time) = meta.execution_time()
    {
        row.scheduled_start_at(execution_time.as_u64() as i64)
    }
    if row.is_completion_retention_defined() {
        row.completion_retention(meta.completion_retention_duration()?.as_millis() as i64);
    }
    if row.is_journal_retention_defined() {
        row.journal_retention(meta.journal_retention_duration()?.as_millis() as i64);
    }

    Ok(())
}

fn needs_invoked_by(row: &SysInvocationStatusRowBuilder) -> bool {
    row.is_invoked_by_defined()
        || row.is_invoked_by_service_name_defined()
        || row.is_invoked_by_id_defined()
        || row.is_invoked_by_target_defined()
        || row.is_invoked_by_subscription_id_defined()
        || row.is_restarted_from_defined()
}

#[inline]
fn fill_invoked_by(
    row: &mut SysInvocationStatusRowBuilder,
    source: SourceRef,
) -> Result<(), ConversionError> {
    match source {
        SourceRef::Service(invocation_id, invocation_target) => {
            row.invoked_by("service");
            row.invoked_by_service_name(invocation_target.service_name()?);
            if row.is_invoked_by_id_defined() {
                row.fmt_invoked_by_id(invocation_id);
            }
            if row.is_invoked_by_target_defined() {
                row.fmt_invoked_by_target(invocation_target.target_fmt()?);
            }
        }
        SourceRef::Ingress => {
            row.invoked_by("ingress");
        }
        SourceRef::Internal => {
            row.invoked_by("restate");
        }
        SourceRef::Subscription(sub_id) => {
            row.invoked_by("subscription");
            if row.is_invoked_by_subscription_id_defined() {
                row.fmt_invoked_by_subscription_id(sub_id)
            }
        }
        SourceRef::RestartAsNew(invocation_id) => {
            row.invoked_by("restart_as_new");
            if row.is_restarted_from_defined() {
                row.fmt_restarted_from(invocation_id)
            }
        }
    }

    Ok(())
}

#[inline]
fn fill_timestamps(row: &mut SysInvocationStatusRowBuilder, stat: impl InvocationMetadataAccessor) {
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
    journal_metadata: &impl JournalMetadataAccessor,
) -> Result<(), ConversionError> {
    if row.is_trace_id_defined() {
        let tid = journal_metadata.trace_id()?;
        if tid != TraceId::INVALID {
            row.fmt_trace_id(tid);
        }
    }
    if row.is_journal_size_defined() {
        row.journal_size(journal_metadata.length());
    }
    if row.is_journal_commands_size_defined() {
        row.journal_commands_size(journal_metadata.commands());
    }

    Ok(())
}
