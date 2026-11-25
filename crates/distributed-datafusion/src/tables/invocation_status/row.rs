// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_partition_store::invocation_status_table::InvocationEntry;
use restate_storage_api::protobuf_types::v1::invocation_status_v2::Status;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_api::protobuf_types::v1::source::Source;
use restate_storage_query_api::tables::invocation_status::{
    InvocationStatusBatchBuilder, InvocationStatusRowWriter,
};
use restate_types::errors::ConversionError;
use restate_types::identifiers::{BaseEntryId, InvocationId, WithPartitionKey};
use restate_types::invocation::{ServiceType, TraceId};
use restate_types::vqueues::{EffectiveStatus, Seq};

/// The columns read from the invocation's vqueue entry, the `entry` read with
/// its status if the read asked for them ([`reads_entries`]): its stage and
/// effective status, NULL without one, and the `status` and `completion_result`
/// the effective status tells ([`EffectiveStatus::invocation_status`]) with
/// `completed`, which fall back to the stored ones without an entry or without
/// an effective status.
pub(super) fn append_row(
    builder: &mut InvocationStatusBatchBuilder,
    id: InvocationId,
    status: &InvocationStatusV2Lazy<'_>,
    entry: Option<InvocationEntry>,
) -> Result<(), ConversionError> {
    let mut row = builder.row();
    row.partition_key(id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(id);
    }
    if row.is_canonical_id_defined()
        && let Some(seq) = status.inner.creation_seq
    {
        row.fmt_canonical_id(BaseEntryId::from(id).canonicalize(Seq::new(seq)));
    }
    if row.is_vqueue_id_defined()
        && let Some(queue) = status.vqueue_id()
    {
        row.fmt_vqueue_id(queue);
    }
    if row.is_limit_key_defined()
        && let Some(key) = status.limit_key_fmt()
    {
        row.fmt_limit_key(key);
    }
    if row.is_idempotency_key_defined()
        && let Some(key) = status.idempotency_key()?
    {
        row.idempotency_key(key);
    }
    if (row.is_target_service_name_defined()
        || row.is_target_service_key_defined()
        || row.is_target_handler_name_defined()
        || row.is_target_defined()
        || row.is_target_service_ty_defined()
        || row.is_scope_defined())
        && let Some(target) = status.invocation_target()?
    {
        if row.is_target_service_name_defined() {
            row.fmt_target_service_name(target.service_name());
        }
        if row.is_target_service_key_defined()
            && let Some(key) = target.key()?
        {
            row.target_service_key(key);
        }
        if row.is_target_handler_name_defined() {
            row.target_handler_name(target.handler_name());
        }
        if row.is_target_defined() {
            row.fmt_target(target.target_fmt()?);
        }
        if row.is_target_service_ty_defined() {
            row.target_service_ty(match target.service_ty()? {
                ServiceType::Service => "service",
                ServiceType::VirtualObject => "virtual_object",
                ServiceType::Workflow => "workflow",
            });
        }
        if row.is_scope_defined()
            && let Some(scope) = target.scope()
        {
            row.scope(scope);
        }
    }
    if row.is_invoked_by_defined()
        || row.is_invoked_by_service_name_defined()
        || row.is_invoked_by_id_defined()
        || row.is_invoked_by_target_defined()
        || row.is_invoked_by_subscription_id_defined()
        || row.is_restarted_from_defined()
    {
        fill_source(&mut row, status.source()?)?;
    }
    row.created_at(status.inner.creation_time as i64);
    row.modified_at(status.inner.modification_time as i64);
    if let Some(at) = status.inner.inboxed_transition_time {
        row.inboxed_at(at as i64);
    }
    if let Some(at) = status.inner.scheduled_transition_time {
        row.scheduled_at(at as i64);
    }
    if let Some(at) = status.inner.running_transition_time {
        row.running_at(at as i64);
    }
    if let Some(at) = status.inner.completed_transition_time {
        row.completed_at(at as i64);
    }

    let state = status.inner.status();
    let effective = entry.map(|entry| EffectiveStatus::new(entry.stage, entry.status));
    if row.is_stage_defined()
        && let Some(name) = entry.and_then(|entry| entry.stage.name())
    {
        row.stage(name);
    }
    if row.is_effective_status_defined()
        && let Some(name) = effective.and_then(EffectiveStatus::name)
    {
        row.effective_status(name);
    }
    // The entry tells the status; the stored one shows only without it.
    let public = match effective.and_then(EffectiveStatus::invocation_status) {
        Some(name) => name,
        None => match state {
            Status::Scheduled => "scheduled",
            Status::Inboxed => "inboxed",
            Status::Invoked => "invoked",
            Status::Paused => "paused",
            Status::Suspended => "suspended",
            Status::Completed => "completed",
            Status::UnknownStatus => return Err(ConversionError::invalid_data_static("status")),
        },
    };
    if row.is_completed_defined() {
        row.completed(public == "completed");
    }
    row.status(public);
    if row.is_pinned_deployment_id_defined()
        && let Some(deployment) = status.pinned_deployment_id()?
    {
        row.fmt_pinned_deployment_id(deployment);
    }
    if row.is_pinned_service_protocol_version_defined()
        && let Some(version) = status.service_protocol_version()?
    {
        row.pinned_service_protocol_version(version.as_repr().unsigned_abs());
    }
    if row.is_created_using_restate_version_defined() {
        row.created_using_restate_version(status.created_using_restate_version()?);
    }
    // Keep V1's projection-dependent scheduled-start behavior for now.
    let scheduled_start = if matches!(
        state,
        Status::Scheduled | Status::Inboxed | Status::Completed
    ) {
        row.is_scheduled_at_defined()
    } else {
        row.is_scheduled_start_at_defined()
    };
    if scheduled_start && let Some(at) = status.inner.execution_time {
        row.scheduled_start_at(at as i64);
    }
    if row.is_completion_retention_defined() {
        row.completion_retention(status.completion_retention_duration()?.as_millis() as i64);
    }
    if row.is_journal_retention_defined() {
        row.journal_retention(status.journal_retention_duration()?.as_millis() as i64);
    }
    if matches!(
        state,
        Status::Invoked | Status::Paused | Status::Suspended | Status::Completed
    ) {
        if row.is_trace_id_defined() {
            let trace = status.trace_id()?;
            if trace != TraceId::INVALID {
                row.fmt_trace_id(trace);
            }
        }
        row.journal_size(status.inner.journal_length);
        row.journal_commands_size(status.inner.commands);
    }
    if state == Status::Suspended {
        if row.is_suspended_waiting_for_completions_defined() {
            row.suspended_waiting_for_completions_iter(
                status
                    .inner
                    .waiting_for_completions
                    .iter()
                    .map(|value| Some(*value)),
            );
        }
        if row.is_suspended_waiting_for_signals_defined() {
            row.suspended_waiting_for_signals_iter(
                status
                    .inner
                    .waiting_for_signal_indexes
                    .iter()
                    .map(|value| Some(*value)),
            );
        }
        if row.is_suspended_waiting_future_json_defined() {
            row.suspended_waiting_future_json(
                serde_json::to_string(&status.awaiting_on()?).map_err(|_| {
                    ConversionError::invalid_data_static("suspended_waiting_future_json")
                })?,
            );
        }
    }
    // The entry tells the result too, if it tells the status; the failure
    // itself is only stored.
    let told = effective.filter(|effective| effective.invocation_status().is_some());
    if let Some(effective) = told
        && row.is_completion_result_defined()
        && let Some(result) = effective.completion_result()
    {
        row.completion_result(result);
    }
    let stored_result = told.is_none() && row.is_completion_result_defined();
    if state == Status::Completed && (stored_result || row.is_completion_failure_defined()) {
        use restate_storage_api::protobuf_types::v1::response_result::ResponseResult;
        match status.response_result()? {
            ResponseResult::ResponseSuccess(_) => {
                if stored_result {
                    row.completion_result("success");
                }
            }
            ResponseResult::ResponseFailure(failure) => {
                if stored_result {
                    row.completion_result("failure");
                }
                if row.is_completion_failure_defined() {
                    let message = str::from_utf8(failure.failure_message.as_ref())
                        .map_err(|_| ConversionError::invalid_data_static("failure_message"))?;
                    row.fmt_completion_failure(format_args!(
                        "[{}] {}",
                        failure.failure_code, message
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Whether a read with `projection` needs each invocation's entry: it shows,
/// or filters on, a column read from it. Filters above a read keep their
/// columns in its projection.
pub(super) fn reads_entries(projection: &datafusion::arrow::datatypes::Schema) -> bool {
    [
        "status",
        "completion_result",
        "stage",
        "effective_status",
        "completed",
    ]
    .iter()
    .any(|name| projection.column_with_name(name).is_some())
}

fn fill_source(
    row: &mut InvocationStatusRowWriter<'_>,
    source: Source,
) -> Result<(), ConversionError> {
    match source {
        Source::Service(service) => {
            row.invoked_by("service");
            if row.is_invoked_by_service_name_defined() || row.is_invoked_by_target_defined() {
                let target = service.invocation_target()?;
                if row.is_invoked_by_service_name_defined() {
                    row.invoked_by_service_name(target.service_name());
                }
                if row.is_invoked_by_target_defined() {
                    row.fmt_invoked_by_target(target.target_fmt()?);
                }
            }
            if row.is_invoked_by_id_defined() {
                row.fmt_invoked_by_id(service.invocation_id()?);
            }
        }
        Source::Ingress(_) => {
            row.invoked_by("ingress");
        }
        Source::Internal(()) => {
            row.invoked_by("restate");
        }
        Source::Subscription(subscription) => {
            row.invoked_by("subscription");
            if row.is_invoked_by_subscription_id_defined() {
                row.fmt_invoked_by_subscription_id(subscription.subscription_id()?);
            }
        }
        Source::RestartAsNew(restart) => {
            row.invoked_by("restart_as_new");
            if row.is_restarted_from_defined() {
                row.fmt_restarted_from(restart.invocation_id()?);
            }
        }
        Source::Ingestion(_) => {
            row.invoked_by("ingress_ingestion");
        }
    }
    Ok(())
}
