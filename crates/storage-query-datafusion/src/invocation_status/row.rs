// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_api::protobuf_types::v1::source::Source;
use restate_types::errors::ConversionError;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{ServiceType, TraceId};

use crate::invocation_status::schema::{SysInvocationStatusBuilder, SysInvocationStatusRowBuilder};

#[inline]
pub(crate) fn append_invocation_status_row<'a>(
    builder: &mut SysInvocationStatusBuilder,
    invocation_id: InvocationId,
    invocation_status: InvocationStatusV2Lazy<'a>,
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
            row.target_service_ty(match invocation_target.service_ty()? {
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

    if needs_invoked_by(&row) {
        fill_invoked_by(&mut row, invocation_status.source()?)?;
    }

    fill_timestamps(&mut row, &invocation_status);

    // Additional invocation metadata
    use restate_storage_api::protobuf_types::v1::invocation_status_v2::Status;
    match invocation_status.inner.status() {
        Status::Scheduled => {
            row.status("scheduled");
            if row.is_created_using_restate_version_defined() {
                row.created_using_restate_version(
                    invocation_status.created_using_restate_version()?,
                );
            }
            if row.is_scheduled_at_defined()
                && let Some(execution_time) = invocation_status.inner.execution_time
            {
                row.scheduled_start_at(execution_time as i64)
            }
            if row.is_completion_retention_defined() {
                row.completion_retention(
                    invocation_status
                        .completion_retention_duration()?
                        .as_millis() as i64,
                );
            }
            if row.is_journal_retention_defined() {
                row.journal_retention(
                    invocation_status.journal_retention_duration()?.as_millis() as i64
                );
            }
        }
        Status::Inboxed => {
            row.status("inboxed");
            if row.is_created_using_restate_version_defined() {
                row.created_using_restate_version(
                    invocation_status.created_using_restate_version()?,
                );
            }
            if row.is_scheduled_at_defined()
                && let Some(execution_time) = invocation_status.inner.execution_time
            {
                row.scheduled_start_at(execution_time as i64)
            }
            if row.is_completion_retention_defined() {
                row.completion_retention(
                    invocation_status
                        .completion_retention_duration()?
                        .as_millis() as i64,
                );
            }
            if row.is_journal_retention_defined() {
                row.journal_retention(
                    invocation_status.journal_retention_duration()?.as_millis() as i64
                );
            }
        }
        Status::Invoked => {
            row.status("invoked");
            fill_journal_metadata(&mut row, &invocation_status)?;
            fill_in_flight_invocation_metadata(&mut row, &invocation_status)?;
        }
        Status::Paused => {
            row.status("paused");
            fill_journal_metadata(&mut row, &invocation_status)?;
            fill_in_flight_invocation_metadata(&mut row, &invocation_status)?;
        }
        Status::Suspended => {
            row.status("suspended");
            fill_journal_metadata(&mut row, &invocation_status)?;
            fill_in_flight_invocation_metadata(&mut row, &invocation_status)?;
        }
        Status::Completed => {
            row.status("completed");
            fill_journal_metadata(&mut row, &invocation_status)?;

            if row.is_pinned_deployment_id_defined()
                && let Some(deployment_id) = invocation_status.deployment_id()?
            {
                row.fmt_pinned_deployment_id(deployment_id);
            }
            if row.is_pinned_service_protocol_version_defined()
                && let Some(service_protocol_version) =
                    invocation_status.service_protocol_version()?
            {
                row.pinned_service_protocol_version(
                    service_protocol_version.as_repr().unsigned_abs(),
                );
            }

            if row.is_created_using_restate_version_defined() {
                row.created_using_restate_version(
                    invocation_status.created_using_restate_version()?,
                );
            }
            if row.is_scheduled_at_defined()
                && let Some(execution_time) = invocation_status.inner.execution_time
            {
                row.scheduled_start_at(execution_time as i64)
            }
            if row.is_completion_retention_defined() {
                row.completion_retention(
                    invocation_status
                        .completion_retention_duration()?
                        .as_millis() as i64,
                );
            }
            if row.is_journal_retention_defined() {
                row.journal_retention(
                    invocation_status.journal_retention_duration()?.as_millis() as i64
                );
            }

            if row.is_completion_result_defined() || row.is_completion_failure_defined() {
                use restate_storage_api::protobuf_types::v1::response_result::ResponseResult;
                match invocation_status.response_result()? {
                    ResponseResult::ResponseSuccess(_) => {
                        row.completion_result("success");
                    }
                    ResponseResult::ResponseFailure(
                        restate_storage_api::protobuf_types::v1::response_result::ResponseFailure {
                            failure_code,
                            failure_message,
                            ..
                        },
                    ) => {
                        row.completion_result("failure");
                        if row.is_completion_failure_defined() {
                            let message = str::from_utf8(failure_message.as_ref())
                                .map_err(|_| ConversionError::invalid_data("failure_message"))?;
                            row.fmt_completion_failure(format_args!(
                                "[{}] {}",
                                failure_code, message,
                            ));
                        }
                    }
                }
            }
        }
        Status::UnknownStatus => return Err(ConversionError::invalid_data("status")),
    };

    Ok(())
}

fn fill_in_flight_invocation_metadata(
    row: &mut SysInvocationStatusRowBuilder,
    status: &InvocationStatusV2Lazy,
) -> Result<(), ConversionError> {
    if row.is_created_using_restate_version_defined() {
        row.created_using_restate_version(status.created_using_restate_version()?);
    }
    // journal_metadata and stats are filled by other functions
    if row.is_pinned_deployment_id_defined()
        && let Some(deployment_id) = status.deployment_id()?
    {
        row.fmt_pinned_deployment_id(deployment_id);
    }
    if row.is_pinned_service_protocol_version_defined()
        && let Some(service_protocol_version) = status.service_protocol_version()?
    {
        row.pinned_service_protocol_version(service_protocol_version.as_repr().unsigned_abs());
    }
    if row.is_scheduled_start_at_defined()
        && let Some(execution_time) = status.inner.execution_time
    {
        row.scheduled_start_at(execution_time as i64)
    }
    if row.is_completion_retention_defined() {
        row.completion_retention(status.completion_retention_duration()?.as_millis() as i64);
    }
    if row.is_journal_retention_defined() {
        row.journal_retention(status.journal_retention_duration()?.as_millis() as i64);
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
    source: Source,
) -> Result<(), ConversionError> {
    match source {
        Source::Service(service) => {
            row.invoked_by("service");
            if row.is_invoked_by_service_name_defined() || row.is_invoked_by_target_defined() {
                let invocation_target = service.invocation_target()?;

                if row.is_invoked_by_service_name_defined() {
                    row.invoked_by_service_name(invocation_target.service_name()?);
                }

                if row.is_invoked_by_target_defined() {
                    row.fmt_invoked_by_target(invocation_target.target_fmt()?);
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
                row.fmt_invoked_by_subscription_id(subscription.subscription_id()?)
            }
        }
        Source::RestartAsNew(restart_as_new) => {
            row.invoked_by("restart_as_new");
            if row.is_restarted_from_defined() {
                row.fmt_restarted_from(restart_as_new.invocation_id()?)
            }
        }
    }

    Ok(())
}

#[inline]
fn fill_timestamps(row: &mut SysInvocationStatusRowBuilder, status: &InvocationStatusV2Lazy) {
    row.created_at(status.inner.creation_time as i64);
    row.modified_at(status.inner.modification_time as i64);
    if let Some(inboxed_at) = status.inner.inboxed_transition_time {
        row.inboxed_at(inboxed_at as i64);
    }
    if let Some(scheduled_at) = status.inner.scheduled_transition_time {
        row.scheduled_at(scheduled_at as i64);
    }
    if let Some(running_at) = status.inner.running_transition_time {
        row.running_at(running_at as i64);
    }
    if let Some(completed_at) = status.inner.completed_transition_time {
        row.completed_at(completed_at as i64);
    }
}

#[inline]
fn fill_journal_metadata(
    row: &mut SysInvocationStatusRowBuilder,
    status: &InvocationStatusV2Lazy,
) -> Result<(), ConversionError> {
    if row.is_trace_id_defined() {
        let tid = status.trace_id()?;
        if tid != TraceId::INVALID {
            row.fmt_trace_id(tid);
        }
    }
    if row.is_journal_size_defined() {
        row.journal_size(status.inner.journal_length);
    }
    if row.is_journal_commands_size_defined() {
        row.journal_commands_size(status.inner.commands);
    }

    Ok(())
}
