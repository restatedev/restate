// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::invocation_state::schema::StateBuilder;
use crate::table_util::format_using;
use restate_invoker_api::InvocationStatusReport;
use restate_types::identifiers::WithPartitionKey;
use restate_types::time::MillisSinceEpoch;

#[inline]
pub(crate) fn append_state_row(
    builder: &mut StateBuilder,
    output: &mut String,
    status_row: InvocationStatusReport,
) {
    let mut row = builder.row();

    let invocation_id = status_row.invocation_id();

    row.partition_key(invocation_id.partition_key());
    if row.is_id_defined() {
        row.id(format_using(output, &invocation_id));
    }
    row.in_flight(status_row.in_flight());
    row.retry_count(status_row.retry_count() as u64);
    row.last_start_at(MillisSinceEpoch::as_u64(&status_row.last_start_at().into()) as i64);
    if let Some(last_attempt_deployment_id) = status_row.last_attempt_deployment_id() {
        row.last_attempt_deployment_id(last_attempt_deployment_id.to_string());
    }
    if let Some(last_attempt_server) = status_row.last_attempt_server() {
        row.last_attempt_server(last_attempt_server);
    }

    if let Some(next_retry_at) = status_row.next_retry_at() {
        row.next_retry_at(MillisSinceEpoch::as_u64(&next_retry_at.into()) as i64);
    }
    if let Some(last_retry_attempt_failure) = status_row.last_retry_attempt_failure() {
        row.last_failure(format_using(output, &last_retry_attempt_failure.err));
        if let Some(doc_error_code) = last_retry_attempt_failure.doc_error_code {
            row.last_failure_error_code(doc_error_code.code())
        }
        if let Some(name) = &last_retry_attempt_failure.related_entry_name {
            if !name.is_empty() {
                row.last_failure_related_entry_name(name);
            }
        }
        if let Some(idx) = last_retry_attempt_failure.related_entry_index {
            row.last_failure_related_entry_index(idx as u64);
        }

        if row.is_last_failure_related_entry_type_defined() {
            if let Some(related_entry_type) = &last_retry_attempt_failure.related_entry_type {
                row.last_failure_related_entry_type(format_using(output, related_entry_type));
            }
        }
    }
}
