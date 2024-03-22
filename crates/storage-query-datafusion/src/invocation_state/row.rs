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
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::time::MillisSinceEpoch;

#[inline]
pub(crate) fn append_state_row(
    builder: &mut StateBuilder,
    output: &mut String,
    status_row: InvocationStatusReport,
) {
    let mut row = builder.row();

    let invocation_id = status_row.full_invocation_id();

    row.partition_key(invocation_id.service_id.partition_key());
    row.component(&invocation_id.service_id.service_name);
    row.component_key(
        std::str::from_utf8(&invocation_id.service_id.key).expect("The key must be a string!"),
    );
    if row.is_id_defined() {
        row.id(format_using(output, &InvocationId::from(invocation_id)));
    }
    row.in_flight(status_row.in_flight());
    row.retry_count(status_row.retry_count() as u64);
    row.last_start_at(MillisSinceEpoch::as_u64(&status_row.last_start_at().into()) as i64);
    if let Some(last_attempt_deployment_id) = status_row.last_attempt_deployment_id() {
        row.last_attempt_deployment_id(last_attempt_deployment_id.to_string());
    }

    if let Some(next_retry_at) = status_row.next_retry_at() {
        row.next_retry_at(MillisSinceEpoch::as_u64(&next_retry_at.into()) as i64);
    }
    if let Some(last_retry_attempt_failure) = status_row.last_retry_attempt_failure() {
        row.last_failure(format_using(
            output,
            &last_retry_attempt_failure.display_err(),
        ));
        if let Some(doc_error_code) = last_retry_attempt_failure.doc_error_code() {
            row.last_error_code(doc_error_code.code())
        }
    }
}
