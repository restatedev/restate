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
use crate::udfs::restate_keys;
use restate_invoker_api::InvocationStatusReport;
use restate_schema_api::key::RestateKeyConverter;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::time::MillisSinceEpoch;
use std::fmt;
use std::fmt::Write;
use uuid::Uuid;

#[inline]
pub(crate) fn append_state_row(
    builder: &mut StateBuilder,
    output: &mut String,
    status_row: InvocationStatusReport,
    resolver: impl RestateKeyConverter,
) {
    let mut row = builder.row();

    let invocation_id = status_row.full_invocation_id();

    row.partition_key(invocation_id.service_id.partition_key());
    row.service(&invocation_id.service_id.service_name);
    row.service_key(&invocation_id.service_id.key);
    if row.is_service_key_utf8_defined() {
        if let Some(utf8) =
            restate_keys::try_decode_restate_key_as_utf8(&invocation_id.service_id.key)
        {
            row.service_key_utf8(utf8);
        }
    }
    if row.is_service_key_int32_defined() {
        if let Some(key) =
            restate_keys::try_decode_restate_key_as_int32(&invocation_id.service_id.key)
        {
            row.service_key_int32(key);
        }
    }
    if row.is_service_key_uuid_defined() {
        let mut buffer = Uuid::encode_buffer();
        if let Some(key) =
            restate_keys::try_decode_restate_key_as_uuid(&invocation_id.service_id.key, &mut buffer)
        {
            row.service_key_uuid(key);
        }
    }
    if row.is_service_key_json_defined() {
        if let Some(key) = restate_keys::try_decode_restate_key_as_json(
            &invocation_id.service_id.service_name,
            &invocation_id.service_id.key,
            output,
            resolver,
        ) {
            row.service_key_json(key);
        }
    }
    if row.is_id_defined() {
        row.id(format_using(
            output,
            &InvocationId::new(invocation_id.partition_key(), invocation_id.invocation_uuid),
        ));
    }
    row.in_flight(status_row.in_flight());
    row.retry_count(status_row.retry_count() as u64);
    row.last_start_at(MillisSinceEpoch::as_u64(&status_row.last_start_at().into()) as i64);
    if let Some(last_retry_attempt_failure) = status_row.last_retry_attempt_failure() {
        row.last_failure(format_using(
            output,
            &last_retry_attempt_failure.display_err(),
        ))
    }
}

#[inline]
fn format_using<'a>(output: &'a mut String, what: &impl fmt::Display) -> &'a str {
    output.clear();
    write!(output, "{}", what).expect("Error occurred while trying to write in String");
    output
}
