// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::ResponseResult;

use super::schema::SysInvocationOutputBuilder;

#[inline]
pub(crate) fn append_output_row(
    builder: &mut SysInvocationOutputBuilder,
    invocation_id: InvocationId,
    output: ResponseResult,
) {
    let mut row = builder.row();
    row.partition_key(invocation_id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(invocation_id);
    }

    match output {
        ResponseResult::Success(payload) => {
            row.result("success");
            if row.is_output_defined() {
                row.output(&payload);
            }
            if row.is_output_utf8_defined()
                && let Ok(str) = std::str::from_utf8(&payload)
            {
                row.output_utf8(str);
            }
        }
        ResponseResult::Failure(error) => {
            row.result("failure");
            row.failure_code(u16::from(error.code).into());
            if row.is_failure_json_defined()
                && let Ok(json) = serde_json::to_string(&error)
            {
                row.failure_json(json);
            }
        }
    }
}
