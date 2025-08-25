// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::SysPromiseBuilder;

use crate::table_util::format_using;
use restate_storage_api::promise_table::{OwnedPromiseRow, PromiseResult, PromiseState};
use restate_types::errors::InvocationError;
use restate_types::identifiers::WithPartitionKey;

#[inline]
pub(crate) fn append_promise_row(
    builder: &mut SysPromiseBuilder,
    output: &mut String,
    owned_promise_row: OwnedPromiseRow,
) {
    let mut row = builder.row();
    row.partition_key(owned_promise_row.service_id.partition_key());

    row.service_name(&owned_promise_row.service_id.service_name);
    row.service_key(&owned_promise_row.service_id.key);
    row.key(&owned_promise_row.key);

    match owned_promise_row.metadata.state {
        PromiseState::Completed(c) => {
            row.completed(true);
            match c {
                PromiseResult::Success(s) => {
                    row.completion_success_value(&s);
                    if row.is_completion_success_value_utf8_defined()
                        && let Ok(str) = std::str::from_utf8(&s)
                    {
                        row.completion_success_value_utf8(str);
                    }
                }
                PromiseResult::Failure(c, m) => {
                    if row.is_completion_failure_defined() {
                        row.completion_failure(format_using(output, &InvocationError::new(c, m)))
                    }
                }
            }
        }
        PromiseState::NotCompleted(_) => {
            row.completed(false);
        }
    }
}
