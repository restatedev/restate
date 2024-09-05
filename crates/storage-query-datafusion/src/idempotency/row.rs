// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::SysIdempotencyBuilder;

use crate::log_data_corruption_error;
use crate::table_util::format_using;
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_types::identifiers::{IdempotencyId, WithPartitionKey};

#[inline]
pub(crate) fn append_idempotency_row(
    builder: &mut SysIdempotencyBuilder,
    output: &mut String,
    idempotency_id: IdempotencyId,
    idempotency_metadata: IdempotencyMetadata,
) {
    let mut row = builder.row();
    row.partition_key(idempotency_id.partition_key());

    row.service_name(&idempotency_id.service_name);
    if row.is_service_key_defined() {
        if let Some(ref k) = idempotency_id.service_key {
            match std::str::from_utf8(k) {
                Ok(val) => row.service_key(val),
                Err(e) => {
                    log_data_corruption_error!("sys_idempotency", idempotency_id, "service_key", e)
                }
            }
        }
    }
    row.service_handler(&idempotency_id.service_handler);
    row.idempotency_key(&idempotency_id.idempotency_key);

    if row.is_invocation_id_defined() {
        row.invocation_id(format_using(output, &idempotency_metadata.invocation_id));
    }
}
