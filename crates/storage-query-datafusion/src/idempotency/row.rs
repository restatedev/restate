// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::IdempotencyBuilder;

use crate::table_util::format_using;
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_types::identifiers::{IdempotencyId, WithPartitionKey};

#[inline]
pub(crate) fn append_idempotency_row(
    builder: &mut IdempotencyBuilder,
    output: &mut String,
    idempotency_id: IdempotencyId,
    idempotency_metadata: IdempotencyMetadata,
) {
    let mut row = builder.row();
    row.partition_key(idempotency_id.partition_key());

    row.component_name(&idempotency_id.component_name);
    if row.is_component_key_defined() {
        if let Some(k) = idempotency_id.component_key {
            row.component_key(std::str::from_utf8(&k).expect("The key must be a string!"));
        }
    }
    row.component_handler(&idempotency_id.component_handler);
    row.idempotency_key(&idempotency_id.idempotency_key);

    if row.is_invocation_id_defined() {
        row.invocation_id(format_using(output, &idempotency_metadata.invocation_id));
    }
}
