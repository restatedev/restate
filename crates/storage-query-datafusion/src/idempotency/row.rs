// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::SysIdempotencyBuilder;

use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_types::identifiers::{IdempotencyId, WithPartitionKey};

#[inline]
pub(crate) fn append_idempotency_row(
    builder: &mut SysIdempotencyBuilder,
    idempotency_id: IdempotencyId,
    idempotency_metadata: IdempotencyMetadata,
) {
    let mut row = builder.row();
    row.partition_key(idempotency_id.partition_key());

    row.service_name(&idempotency_id.service_name);
    if row.is_service_key_defined()
        && let Some(ref k) = idempotency_id.service_key
    {
        row.service_key(k)
    }
    row.service_handler(&idempotency_id.service_handler);
    row.idempotency_key(&idempotency_id.idempotency_key);

    if row.is_invocation_id_defined() {
        row.fmt_invocation_id(idempotency_metadata.invocation_id);
    }
}
