// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::state::schema::StateBuilder;
use bytes::Bytes;
use restate_types::identifiers::{ServiceId, WithPartitionKey};

#[inline]
pub(crate) fn append_state_row(
    builder: &mut StateBuilder,
    service_id: ServiceId,
    state_key: Bytes,
    state_value: Bytes,
) {
    let mut row = builder.row();
    row.partition_key(service_id.partition_key());
    row.service_name(&service_id.service_name);
    row.service_key(&service_id.key);
    if row.is_key_defined()
        && let Ok(str) = std::str::from_utf8(&state_key)
    {
        row.key(str);
    }
    if row.is_value_utf8_defined()
        && let Ok(str) = std::str::from_utf8(&state_value)
    {
        row.value_utf8(str);
    }
    if row.is_value_defined() {
        row.value(&state_value);
    }
}
