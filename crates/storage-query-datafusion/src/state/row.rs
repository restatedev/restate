// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::state::schema::StateBuilder;
use restate_partition_store::state_table::OwnedStateRow;

#[inline]
pub(crate) fn append_state_row(builder: &mut StateBuilder, state_row: OwnedStateRow) {
    let OwnedStateRow {
        partition_key,
        service,
        service_key,
        state_key,
        state_value,
    } = state_row;

    let mut row = builder.row();
    row.partition_key(partition_key);
    row.service_name(&service);
    row.service_key(&service_key);
    if row.is_key_defined() {
        if let Ok(str) = std::str::from_utf8(&state_key) {
            row.key(str);
        }
    }
    if row.is_value_utf8_defined() {
        if let Ok(str) = std::str::from_utf8(&state_value) {
            row.value_utf8(str);
        }
    }
    row.value(&state_value);
}
