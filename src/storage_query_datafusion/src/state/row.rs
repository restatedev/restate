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
use crate::udfs::restate_keys;
use restate_schema_api::key::RestateKeyConverter;
use restate_storage_rocksdb::state_table::OwnedStateRow;
use uuid::Uuid;

#[inline]
pub(crate) fn append_state_row(
    builder: &mut StateBuilder,
    state_row: OwnedStateRow,
    resolver: impl RestateKeyConverter,
) {
    let OwnedStateRow {
        partition_key,
        service,
        service_key,
        state_key,
        state_value,
    } = state_row;

    let mut row = builder.row();
    row.partition_key(partition_key);
    row.service(&service);
    row.service_key(&service_key);
    if row.is_service_key_utf8_defined() {
        if let Some(utf8) = restate_keys::try_decode_restate_key_as_utf8(&service_key) {
            row.service_key_utf8(utf8);
        }
    }
    if row.is_service_key_int32_defined() {
        if let Some(key) = restate_keys::try_decode_restate_key_as_int32(&service_key) {
            row.service_key_int32(key);
        }
    }
    if row.is_service_key_uuid_defined() {
        let mut buffer = Uuid::encode_buffer();
        if let Some(key) = restate_keys::try_decode_restate_key_as_uuid(&service_key, &mut buffer) {
            row.service_key_uuid(key);
        }
    }
    if row.is_service_key_json_defined() {
        if let Some(key) =
            restate_keys::try_decode_restate_key_as_json(&service, &service_key, resolver)
        {
            row.service_key_json(key);
        }
    }
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
