// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keyed_service_status::schema::SysKeyedServiceStatusBuilder;
use restate_storage_api::service_status_table::VirtualObjectStatus;
use restate_types::identifiers::{ServiceId, WithPartitionKey};

#[inline]
pub(crate) fn append_virtual_object_status_row(
    builder: &mut SysKeyedServiceStatusBuilder,
    service_id: ServiceId,
    status: VirtualObjectStatus,
) {
    let mut row = builder.row();

    row.partition_key(service_id.partition_key());
    row.service_name(&service_id.service_name);
    row.service_key(&service_id.key);

    // Invocation id
    if row.is_invocation_id_defined() {
        if let VirtualObjectStatus::Locked(invocation_id) = status {
            row.fmt_invocation_id(invocation_id);
        }
    }
}
