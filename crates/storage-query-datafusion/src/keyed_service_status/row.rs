// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keyed_service_status::schema::KeyedServiceStatusBuilder;
use crate::table_util::format_using;
use restate_partition_store::service_status_table::OwnedVirtualObjectStatusRow;
use restate_storage_api::service_status_table::VirtualObjectStatus;

#[inline]
pub(crate) fn append_virtual_object_status_row(
    builder: &mut KeyedServiceStatusBuilder,
    output: &mut String,
    status_row: OwnedVirtualObjectStatusRow,
) {
    let mut row = builder.row();

    row.partition_key(status_row.partition_key);
    row.service_name(&status_row.name);
    row.service_key(&status_row.key);

    // Invocation id
    if row.is_invocation_id_defined() {
        if let VirtualObjectStatus::Locked(invocation_id) = status_row.status {
            row.invocation_id(format_using(output, &invocation_id));
        }
    }
}
