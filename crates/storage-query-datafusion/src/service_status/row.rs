// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::service_status::schema::ServiceStatusBuilder;
use crate::table_util::format_using;
use restate_storage_api::service_status_table::ServiceStatus;
use restate_storage_rocksdb::service_status_table::OwnedServiceStatusRow;

#[inline]
pub(crate) fn append_service_status_row(
    builder: &mut ServiceStatusBuilder,
    output: &mut String,
    status_row: OwnedServiceStatusRow,
) {
    let mut row = builder.row();

    row.partition_key(status_row.partition_key);
    row.service(&status_row.service);
    row.service_key(
        std::str::from_utf8(&status_row.service_key).expect("The key must be a string!"),
    );

    // Invocation id
    if row.is_invocation_id_defined() {
        if let ServiceStatus::Locked(invocation_id) = status_row.service_status {
            row.invocation_id(format_using(output, &invocation_id));
        }
    }
}
