// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_query_api::tables::vqueue_entry_status::VqueueEntryStatus;

crate::tables::table_adapter!(pub(crate) SysVqueueEntryStatusTable, SysVqueueEntryStatusBuilder, VqueueEntryStatus);

impl crate::sharding::PartitionSource for SysVqueueEntryStatusTable {
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> = Some(super::read::select_primary);
    const INDEXES: &'static [crate::access::IndexAccess<Self::Schema>] =
        &[crate::access::IndexAccess::of::<super::read::StageIndex>()];
    const ORDERING: &'static [restate_storage_query_api::tables::vqueue_entry_status::VqueueEntryStatusColumn] = &[
        restate_storage_query_api::tables::vqueue_entry_status::VqueueEntryStatusColumn::PartitionKey,
    ];
}
