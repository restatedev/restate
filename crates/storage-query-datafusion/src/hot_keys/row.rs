// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_worker_api::HotKeyEntry;

use super::schema::SysHotKeysBuilder;

#[inline]
pub(super) fn append_hot_key_row(builder: &mut SysHotKeysBuilder, entry: &HotKeyEntry) {
    let mut row = builder.row();

    row.partition_key(entry.partition_key);

    if row.is_actions_defined() {
        row.actions(entry.actions);
    }
    if row.is_window_start_defined() {
        row.window_start(entry.window_start.as_u64() as i64);
    }
    if row.is_window_end_defined() {
        row.window_end(entry.window_end.as_u64() as i64);
    }
}
