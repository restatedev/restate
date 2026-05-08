// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_limiter::Level;
use restate_worker_api::UserLimitCounterEntry;

use super::schema::SysUserLimitsBuilder;

#[inline]
pub(super) fn append_user_limit_row(
    builder: &mut SysUserLimitsBuilder,
    entry: &UserLimitCounterEntry,
) {
    let mut row = builder.row();

    row.partition_key(entry.partition_key);

    if row.is_scope_defined() {
        row.scope(&entry.scope);
    }
    if row.is_l1_defined()
        && let Some(l1) = &entry.l1
    {
        row.l1(l1);
    }
    if row.is_l2_defined()
        && let Some(l2) = &entry.l2
    {
        row.l2(l2);
    }
    if row.is_level_defined() {
        row.level(level_name(entry.level));
    }
    if row.is_usage_defined() {
        row.usage(entry.usage);
    }
    if row.is_concurrency_limit_defined()
        && let Some(limit) = entry.concurrency_limit
    {
        row.concurrency_limit(limit);
    }
    if row.is_rule_pattern_defined()
        && let Some(pattern) = &entry.rule_pattern
    {
        row.rule_pattern(pattern);
    }
    if row.is_available_defined()
        && let Some(limit) = entry.concurrency_limit
    {
        row.available(limit.saturating_sub(entry.usage));
    }
    if row.is_num_waiters_defined() {
        row.num_waiters(entry.num_waiters);
    }
}

fn level_name(level: Level) -> &'static str {
    match level {
        Level::Scope => "Scope",
        Level::Level1 => "Level1",
        Level::Level2 => "Level2",
    }
}
