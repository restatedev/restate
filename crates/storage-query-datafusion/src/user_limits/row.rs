// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::PartitionKey;

use super::schema::SysUserLimitsBuilder;

/// A single row of enriched counter data ready to be appended to the table.
///
/// This is produced by the scanner from a `CounterView` + rule resolution.
#[allow(dead_code)]
pub(crate) struct UserLimitRow {
    pub partition_key: PartitionKey,
    pub scope: String,
    pub l1: Option<String>,
    pub l2: Option<String>,
    pub level: &'static str,
    pub usage: u32,
    pub concurrency_limit: Option<u64>,
    pub rule_pattern: Option<String>,
    pub num_waiters: u64,
}

#[inline]
#[allow(dead_code)]
pub(crate) fn append_user_limit_row(builder: &mut SysUserLimitsBuilder, row_data: &UserLimitRow) {
    let mut row = builder.row();

    row.partition_key(row_data.partition_key);

    if row.is_scope_defined() {
        row.scope(&row_data.scope);
    }
    if row.is_l1_defined()
        && let Some(l1) = &row_data.l1
    {
        row.l1(l1);
    }
    if row.is_l2_defined()
        && let Some(l2) = &row_data.l2
    {
        row.l2(l2);
    }
    if row.is_level_defined() {
        row.level(row_data.level);
    }
    if row.is_usage_defined() {
        row.usage(row_data.usage);
    }
    if row.is_concurrency_limit_defined()
        && let Some(limit) = row_data.concurrency_limit
    {
        row.concurrency_limit(limit);
    }
    if row.is_rule_pattern_defined()
        && let Some(pattern) = &row_data.rule_pattern
    {
        row.rule_pattern(pattern);
    }
    if row.is_available_defined()
        && let Some(limit) = row_data.concurrency_limit
    {
        row.available(limit.saturating_sub(row_data.usage as u64));
    }
    if row.is_num_waiters_defined() {
        row.num_waiters(row_data.num_waiters);
    }
}
