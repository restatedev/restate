// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::SysRulesBuilder;
use restate_limiter::{PersistedRule, RulePattern};
use restate_util_string::ReString;

#[inline]
pub(crate) fn append_rule_row(
    builder: &mut SysRulesBuilder,
    pattern: &RulePattern<ReString>,
    rule: &PersistedRule,
) {
    let mut row = builder.row();
    row.fmt_pattern(pattern);
    if let Some(concurrency) = rule.limits.action_concurrency {
        row.action_concurrency(concurrency.get());
    }
    if let Some(description) = rule.description.as_deref() {
        row.description(description);
    }
    row.disabled(rule.disabled);
    row.version(rule.version.into());
    if let Ok(last_modified) = i64::try_from(rule.last_modified.as_u64()) {
        row.last_modified(last_modified);
    }
}
