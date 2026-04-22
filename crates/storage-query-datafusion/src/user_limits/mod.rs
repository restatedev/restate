// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[allow(dead_code)]
mod row;
#[allow(dead_code)]
pub(crate) mod schema;

// TODO: table.rs with register_self() — requires plumbing a handle from the
// scheduler's UserLimiter to the DataFusion context, similar to the StatusHandle
// pattern used by invocation_state/table.rs.
//
// The scan should:
// 1. Call UserLimiter::scan_counters() to get CounterView entries
// 2. For each entry, resolve the rule handle via UserLimiter::resolve_rule()
// 3. Build UserLimitRow and append to the table builder
