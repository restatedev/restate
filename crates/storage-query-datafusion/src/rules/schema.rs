// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::table_macro::*;

use datafusion::arrow::datatypes::DataType;

define_table!(sys_rules(
    /// Server-derived rule id (e.g. `rul_…`).
    id: DataType::Utf8,

    /// Rule pattern in canonical display form (e.g. `scope/*/tenant`).
    pattern: DataType::Utf8,

    /// Concurrent-action limit imposed by this rule. Null means the
    /// rule does not constrain action concurrency.
    action_concurrency: DataType::UInt32,

    /// Free-form description set by the operator.
    reason: DataType::Utf8,

    /// True when the rule is parked (treated as absent at runtime).
    disabled: DataType::Boolean,

    /// Per-rule version, bumped on runtime-relevant changes.
    version: DataType::UInt32,

    /// Last modification time.
    last_modified: TimestampMillisecond
));
