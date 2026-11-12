// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_types::time::MillisSinceEpoch;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Rules that limit concurrency.
    ///
    /// Each rule matches invocations by pattern and can cap how many of them run at the same time.
    rules {
        name: "sys_rules",
        sharding: ShardingScheme::Unsharded(None),
        columns {
            /// Rule pattern in canonical display form (e.g. `scope/*/tenant`).
            pattern: ReString => DataType::LargeUtf8,
            /// Concurrency limit imposed by this rule. Null means the
            /// rule does not constrain concurrency.
            concurrency: Option<u32> => DataType::UInt32,
            /// Free-form description set by the operator.
            description: Option<ReString> => DataType::LargeUtf8,
            /// True when the rule is parked (treated as absent at runtime).
            disabled: bool => DataType::Boolean,
            /// Per-rule version, bumped on runtime-relevant changes.
            version: u32 => DataType::UInt32,
            /// Last modification time.
            last_modified: Option<MillisSinceEpoch> => TimestampMillisecond,
        }
    }
}
