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
use restate_types::time::MillisSinceEpoch;

/// A partition key that was hot in the last completed hot key window of its partition's leader,
/// exposed by the `sys_hot_keys` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotKeyEntry {
    pub partition_key: PartitionKey,
    /// Actions the partition key received within the window. It is at least the hot key threshold.
    pub actions: u64,
    pub window_start: MillisSinceEpoch,
    pub window_end: MillisSinceEpoch,
}
