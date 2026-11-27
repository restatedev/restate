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

define_sort_order!(sys_hot_keys(partition_key));

define_table!(
    /// Partition keys that were hot in the last completed hot key window of their partition.
    ///
    /// One row per partition key that reached `worker.hot-key-threshold` actions within the window. Empty unless `worker.hot-key-threshold` is set.
    sys_hot_keys(
    /// The hot partition key.
    partition_key: DataType::UInt64,

    /// Actions the partition key received within the window.
    actions: DataType::UInt64,

    /// Start of the window.
    window_start: TimestampMillisecond,

    /// End of the window.
    window_end: TimestampMillisecond,
));
