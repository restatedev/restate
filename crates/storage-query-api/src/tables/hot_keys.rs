// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_types::identifiers::PartitionKey;
use restate_types::time::MillisSinceEpoch;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Partition keys that were hot in the last completed hot key window of their partition.
    ///
    /// One row per partition key that reached `worker.hot-key-threshold` actions within the window. Empty unless `worker.hot-key-threshold` is set.
    hot_keys {
        name: "sys_hot_keys",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: true },
        sharding_columns { partition_key: PartitionKey },
        columns {
            /// The hot partition key.
            partition_key: PartitionKey => DataType::UInt64,
            /// Actions the partition key received within the window.
            actions: u64 => DataType::UInt64,
            /// Start of the window.
            window_start: MillisSinceEpoch => TimestampMillisecond,
            /// End of the window.
            window_end: MillisSinceEpoch => TimestampMillisecond,
        }
    }
}
