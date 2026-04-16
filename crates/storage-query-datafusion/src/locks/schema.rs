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

define_sort_order!(sys_locks(partition_key, scope, lock_name));

define_table!(sys_locks(
    /// Internal column that is used for partitioning. Can be ignored.
    partition_key: DataType::UInt64,

    /// The scope of this lock. Only present if this is a scoped lock (i.e. a lock of a scoped virtual
    /// object).
    scope: DataType::Utf8,

    /// The name of the lock (in the format of `service/key`)
    lock_name: DataType::LargeUtf8,

    /// Timestamp of lock acquisition
    acquired_at: TimestampMillisecond,

    /// The invocation (or other operation) that acquired this lock.
    acquired_by: DataType::LargeUtf8,
));
