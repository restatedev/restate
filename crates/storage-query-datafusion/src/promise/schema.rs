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

define_sort_order!(sys_promise(partition_key, service_name, service_key));

define_table!(sys_promise(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// The name of the workflow service.
    service_name: DataType::LargeUtf8,

    /// The workflow ID.
    service_key: DataType::LargeUtf8,

    /// The promise key.
    key: DataType::LargeUtf8,

    /// True if the promise was completed.
    completed: DataType::Boolean,

    /// The completion success, if any.
    completion_success_value: DataType::LargeBinary,

    /// The completion success as UTF-8 string, if any.
    completion_success_value_utf8: DataType::LargeUtf8,

    /// The completion failure, if any.
    completion_failure: DataType::LargeUtf8,
));
