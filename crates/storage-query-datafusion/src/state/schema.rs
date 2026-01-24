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

define_sort_order!(state(partition_key, service_name, service_key));

define_table!(state(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// The name of the invoked service.
    service_name: DataType::LargeUtf8,

    /// The key of the Virtual Object.
    service_key: DataType::LargeUtf8,

    /// The `utf8` state key.
    key: DataType::LargeUtf8,

    /// The byte length of the state key. If you are writing a query that only needs to know the length,
    /// reading this field will be much more efficient than reading octet_length(key).
    key_length: DataType::UInt64,

    /// Only contains meaningful values when a service stores state as `utf8`. This is the case for
    /// services that serialize state using JSON (default for Typescript SDK, Java/Kotlin SDK if
    /// using JsonSerdes).
    value_utf8: DataType::LargeUtf8,

    /// A binary, uninterpreted representation of the value. You can use the more specific column
    /// `value_utf8` if the value is a string.
    value: DataType::LargeBinary,

    /// The byte length of the value. If you are writing a query that only needs to know the length,
    /// reading this field will be much more efficient than reading length(value).
    value_length: DataType::UInt64,
));
