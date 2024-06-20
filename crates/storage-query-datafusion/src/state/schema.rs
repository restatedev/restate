// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use crate::table_macro::*;

use datafusion::arrow::datatypes::DataType;

define_table!(state(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// The name of the invoked service.
    service_name: DataType::LargeUtf8,

    /// The key of the Virtual Object.
    service_key: DataType::LargeUtf8,

    /// The `utf8` state key.
    key: DataType::LargeUtf8,

    /// Only contains meaningful values when a service stores state as `utf8`. This is the case for
    /// services that serialize state using JSON (default for Typescript SDK, Java/Kotlin SDK if
    /// using JsonSerdes).
    value_utf8: DataType::LargeUtf8,

    /// A binary, uninterpreted representation of the value. You can use the more specific column
    /// `value_utf8` if the value is a string.
    value: DataType::LargeBinary,
));
