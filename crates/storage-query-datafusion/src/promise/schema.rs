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

define_table!(promise(
    partition_key: DataType::UInt64,

    service_name: DataType::LargeUtf8,
    service_key: DataType::LargeUtf8,

    key: DataType::LargeUtf8,
    completed: DataType::Boolean,

    completion_success_value: DataType::LargeBinary,
    completion_success_value_utf8: DataType::LargeUtf8,
    completion_failure: DataType::LargeUtf8
));
