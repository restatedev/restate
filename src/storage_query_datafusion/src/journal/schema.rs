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

define_table!(journal(
    partition_key: DataType::UInt64,
    service: DataType::LargeUtf8,
    service_key: DataType::LargeBinary,
    service_key_utf8: DataType::LargeUtf8,
    service_key_int32: DataType::Int32,
    service_key_uuid: DataType::LargeUtf8,
    service_key_json: DataType::LargeUtf8,
    index: DataType::UInt32,
    entry_type: DataType::LargeUtf8,
    completed: DataType::Boolean,
    invoked_id: DataType::LargeUtf8,
    invoked_service: DataType::LargeUtf8,
    invoked_method: DataType::LargeUtf8,
    invoked_service_key: DataType::LargeBinary,
    invoked_service_key_json: DataType::LargeUtf8,
));
