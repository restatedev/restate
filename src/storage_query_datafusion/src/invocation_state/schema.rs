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
    partition_key: DataType::UInt64,
    service: DataType::LargeUtf8,
    service_key: DataType::LargeBinary,
    service_key_utf8: DataType::LargeUtf8,
    service_key_int32: DataType::Int32,
    service_key_uuid: DataType::LargeUtf8,
    service_key_json: DataType::LargeUtf8,
    id: DataType::LargeUtf8,
    in_flight: DataType::Boolean,
    retry_count: DataType::UInt64,
    last_start_at: DataType::Date64,
    last_failure: DataType::LargeUtf8,
));
