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

define_table!(inbox(
    partition_key: DataType::UInt64,

    service_name: DataType::LargeUtf8,
    service_key: DataType::LargeUtf8,

    id: DataType::LargeUtf8,

    sequence_number: DataType::UInt64,

    created_at: DataType::Date64,
));
