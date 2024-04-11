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

define_table!(idempotency(
    partition_key: DataType::UInt64,

    component_name: DataType::LargeUtf8,
    component_key: DataType::LargeUtf8,
    component_handler: DataType::LargeUtf8,
    idempotency_key: DataType::LargeUtf8,

    invocation_id: DataType::LargeUtf8
));
