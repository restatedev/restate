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

define_table!(service(
    name: DataType::LargeUtf8,
    revision: DataType::UInt64,

    public: DataType::Boolean,

    ty: DataType::LargeUtf8,
    deployment_id: DataType::LargeUtf8,
));
