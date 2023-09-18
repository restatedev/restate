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

use datafusion::arrow::datatypes::DataType;

use crate::table_macro::*;

define_table!(service(
        name: DataType::LargeUtf8,
        methods: ExtendedDataType::Utf8List,
        service_type: DataType::LargeUtf8,
        endpoint_id: DataType::LargeUtf8,
        revision: DataType::UInt32,
        public: DataType::Boolean,
        descriptor_pool: DataType::LargeBinary,
));
