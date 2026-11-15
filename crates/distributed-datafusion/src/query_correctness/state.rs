// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Independent reference schema for the row and broad-primary-scan SQL oracles.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

pub(super) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition_key", DataType::UInt64, true),
        Field::new("scope", DataType::LargeUtf8, true),
        Field::new("service_name", DataType::LargeUtf8, true),
        Field::new("service_key", DataType::LargeUtf8, true),
        Field::new("key", DataType::LargeUtf8, true),
        Field::new("key_length", DataType::UInt64, true),
        Field::new("value_utf8", DataType::LargeUtf8, true),
        Field::new("value", DataType::LargeBinary, true),
        Field::new("value_length", DataType::UInt64, true),
    ]))
}
