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
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// The name for the invoked service.
    service_name: DataType::LargeUtf8,

    /// The key of the Virtual Object or of the Workflow. Null for regular services.
    service_key: DataType::LargeUtf8,

    /// The invoked handler.
    service_handler: DataType::LargeUtf8,

    /// The user provided idempotency key.
    idempotency_key: DataType::LargeUtf8,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    invocation_id: DataType::LargeUtf8
));
