// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::table_macro::*;

use datafusion::arrow::datatypes::DataType;

define_sort_order!(sys_idempotency(partition_key, service_name, service_key));

define_table!(sys_idempotency(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// The name of the invoked service.
    service_name: DataType::LargeUtf8,

    /// The key of the virtual object or the workflow ID. Null for regular services.
    service_key: DataType::LargeUtf8,

    /// The invoked handler.
    service_handler: DataType::LargeUtf8,

    /// The user provided idempotency key.
    idempotency_key: DataType::LargeUtf8,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    invocation_id: DataType::LargeUtf8
));
