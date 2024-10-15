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

define_table!(sys_deployment(
    /// The ID of the service deployment.
    id: DataType::LargeUtf8,

    /// The type of the endpoint. Either `http` or `lambda`.
    ty: DataType::LargeUtf8,

    /// The address of the endpoint. Either HTTP URL or Lambda ARN.
    endpoint: DataType::LargeUtf8,

    /// Timestamp indicating the deployment registration time.
    created_at: DataType::Date64,

    /// Minimum supported protocol version.
    min_service_protocol_version: DataType::UInt32,

    /// Maximum supported protocol version.
    max_service_protocol_version: DataType::UInt32
));
