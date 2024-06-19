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

define_table!(sys_keyed_service_status(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// The name for the invoked virtual object/workflow.
    service_name: DataType::LargeUtf8,

    /// The key of the virtual object/workflow.
    service_key: DataType::LargeUtf8,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    invocation_id: DataType::LargeUtf8,
));
