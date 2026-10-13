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

define_table!(sys_service(
    /// The name of the registered user service.
    name: DataType::LargeUtf8,

    /// The latest deployed revision.
    revision: DataType::UInt64,

    /// Whether the service is accessible through the ingress endpoint or not.
    public: DataType::Boolean,

    /// The service type. Either `service` or `virtual_object` or `workflow`.
    ty: DataType::LargeUtf8,

    /// The ID of the latest deployment
    deployment_id: DataType::LargeUtf8,
));
