// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Registered services.
    ///
    /// One row per service, with its type, whether it is public, and the latest deployment serving it.
    service {
        name: "sys_service",
        sharding: ShardingScheme::Unsharded(None),
        columns {
            /// The name of the registered user service.
            #[starts_with]
            name: ReString => DataType::LargeUtf8,
            /// The latest deployed revision.
            revision: u64 => DataType::UInt64,
            /// Whether the service is accessible through the ingress endpoint or not.
            public: bool => DataType::Boolean,
            /// The service type. Either `service` or `virtual_object` or `workflow`.
            #[values(["service", "virtual_object", "workflow"].map(ReString::from_static))]
            ty: ReString => DataType::LargeUtf8,
            /// The ID of the latest deployment
            deployment_id: ReString => DataType::LargeUtf8,
        }
    }
}
