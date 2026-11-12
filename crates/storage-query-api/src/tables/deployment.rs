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

use restate_types::time::MillisSinceEpoch;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Registered deployments.
    ///
    /// A deployment is a version of your service(s) code that Restate calls.
    deployment {
        name: "sys_deployment",
        sharding: ShardingScheme::Unsharded(None),
        columns {
            /// The ID of the service deployment.
            id: ReString => DataType::LargeUtf8,
            /// The type of the endpoint. Either `http` or `lambda`.
            ty: ReString => DataType::LargeUtf8,
            /// The address of the endpoint. Either HTTP URL or Lambda ARN.
            endpoint: ReString => DataType::LargeUtf8,
            /// Timestamp indicating the deployment registration time.
            created_at: MillisSinceEpoch => TimestampMillisecond,
            /// Minimum supported protocol version.
            min_service_protocol_version: u32 => DataType::UInt32,
            /// Maximum supported protocol version.
            max_service_protocol_version: u32 => DataType::UInt32,
            /// List of service names registered by this deployment.
            services: Vec<Option<ReString>> => LargeUtf8List,
        }
    }
}
