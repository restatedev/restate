// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter};

pub const INVOCATION_CLIENT_REQUESTS: &str = "restate.invocation_client.requests.total";

// Values of label `status` in INVOCATION_CLIENT_REQUESTS.
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_INTERNAL_ERROR: &str = "internal_error";
pub const STATUS_OVERLOADED_ERROR: &str = "overloaded_error";
pub const STATUS_PROTOCOL_ERROR: &str = "protocol_error";
pub const STATUS_ROUTING_ERROR: &str = "routing_error";
pub const STATUS_SHUTDOWN: &str = "shutdown";
pub const STATUS_UNAVAILABLE_ERROR: &str = "unavailable_error";

pub fn describe_metrics() {
    describe_counter!(
        INVOCATION_CLIENT_REQUESTS,
        Unit::Count,
        "Total partition processor RPC attempts made by the invocation client"
    );
}
