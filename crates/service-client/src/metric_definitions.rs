// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge};

pub const INGRESS_REQUESTS: &str = "restate.ingress.requests.total";

pub fn describe_metrics() {
    describe_counter!(
        INGRESS_REQUESTS,
        Unit::Count,
        "Number of ingress requests in different states, see label state to classify"
    );
}
