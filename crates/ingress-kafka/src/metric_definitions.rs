// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{describe_counter, Unit};

pub const KAFKA_INGRESS_REQUESTS: &str = "restate.kafka_ingress.requests.total";

pub(crate) fn describe_metrics() {
    describe_counter!(
        KAFKA_INGRESS_REQUESTS,
        Unit::Count,
        "Number of Kafka ingress requests"
    );
}
