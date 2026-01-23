// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge};

pub const KAFKA_INGRESS_REQUESTS: &str = "restate.kafka_ingress.requests.total";
pub const KAFKA_INGRESS_CONSUMER_LAG: &str = "restate.kafka_ingress.consumer.lag";

pub(crate) fn describe_metrics() {
    describe_counter!(
        KAFKA_INGRESS_REQUESTS,
        Unit::Count,
        "Number of Kafka ingress requests"
    );
    describe_gauge!(
        KAFKA_INGRESS_CONSUMER_LAG,
        Unit::Count,
        "Kafka Consumer Lag per partition"
    );
}
