// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_histogram};

pub(crate) const DEPLOYMENT_DISCOVERY_ATTEMPTS: &str =
    "restate.deployment.discovery.attempts.total";
pub(crate) const DEPLOYMENT_DISCOVERY_RETRIES: &str = "restate.deployment.discovery.retries.total";
pub(crate) const DEPLOYMENT_DISCOVERY_DURATION: &str =
    "restate.deployment.discovery.duration.seconds";

pub(crate) fn describe_metrics() {
    describe_counter!(
        DEPLOYMENT_DISCOVERY_ATTEMPTS,
        Unit::Count,
        "Deployment discovery attempts by outcome"
    );
    describe_counter!(
        DEPLOYMENT_DISCOVERY_RETRIES,
        Unit::Count,
        "Deployment discovery retries by error category"
    );
    describe_histogram!(
        DEPLOYMENT_DISCOVERY_DURATION,
        Unit::Seconds,
        "Total time for a discovery handshake including retries"
    );
}
