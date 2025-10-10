// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub trait TelemetryClient {
    fn send_register_deployment_telemetry(&self, sdk_version: Option<String>);
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    impl TelemetryClient for () {
        fn send_register_deployment_telemetry(&self, _: Option<String>) {
            // Nothing
        }
    }
}
