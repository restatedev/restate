// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test-only helpers on `ServiceClient`. Compiled when the crate is built with `cfg(test)` or with
//! the `test_util` feature so integration tests can seed the GCP token cache without touching a
//! real metadata server.

use crate::ServiceClient;
use crate::gcp::GcpTokenClient;

impl ServiceClient {
    /// Test-only accessor for the underlying GCP token client. Lets integration tests seed the
    /// cache so token mint does not need to contact a real metadata server.
    pub fn gcp_for_test(&self) -> &GcpTokenClient {
        &self.gcp
    }
}
