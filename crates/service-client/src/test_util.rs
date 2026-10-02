// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test-only helpers compiled with `cfg(test)` or the `test_util` feature.

pub use crate::gcp::TestOverrideGuard as GcpTokenOverrideGuard;

/// Return a scoped token override so tests do not need ambient Google credentials.
pub fn override_gcp_token(
    service_account: Option<&str>,
    audience: &str,
    token: String,
) -> GcpTokenOverrideGuard {
    crate::gcp::override_token_for_test(service_account, audience, token)
}

/// Return a scoped mint failure for the exact credential key under test.
pub fn override_gcp_mint_failure(
    service_account: Option<&str>,
    audience: &str,
    message: &str,
) -> GcpTokenOverrideGuard {
    crate::gcp::override_failure_for_test(service_account, audience, message)
}
