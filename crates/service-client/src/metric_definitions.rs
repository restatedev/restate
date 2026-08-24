// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub(crate) const GCP_CREDENTIAL_BUILDS: &str = "restate.service_client.gcp.credential_builds.total";
pub(crate) const GCP_CREDENTIAL_BUILD_DURATION: &str =
    "restate.service_client.gcp.credential_build_duration.seconds";
pub(crate) const GCP_TOKEN_MINTS: &str = "restate.service_client.gcp.token_mints.total";
pub(crate) const GCP_CREDENTIALS_ACTIVE: &str = "restate.service_client.gcp.credentials.active";

pub(crate) const RESULT_SUCCESS: &str = "success";
pub(crate) const RESULT_ERROR: &str = "error";

pub(crate) const MINT_OUTCOME_SUCCESS: &str = "success";
pub(crate) const MINT_OUTCOME_TIMEOUT: &str = "timeout";
pub(crate) const MINT_OUTCOME_TRANSIENT_ERROR: &str = "transient_error";
pub(crate) const MINT_OUTCOME_PERMANENT_ERROR: &str = "permanent_error";

/// Label values only -- never audience, service account, endpoint, provider, or error text.
/// Those are unbounded, and the credential registry these metrics describe is a small,
/// process-wide cache, not a per-target one.
pub(crate) fn describe_metrics() {
    describe_counter!(
        GCP_CREDENTIAL_BUILDS,
        Unit::Count,
        "Number of GCP credential build attempts, by result"
    );

    describe_histogram!(
        GCP_CREDENTIAL_BUILD_DURATION,
        Unit::Seconds,
        "Time to build a GCP credential, including ambient source resolution"
    );

    describe_counter!(
        GCP_TOKEN_MINTS,
        Unit::Count,
        "Number of GCP ID-token mint attempts, by outcome"
    );

    describe_gauge!(
        GCP_CREDENTIALS_ACTIVE,
        Unit::Count,
        "Number of GCP credentials currently cached"
    );
}
