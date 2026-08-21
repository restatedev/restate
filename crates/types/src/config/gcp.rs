// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

/// # GCP workload identity federation options
///
/// Configures the shared AWS -> GCP workload identity federation broker used to mint Google ID
/// tokens for deployments that set `workload_identity_provider` in their `auth` block. Restate
/// assumes this AWS role once per process -- not once per deployment -- and every federated
/// deployment's subject token is signed with that one shared session; the deployment's own
/// `workload_identity_provider` and `impersonate_service_account` determine which GCP identity
/// pool and service account the resulting token is exchanged and impersonated against.
///
/// Leave this block unset to disable the federation path entirely: a deployment that requests
/// `workload_identity_provider` authentication on a server without this configured fails
/// registration and every subsequent mint with an actionable error, rather than falling back to
/// an unauthenticated request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GcpFederationOptions {
    /// # Broker role ARN
    ///
    /// The AWS IAM role Restate assumes to obtain the AWS identity used to sign the GCP workload
    /// identity federation subject token. This role is shared by every federated deployment in
    /// the process.
    pub broker_role_arn: String,

    /// # Session name
    ///
    /// The `RoleSessionName` used when assuming `broker_role_arn`. This value alone grants no
    /// isolation between tenants; the broker role's trust policy in the AWS account that owns
    /// `broker_role_arn` is what enforces which session names may assume it.
    pub session_name: String,
}
