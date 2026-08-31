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
/// Configures the AWS identity Restate uses for GCP workload identity federation. Restate assumes
/// this role once per process, and every federated deployment signs its subject tokens with the
/// shared role session. Each deployment still selects its own workload identity provider and
/// service account through its `auth` block.
///
/// Leave this block unset to disable the federation path entirely: a deployment that requests
/// `workload_identity_provider` authentication on a server without this configured fails
/// registration and every subsequent mint with an actionable error, rather than falling back to
/// an unauthenticated request. Removing this block while registered deployments depend on it
/// strands those deployments after restart; restore the block and restart the server to recover.
///
/// **Not live-reloadable.** Changing `aws-role-arn` or `aws-role-session-name` on a running server
/// has no effect until restart. The configuration is captured once at node startup because
/// already-constructed credentials retain the AWS federation identity used at construction.
///
/// **The AWS federation identity belongs to operator config and must not be settable through
/// deployment registration.** A deployment's `workload_identity_provider`,
/// `impersonate_service_account`, and `audience` describe the customer-controlled side of the
/// trust chain. The role ARN and session name describe the operator-controlled side.
///
/// 1. **Confused deputy.** Deployment configuration is registrant-controlled data, while the
///    server's own ambient AWS identity typically holds broad `sts:AssumeRole` permissions (the
///    Lambda assume-role feature depends on exactly that). If a registrant could choose the role
///    ARN, they could direct Restate to assume any role that trusts the server's identity.
/// 2. **Defense in depth.** The AWS role's trust policy is expected to pin the session name.
///    Keeping the session name out of registrant reach means the runtime never even emits an
///    identity the operator didn't configure, rather than relying solely on one hand-written IAM
///    condition to catch a registrant-supplied value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct GcpFederationOptions {
    /// # AWS role ARN
    ///
    /// The AWS IAM role Restate assumes to obtain the AWS identity used to sign the GCP workload
    /// identity federation subject token. This role is shared by every federated deployment in
    /// the process.
    pub aws_role_arn: String,

    /// # AWS role session name
    ///
    /// The `RoleSessionName` used when assuming `aws_role_arn`. This value alone grants no
    /// isolation between tenants; the role's trust policy controls which sessions may assume it.
    pub aws_role_session_name: String,
}
