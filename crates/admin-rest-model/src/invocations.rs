// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::{DeploymentId, InvocationId};
use restate_types::invocation::client as invocation_client;
use serde::{Deserialize, Serialize};

/// Specifies which deployment to use when resuming or restarting an invocation.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub enum PatchDeploymentId {
    /// Keep the currently pinned deployment
    #[default]
    #[serde(alias = "keep")]
    Keep,
    /// Use the latest deployment
    #[serde(alias = "latest")]
    Latest,
    /// Use a specific deployment ID
    #[serde(untagged)]
    Id(String),
}

impl PatchDeploymentId {
    /// Convert to the internal client representation.
    /// Returns an error string if the deployment ID cannot be parsed.
    pub fn into_client(self) -> Result<invocation_client::PatchDeploymentId, String> {
        Ok(match self {
            PatchDeploymentId::Keep => invocation_client::PatchDeploymentId::KeepPinned,
            PatchDeploymentId::Latest => invocation_client::PatchDeploymentId::PinToLatest,
            PatchDeploymentId::Id(dp_id) => invocation_client::PatchDeploymentId::PinTo {
                id: dp_id.parse::<DeploymentId>().map_err(|e| e.to_string())?,
            },
        })
    }
}

/// The invocation was restarted as new.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct RestartAsNewInvocationResponse {
    /// The invocation id of the new invocation.
    pub new_invocation_id: InvocationId,
}

// --- Batch operation types ---

/// Maximum number of invocations in a single batch operation
pub const BATCH_OPERATION_MAX_SIZE: usize = 1000;

/// Request body for batch invocation operations
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct BatchInvocationRequest {
    /// List of invocation IDs to operate on
    pub invocation_ids: Vec<InvocationId>,
}

/// Request body for batch resume operations (with optional deployment)
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct BatchResumeRequest {
    /// List of invocation IDs to resume
    pub invocation_ids: Vec<InvocationId>,
    /// Deployment ID to use when resuming (applies to all invocations).
    /// Use "Keep" to keep the pinned deployment, "Latest" for latest, or a specific deployment ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment: Option<PatchDeploymentId>,
}

/// Request body for batch restart-as-new operations
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct BatchRestartAsNewRequest {
    /// List of invocation IDs to restart
    pub invocation_ids: Vec<InvocationId>,
    /// Deployment ID to use (applies to all invocations).
    /// Use "Keep" to keep the pinned deployment, "Latest" for latest, or a specific deployment ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment: Option<PatchDeploymentId>,
}

/// Information about a failed invocation operation
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct FailedInvocationOperation {
    /// The invocation ID that failed
    pub invocation_id: InvocationId,
    /// Error message describing the failure
    pub error: String,
}

/// Result of a batch invocation operation
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct BatchOperationResult {
    /// Invocations that were successfully processed
    pub succeeded: Vec<InvocationId>,
    /// Invocations that failed with error details
    pub failed: Vec<FailedInvocationOperation>,
}

/// Successful restart-as-new operation result
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct RestartedInvocation {
    /// The original invocation ID
    pub old_invocation_id: InvocationId,
    /// The new invocation ID
    pub new_invocation_id: InvocationId,
}

/// Result of batch restart-as-new operations
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct BatchRestartAsNewResult {
    /// Successfully restarted invocations with their new IDs
    pub succeeded: Vec<RestartedInvocation>,
    /// Invocations that failed with error details
    pub failed: Vec<FailedInvocationOperation>,
}
