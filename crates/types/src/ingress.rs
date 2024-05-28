// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::InvocationError;
use crate::identifiers::{IngressRequestId, InvocationId};
use crate::invocation::InvocationTarget;
use crate::GenerationalNodeId;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IngressResponseEnvelope<T> {
    pub target_node: GenerationalNodeId,
    pub inner: T,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmittedInvocationNotification {
    pub request_id: IngressRequestId,
    /// Invocation id that was submitted in the ServiceInvocation
    pub original_invocation_id: InvocationId,
    /// Invocation id we attached to.
    pub attached_invocation_id: InvocationId,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationResponse {
    pub request_id: IngressRequestId,
    pub invocation_id: Option<InvocationId>,
    pub response: IngressResponseResult,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IngressResponseResult {
    Success(InvocationTarget, Bytes),
    Failure(InvocationError),
}
