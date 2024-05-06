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
use crate::identifiers::{IdempotencyId, InvocationId, ServiceId};
use crate::invocation::InvocationTarget;
use crate::GenerationalNodeId;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IngressResponseEnvelope<T> {
    pub target_node: GenerationalNodeId,
    pub inner: T,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttachedInvocationNotification {
    pub submitted_invocation_id: InvocationId,
    pub attached_invocation_id: InvocationId,
    pub idempotency_id: Option<IdempotencyId>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IngressResponseResult {
    Success(InvocationTarget, Bytes),
    Failure(InvocationError),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationResponseCorrelationIds {
    pub invocation_id: Option<InvocationId>,
    pub idempotency_id: Option<IdempotencyId>,
    pub service_id: Option<ServiceId>,
}

impl InvocationResponseCorrelationIds {
    pub fn from_invocation_id(id: InvocationId) -> Self {
        Self {
            invocation_id: Some(id),
            idempotency_id: None,
            service_id: None,
        }
    }

    pub fn from_idempotency_id(id: IdempotencyId) -> Self {
        Self {
            invocation_id: None,
            idempotency_id: Some(id),
            service_id: None,
        }
    }

    pub fn from_service_id(service_id: ServiceId) -> Self {
        Self {
            invocation_id: None,
            idempotency_id: None,
            service_id: Some(service_id),
        }
    }

    pub fn with_invocation_id(mut self, id: Option<InvocationId>) -> Self {
        self.invocation_id = id;
        self
    }

    pub fn with_idempotency_id(mut self, id: Option<IdempotencyId>) -> Self {
        self.idempotency_id = id;
        self
    }

    pub fn with_service_id(mut self, id: Option<ServiceId>) -> Self {
        self.service_id = id;
        self
    }

    pub fn into_inner(
        self,
    ) -> (
        Option<InvocationId>,
        Option<IdempotencyId>,
        Option<ServiceId>,
    ) {
        debug_assert!(
            self.invocation_id.is_some()
                || self.idempotency_id.is_some()
                || self.service_id.is_some()
        );
        (self.invocation_id, self.idempotency_id, self.service_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationResponse {
    pub correlation_ids: InvocationResponseCorrelationIds,
    pub response: IngressResponseResult,
}
