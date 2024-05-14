// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::{IdempotencyId, InvocationId};
use restate_types::invocation::ResponseResult;
use serde::{Deserialize, Serialize};

use crate::common::TargetName;
use crate::define_message;
use crate::{CodecError, RpcMessage};

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    derive_more::From,
    strum_macros::EnumIs,
    strum_macros::IntoStaticStr,
)]
pub enum IngressMessage {
    InvocationResponse(InvocationResponse),
}

define_message! {
    @message = IngressMessage,
    @target = TargetName::Ingress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationResponse {
    pub invocation_id: InvocationId,
    pub idempotency_id: Option<IdempotencyId>,
    pub response: ResponseResult,
}

// TODO we could eventually remove this type and replace it with something simpler once
//  https://github.com/restatedev/restate/issues/1329 is in place
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IngressCorrelationId {
    InvocationId(InvocationId),
    IdempotencyId(IdempotencyId),
}

impl RpcMessage for InvocationResponse {
    type CorrelationId = IngressCorrelationId;
    fn correlation_id(&self) -> Self::CorrelationId {
        self.idempotency_id
            .as_ref()
            .map(|idempotency_id| IngressCorrelationId::IdempotencyId(idempotency_id.clone()))
            .unwrap_or_else(|| IngressCorrelationId::InvocationId(self.invocation_id))
    }
}
