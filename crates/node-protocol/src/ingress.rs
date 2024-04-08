// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use restate_types::identifiers::{IdempotencyId, InvocationId};
use restate_types::invocation::ResponseResult;
use serde::{Deserialize, Serialize};

use crate::codec::{Targeted, WireSerde};
use crate::common::{ProtocolVersion, TargetName};
use crate::CodecError;

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

impl Targeted for IngressMessage {
    const TARGET: TargetName = TargetName::Ingress;

    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl WireSerde for IngressMessage {
    fn encode(&self, _protocol_version: ProtocolVersion) -> Result<Bytes, CodecError> {
        // serialize message to bytes
        Ok(bincode::serde::encode_to_vec(self, bincode::config::standard())?.into())
    }

    fn decode(payload: Bytes, _protocol_version: ProtocolVersion) -> Result<Self, CodecError> {
        let (output, _) = bincode::serde::decode_from_slice(&payload, bincode::config::standard())?;
        Ok(output)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationResponse {
    pub invocation_id: InvocationId,
    pub idempotency_id: Option<IdempotencyId>,
    pub response: ResponseResult,
}
