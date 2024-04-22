// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut};
use restate_types::identifiers::{IdempotencyId, InvocationId};
use restate_types::invocation::ResponseResult;
use serde::{Deserialize, Serialize};

use crate::codec::{decode_default, encode_default, Targeted, WireDecode, WireEncode};
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

impl WireEncode for IngressMessage {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        protocol_version: ProtocolVersion,
    ) -> Result<(), CodecError> {
        // serialize message into buf
        encode_default(self, buf, protocol_version)
    }
}

impl WireDecode for IngressMessage {
    fn decode<B: Buf>(buf: &mut B, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        decode_default(buf, protocol_version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationResponse {
    pub invocation_id: InvocationId,
    pub idempotency_id: Option<IdempotencyId>,
    pub response: ResponseResult,
}
