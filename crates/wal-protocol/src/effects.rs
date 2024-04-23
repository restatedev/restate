// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use bytes::Bytes;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::ingress::IngressResponse;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BuiltinServiceEffects {
    invocation_id: InvocationId,
    effects: Vec<BuiltinServiceEffect>,
}

impl BuiltinServiceEffects {
    pub fn new(invocation_id: InvocationId, effects: Vec<BuiltinServiceEffect>) -> Self {
        Self {
            invocation_id,
            effects,
        }
    }

    pub fn into_inner(self) -> (InvocationId, Vec<BuiltinServiceEffect>) {
        (self.invocation_id, self.effects)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum BuiltinServiceEffect {
    SetState {
        key: Cow<'static, str>,
        value: Bytes,
    },
    ClearState(Cow<'static, str>),

    OutboxMessage(OutboxMessage),
    End(
        // NBIS can optionally fail, depending on the context the error might or might not be used.
        Option<InvocationError>,
    ),

    IngressResponse(IngressResponse),
}
