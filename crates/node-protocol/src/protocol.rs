// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::GenerationalNodeId;
use serde::{Deserialize, Serialize};

use crate::messages::{GetMetadataRequest, MetadataUpdate};
use crate::MessageKind;

/// All supported network message types under one roof.
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum NetworkMessage {
    GetMetadataRequest(GetMetadataRequest),
    MetadataUpdate(MetadataUpdate),
}

impl NetworkMessage {
    pub fn kind(&self) -> MessageKind {
        match self {
            Self::GetMetadataRequest(_) => MessageKind::GetMetadataRequest,
            Self::MetadataUpdate(_) => MessageKind::MetadataUpdate,
        }
    }
}

/// A wrapper for a message that includes the sender id
pub struct MessageEnvelope {
    pub from: GenerationalNodeId,
    pub msg: NetworkMessage,
}

impl MessageEnvelope {
    pub fn new(from: GenerationalNodeId, msg: NetworkMessage) -> Self {
        Self { from, msg }
    }

    pub fn kind(&self) -> MessageKind {
        self.msg.kind()
    }

    pub fn split(self) -> (GenerationalNodeId, NetworkMessage) {
        (self.from, self.msg)
    }
}
