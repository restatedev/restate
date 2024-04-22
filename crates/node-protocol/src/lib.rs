// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod codec;
pub mod common;
mod error;
pub mod ingress;
pub mod metadata;
pub mod node;

// re-exports for convenience
pub use common::CURRENT_PROTOCOL_VERSION;
pub use common::MIN_SUPPORTED_PROTOCOL_VERSION;
pub use error::*;

use restate_types::GenerationalNodeId;

use self::codec::WireDecode;

/// A wrapper for a message that includes the sender id
pub struct MessageEnvelope<M: WireDecode> {
    peer: GenerationalNodeId,
    connection_id: u64,
    body: M,
}

impl<M: WireDecode> MessageEnvelope<M> {
    pub fn new(peer: GenerationalNodeId, connection_id: u64, body: M) -> Self {
        Self {
            peer,
            connection_id,
            body,
        }
    }

    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub fn split(self) -> (GenerationalNodeId, M) {
        (self.peer, self.body)
    }
}
