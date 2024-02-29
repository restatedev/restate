// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod common;
mod metadata;
pub mod node;
mod protocol;

pub use common::CURRENT_PROTOCOL_VERSION;
pub use common::MIN_SUPPORTED_PROTOCOL_VERSION;

pub use metadata::*;
pub use node::MessageKind;
pub use protocol::{MessageEnvelope, NetworkMessage};

// re-exports of network message types
pub mod messages {
    pub use crate::metadata::GetMetadataRequest;
    pub use crate::metadata::MetadataUpdate;
}
