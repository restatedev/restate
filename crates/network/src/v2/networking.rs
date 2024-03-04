// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::{NetworkSendError, NetworkSender};
use restate_node_protocol::codec::{Targeted, WireSerde};
use restate_types::NodeId;

/// Access to node-to-node networking infrastructure;
#[derive(Default, Clone)]
pub struct Networking {}

impl Networking {}

impl NetworkSender for Networking {
    async fn send<M>(&self, _to: NodeId, _message: &M) -> Result<(), NetworkSendError>
    where
        M: WireSerde + Targeted + Send + Sync,
    {
        Ok(())
    }
}

static_assertions::assert_impl_all!(Networking: Send, Sync);
