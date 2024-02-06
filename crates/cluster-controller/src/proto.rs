// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(warnings)]
#![allow(clippy::all)]
#![allow(unknown_lints)]

tonic::include_proto!("dev.restate.cluster_controller");

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("cluster_controller_descriptor");

impl From<NodeId> for restate_types::identifiers::NodeId {
    fn from(node_id: NodeId) -> Self {
        restate_types::identifiers::NodeId::from(node_id.id)
    }
}

impl From<restate_types::identifiers::NodeId> for NodeId {
    fn from(node_id: restate_types::identifiers::NodeId) -> Self {
        NodeId { id: node_id.into() }
    }
}
