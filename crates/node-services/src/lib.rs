// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod cluster_ctrl {
    tonic::include_proto!("dev.restate.cluster_ctrl");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("cluster_ctrl_svc_descriptor");
}

pub mod node {
    tonic::include_proto!("dev.restate.node");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("node_svc_descriptor");
}

pub mod common {
    tonic::include_proto!("dev.restate.common");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("common_descriptor");

    impl From<Version> for restate_types::Version {
        fn from(version: Version) -> Self {
            restate_types::Version::from(version.value)
        }
    }

    impl From<restate_types::Version> for Version {
        fn from(version: restate_types::Version) -> Self {
            Version {
                value: version.into(),
            }
        }
    }

    impl From<NodeId> for restate_types::NodeId {
        fn from(node_id: NodeId) -> Self {
            restate_types::NodeId::new(node_id.id, node_id.generation)
        }
    }

    impl From<restate_types::NodeId> for NodeId {
        fn from(node_id: restate_types::NodeId) -> Self {
            NodeId {
                id: node_id.id().into(),
                generation: node_id.as_generational().map(|g| g.generation()),
            }
        }
    }

    impl From<restate_types::PlainNodeId> for NodeId {
        fn from(node_id: restate_types::PlainNodeId) -> Self {
            let id: u32 = node_id.into();
            NodeId {
                id,
                generation: None,
            }
        }
    }
}
