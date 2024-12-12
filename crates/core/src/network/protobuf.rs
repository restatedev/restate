// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod node_ctl_svc {
    use restate_types::protobuf::cluster::ClusterConfiguration;

    tonic::include_proto!("restate.node_ctl_svc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("node_ctl_svc_descriptor");

    impl ProvisionClusterResponse {
        pub fn dry_run(cluster_configuration: ClusterConfiguration) -> Self {
            ProvisionClusterResponse {
                kind: ProvisionClusterResponseKind::DryRun.into(),
                cluster_configuration: Some(cluster_configuration),
                ..Default::default()
            }
        }

        pub fn err(message: impl ToString) -> Self {
            ProvisionClusterResponse {
                kind: ProvisionClusterResponseKind::Error.into(),
                error: Some(message.to_string()),
                ..Default::default()
            }
        }

        pub fn success(cluster_configuration: ClusterConfiguration) -> Self {
            ProvisionClusterResponse {
                kind: ProvisionClusterResponseKind::Success.into(),
                cluster_configuration: Some(cluster_configuration),
                ..Default::default()
            }
        }
    }
}

pub mod core_node_svc {
    tonic::include_proto!("restate.core_node_svc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("core_node_svc_descriptor");
}
