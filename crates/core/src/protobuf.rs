// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod cluster_ctrl_svc {
    use restate_types::net::connect_opts::GrpcConnectionOptions;

    tonic::include_proto!("restate.cluster_ctrl");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("cluster_ctrl_svc_descriptor");

    /// Creates a new ClusterCtrlSvcClient with appropriate configuration
    pub fn new_cluster_ctrl_client<O: GrpcConnectionOptions>(
        channel: tonic::transport::Channel,
        connection_options: &O,
    ) -> cluster_ctrl_svc_client::ClusterCtrlSvcClient<tonic::transport::Channel> {
        cluster_ctrl_svc_client::ClusterCtrlSvcClient::new(channel)
            .max_encoding_message_size(connection_options.max_message_size())
            .max_decoding_message_size(connection_options.max_message_size())
            // note: the order of those calls defines the priority
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(crate::network::grpc::DEFAULT_GRPC_COMPRESSION)
    }
}

pub mod node_ctl_svc {
    use tonic::codec::CompressionEncoding;
    use tonic::transport::Channel;

    use restate_types::net::connect_opts::GrpcConnectionOptions;
    use restate_types::protobuf::cluster::ClusterConfiguration;

    use crate::network::grpc::DEFAULT_GRPC_COMPRESSION;

    use self::node_ctl_svc_client::NodeCtlSvcClient;

    tonic::include_proto!("restate.node_ctl_svc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("node_ctl_svc_descriptor");

    impl ProvisionClusterResponse {
        pub fn dry_run(cluster_configuration: ClusterConfiguration) -> Self {
            ProvisionClusterResponse {
                dry_run: true,
                cluster_configuration: Some(cluster_configuration),
            }
        }

        pub fn provisioned(cluster_configuration: ClusterConfiguration) -> Self {
            ProvisionClusterResponse {
                dry_run: false,
                cluster_configuration: Some(cluster_configuration),
            }
        }
    }

    /// Creates a new NodeCtlSvcClient with appropriate configuration
    pub fn new_node_ctl_client<O: GrpcConnectionOptions>(
        channel: Channel,
        connection_options: &O,
    ) -> NodeCtlSvcClient<Channel> {
        node_ctl_svc_client::NodeCtlSvcClient::new(channel)
            .max_encoding_message_size(connection_options.max_message_size())
            .max_decoding_message_size(connection_options.max_message_size())
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(DEFAULT_GRPC_COMPRESSION)
    }
}
