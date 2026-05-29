// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
            .max_decoding_message_size(connection_options.message_size_limit().get())
            // note: the order of those calls defines the priority
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(crate::network::grpc::DEFAULT_GRPC_COMPRESSION)
    }
}

pub mod node_ctl_svc {
    use enumset::EnumSet;
    use restate_types::nodes_config;
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
        pub fn dry_run(
            cluster_configuration: ClusterConfiguration,
            enabled_features: EnumSet<nodes_config::ClusterFeature>,
        ) -> Self {
            ProvisionClusterResponse {
                dry_run: true,
                cluster_configuration: Some(cluster_configuration),
                enabled_features: features_to_proto(enabled_features),
            }
        }

        pub fn provisioned(
            cluster_configuration: ClusterConfiguration,
            enabled_features: EnumSet<nodes_config::ClusterFeature>,
        ) -> Self {
            ProvisionClusterResponse {
                dry_run: false,
                cluster_configuration: Some(cluster_configuration),
                enabled_features: features_to_proto(enabled_features),
            }
        }
    }

    fn features_to_proto(features: EnumSet<nodes_config::ClusterFeature>) -> Vec<i32> {
        features
            .iter()
            .map(|f| ClusterFeature::from(f) as i32)
            .collect()
    }

    /// Creates a new NodeCtlSvcClient with appropriate configuration
    pub fn new_node_ctl_client<O: GrpcConnectionOptions>(
        channel: Channel,
        connection_options: &O,
    ) -> NodeCtlSvcClient<Channel> {
        node_ctl_svc_client::NodeCtlSvcClient::new(channel)
            .max_decoding_message_size(connection_options.message_size_limit().get())
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(DEFAULT_GRPC_COMPRESSION)
    }

    impl From<ClusterFeature> for nodes_config::ClusterFeature {
        fn from(value: ClusterFeature) -> Self {
            match value {
                ClusterFeature::Unknown => Self::Unknown,
                ClusterFeature::ControlledIdempotentSharding => Self::ControlledIdempotentSharding,
            }
        }
    }

    impl From<nodes_config::ClusterFeature> for ClusterFeature {
        fn from(value: nodes_config::ClusterFeature) -> Self {
            match value {
                nodes_config::ClusterFeature::Unknown => Self::Unknown,
                nodes_config::ClusterFeature::ControlledIdempotentSharding => {
                    Self::ControlledIdempotentSharding
                }
            }
        }
    }

    /// Converts the wire representation of a cluster feature list (a slice of i32) into
    /// an [`EnumSet`] of [`nodes_config::ClusterFeature`]. Semantics-neutral — the caller
    /// decides whether the list represents enabled or disabled features. Returns an error
    /// if any entry is out of range or maps to the `Unknown` sentinel.
    pub fn cluster_features_from_proto(
        features: &[i32],
    ) -> anyhow::Result<EnumSet<nodes_config::ClusterFeature>> {
        let mut set = EnumSet::empty();
        for raw in features {
            let proto_feature = ClusterFeature::try_from(*raw)
                .map_err(|_| anyhow::anyhow!("unknown cluster feature id {raw}"))?;
            let feature = nodes_config::ClusterFeature::from(proto_feature);
            if matches!(feature, nodes_config::ClusterFeature::Unknown) {
                anyhow::bail!("cluster feature 'unknown' is not a valid selection");
            }
            set.insert(feature);
        }
        Ok(set)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn cluster_features_from_proto_round_trips_known_features() {
            let raw = [ClusterFeature::ControlledIdempotentSharding as i32];
            let parsed = cluster_features_from_proto(&raw).expect("known feature accepted");
            assert!(parsed.contains(nodes_config::ClusterFeature::ControlledIdempotentSharding));

            assert!(cluster_features_from_proto(&[ClusterFeature::Unknown as i32]).is_err());
            assert!(cluster_features_from_proto(&[i32::MAX]).is_err());
        }
    }
}
