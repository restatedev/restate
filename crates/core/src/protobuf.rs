// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tonic::{Code, Status};

use crate::metadata_store::{ReadError, WriteError};
use restate_types::errors::SimpleStatus;

pub mod cluster_ctrl_svc {
    tonic::include_proto!("restate.cluster_ctrl");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("cluster_ctrl_svc_descriptor");

    /// Creates a new ClusterCtrlSvcClient with appropriate configuration
    pub fn new_cluster_ctrl_client(
        channel: tonic::transport::Channel,
    ) -> cluster_ctrl_svc_client::ClusterCtrlSvcClient<tonic::transport::Channel> {
        cluster_ctrl_svc_client::ClusterCtrlSvcClient::new(channel)
            // note: the order of those calls defines the priority
            .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(crate::network::grpc::DEFAULT_GRPC_COMPRESSION)
    }
}

pub mod node_ctl_svc {
    use tonic::codec::CompressionEncoding;
    use tonic::transport::Channel;

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
    pub fn new_node_ctl_client(channel: Channel) -> NodeCtlSvcClient<Channel> {
        node_ctl_svc_client::NodeCtlSvcClient::new(channel)
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(DEFAULT_GRPC_COMPRESSION)
    }
}

pub mod metadata_proxy_svc {

    use bytestring::ByteString;
    use tonic::{codec::CompressionEncoding, transport::Channel};

    use metadata_proxy_svc_client::MetadataProxySvcClient;
    use restate_types::{
        Version,
        metadata::{Precondition, VersionedValue},
        nodes_config::NodesConfiguration,
    };

    use crate::{
        metadata_store::{MetadataStore, ProvisionError, ReadError, WriteError},
        network::grpc::DEFAULT_GRPC_COMPRESSION,
    };

    use super::{to_read_err, to_write_err};

    tonic::include_proto!("restate.metadata_proxy_svc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("metadata_proxy_svc_descriptor");

    /// Creates a new MetadataProxySvcClient with appropriate configuration
    pub fn new_metadata_proxy_client(
        channel: Channel,
    ) -> metadata_proxy_svc_client::MetadataProxySvcClient<Channel> {
        metadata_proxy_svc_client::MetadataProxySvcClient::new(channel)
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(DEFAULT_GRPC_COMPRESSION)
    }

    pub struct MetadataStoreProxy {
        client: MetadataProxySvcClient<Channel>,
    }

    impl MetadataStoreProxy {
        pub fn new(channel: Channel) -> Self {
            Self {
                client: new_metadata_proxy_client(channel),
            }
        }
    }

    #[async_trait::async_trait]
    impl MetadataStore for MetadataStoreProxy {
        async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
            let mut client = self.client.clone();

            let request = GetRequest {
                key: key.to_string(),
            };

            let response = client.get(request).await.map_err(to_read_err)?.into_inner();

            response
                .value
                .map(TryInto::try_into)
                .transpose()
                .map_err(ReadError::terminal)
        }

        async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
            let mut client = self.client.clone();

            let request = GetRequest {
                key: key.to_string(),
            };

            let response = client
                .get_version(request)
                .await
                .map_err(to_read_err)?
                .into_inner();

            response
                .version
                .map(TryInto::try_into)
                .transpose()
                .map_err(ReadError::terminal)
        }

        async fn put(
            &self,
            key: ByteString,
            value: VersionedValue,
            precondition: Precondition,
        ) -> Result<(), WriteError> {
            let mut client = self.client.clone();

            let request = PutRequest {
                key: key.to_string(),
                value: Some(value.into()),
                precondition: Some(precondition.into()),
            };

            client.put(request).await.map_err(to_write_err)?;

            Ok(())
        }

        async fn delete(
            &self,
            key: ByteString,
            precondition: Precondition,
        ) -> Result<(), WriteError> {
            let mut client = self.client.clone();

            let request = DeleteRequest {
                key: key.to_string(),
                precondition: Some(precondition.into()),
            };

            client.delete(request).await.map_err(to_write_err)?;

            Ok(())
        }

        async fn provision(&self, _: &NodesConfiguration) -> Result<bool, ProvisionError> {
            Err(ProvisionError::NotSupported(
                "Provision not supported over metadata proxy".to_owned(),
            ))
        }
    }
}

fn to_read_err(status: Status) -> ReadError {
    // Currently, we treat all returned statuses as terminal errors since
    // retryability is not encoded in the status response.
    //
    // Additionally, users of this proxy client (e.g., `restatectl`) are
    // unlikely to retry on errors and will typically attempt to use
    // another node instead.
    ReadError::terminal(SimpleStatus::from(status))
}

fn to_write_err(status: Status) -> WriteError {
    match status.code() {
        Code::FailedPrecondition => {
            WriteError::FailedPrecondition(SimpleStatus::from(status).to_string())
        }
        _ => WriteError::terminal(SimpleStatus::from(status)),
    }
}
