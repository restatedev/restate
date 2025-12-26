// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(any(feature = "grpc-client", feature = "grpc-server"))]
pub mod metadata_proxy_svc {

    tonic::include_proto!("restate.metadata_proxy_svc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("metadata_proxy_svc_descriptor");

    #[cfg(feature = "grpc-client")]
    pub mod client {
        /// Default send compression for grpc clients
        const DEFAULT_GRPC_COMPRESSION: CompressionEncoding = CompressionEncoding::Zstd;

        use bytestring::ByteString;
        use restate_types::net::connect_opts::GrpcConnectionOptions;
        use tonic::{Code, Status};
        use tonic::{codec::CompressionEncoding, transport::Channel};

        use restate_types::errors::SimpleStatus;
        use restate_types::{
            Version,
            metadata::{Precondition, VersionedValue},
            nodes_config::NodesConfiguration,
        };

        use super::metadata_proxy_svc_client::MetadataProxySvcClient;
        use crate::metadata_store::{MetadataStore, ProvisionError, ReadError, WriteError};

        /// Creates a new MetadataProxySvcClient with appropriate configuration
        pub fn new_metadata_proxy_client<O: GrpcConnectionOptions>(
            connection_options: Channel,
            options: &O,
        ) -> MetadataProxySvcClient<Channel> {
            MetadataProxySvcClient::new(connection_options)
                .max_encoding_message_size(options.max_message_size())
                .max_decoding_message_size(options.max_message_size())
                // note: the order of those calls defines the priority
                .accept_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(DEFAULT_GRPC_COMPRESSION)
        }

        pub struct MetadataStoreProxy {
            client: MetadataProxySvcClient<Channel>,
        }

        impl MetadataStoreProxy {
            pub fn new<O: GrpcConnectionOptions>(channel: Channel, connection_options: &O) -> Self {
                Self {
                    client: new_metadata_proxy_client(channel, connection_options),
                }
            }
        }

        #[async_trait::async_trait]
        impl MetadataStore for MetadataStoreProxy {
            async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
                let mut client = self.client.clone();

                let request = super::GetRequest {
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

                let request = super::GetRequest {
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

                let request = super::PutRequest {
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

                let request = super::DeleteRequest {
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
    }
}
