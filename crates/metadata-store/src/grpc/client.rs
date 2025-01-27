// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc::metadata_store_svc_client::MetadataStoreSvcClient;
use crate::grpc::pb_conversions::ConversionError;
use crate::grpc::{DeleteRequest, GetRequest, ProvisionRequest, PutRequest};
use crate::KnownLeader;
use async_trait::async_trait;
use bytes::BytesMut;
use bytestring::ByteString;
use parking_lot::Mutex;
use rand::prelude::IteratorRandom;
use restate_core::metadata_store::{
    retry_on_network_error, MetadataStore, MetadataStoreClientError, Precondition, ProvisionError,
    ReadError, RemoteError, VersionedValue, WriteError,
};
use restate_core::network::net_util::create_tonic_channel;
use restate_core::{cancellation_watcher, Metadata, TaskCenter, TaskKind};
use restate_types::config::{Configuration, MetadataStoreClientOptions};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::AdvertisedAddress;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration, Role};
use restate_types::retries::RetryPolicy;
use restate_types::storage::StorageCodec;
use restate_types::{PlainNodeId, Version};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::debug;

/// Client end to interact with the metadata store.
#[derive(Debug, Clone)]
pub struct GrpcMetadataStoreClient {
    channel_manager: ChannelManager,
    svc_client: Arc<Mutex<Option<MetadataStoreSvcClient<Channel>>>>,
}

impl GrpcMetadataStoreClient {
    pub fn new(
        metadata_store_addresses: Vec<AdvertisedAddress>,
        client_options: MetadataStoreClientOptions,
    ) -> Self {
        let channel_manager = ChannelManager::new(metadata_store_addresses, client_options);
        let svc_client = Arc::new(Mutex::new(
            channel_manager
                .choose_random()
                .map(MetadataStoreSvcClient::new),
        ));

        if let Some(tc) = TaskCenter::try_with_current(|handle| handle.clone()) {
            if let Some(metadata) = Metadata::try_with_current(|m| m.clone()) {
                tc.spawn_child(
                    TaskKind::Background,
                    "update-metadata-store-channels",
                    channel_manager.clone().run(metadata),
                )
                .expect("to spawn new tasks");
            } else {
                debug!("The GrpcMetadataStoreClient has been started w/o access to the Metadata. Therefore, it will not update the metadata store endpoints automatically.");
            }
        } else {
            debug!("The GrpcMetadataStoreClient has been started outside of the TaskCenter. Therefore, it will not update the metadata store endpoints automatically.");
        }

        Self {
            channel_manager,
            svc_client,
        }
    }

    fn retry_policy() -> RetryPolicy {
        Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone()
    }

    fn choose_random_endpoint(&self) {
        // let's try another endpoint
        *self.svc_client.lock() = self
            .channel_manager
            .choose_random()
            .map(MetadataStoreSvcClient::new);
    }

    fn choose_known_leader(&self, known_leader: KnownLeader) {
        let channel = self
            .channel_manager
            .register_address(known_leader.node_id, known_leader.address);
        *self.svc_client.lock() = Some(MetadataStoreSvcClient::new(channel));
    }

    fn current_client(&self) -> Option<MetadataStoreSvcClient<Channel>> {
        let mut svc_client_guard = self.svc_client.lock();

        if svc_client_guard.is_none() {
            *svc_client_guard = self
                .channel_manager
                .choose_random()
                .map(MetadataStoreSvcClient::new);
        }

        svc_client_guard.clone()
    }

    fn handle_grpc_response<T, E>(
        &self,
        response: Result<T, Status>,
        map_status: impl Fn(Status) -> E,
    ) -> Result<T, E>
    where
        E: MetadataStoreClientError,
    {
        response.map_err(|status| {
            let known_leader = KnownLeader::from_status(&status);
            let err = map_status(status);

            if let Some(known_leader) = known_leader {
                self.choose_known_leader(known_leader);
            } else if err.is_network_error() {
                self.choose_random_endpoint();
            }
            err
        })
    }
}

#[async_trait]
impl MetadataStore for GrpcMetadataStoreClient {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let retry_policy = Self::retry_policy();

        let response = retry_on_network_error(retry_policy, || async {
            let mut client = self.current_client().ok_or_else(|| {
                ReadError::Internal("No metadata store address known.".to_string())
            })?;

            self.handle_grpc_response(
                client
                    .get(GetRequest {
                        key: key.clone().into(),
                    })
                    .await,
                map_status_to_read_error,
            )
        })
        .await?;

        response
            .into_inner()
            .try_into()
            .map_err(|err: ConversionError| ReadError::Internal(err.to_string()))
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let retry_policy = Self::retry_policy();

        let response = retry_on_network_error(retry_policy, || async {
            let mut client = self.current_client().ok_or_else(|| {
                ReadError::Internal("No metadata store address known.".to_string())
            })?;

            self.handle_grpc_response(
                client
                    .get_version(GetRequest {
                        key: key.clone().into(),
                    })
                    .await,
                map_status_to_read_error,
            )
        })
        .await?;

        Ok(response.into_inner().into())
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let retry_policy = Self::retry_policy();

        retry_on_network_error(retry_policy, || async {
            let mut client = self.current_client().ok_or_else(|| {
                WriteError::Internal("No metadata store address known.".to_string())
            })?;

            self.handle_grpc_response(
                client
                    .put(PutRequest {
                        key: key.clone().into(),
                        value: Some(value.clone().into()),
                        precondition: Some(precondition.clone().into()),
                    })
                    .await,
                map_status_to_write_error,
            )
        })
        .await?;

        Ok(())
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let retry_policy = Self::retry_policy();

        retry_on_network_error(retry_policy, || async {
            let mut client = self.current_client().ok_or_else(|| {
                WriteError::Internal("No metadata store address known.".to_string())
            })?;

            self.handle_grpc_response(
                client
                    .delete(DeleteRequest {
                        key: key.clone().into(),
                        precondition: Some(precondition.clone().into()),
                    })
                    .await,
                map_status_to_write_error,
            )
        })
        .await?;

        Ok(())
    }

    async fn provision(
        &self,
        nodes_configuration: &NodesConfiguration,
    ) -> Result<bool, ProvisionError> {
        // Only provision ourselves if we are the metadata store. Otherwise, we would have to
        // consistently pick a single node to reach out to avoid provisioning multiple nodes in
        // case of network errors.

        // We can't assume that we have joined the cluster yet. That's why we read our roles
        // from the configuration and not from the NodesConfiguration.
        let config = Configuration::pinned();

        if !config.common.roles.contains(Role::MetadataServer) {
            return Err(ProvisionError::NotSupported(format!("Node '{}' does not run the metadata-server role. Try to provision a different node.", config.common.advertised_address)));
        }

        let mut client = MetadataStoreSvcClient::new(create_tonic_channel(
            config.common.advertised_address.clone(),
            &config.networking,
        ));

        let mut buffer = BytesMut::new();
        StorageCodec::encode(nodes_configuration, &mut buffer)
            .map_err(|err| ProvisionError::Codec(err.into()))?;

        // no retry policy needed since we are connecting to ourselves.
        let response = client
            .provision(ProvisionRequest {
                nodes_configuration: buffer.freeze(),
            })
            .await
            .map_err(map_status_to_provision_error);

        if response.is_ok() {
            *self.svc_client.lock() = Some(client);
        }

        response.map(|response| response.into_inner().newly_provisioned)
    }
}

fn map_status_to_read_error(status: Status) -> ReadError {
    match &status.code() {
        Code::Unavailable => ReadError::Network(status.into()),
        Code::Unimplemented => ReadError::RemoteError(RemoteError::Unimplemented),
        _ => ReadError::Internal(status.to_string()),
    }
}

fn map_status_to_write_error(status: Status) -> WriteError {
    match &status.code() {
        Code::Unavailable => WriteError::Network(status.into()),
        Code::FailedPrecondition => WriteError::FailedPrecondition(status.message().to_string()),
        Code::Unimplemented => WriteError::RemoteError(RemoteError::Unimplemented),
        _ => WriteError::Internal(status.to_string()),
    }
}

fn map_status_to_provision_error(status: Status) -> ProvisionError {
    match &status.code() {
        Code::Unavailable => ProvisionError::Network(status.into()),
        _ => ProvisionError::Internal(status.to_string()),
    }
}

#[derive(Clone, Debug)]
struct ChannelManager {
    channels: Arc<Mutex<Channels>>,
    client_options: MetadataStoreClientOptions,
}

impl ChannelManager {
    fn new(
        initial_addresses: Vec<AdvertisedAddress>,
        client_options: MetadataStoreClientOptions,
    ) -> Self {
        let initial_channels: Vec<_> = initial_addresses
            .into_iter()
            .map(|address| create_tonic_channel(address, &client_options))
            .collect();

        ChannelManager {
            channels: Arc::new(Mutex::new(Channels::new(initial_channels))),
            client_options,
        }
    }

    fn register_address(&self, plain_node_id: PlainNodeId, address: AdvertisedAddress) -> Channel {
        let channel = create_tonic_channel(address, &self.client_options);
        self.channels
            .lock()
            .register(plain_node_id, channel.clone());
        channel
    }

    fn choose_random(&self) -> Option<Channel> {
        self.channels.lock().choose_random()
    }

    /// Watches the [`NodesConfiguration`] and updates the channels based on which nodes run the
    /// metadata store role.
    async fn run(self, metadata: Metadata) -> anyhow::Result<()> {
        let mut nodes_config_watch = metadata.watch(MetadataKind::NodesConfiguration);
        nodes_config_watch.mark_changed();
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                }
                _ = nodes_config_watch.changed() => {
                    let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
                    self.update_channels(nodes_config.as_ref(), &metadata);
                }
            }
        }

        Ok(())
    }

    fn update_channels(&self, nodes_configuration: &NodesConfiguration, metadata: &Metadata) {
        let has_joined_cluster = metadata.my_node_id_opt().is_some();

        let new_channels = nodes_configuration
            .iter()
            .filter_map(|(node_id, node_config)| {
                // We only consider metadata store members.
                if node_config.roles.contains(Role::MetadataServer)
                    && matches!(
                        node_config.metadata_server_config.metadata_server_state,
                        MetadataServerState::Member
                    )
                {
                    Some((
                        node_id,
                        create_tonic_channel(node_config.address.clone(), &self.client_options),
                    ))
                } else {
                    None
                }
            });

        let mut channels = self.channels.lock();

        channels.update_channels(new_channels);

        if has_joined_cluster && !channels.is_empty() {
            // Only drop initial channels if we have found others. This is to handle the case where
            // we are resuming from an older Restate version which didn't write the
            // MetadataServerState properly.
            channels.drop_initial_channels();
        }
    }
}

#[derive(Debug)]
struct Channels {
    initial_channels: Vec<Channel>,
    channels: HashMap<PlainNodeId, Channel>,
}

impl Channels {
    fn new(initial_channels: Vec<Channel>) -> Self {
        Channels {
            initial_channels,
            channels: HashMap::default(),
        }
    }

    fn register(&mut self, plain_node_id: PlainNodeId, channel: Channel) {
        self.channels.insert(plain_node_id, channel);
    }

    fn choose_random(&self) -> Option<Channel> {
        let mut rng = rand::thread_rng();
        let chosen_channel = self
            .channels
            .values()
            .chain(self.initial_channels.iter())
            .choose(&mut rng)
            .cloned();
        chosen_channel
    }

    /// Returns true if there are no channels. It ignores the initial channels.
    fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    fn drop_initial_channels(&mut self) {
        if !self.initial_channels.is_empty() {
            self.initial_channels = Vec::new();
        }
    }

    fn update_channels(&mut self, new_channels: impl IntoIterator<Item = (PlainNodeId, Channel)>) {
        self.channels.clear();
        self.channels.extend(new_channels);
    }
}
