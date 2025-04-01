// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::KnownLeader;
use crate::grpc::metadata_server_svc_client::MetadataServerSvcClient;
use crate::grpc::{DeleteRequest, GetRequest, ProvisionRequest, PutRequest};
use async_trait::async_trait;
use bytes::BytesMut;
use bytestring::ByteString;
use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::Rng;
use restate_core::metadata_store::{MetadataStore, ProvisionError, ReadError, WriteError};
use restate_core::network::net_util::{CommonClientConnectionOptions, create_tonic_channel};
use restate_core::{Metadata, TaskCenter, TaskKind, cancellation_watcher};
use restate_types::config::Configuration;
use restate_types::errors::ConversionError;
use restate_types::metadata::{Precondition, VersionedValue};
use restate_types::net::AdvertisedAddress;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration, Role};
use restate_types::storage::StorageCodec;
use restate_types::{PlainNodeId, Version};
use std::ops::Deref;
use std::sync::Arc;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::{debug, instrument};

use super::new_metadata_server_client;

const MAX_RETRY_ATTEMPTS: usize = 3;

#[derive(Debug, Clone, derive_more::Deref, derive_more::DerefMut)]
struct MetadataServerSvcClientWithAddress {
    #[deref]
    #[deref_mut]
    client: MetadataServerSvcClient<Channel>,
    address: AdvertisedAddress,
}

impl MetadataServerSvcClientWithAddress {
    fn new(channel: ChannelWithAddress) -> Self {
        Self {
            client: new_metadata_server_client(channel.channel.clone()),
            address: channel.address,
        }
    }

    fn address(&self) -> AdvertisedAddress {
        self.address.clone()
    }
}

/// Client end to interact with a set of metadata servers.
#[derive(Clone)]
pub struct GrpcMetadataServerClient {
    channel_manager: ChannelManager,
    current_leader: Arc<Mutex<Option<MetadataServerSvcClientWithAddress>>>,
}

impl GrpcMetadataServerClient {
    pub fn new(
        metadata_store_addresses: Vec<AdvertisedAddress>,
        connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
    ) -> Self {
        let channel_manager = ChannelManager::new(metadata_store_addresses, connection_options);
        let svc_client = Arc::new(Mutex::new(
            channel_manager
                .choose_channel()
                .map(MetadataServerSvcClientWithAddress::new),
        ));

        if let Some(tc) = TaskCenter::try_with_current(|handle| handle.clone()) {
            if let Some(metadata) = Metadata::try_with_current(|m| m.clone()) {
                tc.spawn_child(
                    TaskKind::Background,
                    "update-metadata-server-channels",
                    channel_manager.clone().run(metadata),
                )
                .expect("to spawn new tasks");
            } else {
                debug!(
                    "The GrpcMetadataServerClient has been started w/o access to the Metadata. Therefore, it will not update the metadata store endpoints automatically."
                );
            }
        } else {
            debug!(
                "The GrpcMetadataServerClient has been started outside of the TaskCenter. Therefore, it will not update the metadata store endpoints automatically."
            );
        }

        Self {
            channel_manager,
            current_leader: svc_client,
        }
    }

    fn choose_random_endpoint(&self) {
        // let's try another endpoint
        *self.current_leader.lock() = self
            .channel_manager
            .choose_channel()
            .map(MetadataServerSvcClientWithAddress::new);
    }

    fn choose_known_leader(&self, known_leader: KnownLeader) {
        let channel = self
            .channel_manager
            .register_address(known_leader.node_id, known_leader.address);
        *self.current_leader.lock() = Some(MetadataServerSvcClientWithAddress::new(channel));
    }

    fn current_client(&self) -> Option<MetadataServerSvcClientWithAddress> {
        let mut svc_client_guard = self.current_leader.lock();

        if svc_client_guard.is_none() {
            *svc_client_guard = self
                .channel_manager
                .choose_channel()
                .map(MetadataServerSvcClientWithAddress::new);
        }

        svc_client_guard.clone()
    }

    fn has_known_leader(&self, status: &Status) -> bool {
        if let Some(known_leader) = KnownLeader::from_status(status) {
            self.choose_known_leader(known_leader);
            true
        } else {
            self.choose_random_endpoint();
            false
        }
    }
}

#[async_trait]
impl MetadataStore for GrpcMetadataServerClient {
    #[instrument(level = "debug", skip(self))]
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let mut attempt = 0;
        loop {
            let mut client = self
                .current_client()
                .ok_or_else(|| ReadError::terminal(NoKnownMetadataServer))?;

            return match client
                .get(GetRequest {
                    key: key.clone().into(),
                })
                .await
            {
                Ok(response) => response
                    .into_inner()
                    .try_into()
                    .map_err(|err: ConversionError| ReadError::terminal(err)),
                Err(status) => {
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, "Retrying failed operation because we learned about the current leader.");
                        continue;
                    }
                    Err(map_status_to_read_error(client.address(), status))
                }
            };
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let mut attempt = 0;
        loop {
            let mut client = self
                .current_client()
                .ok_or_else(|| ReadError::terminal(NoKnownMetadataServer))?;

            return match client
                .get_version(GetRequest {
                    key: key.clone().into(),
                })
                .await
            {
                Ok(response) => return Ok(response.into_inner().into()),
                Err(status) => {
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, "Retrying failed operation because we learned about the current leader.");
                        continue;
                    }
                    Err(map_status_to_read_error(client.address(), status))
                }
            };
        }
    }

    #[instrument(level = "debug", skip_all, fields(%key, version = %value.version, %precondition))]
    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let mut attempt = 0;
        loop {
            let mut client = self
                .current_client()
                .ok_or_else(|| WriteError::terminal(NoKnownMetadataServer))?;

            return match client
                .put(PutRequest {
                    key: key.clone().into(),
                    value: Some(value.clone().into()),
                    precondition: Some(precondition.clone().into()),
                })
                .await
            {
                Ok(_) => return Ok(()),
                Err(status) => {
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, "Retrying failed operation because we learned about the current leader.");
                        continue;
                    }
                    Err(map_status_to_write_error(client.address(), status))
                }
            };
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let mut attempt = 0;
        loop {
            let mut client = self
                .current_client()
                .ok_or_else(|| WriteError::terminal(NoKnownMetadataServer))?;

            return match client
                .delete(DeleteRequest {
                    key: key.clone().into(),
                    precondition: Some(precondition.clone().into()),
                })
                .await
            {
                Ok(_) => return Ok(()),
                Err(status) => {
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, "Retrying failed operation because we learned about the current leader.");
                        continue;
                    }
                    Err(map_status_to_write_error(client.address(), status))
                }
            };
        }
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
            return Err(ProvisionError::NotSupported(format!(
                "Node '{}' does not run the metadata-server role. Try to provision a different node.",
                config.common.advertised_address
            )));
        }

        let mut client = MetadataServerSvcClientWithAddress::new(ChannelWithAddress::new(
            config.common.advertised_address.clone(),
            create_tonic_channel(config.common.advertised_address.clone(), &config.networking),
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
            .map_err(|status| map_status_to_provision_error(client.address(), status));

        if response.is_ok() {
            *self.current_leader.lock() = Some(client);
        }

        response.map(|response| response.into_inner().newly_provisioned)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("No known metadata server")]
struct NoKnownMetadataServer;

fn map_status_to_read_error(address: AdvertisedAddress, status: Status) -> ReadError {
    match &status.code() {
        // Transport errors manifest as unknown statuses, hence mark them as retryable
        // Killing a remote server that is connected via UDS sometimes results into a cancelled statuses, hence mark them as retryable
        Code::Unavailable | Code::Unknown | Code::Cancelled => {
            ReadError::retryable(StatusError::new(address, status))
        }
        _ => ReadError::terminal(StatusError::new(address, status)),
    }
}

fn map_status_to_write_error(address: AdvertisedAddress, status: Status) -> WriteError {
    match &status.code() {
        // Transport errors manifest as unknown statuses, hence mark them as retryable
        // Killing a remote server that is connected via UDS sometimes results into a cancelled statuses, hence mark them as retryable
        Code::Unavailable | Code::Unknown | Code::Cancelled => {
            WriteError::retryable(StatusError::new(address, status))
        }
        Code::FailedPrecondition => {
            WriteError::FailedPrecondition(format!("[{address}] {}", status.message()))
        }
        _ => WriteError::terminal(StatusError::new(address, status)),
    }
}

fn map_status_to_provision_error(address: AdvertisedAddress, status: Status) -> ProvisionError {
    match &status.code() {
        // Transport errors manifest as unknown statuses, hence mark them as retryable
        // Killing a remote server that is connected via UDS sometimes results into a cancelled statuses, hence mark them as retryable
        Code::Unavailable | Code::Unknown | Code::Cancelled => {
            ProvisionError::retryable(StatusError::new(address, status))
        }
        _ => ProvisionError::terminal(StatusError::new(address, status)),
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("[{address}] {status}")]
struct StatusError {
    address: AdvertisedAddress,
    status: Status,
}

impl StatusError {
    fn new(address: AdvertisedAddress, status: Status) -> Self {
        Self { address, status }
    }
}

#[derive(Clone)]
struct ChannelManager {
    channels: Arc<Mutex<Channels>>,
    connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
}

impl ChannelManager {
    fn new(
        initial_addresses: Vec<AdvertisedAddress>,
        connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
    ) -> Self {
        let initial_channels: Vec<_> = initial_addresses
            .into_iter()
            .map(|address| {
                ChannelWithAddress::new(
                    address.clone(),
                    create_tonic_channel(address, connection_options.deref()),
                )
            })
            .collect();

        ChannelManager {
            channels: Arc::new(Mutex::new(Channels::new(initial_channels))),
            connection_options,
        }
    }

    fn register_address(
        &self,
        plain_node_id: PlainNodeId,
        address: AdvertisedAddress,
    ) -> ChannelWithAddress {
        let channel = ChannelWithAddress::new(
            address.clone(),
            create_tonic_channel(address, self.connection_options.deref()),
        );
        self.channels
            .lock()
            .register(plain_node_id, channel.clone());

        channel
    }

    fn choose_channel(&self) -> Option<ChannelWithAddress> {
        self.channels.lock().choose_next_round_robin()
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
                        ChannelWithAddress::new(
                            node_config.address.clone(),
                            create_tonic_channel(
                                node_config.address.clone(),
                                self.connection_options.deref(),
                            ),
                        ),
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

#[derive(Clone, Debug, derive_more::Deref)]
struct ChannelWithAddress {
    #[deref]
    channel: Channel,
    address: AdvertisedAddress,
}

impl ChannelWithAddress {
    fn new(address: AdvertisedAddress, inner: Channel) -> Self {
        Self {
            channel: inner,
            address,
        }
    }
}

#[derive(Debug)]
struct Channels {
    initial_channels: Vec<ChannelWithAddress>,
    channels: IndexMap<PlainNodeId, ChannelWithAddress>,
    channel_index: usize,
}

impl Channels {
    fn new(initial_channels: Vec<ChannelWithAddress>) -> Self {
        assert!(!initial_channels.is_empty());
        let initial_index = rand::rng().random_range(..initial_channels.len());
        Channels {
            initial_channels,
            channels: IndexMap::default(),
            channel_index: initial_index,
        }
    }

    fn register(&mut self, plain_node_id: PlainNodeId, channel: ChannelWithAddress) {
        self.channels.insert(plain_node_id, channel);
    }

    fn choose_next_round_robin(&mut self) -> Option<ChannelWithAddress> {
        let num_channels = self.channels.len();

        self.channel_index += 1;
        if self.channel_index >= num_channels + self.initial_channels.len() {
            self.channel_index = 0;
        }

        if self.channel_index < num_channels {
            self.channels
                .get_index(self.channel_index)
                .map(|(_, channel)| channel)
                .cloned()
        } else if !self.initial_channels.is_empty() {
            self.initial_channels
                .get(self.channel_index - num_channels)
                .cloned()
        } else {
            None
        }
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

    fn update_channels(
        &mut self,
        new_channels: impl IntoIterator<Item = (PlainNodeId, ChannelWithAddress)>,
    ) {
        self.channels.clear();
        self.channels.extend(new_channels);
    }
}

#[cfg(test)]
mod tests {
    use restate_types::{PlainNodeId, net::AdvertisedAddress};
    use test_log::test;
    use tonic::transport::Channel;

    use super::{ChannelWithAddress, Channels};

    #[test]
    #[should_panic(expected = "!initial_channels.is_empty()")]
    fn empty_initial_channels() {
        Channels::new(vec![]);
    }

    #[test(restate_core::test)]
    async fn update_channels() {
        let initial_addr: AdvertisedAddress = "http://localhost".parse().unwrap();
        let mut channels = Channels::new(vec![ChannelWithAddress::new(
            initial_addr.clone(),
            Channel::from_static("http://localhost").connect_lazy(),
        )]);

        assert!(channels.choose_next_round_robin().is_some());

        // Define node addresses for easier comparison later
        let node1_addr: AdvertisedAddress = "http://node1".parse().unwrap();
        let node2_addr: AdvertisedAddress = "http://node2".parse().unwrap();
        let node3_addr: AdvertisedAddress = "http://node3".parse().unwrap();

        channels.update_channels(vec![
            (
                PlainNodeId::new(1),
                ChannelWithAddress::new(
                    node1_addr.clone(),
                    Channel::from_static("http://node1").connect_lazy(),
                ),
            ),
            (
                PlainNodeId::new(2),
                ChannelWithAddress::new(
                    node2_addr.clone(),
                    Channel::from_static("http://node2").connect_lazy(),
                ),
            ),
            (
                PlainNodeId::new(3),
                ChannelWithAddress::new(
                    node3_addr.clone(),
                    Channel::from_static("http://node3").connect_lazy(),
                ),
            ),
        ]);

        let mut seen_addresses = Vec::new();
        for _ in 0..4 {
            if let Some(channel) = channels.choose_next_round_robin() {
                seen_addresses.push(channel.address);
            }
        }

        assert!(seen_addresses.contains(&initial_addr));
        assert!(seen_addresses.contains(&node1_addr));
        assert!(seen_addresses.contains(&node2_addr));
        assert!(seen_addresses.contains(&node3_addr));

        channels.drop_initial_channels();

        seen_addresses.clear();
        for _ in 0..3 {
            if let Some(channel) = channels.choose_next_round_robin() {
                seen_addresses.push(channel.address);
            }
        }

        assert!(seen_addresses.contains(&node1_addr));
        assert!(seen_addresses.contains(&node2_addr));
        assert!(seen_addresses.contains(&node3_addr));

        channels.update_channels(vec![]);

        assert!(channels.choose_next_round_robin().is_none());
    }
}
