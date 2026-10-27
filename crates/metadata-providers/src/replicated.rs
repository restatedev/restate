// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use bytestring::ByteString;
use indexmap::IndexMap;
use parking_lot::Mutex;
use restate_types::retries::RetryPolicy;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::{debug, instrument, trace};

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::network::select_ctrl_address;
use restate_core::{Metadata, TaskCenter, TaskKind, cancellation_watcher};
use restate_metadata_server_grpc::grpc::metadata_server_svc_client::MetadataServerSvcClient;
use restate_metadata_server_grpc::grpc::new_metadata_server_client;
use restate_metadata_store::{
    MetadataStore, MetadataStoreClient, ProvisionError, ReadError, WriteError,
};
use restate_types::config::Configuration;
use restate_types::errors::{ConversionError, SimpleStatus, is_retryable_status};
use restate_types::metadata::{Precondition, VersionedValue};
use restate_types::net::address::{AdvertisedAddress, ControlPort};
use restate_types::net::connect_opts::{CommonClientConnectionOptions, GrpcConnectionOptions};
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{
    ClusterFingerprint, MetadataServerState, NodesConfiguration, Role,
};
use restate_types::storage::StorageCodec;
use restate_types::{PlainNodeId, Version};

use restate_metadata_server_grpc::grpc::{DeleteRequest, GetRequest, ProvisionRequest, PutRequest};

const MAX_RETRY_ATTEMPTS: usize = 3;
pub const KNOWN_LEADER_KEY: &str = "x-restate-known-leader";

/// Creates the [`MetadataStoreClient`] for the replicated metadata server.
pub fn create_replicated_metadata_client(
    addresses: Vec<AdvertisedAddress<ControlPort>>,
    backoff_policy: Option<RetryPolicy>,
    connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
) -> MetadataStoreClient {
    let inner_client = GrpcMetadataServerClient::new(addresses, connection_options);
    MetadataStoreClient::new(inner_client, backoff_policy)
}

#[derive(Debug, Clone, derive_more::Deref, derive_more::DerefMut)]
struct MetadataServerSvcClientWithAddress {
    #[deref]
    #[deref_mut]
    client: MetadataServerSvcClient<Channel>,
    address: AdvertisedAddress<ControlPort>,
}

impl MetadataServerSvcClientWithAddress {
    fn new<O>(channel: ChannelWithAddress, connection_options: &O) -> Self
    where
        O: GrpcConnectionOptions + Send + Sync + ?Sized,
    {
        let address = channel.address;
        Self {
            client: new_metadata_server_client(channel.channel, connection_options),
            address,
        }
    }

    fn address(&self) -> AdvertisedAddress<ControlPort> {
        self.address.clone()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KnownLeader {
    pub node_id: PlainNodeId,
    pub address: AdvertisedAddress<ControlPort>,
}

impl KnownLeader {
    /// The address to dial this leader at: the one selected for it when `nodes_config` knows the
    /// node, else the plain control address the hint carries, which every receiver can read.
    pub fn dial_address(
        &self,
        nodes_config: &NodesConfiguration,
    ) -> AdvertisedAddress<ControlPort> {
        nodes_config.find_node_by_id(self.node_id).map_or_else(
            |_| self.address.clone(),
            |node| select_ctrl_address(node).into_owned(),
        )
    }

    pub fn add_to_status(&self, status: &mut tonic::Status) {
        status.metadata_mut().insert(
            KNOWN_LEADER_KEY,
            serde_json::to_string(self)
                .expect("KnownLeader to be serializable")
                .parse()
                .expect("to be valid metadata"),
        );
    }

    pub fn from_status(status: &tonic::Status) -> Option<KnownLeader> {
        if let Some(value) = status.metadata().get(KNOWN_LEADER_KEY) {
            match value.to_str() {
                Ok(value) => match serde_json::from_str(value) {
                    Ok(known_leader) => Some(known_leader),
                    Err(err) => {
                        debug!("failed parsing known leader from metadata: {err}");
                        None
                    }
                },
                Err(err) => {
                    debug!("failed parsing known leader from metadata: {err}");
                    None
                }
            }
        } else {
            None
        }
    }
}

/// Client end to interact with a set of metadata servers.
#[derive(Clone)]
pub struct GrpcMetadataServerClient {
    channel_manager: ChannelManager,
    connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
    current_leader: Arc<Mutex<Option<MetadataServerSvcClientWithAddress>>>,
}

impl GrpcMetadataServerClient {
    pub fn new(
        metadata_store_addresses: Vec<AdvertisedAddress<ControlPort>>,
        connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
    ) -> Self {
        let channel_manager =
            ChannelManager::new(metadata_store_addresses, Arc::clone(&connection_options));
        let svc_client = Arc::new(Mutex::new(channel_manager.choose_channel().map(
            |channel| MetadataServerSvcClientWithAddress::new(channel, connection_options.deref()),
        )));

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
            connection_options,
            current_leader: svc_client,
        }
    }

    fn choose_random_endpoint(&self) {
        // let's try another endpoint
        *self.current_leader.lock() = self.channel_manager.choose_channel().map(|channel| {
            MetadataServerSvcClientWithAddress::new(channel, self.connection_options.deref())
        });
    }

    fn choose_known_leader(&self, known_leader: KnownLeader) {
        let channel = self.channel_manager.register_known_leader(known_leader);
        *self.current_leader.lock() = Some(MetadataServerSvcClientWithAddress::new(
            channel,
            self.connection_options.deref(),
        ));
    }

    fn current_client(&self) -> Option<MetadataServerSvcClientWithAddress> {
        let mut svc_client_guard = self.current_leader.lock();

        if svc_client_guard.is_none() {
            *svc_client_guard = self.channel_manager.choose_channel().map(|channel| {
                MetadataServerSvcClientWithAddress::new(channel, self.connection_options.deref())
            });
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

/// Returns the cluster identity for metadata store requests.
/// If the cluster fingerprint isn't available yet (not valid NodesConfiguration), then it skips it.
fn cluster_identity() -> (String, Option<ClusterFingerprint>) {
    // use try_with_current to allow this method be called from outside of the TaskCenter context
    let cluster_fingerprint = TaskCenter::try_with_current(|handle| {
        handle
            .metadata()
            .and_then(|m| m.nodes_config_ref().cluster_fingerprint())
    })
    .flatten();

    (
        Configuration::pinned().common.cluster_name().to_owned(),
        cluster_fingerprint,
    )
}

#[async_trait]
impl MetadataStore for GrpcMetadataServerClient {
    #[instrument(level = "debug", skip_all, fields(%key))]
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let mut attempt = 0;
        loop {
            let mut client = self
                .current_client()
                .ok_or_else(|| ReadError::terminal(NoKnownMetadataServer))?;

            let (cluster_name, cluster_fingerprint) = cluster_identity();

            trace!(attempt, %client.address, "Sending request");
            return match client
                .get(GetRequest {
                    key: key.clone().into(),
                    cluster_fingerprint: cluster_fingerprint.map(|f| f.to_u64()).unwrap_or(0),
                    cluster_name: Some(cluster_name),
                })
                .await
            {
                Ok(response) => {
                    trace!(attempt, %client.address, "success");
                    response
                        .into_inner()
                        .try_into()
                        .map_err(|err: ConversionError| ReadError::terminal(err))
                }
                Err(status) => {
                    trace!(attempt, ?status, %client.address, "received error");
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, %client.address, "Retrying failed operation because we learned about the current leader");
                        continue;
                    }
                    Err(map_status_to_read_error(client.address(), status))
                }
            };
        }
    }

    #[instrument(level = "debug", skip_all, fields(%key))]
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let mut attempt = 0;
        loop {
            let mut client = self
                .current_client()
                .ok_or_else(|| ReadError::terminal(NoKnownMetadataServer))?;

            let (cluster_name, cluster_fingerprint) = cluster_identity();

            trace!(attempt, %client.address, "Sending request");
            return match client
                .get_version(GetRequest {
                    key: key.clone().into(),
                    cluster_fingerprint: cluster_fingerprint.map(|f| f.to_u64()).unwrap_or(0),
                    cluster_name: Some(cluster_name),
                })
                .await
            {
                Ok(response) => {
                    trace!(attempt, %client.address, "success");
                    return Ok(response.into_inner().into());
                }
                Err(status) => {
                    trace!(attempt, ?status, %client.address, "received error");
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, %client.address, "Retrying failed operation because we learned about the current leader");
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

            let (cluster_name, cluster_fingerprint) = cluster_identity();

            trace!(attempt, %client.address, "Sending request");
            return match client
                .put(PutRequest {
                    key: key.clone().into(),
                    value: Some(value.clone().into()),
                    precondition: Some(precondition.into()),
                    cluster_fingerprint: cluster_fingerprint.map(|f| f.to_u64()).unwrap_or(0),
                    cluster_name: Some(cluster_name),
                })
                .await
            {
                Ok(_) => {
                    trace!(attempt, %client.address, "success");
                    return Ok(());
                }
                Err(status) => {
                    trace!(attempt, ?status, %client.address, "received error");
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, %client.address, "Retrying failed operation because we learned about the current leader");
                        continue;
                    }
                    Err(map_status_to_write_error(client.address(), status))
                }
            };
        }
    }

    #[instrument(level = "debug", skip_all, fields(%key))]
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let mut attempt = 0;
        loop {
            let mut client = self
                .current_client()
                .ok_or_else(|| WriteError::terminal(NoKnownMetadataServer))?;

            let (cluster_name, cluster_fingerprint) = cluster_identity();

            trace!(attempt, %client.address, "Sending request");

            return match client
                .delete(DeleteRequest {
                    key: key.clone().into(),
                    precondition: Some(precondition.into()),
                    cluster_fingerprint: cluster_fingerprint.map(|f| f.to_u64()).unwrap_or(0),
                    cluster_name: Some(cluster_name),
                })
                .await
            {
                Ok(_) => {
                    trace!(attempt, %client.address, "success");
                    return Ok(());
                }
                Err(status) => {
                    trace!(attempt, ?status, %client.address, "received error");
                    // try again if the error response contains information about the known leader,
                    // and we have an attempt left
                    if self.has_known_leader(&status) && attempt < MAX_RETRY_ATTEMPTS {
                        attempt += 1;
                        debug!(%attempt, %status, %client.address, "Retrying failed operation because we learned about the current leader");
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

        let advertised_address =
            TaskCenter::with_current(|tc| config.own_control_address(tc.address_book()));
        if !config.common.roles.contains(Role::MetadataServer) {
            return Err(ProvisionError::NotSupported(format!(
                "Node '{}' does not run the metadata-server role. Try to provision a different node.",
                advertised_address
            )));
        }

        let mut client = MetadataServerSvcClientWithAddress::new(
            ChannelWithAddress::new(
                advertised_address.clone(),
                create_tonic_channel(
                    advertised_address.clone(),
                    self.connection_options.deref(),
                    DNSResolution::Gai,
                ),
            ),
            self.connection_options.deref(),
        );

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

fn map_status_to_read_error(address: AdvertisedAddress<ControlPort>, status: Status) -> ReadError {
    if is_retryable_status(&status) {
        ReadError::retryable(StatusError::new(address, status))
    } else {
        ReadError::terminal(StatusError::new(address, status))
    }
}

fn map_status_to_write_error(
    address: AdvertisedAddress<ControlPort>,
    status: Status,
) -> WriteError {
    if is_retryable_status(&status) {
        WriteError::retryable(StatusError::new(address, status))
    } else if status.code() == Code::FailedPrecondition {
        WriteError::FailedPrecondition(format!("[{address}] {}", status.message()))
    } else {
        WriteError::terminal(StatusError::new(address, status))
    }
}

fn map_status_to_provision_error(
    address: AdvertisedAddress<ControlPort>,
    status: Status,
) -> ProvisionError {
    if is_retryable_status(&status) {
        ProvisionError::retryable(StatusError::new(address, status))
    } else {
        ProvisionError::terminal(StatusError::new(address, status))
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("[{address}] {status}")]
struct StatusError {
    address: AdvertisedAddress<ControlPort>,
    status: SimpleStatus,
}

impl StatusError {
    fn new(address: AdvertisedAddress<ControlPort>, status: Status) -> Self {
        Self {
            address,
            status: SimpleStatus(status),
        }
    }
}

#[derive(Clone)]
struct ChannelManager {
    channels: Arc<Mutex<Channels>>,
    connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
}

impl ChannelManager {
    fn new(
        initial_addresses: Vec<AdvertisedAddress<ControlPort>>,
        connection_options: Arc<dyn CommonClientConnectionOptions + Send + Sync>,
    ) -> Self {
        ChannelManager {
            channels: Arc::new(Mutex::new(Channels::new(initial_addresses))),
            connection_options,
        }
    }

    /// The channel to the leader a server pointed at, dialed at its [`KnownLeader::dial_address`];
    /// a channel that already dials it is kept.
    fn register_known_leader(&self, known_leader: KnownLeader) -> ChannelWithAddress {
        let node_id = known_leader.node_id;
        let address = Metadata::try_with_current(|metadata| {
            known_leader.dial_address(&metadata.nodes_config_ref())
        })
        .unwrap_or(known_leader.address);
        let mut channels = self.channels.lock();
        if let Some(channel) = channels.channels.get(&node_id)
            && channel.address == address
        {
            return channel.clone();
        }
        let channel = ChannelWithAddress::new(
            address.clone(),
            create_tonic_channel(address, self.connection_options.deref(), DNSResolution::Gai),
        );
        channels.register(node_id, channel.clone());
        channel
    }

    fn choose_channel(&self) -> Option<ChannelWithAddress> {
        self.channels
            .lock()
            .choose_next(&mut rand::rng())
            .map(|c| c.into_channel(self.connection_options.deref()))
    }

    /// Watches the [`NodesConfiguration`] and updates the channels based on which nodes run the
    /// metadata store role.
    async fn run(self, metadata: Metadata) -> anyhow::Result<()> {
        let mut nodes_config_watch = metadata.watch(MetadataKind::NodesConfiguration);
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
            .iter_role(Role::MetadataServer)
            .filter(|(_, node_config)| {
                matches!(
                    node_config.metadata_server_config.metadata_server_state,
                    MetadataServerState::Member
                )
            })
            .map(|(node_id, node_config)| {
                let address = select_ctrl_address(node_config).into_owned();
                debug!(%node_id, %address, "Metadata server channel");
                (
                    node_id,
                    ChannelWithAddress::new(
                        address.clone(),
                        create_tonic_channel(
                            address,
                            self.connection_options.deref(),
                            DNSResolution::Gai,
                        ),
                    ),
                )
            });

        let mut channels = self.channels.lock();

        channels.update_channels(new_channels);

        if has_joined_cluster && !channels.is_empty() {
            // Only drop initial channels if we have found others. This is to handle the case where
            // we are resuming from an older Restate version which didn't write the
            // MetadataServerState properly.
            channels.drop_initial_addresses();
        }
    }
}

#[derive(Clone, Debug, derive_more::Deref)]
struct ChannelWithAddress {
    #[deref]
    channel: Channel,
    address: AdvertisedAddress<ControlPort>,
}

impl ChannelWithAddress {
    fn new(address: AdvertisedAddress<ControlPort>, inner: Channel) -> Self {
        Self {
            channel: inner,
            address,
        }
    }
}

#[derive(Clone, Debug, derive_more::From)]
enum ChannelOrInitialAddress {
    InitialAddress(#[from] AdvertisedAddress<ControlPort>),
    Channel(#[from] ChannelWithAddress),
}

impl ChannelOrInitialAddress {
    fn into_channel<T: CommonClientConnectionOptions + Send + Sync + ?Sized>(
        self,
        connection_options: &T,
    ) -> ChannelWithAddress {
        match self {
            ChannelOrInitialAddress::InitialAddress(address) => ChannelWithAddress::new(
                address.clone(),
                create_tonic_channel(
                    address,
                    connection_options,
                    // initial addresses may be headless, ie they point to multiple nodes
                    DNSResolution::Headless,
                ),
            ),
            ChannelOrInitialAddress::Channel(channel) => channel,
        }
    }

    #[allow(dead_code)]
    fn address(&self) -> &AdvertisedAddress<ControlPort> {
        match self {
            ChannelOrInitialAddress::InitialAddress(address) => address,
            ChannelOrInitialAddress::Channel(channel) => &channel.address,
        }
    }
}

#[derive(Debug)]
struct Channels {
    // initial addresses we will keep in address form and resolve+connect every time they are used
    // this is to allow for the initial addresses being dns entries that resolve to multiple node IPs
    initial_addresses: Vec<AdvertisedAddress<ControlPort>>,
    channels: IndexMap<PlainNodeId, ChannelWithAddress>,
    last: Option<AdvertisedAddress<ControlPort>>,
}

impl Channels {
    fn new(initial_addresses: Vec<AdvertisedAddress<ControlPort>>) -> Self {
        assert!(!initial_addresses.is_empty());
        Channels {
            initial_addresses,
            channels: IndexMap::default(),
            last: None,
        }
    }

    fn register(&mut self, plain_node_id: PlainNodeId, channel: ChannelWithAddress) {
        self.channels.insert(plain_node_id, channel);
    }

    fn get(&self, i: usize) -> Option<ChannelOrInitialAddress> {
        let num_channels = self.channels.len();

        if i < num_channels {
            Some(ChannelOrInitialAddress::from(self.channels[i].clone()))
        } else if i < num_channels + self.initial_addresses.len() {
            Some(ChannelOrInitialAddress::from(
                self.initial_addresses[i - num_channels].clone(),
            ))
        } else {
            None
        }
    }

    fn choose_next(&mut self, rng: &mut impl rand::Rng) -> Option<ChannelOrInitialAddress> {
        // sample up to two distinct channels/initial addresses from the full list
        let mut random_channels = rand::seq::IteratorRandom::sample(
            0..(self.channels.len() + self.initial_addresses.len()),
            rng,
            2,
        );

        // choose_multiple picks random items but the order is not random
        rand::seq::SliceRandom::shuffle(random_channels.as_mut_slice(), rng);

        let mut random_channels = random_channels
            .into_iter()
            .map(|channel_index| self.get(channel_index).unwrap());

        let next = match random_channels.next() {
            Some(channel) if Some(channel.address()) == self.last.as_ref() => {
                // same as last time, try another if available
                Some(random_channels.next().unwrap_or(channel))
            }
            Some(channel) => Some(channel), // different to last time
            None => None,                   // no channels
        };

        self.last = next.as_ref().map(|n| n.address().clone());
        next
    }

    /// Returns true if there are no channels. It ignores the initial channels.
    fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    fn drop_initial_addresses(&mut self) {
        if !self.initial_addresses.is_empty() {
            self.initial_addresses = Vec::new();
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
    use std::collections::HashSet;
    use std::sync::Arc;

    use rand::SeedableRng;
    use test_log::test;
    use tonic::transport::Channel;

    use restate_core::TestCoreEnvBuilder;
    use restate_types::config::{
        CommonOptionsBuilder, Configuration, NetworkingOptions, set_current_config,
    };
    use restate_types::locality::NodeLocation;
    use restate_types::net::address::AdvertisedAddress;
    use restate_types::nodes_config::{
        ClusterFingerprint, NodeConfig, NodesConfiguration, QualifiedAddress, Role,
    };
    use restate_types::{GenerationalNodeId, PlainNodeId, RestateVersion, Version};

    use super::{ChannelManager, ChannelWithAddress, Channels, KnownLeader};

    #[test(restate_core::test)]
    async fn known_leaders_are_dialed_at_their_selected_address() {
        let common = CommonOptionsBuilder::default()
            .location("us-east1.az1".parse::<NodeLocation>().unwrap())
            .build()
            .unwrap();
        set_current_config(Configuration {
            common,
            ..Default::default()
        });
        let node = |id: u32, name: &str, ctrl_addresses: Vec<QualifiedAddress<_>>| {
            NodeConfig::builder()
                .name(name.to_owned())
                .current_generation(GenerationalNodeId::new(id, 1))
                .address(format!("http://{name}:5122").parse().unwrap())
                .ctrl_address(Some(format!("http://{name}:5122").parse().unwrap()))
                .ctrl_addresses(ctrl_addresses)
                .roles(Role::MetadataServer.into())
                .binary_version(RestateVersion::current())
                .build()
        };
        let mut nodes = NodesConfiguration::new(
            Version::MIN,
            "hints".to_owned(),
            ClusterFingerprint::generate(),
        );
        nodes.upsert_node(node(1, "me", vec![]));
        nodes.upsert_node(node(
            2,
            "leader",
            vec![QualifiedAddress {
                address: "http://10.1.7.2:5122".parse().unwrap(),
                location: Some("us-east1.az1".parse().unwrap()),
            }],
        ));
        let _env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes)
            .build()
            .await;
        let manager = ChannelManager::new(
            vec!["http://seed:5122".parse().unwrap()],
            Arc::new(NetworkingOptions::default()),
        );
        let hint = |node_id: u32| KnownLeader {
            node_id: PlainNodeId::new(node_id),
            address: "http://leader:5122".parse().unwrap(),
        };

        // A known node is dialed at the address selected for this process, not the hint's; an
        // unknown node where the hint says.
        assert_eq!(
            manager.register_known_leader(hint(2)).address.to_string(),
            "http://10.1.7.2:5122/"
        );
        assert_eq!(
            manager.register_known_leader(hint(9)).address.to_string(),
            "http://leader:5122/"
        );
    }

    #[test]
    #[should_panic(expected = "!initial_addresses.is_empty()")]
    fn empty_initial_channels() {
        Channels::new(vec![]);
    }

    #[test(restate_core::test)]
    async fn update_channels() {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(4360796539057359171);
        let initial_addr: AdvertisedAddress<_> = "http://localhost".parse().unwrap();
        let mut channels = Channels::new(vec![initial_addr.clone()]);

        assert!(channels.choose_next(&mut rng).is_some());

        // Define node addresses for easier comparison later
        let node1_addr: AdvertisedAddress<_> = "http://node1".parse().unwrap();
        let node2_addr: AdvertisedAddress<_> = "http://node2".parse().unwrap();
        let node3_addr: AdvertisedAddress<_> = "http://node3".parse().unwrap();

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

        let mut seen_addresses = HashSet::new();

        let mut last = None;

        for _ in 0..50 {
            let next_address = channels
                .choose_next(&mut rng)
                .as_ref()
                .map(|c| c.address().clone());
            assert!(next_address.is_some());
            assert!(last != next_address); // check that we never get a repeat
            last = next_address.clone();
            seen_addresses.insert(next_address.unwrap());
        }

        // check that all values turn up eventually
        assert!(seen_addresses.contains(&initial_addr));
        assert!(seen_addresses.contains(&node1_addr));
        assert!(seen_addresses.contains(&node2_addr));
        assert!(seen_addresses.contains(&node3_addr));

        channels.drop_initial_addresses();

        seen_addresses.clear();
        last = None;

        for _ in 0..50 {
            let next_address = channels
                .choose_next(&mut rng)
                .as_ref()
                .map(|c| c.address().clone());
            assert!(next_address.is_some());
            assert!(last != next_address); // check that we never get a repeat
            last = next_address.clone();
            seen_addresses.insert(next_address.unwrap());
        }

        // check that all values turn up eventually
        assert!(!seen_addresses.contains(&initial_addr));
        assert!(seen_addresses.contains(&node1_addr));
        assert!(seen_addresses.contains(&node2_addr));
        assert!(seen_addresses.contains(&node3_addr));

        channels.update_channels(vec![]);

        assert!(channels.choose_next(&mut rng).is_none());
    }
}
