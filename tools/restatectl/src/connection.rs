// Copyright (c) 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::{cmp::Ordering, fmt::Display, sync::Arc};

use cling::{Collect, prelude::Parser};
use itertools::{Either, Itertools, Position};
use rand::{rng, seq::SliceRandom};
use tokio::sync::{Mutex, MutexGuard};
use tonic::{Code, Status, transport::Channel};
use tracing::{debug, info};

use crate::util::grpc_channel;
use restate_core::protobuf::node_ctl_svc::{
    GetMetadataRequest, IdentResponse, new_node_ctl_client,
};
use restate_metadata_store::ReadModifyWriteError;
use restate_types::partition_table::PartitionTable;
use restate_types::{
    Version, Versioned,
    errors::SimpleStatus,
    logs::metadata::Logs,
    net::AdvertisedAddress,
    nodes_config::{NodesConfiguration, Role},
    protobuf::common::{MetadataKind, NodeStatus},
    storage::{StorageCodec, StorageDecode, StorageDecodeError},
};

#[derive(Clone, Parser, Collect, Debug)]
pub struct ConnectionInfo {
    /// Specify one or more server addresses to connect to.
    ///
    /// Needs access to the node-to-node address (aka node advertised address).
    /// Specify multiple addresses as a comma-separated list, or pass multiple
    /// `--address=<host>` arguments. Additional addresses may be discovered
    /// based on the configuration of reachable nodes.
    #[clap(
        long,
        short('s'),
        visible_alias("server"),
        alias("addresses"),
        value_hint = clap::ValueHint::Url,
        default_value = "http://localhost:5122/",
        env = "RESTATECTL_ADDRESS",
        global = true,
        value_delimiter = ',',
    )]
    pub address: Vec<AdvertisedAddress>,

    #[clap(skip)]
    nodes_configuration: Arc<Mutex<Option<NodesConfiguration>>>,

    #[clap(skip)]
    logs: Arc<Mutex<Option<Logs>>>,

    #[clap(skip)]
    partition_table: Arc<Mutex<Option<PartitionTable>>>,

    #[clap(skip)]
    open_connections: Arc<Mutex<HashMap<AdvertisedAddress, Channel>>>,

    #[clap(skip)]
    dead_nodes: Arc<RwLock<HashSet<AdvertisedAddress>>>,
}

impl ConnectionInfo {
    /// Gets NodesConfiguration object. Tries all provided addresses and caches the
    /// response. Always uses the address seed provided on the command line.
    pub async fn get_nodes_configuration(&self) -> Result<NodesConfiguration, ConnectionInfoError> {
        if self.address.is_empty() {
            return Err(ConnectionInfoError::NoAvailableNodes(NoRoleError(None)));
        }

        let guard = self.nodes_configuration.lock().await;
        if guard.is_some() {
            debug!("Using cached nodes configuration");
        }

        self.get_latest_metadata(
            self.address.iter(),
            self.address.len(),
            MetadataKind::NodesConfiguration,
            guard,
            |ident| Version::from(ident.nodes_config_version),
        )
        .await
    }

    /// Gets Logs object.
    ///
    /// This function will try multiple nodes learned from nodes_configuration
    /// to get the best guess of the latest logs version is.
    pub async fn get_logs(&self) -> Result<Logs, ConnectionInfoError> {
        let nodes_config = self.get_nodes_configuration().await?;

        let guard = self.logs.lock().await;

        let mut nodes_addresses = nodes_config
            .iter()
            .map(|(_, node)| &node.address)
            .collect::<Vec<_>>();

        nodes_addresses.shuffle(&mut rng());

        let cluster_size = nodes_addresses.len();
        let cached = self
            .open_connections
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        assert!(!cached.is_empty(), "must have cached connections");

        // To improve our chance of getting the latest logs definition, we read from a simple
        // majority of nodes. Existing connections take precedence.
        let logs_source_nodes = cached
            .iter()
            .chain(
                nodes_addresses
                    .into_iter()
                    .filter(|address| !cached.contains(address)),
            )
            .collect::<Vec<_>>();

        self.get_latest_metadata(
            logs_source_nodes.into_iter(),
            (cluster_size / 2) + 1,
            MetadataKind::Logs,
            guard,
            |ident| Version::from(ident.logs_version),
        )
        .await
    }

    pub async fn get_partition_table(&self) -> Result<PartitionTable, ConnectionInfoError> {
        let nodes_config = self.get_nodes_configuration().await?;

        let guard = self.partition_table.lock().await;

        let mut nodes_addresses = nodes_config
            .iter()
            .map(|(_, node)| &node.address)
            .collect::<Vec<_>>();

        nodes_addresses.shuffle(&mut rng());

        let cluster_size = nodes_addresses.len();
        let cached = self
            .open_connections
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        assert!(!cached.is_empty(), "must have cached connections");

        // To improve our chance of getting the latest partition table, we read from a simple
        // majority of nodes. Existing connections take precedence.
        let partition_table_nodes = cached
            .iter()
            .chain(
                nodes_addresses
                    .into_iter()
                    .filter(|address| !cached.contains(address)),
            )
            .collect::<Vec<_>>();

        self.get_latest_metadata(
            partition_table_nodes.into_iter(),
            (cluster_size / 2) + 1,
            MetadataKind::PartitionTable,
            guard,
            |ident| Version::from(ident.partition_table_version),
        )
        .await
    }

    /// Gets the latest metadata value. Stops after `stop_after_responses` nodes
    /// respond, otherwise keeps trying until all addresses are exhausted.
    async fn get_latest_metadata<T, M>(
        &self,
        addresses: impl Iterator<Item = &AdvertisedAddress>,
        mut stop_after_responses: usize,
        kind: MetadataKind,
        mut guard: MutexGuard<'_, Option<T>>,
        extract_version: M,
    ) -> Result<T, ConnectionInfoError>
    where
        T: StorageDecode + Versioned + Clone,
        M: Fn(&IdentResponse) -> Version,
    {
        if let Some(meta) = &*guard {
            return Ok(meta.clone());
        }

        let mut latest_value: Option<T> = None;
        let mut ident_responses = HashMap::new();
        let mut any_node_responded = false;
        let mut errors = NodesErrors::default();
        let mut open_connections = self.open_connections.lock().await;

        let request = GetMetadataRequest { kind: kind.into() };

        for address in addresses {
            let channel = open_connections.entry(address.clone()).or_insert_with(|| {
                info!("Connecting to {address}");
                grpc_channel(address.clone())
            });

            let mut client = new_node_ctl_client(channel.clone());

            let response = match client.get_ident(()).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    errors.error(address.clone(), SimpleStatus::from(status));
                    self.dead_nodes.write().unwrap().insert(address.clone());
                    continue;
                }
            };
            ident_responses.insert(address.clone(), response.clone());

            any_node_responded = true;
            if response.status != NodeStatus::Alive as i32 {
                debug!(
                    "Node {address} responded to GetIdent but it is not reporting itself as alive, and will be skipped"
                );
                continue;
            }

            let response_version = extract_version(&response);
            match response_version.cmp(
                &latest_value
                    .as_ref()
                    .map(|c| c.version())
                    .unwrap_or(Version::INVALID),
            ) {
                Ordering::Less => {
                    debug!(
                        "Node {address} returned an older version {response_version} than we currently have"
                    );
                    continue;
                }
                Ordering::Equal => continue,
                Ordering::Greater => {
                    debug!(
                        "Node {address} returned a newer version {response_version} than we currently have"
                    );
                }
            }

            let mut response = match client.get_metadata(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    errors.error(address.clone(), SimpleStatus::from(status));
                    continue;
                }
            };

            let meta = StorageCodec::decode::<T, _>(&mut response.encoded)
                .map_err(|err| ConnectionInfoError::DecoderError(address.clone(), err))?;

            if meta.version()
                > latest_value
                    .as_ref()
                    .map(|c| c.version())
                    .unwrap_or(Version::INVALID)
            {
                latest_value = Some(meta);
            }

            stop_after_responses -= 1;
            if stop_after_responses == 0 {
                break;
            }
        }

        if !any_node_responded {
            return Err(ConnectionInfoError::NodesErrors(errors));
        }

        *guard = latest_value.clone();
        latest_value.ok_or(ConnectionInfoError::MetadataValueNotAvailable {
            contacted_nodes: ident_responses,
        })
    }

    /// Attempts to contact each node in the cluster that matches the specified role
    /// (or all nodes if `None` is provided). The function returns upon receiving the
    /// first successful response from a node. If an error occurs, the next matching
    /// node is tried.
    ///
    /// The provided closure is responsible for executing the request using the given
    /// channel to the node and returning the result of the response.
    ///
    /// Returns an error if:
    /// - No nodes match the requested role.
    /// - All nodes return an error.
    pub async fn try_each<F, T, E, Fut>(
        &self,
        role: Option<Role>,
        mut node_operation: F,
    ) -> Result<T, ConnectionInfoError>
    where
        F: FnMut(Channel) -> Fut,
        E: Into<NodeOperationError>,
        Fut: Future<Output = Result<T, E>>,
    {
        let nodes_config = self.get_nodes_configuration().await?;
        let mut open_connections = self.open_connections.lock().await;

        let iterator = match role {
            Some(role) => Either::Left(nodes_config.iter_role(role)),
            None => Either::Right(nodes_config.iter()),
        }
        .sorted_by(|a, b| {
            // nodes for which we already have open channels get higher precedence.
            match (
                open_connections.contains_key(&a.1.address),
                open_connections.contains_key(&b.1.address),
            ) {
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (_, _) => a.0.cmp(&b.0),
            }
        });

        let mut errors = NodesErrors::default();

        for (_, node) in iterator {
            let channel = self
                .connect_internal(&node.address, &mut open_connections)
                .await;

            if let Some(channel) = channel {
                debug!("Trying {}...", node.address);
                let result = node_operation(channel).await.map_err(Into::into);
                match result {
                    Ok(response) => return Ok(response),
                    Err(NodeOperationError::RetryElsewhere(simple_status)) => {
                        if simple_status.code() == Code::Unavailable
                            || simple_status.code() == Code::DeadlineExceeded
                        {
                            self.dead_nodes
                                .write()
                                .unwrap()
                                .insert(node.address.clone());
                        }
                        errors.error(node.address.clone(), simple_status);
                    }
                    Err(NodeOperationError::Terminal(status)) => {
                        errors.error(node.address.clone(), status);
                        break;
                    }
                }
            } else {
                errors.error(
                    node.address.clone(),
                    SimpleStatus::from(Status::unavailable(format!(
                        "Node {} was previously flagged as unreachable, not attempting to connect",
                        node.address
                    ))),
                );
            }
        }

        if errors.is_empty() {
            Err(ConnectionInfoError::NoAvailableNodes(NoRoleError(role)))
        } else {
            Err(ConnectionInfoError::NodesErrors(errors))
        }
    }

    pub(crate) async fn connect(
        &self,
        address: &AdvertisedAddress,
    ) -> Result<Channel, ConnectionInfoError> {
        self.connect_internal(address, &mut self.open_connections.lock().await)
            .await
            .map(Ok)
            .unwrap_or(Err(ConnectionInfoError::NodeUnreachable))
    }

    /// Creates and returns a (lazy) connection to the specified address, or `None` if this
    /// address was previously flagged as unreachable.
    async fn connect_internal(
        &self,
        address: &AdvertisedAddress,
        open_connections: &mut MutexGuard<'_, HashMap<AdvertisedAddress, Channel>>,
    ) -> Option<Channel> {
        if self.dead_nodes.read().unwrap().contains(address) {
            debug!(
                "Node {address} was previously flagged as unreachable, not attempting to connect"
            );
            return None;
        };

        Some(
            open_connections
                .entry(address.clone())
                .or_insert_with(|| {
                    info!("Adding new connection to {address}");
                    grpc_channel(address.clone())
                })
                .clone(),
        )
    }

    /// Creates and returns a (lazy) connection to any of the addresses specified in the `address`
    /// field. This method fails with [`ConnectionInfoError::NoAvailableNodes`] if it cannot
    /// establish a connection to any of the specified nodes.
    pub(crate) async fn open_connection(&self) -> Result<Channel, ConnectionInfoError> {
        let mut open_connections = self.open_connections.lock().await;

        for address in &self.address {
            if self.dead_nodes.read().unwrap().contains(address) {
                continue;
            }

            if let Some(channel) = self.connect_internal(address, &mut open_connections).await {
                // check whether we can reach the node
                let mut node_client = new_node_ctl_client(channel.clone());
                if node_client.get_ident(()).await.is_err() {
                    // todo maybe retry before marking a node as dead?
                    self.dead_nodes.write().unwrap().insert(address.clone());
                } else {
                    return Ok(channel);
                }
            }
        }

        Err(ConnectionInfoError::NoAvailableNodes(NoRoleError(None)))
    }
}

/// Error type returned by a [`ConnectionInfo::try_each`] node_operation closure
#[derive(Debug, thiserror::Error)]
pub enum NodeOperationError {
    /// An error that can be retried on a different node
    /// but terminal on this node.
    #[error(transparent)]
    RetryElsewhere(SimpleStatus),
    /// Don not retry on any other node(s)
    #[error(transparent)]
    Terminal(SimpleStatus),
}

impl From<Status> for NodeOperationError {
    fn from(value: Status) -> Self {
        match value.code() {
            Code::FailedPrecondition => Self::Terminal(SimpleStatus(value)),
            _ => Self::RetryElsewhere(SimpleStatus(value)),
        }
    }
}

impl<E> From<ReadModifyWriteError<E>> for NodeOperationError
where
    E: ToString,
{
    fn from(value: ReadModifyWriteError<E>) -> Self {
        match value {
            ReadModifyWriteError::FailedOperation(err) => {
                // we don't ever try again
                NodeOperationError::Terminal(SimpleStatus(Status::unknown(err.to_string())))
            }
            ReadModifyWriteError::ReadWrite(err) => {
                // possible node failure, we can try the next reachable node
                NodeOperationError::RetryElsewhere(SimpleStatus(Status::unknown(err.to_string())))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionInfoError {
    /// The requested metadata key could not be retrieved from any known metadata servers.
    /// The servers which did respond are included in the error.
    #[error("Could not retrieve cluster metadata. Has the cluster been provisioned yet?")]
    MetadataValueNotAvailable {
        contacted_nodes: HashMap<AdvertisedAddress, IdentResponse>,
    },

    #[error("The cluster appears to not be provisioned. You can do so with `restatectl provision`")]
    ClusterNotProvisioned,

    #[error("Failed to decode metadata from node {0}: {1}")]
    DecoderError(AdvertisedAddress, StorageDecodeError),

    #[error(transparent)]
    NodesErrors(NodesErrors),

    #[error(transparent)]
    NoAvailableNodes(NoRoleError),

    #[error("Node is unreachable")]
    NodeUnreachable,
}

#[derive(Debug, thiserror::Error)]
pub struct NoRoleError(Option<Role>);

impl Display for NoRoleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(role) => {
                write!(f, "No available {role} nodes to satisfy the request")?;
            }
            None => {
                write!(f, "No available nodes to satisfy the request")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NodesErrors {
    node_status: Vec<(AdvertisedAddress, SimpleStatus)>,
}

impl NodesErrors {
    fn error(&mut self, node: AdvertisedAddress, simple_status: SimpleStatus) {
        self.node_status.push((node, simple_status));
    }

    fn is_empty(&self) -> bool {
        self.node_status.is_empty()
    }
}

impl Display for NodesErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (position, (address, status)) in self.node_status.iter().with_position() {
            match position {
                Position::Only => {
                    writeln!(f, "{address}: {status}")?;
                }
                Position::First => {
                    writeln!(f, "Encountered multiple errors:")?;
                    writeln!(f, " - {address} -> {status}")?;
                }
                Position::Middle | Position::Last => {
                    writeln!(f, " - {address} -> {status}")?;
                }
            }
        }
        Ok(())
    }
}

impl std::error::Error for NodesErrors {
    fn description(&self) -> &str {
        "aggregated nodes error"
    }
}
