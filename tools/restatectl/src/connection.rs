// Copyright (c) 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{cmp::Ordering, collections::HashMap, fmt::Display, future::Future, sync::Arc};

use cling::{prelude::Parser, Collect};
use itertools::{Either, Itertools};
use rand::{rng, seq::SliceRandom};
use tokio::sync::{Mutex, MutexGuard};
use tonic::{codec::CompressionEncoding, transport::Channel, Response, Status};
use tracing::info;

use restate_core::protobuf::node_ctl_svc::{
    node_ctl_svc_client::NodeCtlSvcClient, GetMetadataRequest, IdentResponse,
};
use restate_types::{
    logs::metadata::Logs,
    net::AdvertisedAddress,
    nodes_config::{NodesConfiguration, Role},
    protobuf::common::{MetadataKind, NodeStatus},
    storage::{StorageCodec, StorageDecode, StorageDecodeError},
    Version, Versioned,
};

use crate::util::grpc_channel;

#[derive(Clone, Parser, Collect, Debug)]
pub struct ConnectionInfo {
    /// Specify server address to connect to.
    ///
    /// It needs access to the node-to-node address (aka. node advertised address)
    /// Can also accept a comma-separated list or by repeating `--address=<host>`.
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

    /// Use this option to avoid receiving stale metadata information from the nodes by reading it
    /// from the metadata store.
    #[arg(long)]
    pub sync_metadata: bool,

    #[clap(skip)]
    nodes_configuration: Arc<Mutex<Option<NodesConfiguration>>>,

    #[clap(skip)]
    logs: Arc<Mutex<Option<Logs>>>,

    #[clap(skip)]
    cache: Arc<Mutex<HashMap<AdvertisedAddress, Channel>>>,
}

impl ConnectionInfo {
    /// Gets NodesConfiguration object. This function tries all provided addresses and makes sure
    /// nodes configuration is cached.
    pub async fn get_nodes_configuration(&self) -> Result<NodesConfiguration, ConnectionInfoError> {
        if self.address.is_empty() {
            return Err(ConnectionInfoError::NoAvailableNodes(NoRoleError(None)));
        }

        let guard = self.nodes_configuration.lock().await;

        // get nodes configuration will always use the addresses seed
        // provided via the cmdline
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
        let cached = self.cache.lock().await.keys().cloned().collect::<Vec<_>>();

        assert!(!cached.is_empty(), "must have cached connections");

        let try_nodes = cached
            .iter()
            .chain(
                nodes_addresses
                    .into_iter()
                    .filter(|address| !cached.contains(address)),
            )
            .collect::<Vec<_>>();

        // To be sure we landed on the best guess of the latest version of the logs
        // we need to ask multiple nodes in the nodes config.
        // We make sure cached nodes has higher precedence + 50% of node set size trimmed to
        // a total of 50% + 1 nodes.

        self.get_latest_metadata(
            try_nodes.into_iter(),
            (cluster_size / 2) + 1,
            MetadataKind::Logs,
            guard,
            |ident| Version::from(ident.logs_version),
        )
        .await
    }

    /// Gets Metadata object. On successful responses, [`get_latest_metadata`] will only try `try_best_of`
    /// of the provided addresses, or until all nodes are exhausted.
    async fn get_latest_metadata<T, M>(
        &self,
        addresses: impl Iterator<Item = &AdvertisedAddress>,
        mut try_best_of: usize,
        kind: MetadataKind,
        mut guard: MutexGuard<'_, Option<T>>,
        version_map: M,
    ) -> Result<T, ConnectionInfoError>
    where
        T: StorageDecode + Versioned + Clone,
        M: Fn(&IdentResponse) -> Version,
    {
        if let Some(meta) = &*guard {
            return Ok(meta.clone());
        }

        let mut latest_meta: Option<T> = None;
        let mut answer = false;
        let mut errors = NodesErrors::default();
        let mut cache = self.cache.lock().await;

        let request = GetMetadataRequest {
            kind: kind.into(),
            sync: self.sync_metadata,
        };

        for address in addresses {
            let channel = cache.entry(address.clone()).or_insert_with(|| {
                info!("Connecting to {address}");
                grpc_channel(address.clone())
            });

            let mut client = NodeCtlSvcClient::new(channel.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);

            let response = match client.get_ident(()).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    errors.error(address.clone(), status);
                    continue;
                }
            };

            // node is reachable and has answered
            answer = true;
            if response.status != NodeStatus::Alive as i32 {
                // node did not join the cluster yet.
                continue;
            }

            if version_map(&response)
                <= latest_meta
                    .as_ref()
                    .map(|c| c.version())
                    .unwrap_or(Version::INVALID)
            {
                // has older version than we have.
                continue;
            }

            let mut response = match client.get_metadata(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    errors.error(address.clone(), status);
                    continue;
                }
            };

            let meta = StorageCodec::decode::<T, _>(&mut response.encoded)
                .map_err(|err| ConnectionInfoError::DecoderError(address.clone(), err))?;

            if meta.version()
                > latest_meta
                    .as_ref()
                    .map(|c| c.version())
                    .unwrap_or(Version::INVALID)
            {
                latest_meta = Some(meta);
            }

            try_best_of -= 1;
            if try_best_of == 0 {
                break;
            }
        }

        if !answer {
            // all nodes have returned error
            return Err(ConnectionInfoError::NodesErrors(errors));
        }

        *guard = latest_meta.clone();
        latest_meta.ok_or(ConnectionInfoError::MissingMetadata)
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
    pub async fn try_each<F, T, Fut>(
        &self,
        role: Option<Role>,
        mut closure: F,
    ) -> Result<Response<T>, ConnectionInfoError>
    where
        F: FnMut(Channel) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let nodes_config = self.get_nodes_configuration().await?;
        let mut channels = self.cache.lock().await;

        let iterator = match role {
            Some(role) => Either::Left(nodes_config.iter_role(role)),
            None => Either::Right(nodes_config.iter()),
        }
        .sorted_by(|a, b| {
            // nodes for which we already have open channels get higher precedence.
            match (
                channels.contains_key(&a.1.address),
                channels.contains_key(&b.1.address),
            ) {
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (_, _) => a.0.cmp(&b.0),
            }
        });

        let mut errors = NodesErrors::default();

        for (_, node) in iterator {
            // avoid creating new channels on each iteration. Instead cheaply copy the channels
            let channel = channels
                .entry(node.address.clone())
                .or_insert_with(|| grpc_channel(node.address.clone()));

            let result = closure(channel.clone()).await;
            match result {
                Ok(response) => return Ok(response),
                Err(status) => {
                    errors.error(node.address.clone(), status);
                }
            }
        }

        if errors.is_empty() {
            Err(ConnectionInfoError::NoAvailableNodes(NoRoleError(role)))
        } else {
            Err(ConnectionInfoError::NodesErrors(errors))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionInfoError {
    #[error("Could not retrieve cluster metadata. Has the cluster been provisioned yet?")]
    MissingMetadata,

    #[error("Failed to decode metadata from node {0}: {1}")]
    DecoderError(AdvertisedAddress, StorageDecodeError),

    #[error(transparent)]
    NodesErrors(NodesErrors),

    #[error(transparent)]
    NoAvailableNodes(NoRoleError),
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
    node_status: Vec<(AdvertisedAddress, Status)>,
}

impl NodesErrors {
    fn error(&mut self, node: AdvertisedAddress, status: Status) {
        self.node_status.push((node, status));
    }

    fn is_empty(&self) -> bool {
        self.node_status.is_empty()
    }
}

impl Display for NodesErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Encountered multiple errors:")?;
        for (address, status) in &self.node_status {
            writeln!(f, " - {address} -> {status}")?;
        }
        Ok(())
    }
}

impl std::error::Error for NodesErrors {
    fn description(&self) -> &str {
        "aggregated nodes error"
    }
}
