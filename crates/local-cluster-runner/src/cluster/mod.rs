// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    io,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use futures::future::{self};
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use tracing::info;
use typed_builder::TypedBuilder;

use restate_metadata_server_grpc::MetadataServerConfiguration;
use restate_types::{
    PlainNodeId,
    net::address::{AdvertisedAddress, FabricPort},
};
use restate_types::{errors::GenericError, nodes_config::Role};

use crate::node::{HealthCheck, HealthError, NodeSpec, NodeStartError, StartedNode};

#[derive(Debug, Serialize, Deserialize, TypedBuilder)]
pub struct Cluster {
    #[builder(setter(into), default = default_cluster_name())]
    #[serde(default = "default_cluster_name")]
    cluster_name: String,
    nodes: Vec<NodeSpec>,
    #[builder(setter(into), default = default_base_dir())]
    #[serde(default = "default_base_dir")]
    base_dir: MaybeTempDir,
}

impl<C, N> ClusterBuilder<(C, N, ())> {
    /// Use a tempdir as the basedir; this will be removed on Cluster/StartedCluster drop.
    /// You may set LOCAL_CLUSTER_RUNNER_RETAIN_TEMPDIR=true to instead log it out and retain
    /// it, and use LOCAL_CLUSTER_RUNNER_TEMPDIR to set the base dir.
    /// dir_name is the subdirectory's name inside the base dir, and used as node's base dir.
    pub fn temp_base_dir(self, dir_name: &str) -> ClusterBuilder<(C, N, (MaybeTempDir,))> {
        let maybe_temp_dir = MaybeTempDir::new(&dir_name);

        let base_dir = (maybe_temp_dir,);
        let (cluster_name, nodes, ()) = self.fields;
        ClusterBuilder {
            fields: (cluster_name, nodes, base_dir),
            phantom: self.phantom,
        }
    }
}

fn default_base_dir() -> MaybeTempDir {
    std::env::current_dir().unwrap().join("restate-data").into()
}

fn default_cluster_name() -> String {
    "local-cluster".to_owned()
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterStartError {
    #[error("Failed to start node {0}: {1}")]
    NodeStartError(usize, NodeStartError),
    #[error("Admin node is not healthy after waiting 60 seconds")]
    AdminUnhealthy(#[from] HealthError),
    #[error("Failed to create cluster base directory: {0}")]
    CreateDirectory(io::Error),
    #[error("Failed to create metadata client: {0}")]
    CreateMetadataClient(GenericError),
    #[error("Clusters must have at least one node")]
    NoNodes,
}

impl Cluster {
    pub async fn start(self) -> Result<StartedCluster, ClusterStartError> {
        let clock_guard = restate_clock::ClockUpkeep::start().expect("to start the clock upkeep");
        let Self {
            cluster_name,
            base_dir,
            nodes,
        } = self;

        if nodes.is_empty() {
            return Err(ClusterStartError::NoNodes);
        }

        if !base_dir.as_path().exists() {
            std::fs::create_dir_all(base_dir.as_path())
                .map_err(ClusterStartError::CreateDirectory)?;
        }

        let mut started_nodes = Vec::with_capacity(nodes.len());

        info!(
            "Starting cluster {} in {}",
            &cluster_name,
            base_dir.as_path().display()
        );

        // todo: Add support for cluster address book to acquire tcp listeners for all servers
        // prior to starting the nodes and keep a global registry of those file descriptors.

        let metadata_server_addresses: Vec<_> = nodes
            .iter()
            .filter_map(|node| {
                node.has_role(Role::MetadataServer).then_some(
                    AdvertisedAddress::with_node_base_dir(
                        &base_dir.as_path().join(node.node_name()),
                    ),
                )
            })
            .collect();

        for (i, mut node) in nodes.into_iter().enumerate() {
            node.set_metadata_servers(&metadata_server_addresses);
            let node = node
                .start_clustered(base_dir.as_path(), &cluster_name)
                .await
                .map_err(|err| ClusterStartError::NodeStartError(i, err))?;
            started_nodes.push(node)
        }

        Ok(StartedCluster {
            clock_guard,
            cluster_name,
            base_dir,
            nodes: started_nodes,
        })
    }
}

pub struct StartedCluster {
    #[allow(dead_code)]
    clock_guard: restate_clock::ClockUpkeep,
    cluster_name: String,
    base_dir: MaybeTempDir,
    pub nodes: Vec<StartedNode>,
}

impl StartedCluster {
    pub fn base_dir(&self) -> &Path {
        self.base_dir.as_path()
    }

    pub fn collect_metadata_server_addresses(&self) -> Vec<AdvertisedAddress<FabricPort>> {
        self.nodes
            .iter()
            .filter_map(|node| {
                node.has_role(Role::MetadataServer).then_some(
                    AdvertisedAddress::with_node_base_dir(&self.base_dir().join(node.node_name())),
                )
            })
            .collect()
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Send a SIGKILL to every node in the cluster
    pub async fn kill(&mut self) -> io::Result<()> {
        future::try_join_all(self.nodes.iter_mut().map(|n| n.kill()))
            .await
            .map(drop)
    }

    /// Send a SIGTERM to every node in the cluster
    pub fn terminate(&self) -> io::Result<()> {
        for node in &self.nodes {
            node.terminate()?
        }
        Ok(())
    }

    pub fn node(&self, name: &str) -> Option<&StartedNode> {
        self.nodes.iter().find(|n| n.node_name() == name)
    }

    pub fn node_mut(&mut self, name: &str) -> Option<&mut StartedNode> {
        self.nodes.iter_mut().find(|n| n.node_name() == name)
    }

    /// Send a SIGTERM to every node in the cluster, then wait for `dur` for them to exit,
    /// otherwise send a SIGKILL to nodes that are still running.
    pub async fn graceful_shutdown(&mut self, dur: Duration) -> io::Result<()> {
        future::try_join_all(self.nodes.iter_mut().map(|n| n.graceful_shutdown(dur)))
            .await
            .map(drop)
    }

    pub async fn shutdown_node(&mut self, name: &str, dur: Duration) -> io::Result<ExitStatus> {
        let node = self
            .nodes
            .iter_mut()
            .find(|n| n.node_name() == name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "node not found"))?;
        let exit = node.graceful_shutdown(dur).await?;

        self.nodes.retain(|n| n.node_name() != name);

        Ok(exit)
    }

    /// For every relevant node in the cluster for this check, wait for up to dur for the check
    /// to pass
    pub async fn wait_check_healthy(
        &self,
        check: HealthCheck,
        dur: Duration,
    ) -> Result<(), HealthError> {
        future::try_join_all(
            self.nodes
                .iter()
                .filter(|n| check.applicable(n))
                .map(|n| check.wait_healthy(n, dur)),
        )
        .await
        .map(drop)
    }

    /// Wait for all ingress, admin, logserver roles in the cluster to be healthy/provisioned and
    /// the embedded metadata cluster to include all nodes.
    pub async fn wait_healthy(&self, dur: Duration) -> Result<(), HealthError> {
        tokio::try_join!(
            self.wait_check_healthy(HealthCheck::MetadataServer, dur),
            self.wait_check_healthy(HealthCheck::Admin, dur),
            self.wait_check_healthy(HealthCheck::Worker, dur),
            self.wait_check_healthy(HealthCheck::Ingress, dur),
            self.wait_check_healthy(HealthCheck::LogServer, dur),
        )?;
        Ok(())
    }

    pub async fn expand(&mut self, mut node: NodeSpec) -> Result<(), NodeStartError> {
        let addresses = self.collect_metadata_server_addresses();
        node.set_metadata_servers(&addresses);
        self.nodes.push(
            node.start_clustered(self.base_dir.as_path(), self.cluster_name.clone())
                .await?,
        );
        Ok(())
    }

    pub async fn get_metadata_cluster_status(&self) -> Option<MetadataClusterStatus> {
        let mut server_statuses: FuturesUnordered<_> = self
            .nodes
            .iter()
            .map(|node| node.get_metadata_server_status())
            .collect();

        let mut max_status = None;

        // pick the server status with the highest configuration version
        while let Some(server_status) = server_statuses.next().await {
            if let Ok(server_status) = server_status {
                let metadata_server_configuration = server_status
                    .configuration
                    .map(MetadataServerConfiguration::from);

                if metadata_server_configuration
                    .as_ref()
                    .map(|config| config.version())
                    > max_status.as_ref().map(|status: &MetadataClusterStatus| {
                        status.metadata_server_configuration.version()
                    })
                {
                    max_status = Some(MetadataClusterStatus {
                        leader: server_status.leader.map(PlainNodeId::from),
                        metadata_server_configuration: metadata_server_configuration.unwrap(),
                    })
                }
            }
        }

        max_status
    }
}

pub struct MetadataClusterStatus {
    // leader if one is known
    leader: Option<PlainNodeId>,
    // latest known metadata server configuration
    metadata_server_configuration: MetadataServerConfiguration,
}

impl MetadataClusterStatus {
    pub fn into_inner(self) -> (Option<PlainNodeId>, MetadataServerConfiguration) {
        (self.leader, self.metadata_server_configuration)
    }
}

#[derive(Debug, Clone)]
pub enum MaybeTempDir {
    PathBuf(PathBuf),
    TempDir(Arc<tempfile::TempDir>),
}

impl MaybeTempDir {
    pub fn new<P: AsRef<Path>>(path: &P) -> MaybeTempDir {
        if let Ok("true" | "1") = std::env::var("LOCAL_CLUSTER_RUNNER_RETAIN_TEMPDIR").as_deref() {
            let dir = match std::env::var("LOCAL_CLUSTER_RUNNER_TEMPDIR") {
                Ok(v) => Path::new(&v).join(path).to_path_buf(),
                Err(_) => tempfile::tempdir().expect("to create a tempdir").keep(),
            };
            eprintln!(
                "Will retain local cluster runner tempdir upon cluster drop: {}",
                dir.display()
            );
            Self::PathBuf(dir)
        } else {
            Self::TempDir(Arc::new(tempfile::tempdir().expect("to create a tempdir")))
        }
    }

    pub fn as_path(&self) -> &Path {
        match self {
            MaybeTempDir::PathBuf(p) => p.as_path(),
            MaybeTempDir::TempDir(d) => d.path(),
        }
    }
}

impl From<PathBuf> for MaybeTempDir {
    fn from(value: PathBuf) -> Self {
        Self::PathBuf(value)
    }
}

impl Serialize for MaybeTempDir {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_path().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MaybeTempDir {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::PathBuf(PathBuf::deserialize(deserializer)?))
    }
}
