use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures::future::{self};
use serde::{Deserialize, Serialize};
use tracing::info;
use typed_builder::TypedBuilder;

use restate_types::errors::GenericError;

use crate::node::{Node, NodeStartError, StartedNode};

#[derive(Debug, Serialize, Deserialize, TypedBuilder)]
pub struct Cluster {
    #[builder(setter(into), default = default_cluster_name())]
    #[serde(default = "default_cluster_name")]
    cluster_name: String,
    nodes: Vec<Node>,
    #[builder(setter(into), default = default_base_dir())]
    #[serde(default = "default_base_dir")]
    base_dir: MaybeTempDir,
}

impl<C, N> ClusterBuilder<(C, N, ())> {
    // Use a tempdir as the basedir; this will be removed on Cluster/StartedCluster drop.
    // You may set LOCAL_CLUSTER_RUNNER_RETAIN_TEMPDIR=true to instead log it out and retain
    // it.
    pub fn temp_base_dir(self) -> ClusterBuilder<(C, N, (MaybeTempDir,))> {
        let maybe_temp_dir = tempfile::tempdir().expect("to create a tempdir").into();
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
    #[error("Failed to create cluster base directory: {0}")]
    CreateDirectory(io::Error),
    #[error("Failed to create metadata client: {0}")]
    CreateMetadataClient(GenericError),
    #[error("Clusters must have at least one node")]
    NoNodes,
}

impl Cluster {
    pub async fn start(self) -> Result<StartedCluster, ClusterStartError> {
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

        for (i, node) in nodes.into_iter().enumerate() {
            started_nodes.push(
                node.start_clustered(base_dir.as_path(), &cluster_name)
                    .await
                    .map_err(|err| ClusterStartError::NodeStartError(i, err))?,
            )
        }

        Ok(StartedCluster {
            cluster_name,
            base_dir,
            nodes: started_nodes,
        })
    }
}

pub struct StartedCluster {
    cluster_name: String,
    base_dir: MaybeTempDir,
    pub nodes: Vec<StartedNode>,
}

impl StartedCluster {
    pub fn base_dir(&self) -> &Path {
        self.base_dir.as_path()
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    // Send a SIGKILL to every node in the cluster
    pub async fn kill(&mut self) -> io::Result<()> {
        future::try_join_all(self.nodes.iter_mut().map(|n| n.kill()))
            .await
            .map(drop)
    }

    // Send a SIGTERM to every node in the cluster
    pub fn terminate(&self) -> io::Result<()> {
        for node in &self.nodes {
            node.terminate()?
        }
        Ok(())
    }

    // Send a SIGTERM to every node in the cluster, then wait for `dur` for them to exit,
    // otherwise send a SIGKILL to nodes that are still running.
    pub async fn graceful_shutdown(&mut self, dur: Duration) -> io::Result<()> {
        future::try_join_all(self.nodes.iter_mut().map(|n| n.graceful_shutdown(dur)))
            .await
            .map(drop)
    }

    // For every node in the cluster with an admin role, wait for up to dur for the admin endpoint
    // to respond to health checks, otherwise return false.
    pub async fn wait_admins_healthy(&self, dur: Duration) -> bool {
        future::join_all(
            self.nodes
                .iter()
                .filter(|n| n.admin_address().is_some())
                .map(|n| n.wait_admin_healthy(dur)),
        )
        .await
        .into_iter()
        .all(|b| b)
    }

    // For every node in the cluster with an ingress role, wait for up to dur for the admin endpoint
    // to respond to health checks, otherwise return false.
    pub async fn wait_ingresses_healthy(&self, dur: Duration) -> bool {
        future::join_all(
            self.nodes
                .iter()
                .filter(|n| n.ingress_address().is_some())
                .map(|n| n.wait_ingress_healthy(dur)),
        )
        .await
        .into_iter()
        .all(|b| b)
    }

    // Wait for all ingress and admin endpoints in the cluster to be healthy
    pub async fn wait_healthy(&self, dur: Duration) -> bool {
        future::join(
            self.wait_admins_healthy(dur),
            self.wait_ingresses_healthy(dur),
        )
        .await
            == (true, true)
    }

    pub async fn push_node(&mut self, node: Node) -> Result<(), NodeStartError> {
        self.nodes.push(
            node.start_clustered(self.base_dir.as_path(), self.cluster_name.clone())
                .await?,
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum MaybeTempDir {
    PathBuf(PathBuf),
    TempDir(Arc<tempfile::TempDir>),
}

impl MaybeTempDir {
    fn as_path(&self) -> &Path {
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

impl From<tempfile::TempDir> for MaybeTempDir {
    fn from(value: tempfile::TempDir) -> Self {
        if let Ok("true") = std::env::var("LOCAL_CLUSTER_RUNNER_RETAIN_TEMPDIR").as_deref() {
            eprintln!(
                "Will retain local cluster runner tempdir upon cluster drop: {}",
                value.path().display()
            );
            Self::PathBuf(value.into_path())
        } else {
            Self::TempDir(Arc::new(value))
        }
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
