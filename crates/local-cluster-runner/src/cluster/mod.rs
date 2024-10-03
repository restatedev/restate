use std::{io, path::PathBuf, time::Duration};

use futures::future::{self};
use restate_types::errors::GenericError;
use serde::{Deserialize, Serialize};
use tracing::info;
use typed_builder::TypedBuilder;

use crate::node::{Node, NodeStartError, StartedNode};

#[derive(Debug, Serialize, Deserialize, TypedBuilder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Cluster {
    #[builder(setter(into), default = default_cluster_name())]
    #[serde(default = "default_cluster_name")]
    cluster_name: String,
    nodes: Vec<Node>,
    #[builder(default = default_base_dir())]
    #[serde(default = "default_base_dir")]
    base_dir: PathBuf,
}

fn default_base_dir() -> PathBuf {
    std::env::current_dir().unwrap().join("restate-data")
}

fn default_cluster_name() -> String {
    "local-cluster".to_owned()
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum MetadataClientSource {
    // Use a metadata service run by the local-cluster-runner, inject the socket into base_config as needed
    #[default]
    Runner,
    // Use whatever metadata service is specified in base_config
    Configuration,
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

        if !base_dir.exists() {
            std::fs::create_dir_all(&base_dir).map_err(ClusterStartError::CreateDirectory)?;
        }

        let mut started_nodes = Vec::with_capacity(nodes.len());

        info!(
            "Starting cluster {} in {}",
            &cluster_name,
            base_dir.display()
        );

        for (i, node) in nodes.into_iter().enumerate() {
            started_nodes.push(
                node.start_clustered(base_dir.clone(), cluster_name.clone())
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
    base_dir: PathBuf,
    pub nodes: Vec<StartedNode>,
}

impl StartedCluster {
    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub async fn kill(&mut self) -> io::Result<()> {
        future::try_join_all(self.nodes.iter_mut().map(|n| n.kill()))
            .await
            .map(drop)
    }

    pub fn terminate(&self) -> io::Result<()> {
        for node in &self.nodes {
            node.terminate()?
        }
        Ok(())
    }

    pub async fn graceful_shutdown(&mut self, dur: Duration) -> io::Result<()> {
        future::try_join_all(self.nodes.iter_mut().map(|n| n.graceful_shutdown(dur)))
            .await
            .map(drop)
    }

    pub async fn wait_admins_healthy(&self, dur: Duration) -> bool {
        future::join_all(self.nodes.iter().map(|n| n.wait_admin_healthy(dur)))
            .await
            .into_iter()
            .all(|b| b)
    }

    pub async fn push_node(&mut self, node: Node) -> Result<(), NodeStartError> {
        self.nodes.push(
            node.start_clustered(self.base_dir.clone(), self.cluster_name.clone())
                .await?,
        );
        Ok(())
    }
}
