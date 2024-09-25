use std::{io, path::PathBuf, time::Duration};

use futures::future::{self};
use pin_project::pin_project;
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

        info!("Starting cluster {}", &cluster_name);

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

#[pin_project]
pub struct StartedCluster {
    cluster_name: String,
    base_dir: PathBuf,
    #[pin]
    pub nodes: Vec<StartedNode>,
}

impl StartedCluster {
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

    pub async fn push_node(&mut self, mut node: Node) -> Result<(), NodeStartError> {
        let node_config = node.config_mut();

        node_config
            .common
            .set_cluster_name(self.cluster_name.clone());
        node_config.common.set_base_dir(self.base_dir.clone());

        self.nodes.push(node.start().await?);
        Ok(())
    }
}
