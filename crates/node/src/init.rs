// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::metadata_store::{MetadataStoreClient, MetadataStoreClientError, ReadWriteError};
use restate_core::{
    cancellation_watcher, Metadata, MetadataWriter, ShutdownError, SyncError, TargetVersion,
};
use restate_types::config::{CommonOptions, Configuration};
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration};
use restate_types::retries::RetryPolicy;
use restate_types::PlainNodeId;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};

#[derive(Debug, thiserror::Error)]
enum JoinError {
    #[error("missing nodes configuration")]
    MissingNodesConfiguration,
    #[error("detected a concurrent registration for node '{0}'")]
    ConcurrentNodeRegistration(String),
    #[error("failed writing to metadata store: {0}")]
    MetadataStore(#[from] ReadWriteError),
    #[error("trying to join wrong cluster; expected '{expected_cluster_name}', actual '{actual_cluster_name}'")]
    ClusterMismatch {
        expected_cluster_name: String,
        actual_cluster_name: String,
    },
    #[error("node id mismatch; configured node id '{configured_node_id}', actual node id '{actual_node_id}'")]
    NodeIdMismatch {
        configured_node_id: PlainNodeId,
        actual_node_id: PlainNodeId,
    },
}

pub struct NodeInit<'a> {
    metadata_store_client: &'a MetadataStoreClient,
    metadata_writer: &'a MetadataWriter,
}

impl<'a> NodeInit<'a> {
    pub fn new(
        metadata_store_client: &'a MetadataStoreClient,
        metadata_writer: &'a MetadataWriter,
    ) -> Self {
        Self {
            metadata_store_client,
            metadata_writer,
        }
    }

    pub async fn init(self) -> anyhow::Result<()> {
        let config = Configuration::pinned().into_arc();

        let join_cluster = Self::join_cluster(self.metadata_store_client, &config.common);

        let nodes_configuration = tokio::select! {
                _ = cancellation_watcher() => {
                    return Err(ShutdownError.into());
                },
                result = join_cluster => result?,
        };

        // Find my node in nodes configuration.
        let my_node_config = nodes_configuration
            .find_node_by_name(config.common.node_name())
            .expect("node config should have been upserted");

        let my_node_id = my_node_config.current_generation;

        info!(
            roles = %my_node_config.roles,
            address = %my_node_config.address,
            location = %my_node_config.location,
            "My Node ID is {}", my_node_config.current_generation
        );

        self.metadata_writer
            .update(Arc::new(nodes_configuration))
            .await?;

        // My Node ID is set
        self.metadata_writer.set_my_node_id(my_node_id);
        restate_tracing_instrumentation::set_global_node_id(my_node_id);

        self.sync_metadata().await;

        info!("Node initialization complete");

        Ok(())
    }

    async fn sync_metadata(&self) {
        // fetch the latest metadata
        let metadata = Metadata::current();

        let config = Configuration::pinned();

        let retry_policy = config.common.network_error_retry_policy.clone();

        if let Err(err) = retry_policy
            .retry_if(
                || async {
                    metadata
                        .sync(MetadataKind::Schema, TargetVersion::Latest)
                        .await?;
                    metadata
                        .sync(MetadataKind::PartitionTable, TargetVersion::Latest)
                        .await
                },
                |err| match err {
                    SyncError::MetadataStore(err) => err.is_network_error(),
                    SyncError::Shutdown(_) => false,
                },
            )
            .await
        {
            warn!("Failed to fetch the latest metadata when initializing the node: {err}");
        }
    }

    async fn join_cluster(
        metadata_store_client: &MetadataStoreClient,
        common_opts: &CommonOptions,
    ) -> anyhow::Result<NodesConfiguration> {
        info!("Trying to join cluster '{}'", common_opts.cluster_name());

        // todo make configurable
        // Never give up trying to join the cluster. Users of this struct will set a timeout if
        // needed.
        let join_retry = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            None,
            Some(Duration::from_secs(5)),
        );

        let join_start = Instant::now();
        let mut printed_provision_message = false;

        join_retry
            .retry_if(
                || Self::join_cluster_inner(metadata_store_client, common_opts),
                |err| {
                    if join_start.elapsed() < Duration::from_secs(10) {
                        trace!("Failed joining the cluster: {err}; retrying");
                    } else if !printed_provision_message {
                        info!("Still can't join the cluster, will retry. Did you forget to provision it?");
                        printed_provision_message = true;
                    } else {
                        debug!("Failed joining cluster: {err}; retrying");
                    }
                    match err {
                        JoinError::MissingNodesConfiguration => true,
                        JoinError::ConcurrentNodeRegistration(_) => false,
                        JoinError::MetadataStore(err) => err.is_network_error(),
                        JoinError::ClusterMismatch { .. } => false,
                        JoinError::NodeIdMismatch { .. } => false,
                    }
                },
            )
            .await
            .map_err(Into::into)
    }

    async fn join_cluster_inner(
        metadata_store_client: &MetadataStoreClient,
        common_opts: &CommonOptions,
    ) -> Result<NodesConfiguration, JoinError> {
        let mut previous_node_generation = None;

        metadata_store_client
            .read_modify_write::<NodesConfiguration, _, _>(
                NODES_CONFIG_KEY.clone(),
                move |nodes_config| {
                    let mut nodes_config =
                        nodes_config.ok_or(JoinError::MissingNodesConfiguration)?;

                    // check that we are joining the right cluster
                    if nodes_config.cluster_name() != common_opts.cluster_name() {
                        return Err(JoinError::ClusterMismatch {
                            expected_cluster_name: common_opts.cluster_name().to_owned(),
                            actual_cluster_name: nodes_config.cluster_name().to_owned(),
                        });
                    }

                    // check whether we have registered before
                    let node_config = nodes_config
                        .find_node_by_name(common_opts.node_name())
                        .cloned();

                    let my_node_config = if let Some(mut node_config) = node_config {
                        assert_eq!(
                            common_opts.node_name(),
                            node_config.name,
                            "node name must match"
                        );

                        // do location changes according to the following truth table
                        let current_location = &node_config.location;
                        let new_location = common_opts.location();
                        match (current_location.is_empty(), new_location.is_empty()) {
                            (true, false) => {
                                // relatively safe and an expected change for someone enabling locality for the first time.
                                node_config.location = common_opts.location().clone();
                            }
                            (false, false) if current_location != new_location => {
                                warn!(
                                    "Node location has changed from '{current_location}' to '{new_location}'. \
                                    This change can be dangerous if the cluster is configured with geo-aware replication, but we'll still apply it. \
                                    You can reverted back on the next server restart.",
                                );
                                node_config.location = common_opts.location().clone();
                            }
                            (false, false) => { /* do nothing; location didn't change */ }
                            (true, true) => { /* do nothing; both are empty */}
                            (false, true) => {
                                // leave current location as is, warn about it.
                                warn!(
                                    "Node location was '{current_location}' in a previous configuration, and it's empty in this configuration. \
                                    Setting the location back to empty is not permitted, location will stay as '{current_location}'",
                                );
                            }
                        }

                        if common_opts.force_node_id.is_some_and(|configured_node_id| {
                            configured_node_id != node_config.current_generation.as_plain()
                        }) {
                            return Err(JoinError::NodeIdMismatch {
                                configured_node_id: common_opts.force_node_id.unwrap(),
                                actual_node_id: node_config.current_generation.as_plain(),
                            });
                        }

                        if let Some(previous_node_generation) = previous_node_generation {
                            if node_config
                                .current_generation
                                .is_newer_than(previous_node_generation)
                            {
                                // detected a concurrent registration of the same node
                                return Err(JoinError::ConcurrentNodeRegistration(
                                    common_opts.node_name().to_owned(),
                                ));
                            }
                        } else {
                            // remember the previous node generation to detect concurrent modifications
                            previous_node_generation = Some(node_config.current_generation);
                        }

                        // update node_config
                        node_config.roles = common_opts.roles;
                        node_config.address = common_opts.advertised_address.clone();
                        node_config.current_generation.bump_generation();

                        node_config
                    } else {
                        let plain_node_id = common_opts.force_node_id.unwrap_or_else(|| {
                            nodes_config
                                .max_plain_node_id()
                                .map(|n| n.next())
                                .unwrap_or_default()
                        });

                        assert!(
                            nodes_config.find_node_by_id(plain_node_id).is_err(),
                            "duplicate plain node id '{plain_node_id}'"
                        );

                        let my_node_id = plain_node_id.with_generation(1);

                        NodeConfig::new(
                            common_opts.node_name().to_owned(),
                            my_node_id,
                            common_opts.location().clone(),
                            common_opts.advertised_address.clone(),
                            common_opts.roles,
                            LogServerConfig::default(),
                        )
                    };

                    nodes_config.upsert_node(my_node_config);
                    nodes_config.increment_version();

                    Ok(nodes_config)
                },
            )
            .await
            .map_err(|err| err.transpose())
    }
}
