// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cluster_marker::mark_cluster_as_provisioned;
use restate_core::metadata_store::{MetadataStoreClient, ReadWriteError};
use restate_core::{
    Metadata, MetadataWriter, ShutdownError, SyncError, TargetVersion, cancellation_token,
};
use restate_types::PlainNodeId;
use restate_types::config::{CommonOptions, Configuration};
use restate_types::errors::MaybeRetryableError;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{
    LogServerConfig, MetadataServerConfig, NodeConfig, NodesConfiguration,
};
use restate_types::retries::RetryPolicy;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{Level, debug, enabled, info, trace, warn};

#[derive(Debug, thiserror::Error)]
enum JoinError {
    #[error("missing nodes configuration")]
    MissingNodesConfiguration,
    #[error("detected a concurrent registration for node '{0}'")]
    ConcurrentNodeRegistration(String),
    #[error("failed writing to metadata store: {0}")]
    MetadataStore(#[from] ReadWriteError),
    #[error(
        "trying to join wrong cluster; expected '{expected_cluster_name}', actual '{actual_cluster_name}'"
    )]
    ClusterMismatch {
        expected_cluster_name: String,
        actual_cluster_name: String,
    },
    #[error(
        "node id mismatch; configured node id '{configured_node_id}', actual node id '{actual_node_id}'"
    )]
    NodeIdMismatch {
        configured_node_id: PlainNodeId,
        actual_node_id: PlainNodeId,
    },
}

pub struct NodeInit<'a> {
    metadata_store_client: &'a MetadataStoreClient,
    metadata_writer: &'a MetadataWriter,
    is_provisioned: bool,
}

impl<'a> NodeInit<'a> {
    pub fn new(
        metadata_store_client: &'a MetadataStoreClient,
        metadata_writer: &'a MetadataWriter,
        is_provisioned: bool,
    ) -> Self {
        Self {
            metadata_store_client,
            metadata_writer,
            is_provisioned,
        }
    }

    pub async fn init(self) -> anyhow::Result<()> {
        let config = Configuration::pinned().into_arc();

        let join_cluster = Self::join_cluster(
            self.metadata_store_client,
            &config.common,
            self.is_provisioned,
        );

        let nodes_configuration = cancellation_token()
            .run_until_cancelled(join_cluster)
            .await
            .ok_or(ShutdownError)??;

        if !self.is_provisioned {
            // If we fail at this point, then we might restart as if the cluster has not been
            // provisioned yet. This is not a problem because the provisioning operation is
            // idempotent.
            mark_cluster_as_provisioned()?;
        }

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

        trace!("Node initialization complete");

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
                    SyncError::MetadataStore(err) => err.retryable(),
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
        is_provisioned: bool,
    ) -> anyhow::Result<NodesConfiguration> {
        if is_provisioned {
            info!(
                "Trying to join the provisioned cluster '{}'",
                common_opts.cluster_name()
            );
        } else {
            info!(
                "Trying to join the cluster '{}'",
                common_opts.cluster_name()
            );
        }

        // todo make configurable
        // Never give up trying to join the cluster. Users of this struct will set a timeout if
        // needed.
        let join_retry = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            None,
            Some(Duration::from_secs(1)),
        );

        let join_start = Instant::now();
        let mut next_info_message = Duration::from_secs(10);
        let tone_escalation_after = Duration::from_secs(120);

        join_retry
            .retry_if(
                || Self::join_cluster_inner(metadata_store_client, common_opts),
                |err| {
                    let elapsed_since_join_start = join_start.elapsed();
                    if elapsed_since_join_start < next_info_message {
                        debug!(%err, "Failed joining the cluster; retrying");
                    } else {
                        if is_provisioned {
                            if elapsed_since_join_start <= tone_escalation_after {
                                if enabled!(Level::DEBUG) {
                                    info!(%err, "Failed to join the provisioned cluster '{}'. Please make sure that the cluster is up and running. Still trying to join...", common_opts.cluster_name());
                                } else {
                                    info!("Failed to join the provisioned cluster '{}'. Please make sure that the cluster is up and running. Still trying to join...", common_opts.cluster_name());
                                }
                            } else if enabled!(Level::DEBUG) {
                                warn!(%err, "Failed to join the provisioned cluster '{}'. Please make sure that the cluster is up and running. Still trying to join...", common_opts.cluster_name());
                            } else {
                                warn!("Failed to join the provisioned cluster '{}'. Please make sure that the cluster is up and running. Still trying to join...", common_opts.cluster_name());
                            }
                        } else if elapsed_since_join_start <= tone_escalation_after {
                            if enabled!(Level::DEBUG) {
                                info!(%err, "Failed to join the cluster '{}'. Has the cluster been provisioned, yet? Still trying to join...", common_opts.cluster_name());
                            } else {
                                info!("Failed to join the cluster '{}'. Has the cluster been provisioned, yet? Still trying to join...", common_opts.cluster_name());
                            }
                        } else if enabled!(Level::DEBUG) {
                            warn!(%err, "Failed to join the cluster '{}'. Has the cluster been provisioned, yet? Still trying to join...", common_opts.cluster_name());
                        } else {
                            warn!("Failed to join the cluster '{}'. Has the cluster been provisioned, yet? Still trying to join...", common_opts.cluster_name());
                        }
                        next_info_message += Duration::from_secs(30);
                    }
                    match err {
                        JoinError::MissingNodesConfiguration => true,
                        JoinError::ConcurrentNodeRegistration(_) => false,
                        JoinError::MetadataStore(err) => err.retryable(),
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
                            MetadataServerConfig::default(),
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

#[cfg(test)]
mod tests {
    use crate::init::NodeInit;
    use googletest::assert_that;
    use googletest::matchers::{contains_substring, displays_as, err};
    use restate_core::TestCoreEnvBuilder;
    use restate_types::config::{Configuration, set_current_config};
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration};
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    #[test_log::test(restate_core::test)]
    async fn node_id_mismatch() -> googletest::Result<()> {
        let cluster_name = "node_id_mismatch".to_owned();
        let node_name = "node".to_owned();
        let mut config = Configuration::default();
        config.common.set_cluster_name(&cluster_name);
        config.common.set_node_name(&node_name);
        config.common.force_node_id = Some(PlainNodeId::new(1337));
        set_current_config(config);

        let node_config = NodeConfig::new(
            node_name,
            GenerationalNodeId::INITIAL_NODE_ID,
            Default::default(),
            "http://localhost:1337".parse().unwrap(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        let mut nodes_configuration = NodesConfiguration::new(Version::MIN, cluster_name);
        nodes_configuration.upsert_node(node_config);

        let builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes_configuration);
        let node_env = builder.build().await;

        let metadata_store_client = node_env.metadata_store_client.clone();
        let metadata_writer = node_env.metadata_writer.clone();

        let init = NodeInit::new(&metadata_store_client, &metadata_writer, true);
        let result = init.init().await;

        assert_that!(
            result,
            err(displays_as(contains_substring("node id mismatch")))
        );

        Ok(())
    }

    #[test_log::test(restate_core::test)]
    async fn cluster_name_mismatch() -> googletest::Result<()> {
        let cluster_name = "cluster_name_mismatch".to_owned();
        let other_cluster_name = "other_cluster_name".to_owned();
        let node_name = "node".to_owned();
        let mut config = Configuration::default();
        config.common.set_cluster_name(&cluster_name);
        config.common.set_node_name(&node_name);
        set_current_config(config);

        let nodes_configuration = NodesConfiguration::new(Version::MIN, other_cluster_name);

        let builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes_configuration);
        let node_env = builder.build().await;

        let metadata_store_client = node_env.metadata_store_client.clone();
        let metadata_writer = node_env.metadata_writer.clone();

        let init = NodeInit::new(&metadata_store_client, &metadata_writer, true);
        let result = init.init().await;

        assert_that!(
            result,
            err(displays_as(contains_substring(
                "trying to join wrong cluster"
            )))
        );

        Ok(())
    }
}
