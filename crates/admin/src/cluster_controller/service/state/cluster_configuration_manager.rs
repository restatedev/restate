// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{cmp::Ordering, sync::Arc};

use tracing::{debug, trace, warn};

use restate_core::{
    metadata_store::retry_on_network_error, MetadataWriter, ShutdownError, TaskCenter, TaskHandle,
    TaskKind,
};
use restate_metadata_store::MetadataStoreClient;
use restate_types::{
    cluster_controller::ClusterConfiguration,
    config::{ClusterConfigurationType, Configuration},
    live::Live,
    metadata_store::keys::{CLUSTER_CONFIG_KEY, PARTITION_TABLE_KEY},
    partition_table::PartitionTable,
    retries::RetryPolicy,
    Version, Versioned,
};
use tokio::sync::watch;

pub struct ClusterConfigurationManager {
    tx: watch::Sender<Option<Arc<ClusterConfiguration>>>,
    configuration: Live<Configuration>,
    metadata_store_client: MetadataStoreClient,
    metadata_writer: MetadataWriter,
    in_flight_refresh: Option<TaskHandle<()>>,
}

impl ClusterConfigurationManager {
    pub fn new(
        configuration: Live<Configuration>,
        metadata_store_client: MetadataStoreClient,
        metadata_writer: MetadataWriter,
    ) -> Self {
        Self {
            tx: watch::Sender::new(None),
            configuration,
            metadata_store_client,
            metadata_writer,
            in_flight_refresh: None,
        }
    }

    /// Initializes the configuration if not yet initialized and returns the current
    /// available cluster configuration
    pub async fn initialize(&mut self) -> anyhow::Result<Option<Arc<ClusterConfiguration>>> {
        let configuration: &Configuration = self.configuration.live_load();
        let cluster_config: Option<ClusterConfiguration> = match configuration
            .common
            .cluster_configuration
        {
            ClusterConfigurationType::Empty => {
                // bootstrapping is not enabled!
                // just load current config if exists.
                retry_on_network_error(
                    configuration.common.network_error_retry_policy.clone(),
                    || self.metadata_store_client.get(CLUSTER_CONFIG_KEY.clone()),
                )
                .await?
            }
            ClusterConfigurationType::Default => {
                let default_cluster_configuration = configuration.default_cluster_configuration();

                let cluster_configuration = retry_on_network_error(
                    configuration.common.network_error_retry_policy.clone(),
                    || {
                        self.metadata_store_client
                            .get_or_insert(CLUSTER_CONFIG_KEY.clone(), || {
                                debug!("Initializing cluster configuration from seed");
                                trace!(
                                    "Cluster Configuration: {:#?}",
                                    default_cluster_configuration
                                );

                                debug!("Cluster configuration: {default_cluster_configuration:#?}");
                                ClusterConfiguration {
                                    version: Version::MIN,
                                    configuration: default_cluster_configuration.clone(),
                                }
                            })
                    },
                )
                .await?;

                Some(cluster_configuration)
            }
        };

        if let Some(ref cluster_config) = cluster_config {
            // if cluster config is available either loaded from the metastore
            // or was just created, we try to initialize the partition table
            initialize_partition_table(
                configuration.common.network_error_retry_policy.clone(),
                cluster_config,
                &self.metadata_store_client,
                &self.metadata_writer,
            )
            .await?;
        }

        let cluster_config = cluster_config.map(Arc::new);
        // make new cluster config immediately available for all
        // watchers
        self.tx.send_replace(cluster_config.clone());

        Ok(cluster_config)
    }

    pub fn watch(&self) -> watch::Receiver<Option<Arc<ClusterConfiguration>>> {
        let mut rx = self.tx.subscribe();
        rx.mark_changed();
        rx
    }

    pub fn schedule_refresh(&mut self) -> Result<(), ShutdownError> {
        if let Some(handle) = &self.in_flight_refresh {
            if !handle.is_finished() {
                // nothing to do
                return Ok(());
            }
        }

        let fetch_task = ConfigurationFetchTask {
            tx: self.tx.clone(),
            configuration: self.configuration.clone(),
            metadata_store_client: self.metadata_store_client.clone(),
            metadata_writer: self.metadata_writer.clone(),
        };

        let task_handle = TaskCenter::spawn_unmanaged(
            TaskKind::Disposable,
            "refresh-cluster-configuration",
            fetch_task.run(),
        )?;

        self.in_flight_refresh = Some(task_handle);

        Ok(())
    }
}

struct ConfigurationFetchTask {
    tx: watch::Sender<Option<Arc<ClusterConfiguration>>>,
    configuration: Live<Configuration>,
    metadata_store_client: MetadataStoreClient,
    metadata_writer: MetadataWriter,
}

impl ConfigurationFetchTask {
    async fn run(mut self) {
        trace!("Fetching new cluster configuration");

        let cluster_config: Option<ClusterConfiguration> = match self
            .metadata_store_client
            .get::<ClusterConfiguration>(CLUSTER_CONFIG_KEY.clone())
            .await
        {
            Ok(cluster_config) => cluster_config,
            Err(err) => {
                debug!("Failed to fetch cluster configuration from metadata store: {err}");
                return;
            }
        };

        if self.tx.borrow().is_none() {
            if let Some(ref cluster_config) = cluster_config {
                // possibly need to initialize partition table
                let configuration = self.configuration.live_load();
                if let Err(err) = initialize_partition_table(
                    configuration.common.network_error_retry_policy.clone(),
                    cluster_config,
                    &self.metadata_store_client,
                    &self.metadata_writer,
                )
                .await
                {
                    warn!("Failed to initialize partition table: {err}");
                    // we return here so we retry to create the partition table on next retry
                    // if current configuration was published there is no way we are recreating
                    // it again.
                    return;
                }
            }
        }

        self.tx.send_if_modified(|current| {
            let current_version = current.as_ref().map(|cfg| cfg.version());

            match cluster_config
                .as_ref()
                .map(|cfg| cfg.version())
                .cmp(&current_version)
            {
                Ordering::Equal | Ordering::Less => false,
                Ordering::Greater => {
                    debug!("New cluster configuration: {:?}", cluster_config);
                    *current = cluster_config.map(Arc::new);
                    true
                }
            }
        });
    }
}

async fn initialize_partition_table(
    retry_policy: RetryPolicy,
    cluster_config: &ClusterConfiguration,
    metadata_store_client: &MetadataStoreClient,
    metadata_writer: &MetadataWriter,
) -> anyhow::Result<()> {
    let partition_table = retry_on_network_error(retry_policy, || {
        metadata_store_client.get_or_insert(PARTITION_TABLE_KEY.clone(), || {
            let partition_table = PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                cluster_config.num_partitions.into(),
            );

            debug!("Initializing the partition table with '{partition_table:?}'");

            partition_table
        })
    })
    .await?;

    metadata_writer.update(Arc::new(partition_table)).await?;

    Ok(())
}
