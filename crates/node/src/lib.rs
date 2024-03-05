// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod network_server;
mod options;
mod roles;

pub use options::{Options, OptionsBuilder as NodeOptionsBuilder};
pub use restate_admin::OptionsBuilder as AdminOptionsBuilder;
use restate_bifrost::BifrostService;
use restate_core::network::MessageRouterBuilder;
pub use restate_meta::OptionsBuilder as MetaOptionsBuilder;
use restate_network::Networking;
pub use restate_worker::{OptionsBuilder as WorkerOptionsBuilder, RocksdbOptionsBuilder};

use std::ops::Deref;

use anyhow::bail;
use codederror::CodedError;
use tracing::{error, info};

use restate_core::{spawn_metadata_manager, MetadataManager};
use restate_core::{task_center, TaskKind};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::{GenerationalNodeId, Version};

use crate::network_server::{AdminDependencies, NetworkServer, WorkerDependencies};
use crate::roles::{AdminRole, WorkerRole};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("node failed to start due to failed safety check: {0}")]
    #[code(unknown)]
    SafetyCheck(String),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("building worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        roles::WorkerRoleBuildError,
    ),
    #[error("building cluster controller failed: {0}")]
    ClusterController(
        #[from]
        #[code]
        roles::AdminRoleBuildError,
    ),
    #[error("node neither runs cluster controller nor its address has been configured")]
    #[code(unknown)]
    UnknownClusterController,
    #[error("cluster bootstrap failed: {0}")]
    #[code(unknown)]
    Bootstrap(String),
}

pub struct Node {
    options: Options,
    metadata_manager: MetadataManager<Networking>,
    bifrost: BifrostService,
    admin_role: Option<AdminRole>,
    worker_role: Option<WorkerRole>,
    server: NetworkServer,
}

impl Node {
    pub fn new(options: Options) -> Result<Self, BuildError> {
        let opts = options.clone();
        // ensure we have cluster admin role if bootstrapping.
        if options.bootstrap_cluster {
            info!("Bootstrapping cluster");
            if !options.roles.contains(Role::Admin) {
                return Err(BuildError::Bootstrap(format!(
                    "Node must include the 'Admin' role when starting in bootstrap mode. Currently it has roles {}", options.roles
                )));
            }
        }

        let mut sr_builder = MessageRouterBuilder::default();
        let networking = Networking::default();
        let metadata_manager = MetadataManager::build(networking.clone());
        metadata_manager.register_in_message_router(&mut sr_builder);

        let admin_role = if options.roles.contains(Role::Admin) {
            Some(AdminRole::new(options.clone(), networking.clone())?)
        } else {
            None
        };

        let worker_role = if options.roles.contains(Role::Worker) {
            Some(WorkerRole::new(options.clone(), networking.clone())?)
        } else {
            None
        };

        let bifrost = options.bifrost.build(options.worker.partitions);

        let server = options.server.build(
            networking.connection_manager(),
            worker_role.as_ref().map(|worker| {
                WorkerDependencies::new(
                    worker.rocksdb_storage().clone(),
                    worker.worker_command_tx(),
                    worker.storage_query_context().clone(),
                    worker.schemas(),
                    worker.subscription_controller(),
                )
            }),
            admin_role.as_ref().map(|cluster_controller| {
                AdminDependencies::new(
                    cluster_controller.cluster_controller_handle(),
                    cluster_controller.schema_reader(),
                )
            }),
        );

        // Ensures that message router is updated after all services have registered themselves in
        // the builder.
        let message_router = sr_builder.build();
        networking
            .connection_manager()
            .set_message_router(message_router);

        Ok(Node {
            options: opts,
            metadata_manager,
            bifrost,
            admin_role,
            worker_role,
            server,
        })
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
        let tc = task_center();
        let metadata_writer = self.metadata_manager.writer();
        let metadata = self.metadata_manager.metadata();
        let is_set = tc.try_set_global_metadata(metadata.clone());
        debug_assert!(is_set, "Global metadata was already set");

        // Start metadata manager
        spawn_metadata_manager(&tc, self.metadata_manager)?;

        // If starting in bootstrap mode, we initialize the nodes configuration
        // with a static config.
        if self.options.bootstrap_cluster {
            let temp_id: GenerationalNodeId = if let Some(my_id) = self.options.node_id {
                my_id.with_generation(1)
            } else {
                // default to node-id 1 generation 1
                GenerationalNodeId::new(1, 0)
            };
            // Temporary: nodes configuration from current node.
            let mut nodes_config =
                NodesConfiguration::new(Version::MIN, self.options.cluster_name.clone());
            let address = self.options.server.advertise_address.clone();

            let my_node = NodeConfig::new(
                self.options.node_name.clone(),
                temp_id,
                address,
                self.options.roles,
            );
            nodes_config.upsert_node(my_node);
            info!(
                "Created a bootstrap nodes-configuration version {} for cluster {}",
                nodes_config.version(),
                self.options.cluster_name.clone(),
            );
            metadata_writer.update(nodes_config).await?;
            info!("Initial nodes configuration is loaded");
        } else {
            // TODO: Fetch nodes configuration from metadata store.
            //
            // Not supported at the moment
            bail!("Only cluster bootstrap mode is supported at the moment");
        }

        let nodes_config = metadata.nodes_config();
        // Find my node in nodes configuration.
        let Some(current_config) = nodes_config.find_node_by_name(&self.options.node_name) else {
            // Node is not in configuration. This is currently not supported. We only support
            // static configuration.
            bail!("Node is not in configuration. This is currently not supported.");
        };

        // Safety checks, same node (if set)?
        if self
            .options
            .node_id
            .is_some_and(|n| n != current_config.current_generation.as_plain())
        {
            return Err(Error::SafetyCheck(
                format!(
                    "Node ID mismatch: configured node ID is {}, but the nodes configuration contains {}",
                    self.options.node_id.unwrap(),
                    current_config.current_generation.as_plain()
                    )))?;
        }

        // Same cluster?
        if self.options.cluster_name != nodes_config.cluster_name() {
            return Err(Error::SafetyCheck(
                format!(
                    "Cluster name mismatch: configured cluster name is '{}', but the nodes configuration contains '{}'",
                    self.options.cluster_name,
                    nodes_config.cluster_name()
                    )))?;
        }

        // Setup node generation.
        // Bump the last generation
        let mut my_node_id = current_config.current_generation;
        my_node_id.bump_generation();

        // TODO: replace this temporary code with proper CAS write to metadata store
        // Simulate a node configuration update and commit
        {
            let address = self.options.server.advertise_address.clone();

            let my_node = NodeConfig::new(
                self.options.node_name.clone(),
                my_node_id,
                address,
                self.options.roles,
            );
            let mut editable_nodes_config: NodesConfiguration = nodes_config.deref().to_owned();
            editable_nodes_config.upsert_node(my_node);
            editable_nodes_config.increment_version();
            // push new nodes config to local cache. Simulates the write.
            metadata_writer.update(editable_nodes_config).await?;
        }
        // My Node ID is set
        metadata_writer.set_my_node_id(my_node_id);
        info!("My Node ID is {}", my_node_id);

        // Ensures bifrost has initial metadata synced up before starting the worker.
        self.bifrost.start().await?;

        if let Some(admin_role) = self.admin_role {
            tc.spawn(
                TaskKind::SystemBoot,
                "admin-init",
                None,
                admin_role.start(self.options.bootstrap_cluster),
            )?;
        }

        if let Some(worker_role) = self.worker_role {
            tc.spawn(
                TaskKind::SystemBoot,
                "worker-init",
                None,
                worker_role.start(),
            )?;
        }

        tc.spawn(
            TaskKind::RpcServer,
            "node-rpc-server",
            None,
            self.server.run(),
        )?;

        Ok(())
    }
}
