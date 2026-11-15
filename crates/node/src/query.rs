// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Node query-service initialization and admin table registration.

use std::sync::Arc;

use tokio::sync::watch;

use restate_bifrost::Bifrost;
use restate_core::network::{MessageRouterBuilder, Networking, TransportConnect};
use restate_core::partitions::PartitionRouting;
use restate_core::{Metadata, TaskCenter, TaskKind};
use restate_distributed_datafusion as v2;
use restate_limiter::rule_book::RuleBookObserver;
use restate_log_server::LogServerService;
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::PartitionStoreManager;
use restate_storage_query_api::QueryEngine;
use restate_storage_query_datafusion as v1;
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::config::{Configuration, QueryEngineOptions};
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_worker_api::ProcessorsManagerHandle;

use crate::{BuildError, introspection};

/// Startup-only environments and scanner registries shared by query services and the admin engine.
pub(crate) struct QueryEngineBuilder {
    env: v1::DataFusionEnv,
    scanners: v1::remote_query_scanner_manager::RemoteScannerManager,
    distributed: Option<(v2::DataFusionEnv, v2::source_registry::SourceRegistry)>,
    metadata: Metadata,
    replica_set_states: PartitionReplicaSetStates,
}

impl QueryEngineBuilder {
    pub(crate) fn new<T: TransportConnect>(
        config: &Configuration,
        networking: Networking<T>,
        metadata: Metadata,
        replica_set_states: PartitionReplicaSetStates,
    ) -> Result<Self, BuildError> {
        let routing = PartitionRouting::new(replica_set_states.clone(), TaskCenter::current());
        let env = v1::DataFusionEnv::from_options(&config.admin.query_engine)
            .map_err(v1::BuildError::from)?;
        let scanners = v1::remote_query_scanner_manager::RemoteScannerManager::new(
            v1::remote_query_scanner_client::create_remote_scanner_service(networking.clone()),
            v1::remote_query_scanner_manager::create_partition_locator(
                routing.clone(),
                metadata.clone(),
            ),
            metadata.clone(),
        );
        let distributed = if config.common.experimental.is_query_engine_v2_running() {
            // V2 runs on V1's DataFusion runtime: both engines draw on one memory pool.
            let distributed_env = v2::DataFusionEnv::from_options(&config.admin.query_engine)
                .map_err(v2::BuildError::from)?
                .with_runtime(Arc::clone(env.runtime()))
                .with_distributed_execution(networking);
            let scanners = v2::source_registry::SourceRegistry::new(
                v2::source_registry::create_partition_locator(routing, metadata.clone()),
                metadata.clone(),
                TaskCenter::current().cluster_state().clone(),
            );
            Some((distributed_env, scanners))
        } else {
            None
        };
        Ok(Self {
            env,
            scanners,
            distributed,
            metadata,
            replica_set_states,
        })
    }

    pub(crate) fn register_local_scanners(
        &self,
        partition_store_manager: &Arc<PartitionStoreManager>,
        processor_manager: Option<&ProcessorsManagerHandle>,
        log_server: Option<&LogServerService>,
        bifrost: &Bifrost,
        metadata_store_client: &MetadataStoreClient,
    ) {
        if let Some((_, scanners)) = &self.distributed {
            v2::local_scanners::register_metadata_scanners(
                self.metadata.updateable_schema(),
                metadata_store_client.clone(),
                processor_manager.map(ProcessorsManagerHandle::rule_book_observer),
                scanners,
            );
        }
        if let Some(processor_manager) = processor_manager {
            // Both engines read storage on V1's query IO pool, one pool per node.
            let io = self.env.io_executor();
            v1::local_scanners::register_partition_scanners(
                Arc::clone(partition_store_manager),
                io.clone(),
                &self.scanners,
            );
            v1::local_scanners::register_cluster_scanners(
                Arc::clone(partition_store_manager),
                self.metadata.clone(),
                io.clone(),
                &self.scanners,
            );
            v1::local_scanners::register_live_scanners(
                processor_manager.query_access(),
                &self.scanners,
            );
            if let Some((_, scanners)) = &self.distributed {
                v2::local_scanners::register_worker_partition_scanners(
                    processor_manager.clone(),
                    io.clone(),
                    scanners,
                );
                v2::local_scanners::register_cluster_scanners(
                    Arc::clone(partition_store_manager),
                    self.metadata.clone(),
                    io.clone(),
                    scanners,
                );
                v2::local_scanners::register_live_scanners(scanners);
            }
        }
        if let Some(log_server) = log_server {
            self.scanners
                .register_node_scanner::<v1::loglet_worker::LogletWorkersTable>(
                    introspection::loglet_workers::create_local_scanner(
                        log_server.state_map().clone(),
                        log_server.active_worker_map().clone(),
                        self.metadata.clone(),
                    ),
                );
        }
        let scanner = introspection::bifrost_read_streams::create_local_scanner(
            bifrost.read_stream_registry().clone(),
            self.metadata.clone(),
        );
        self.scanners
            .register_node_scanner::<v1::bifrost_read_stream::BifrostReadStreamsTable>(Arc::clone(
                &scanner,
            ));
        if let Some((_, scanners)) = &self.distributed {
            scanners
                .register_node_scanner::<v2::tables::bifrost_read_stream::BifrostReadStreamsTable>(
                    scanner,
                );
        }
        self.scanners
            .register_node_scanner::<v1::config::ConfigTable>(v1::config::create_scanner(
                self.metadata.clone(),
                Configuration::live(),
            ));
    }

    pub(crate) fn create_services(&self, router: &mut MessageRouterBuilder) -> QueryServices {
        QueryServices {
            scanner: v1::remote_query_scanner_server::RemoteQueryScannerServer::new(
                self.env.clone(),
                self.scanners.clone(),
                router,
            ),
            distributed: self.distributed.as_ref().map(|(env, scanners)| {
                v2::distributed::DistributedQueryServer::new(env.clone(), scanners.clone(), router)
            }),
        }
    }

    pub(crate) async fn build_admin_engine(
        self,
        options: &QueryEngineOptions,
        metadata_store_client: MetadataStoreClient,
        cluster_state_watch: Option<watch::Receiver<Arc<LegacyClusterState>>>,
        local_rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    ) -> Result<Arc<QueryEngine>, BuildError> {
        self.env
            .register_tables(v1::ClusterTables::new(
                self.replica_set_states,
                cluster_state_watch.clone(),
                self.scanners.clone(),
            ))
            .await?;
        self.env
            .register_tables(
                v1::UserTables::new(v1::context::SelectPartitionsFromMetadata, self.scanners)
                    .with_metadata(v1::MetadataTables::new(
                        self.metadata.updateable_schema(),
                        metadata_store_client,
                        local_rule_book_observer,
                    )),
            )
            .await?;
        // One `/query` rate limit per node, shared by both engines.
        let rate_limiter = v1::context::query_rate_limiter(options.rate_limiting.as_ref());
        let mut engine =
            v1::context::DataFusionQueryEngine::with_rate_limiter(self.env, rate_limiter.clone());
        if let Some((env, scanners)) = self.distributed {
            env.register_tables(
                v2::ClusterTables::new(scanners.clone())
                    .with_cluster_state_watch(cluster_state_watch),
            )
            .await?;
            env.register_tables(
                v2::UserTables::new(v2::context::SelectPartitionsFromMetadata, scanners)
                    .with_metadata(),
            )
            .await?;
            engine = engine.with_engine(v2::context::DataFusionQueryEngine::with_rate_limiter(
                env,
                rate_limiter,
            ));
        }
        Ok(Arc::new(engine))
    }
}

pub(crate) struct QueryServices {
    scanner: v1::remote_query_scanner_server::RemoteQueryScannerServer,
    distributed: Option<v2::distributed::DistributedQueryServer>,
}

impl QueryServices {
    pub(crate) fn start(self) -> anyhow::Result<()> {
        TaskCenter::spawn(
            TaskKind::SystemService,
            "datafusion-scan-server",
            self.scanner.run(),
        )?;
        if let Some(server) = self.distributed {
            TaskCenter::spawn(
                TaskKind::SystemService,
                "distributed-query-server",
                server.run(),
            )?;
        }
        Ok(())
    }
}
