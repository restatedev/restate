// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::debug;

use restate_core::network::{MessageRouterBuilder, Networking, TransportConnect};
use restate_core::{my_node_id, Metadata, TaskCenter, TaskKind};
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::Configuration;
use restate_types::logs::metadata::{
    Chain, LogletParams, ProviderConfiguration, ProviderKind, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId, RecordCache};
use restate_types::replicated_loglet::ReplicatedLogletParams;

use super::loglet::ReplicatedLoglet;
use super::metric_definitions;
use super::network::RequestPump;
use super::nodeset_selector::{NodeSelectionError, NodeSetSelector, ObservedClusterState};
use super::rpc_routers::{LogServersRpc, SequencersRpc};
use crate::loglet::{Loglet, LogletProvider, LogletProviderFactory, OperationError};
use crate::providers::replicated_loglet::error::ReplicatedLogletError;
use crate::providers::replicated_loglet::tasks::PeriodicTailChecker;
use crate::Error;

pub struct Factory<T> {
    metadata_store_client: MetadataStoreClient,
    networking: Networking<T>,
    logserver_rpc_routers: LogServersRpc,
    sequencer_rpc_routers: SequencersRpc,
    request_pump: RequestPump,
    record_cache: RecordCache,
}

impl<T: TransportConnect> Factory<T> {
    pub fn new(
        metadata_store_client: MetadataStoreClient,
        networking: Networking<T>,
        record_cache: RecordCache,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        // Handling Sequencer(s) incoming requests
        let request_pump = RequestPump::new(
            &Configuration::pinned().bifrost.replicated_loglet,
            networking.metadata().clone(),
            router_builder,
        );

        let logserver_rpc_routers = LogServersRpc::new(router_builder);
        let sequencer_rpc_routers = SequencersRpc::new(router_builder);
        // todo(asoli): Create a handler to answer to control plane monitoring questions
        Self {
            metadata_store_client,
            networking,
            logserver_rpc_routers,
            sequencer_rpc_routers,
            request_pump,
            record_cache,
        }
    }
}

#[async_trait]
impl<T: TransportConnect> LogletProviderFactory for Factory<T> {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Replicated
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, OperationError> {
        metric_definitions::describe_metrics();
        let provider = Arc::new(ReplicatedLogletProvider::new(
            self.metadata_store_client,
            self.networking,
            self.logserver_rpc_routers,
            self.sequencer_rpc_routers,
            self.record_cache,
        ));
        // run the request pump. The request pump handles/routes incoming messages to our
        // locally hosted sequencers.
        TaskCenter::spawn(TaskKind::NetworkMessageHandler, "sequencers-ingress", {
            let request_pump = self.request_pump;
            let provider = provider.clone();
            request_pump.run(provider)
        })?;

        Ok(provider)
    }
}

pub(super) struct ReplicatedLogletProvider<T> {
    active_loglets: DashMap<(LogId, SegmentIndex), Arc<ReplicatedLoglet<T>>>,
    _metadata_store_client: MetadataStoreClient,
    networking: Networking<T>,
    record_cache: RecordCache,
    logserver_rpc_routers: LogServersRpc,
    sequencer_rpc_routers: SequencersRpc,
}

impl<T: TransportConnect> ReplicatedLogletProvider<T> {
    fn new(
        metadata_store_client: MetadataStoreClient,
        networking: Networking<T>,
        logserver_rpc_routers: LogServersRpc,
        sequencer_rpc_routers: SequencersRpc,
        record_cache: RecordCache,
    ) -> Self {
        Self {
            active_loglets: Default::default(),
            _metadata_store_client: metadata_store_client,
            networking,
            record_cache,
            logserver_rpc_routers,
            sequencer_rpc_routers,
        }
    }

    pub(crate) fn networking(&self) -> &Networking<T> {
        &self.networking
    }
    /// Gets a loglet if it's already have been activated
    pub(crate) fn get_active_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
    ) -> Option<Arc<ReplicatedLoglet<T>>> {
        self.active_loglets
            .get(&(log_id, segment_index))
            .map(|l| l.clone())
    }

    pub(crate) fn get_or_create_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: &LogletParams,
    ) -> Result<Arc<ReplicatedLoglet<T>>, ReplicatedLogletError> {
        let loglet = match self.active_loglets.entry((log_id, segment_index)) {
            dashmap::Entry::Vacant(entry) => {
                // NOTE: replicated-loglet expects params to be a `json` string.
                let params =
                    ReplicatedLogletParams::deserialize_from(params.as_bytes()).map_err(|e| {
                        ReplicatedLogletError::LogletParamsParsingError(log_id, segment_index, e)
                    })?;

                debug!(
                    log_id = %log_id,
                    segment_index = %segment_index,
                    loglet_id = %params.loglet_id,
                    nodeset = %params.nodeset,
                    sequencer = %params.sequencer,
                    replication = %params.replication,
                    "Creating a replicated loglet client"
                );

                let loglet_id = params.loglet_id;
                // Create loglet
                let loglet = ReplicatedLoglet::new(
                    log_id,
                    segment_index,
                    params,
                    self.networking.clone(),
                    self.logserver_rpc_routers.clone(),
                    self.sequencer_rpc_routers.clone(),
                    self.record_cache.clone(),
                );
                let key_value = entry.insert(Arc::new(loglet));
                let loglet = Arc::downgrade(key_value.value());
                let _ = TaskCenter::spawn(
                    TaskKind::BifrostBackgroundLowPriority,
                    "periodic-tail-checker",
                    // todo: configuration
                    PeriodicTailChecker::run(loglet_id, loglet, Duration::from_secs(2)),
                );
                Arc::clone(key_value.value())
            }
            dashmap::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet)
    }
}

#[async_trait]
impl<T: TransportConnect> LogletProvider for ReplicatedLogletProvider<T> {
    async fn get_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet>, Error> {
        let loglet = self.get_or_create_loglet(log_id, segment_index, params)?;
        Ok(loglet as Arc<dyn Loglet>)
    }

    fn propose_new_loglet_params(
        &self,
        log_id: LogId,
        chain: Option<&Chain>,
        defaults: &ProviderConfiguration,
    ) -> Result<LogletParams, OperationError> {
        let ProviderConfiguration::Replicated(defaults) = defaults else {
            panic!("ProviderConfiguration::Replicated is expected");
        };

        let new_segment_index = chain
            .map(|c| c.tail_index().next())
            .unwrap_or(SegmentIndex::OLDEST);

        let loglet_id = LogletId::new(log_id, new_segment_index);

        let mut rng = rand::thread_rng();

        let replication = defaults.replication_property.clone();

        // if the last loglet in the chain is of the same provider kind, we can use this to
        // influence the nodeset selector.
        let previous_params = chain.and_then(|chain| {
            let tail_config = chain.tail().config;
            match tail_config.kind {
                ProviderKind::Replicated => Some(
                    ReplicatedLogletParams::deserialize_from(tail_config.params.as_bytes())
                        .expect("params serde must be infallible"),
                ),
                // Another kind, we don't care about its config
                _ => None,
            }
        });

        let preferred_nodes = previous_params
            .map(|p| p.nodeset.clone())
            .unwrap_or_default();
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        let selection = NodeSetSelector::new(&nodes_config, &ObservedClusterState).select(
            &replication,
            &mut rng,
            &preferred_nodes,
        );

        match selection {
            Ok(nodeset) => Ok(LogletParams::from(
                ReplicatedLogletParams {
                    loglet_id,
                    // We choose ourselves to be the sequencer for this loglet
                    sequencer: my_node_id(),
                    replication,
                    nodeset,
                }
                .serialize()
                .expect("params serde must be infallible"),
            )),
            Err(e @ NodeSelectionError::InsufficientWriteableNodes) => {
                debug!(
                    ?loglet_id,
                    "Insufficient writeable nodes to select new nodeset for replicated loglet"
                );

                Err(OperationError::terminal(e))
            }
        }
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}
