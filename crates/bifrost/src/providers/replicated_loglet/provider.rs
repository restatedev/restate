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
use restate_types::PlainNodeId;
use restate_types::nodes_config::{NodeConfig, Role, StorageState};
use restate_types::replication::{NodeSet, NodeSetSelector, NodeSetSelectorOptions};
use tracing::{debug, warn};

use restate_core::network::{MessageRouterBuilder, Networking, TransportConnect};
use restate_core::{Metadata, TaskCenter, TaskKind, my_node_id};
use restate_metadata_server::MetadataStoreClient;
use restate_types::config::Configuration;
use restate_types::logs::metadata::{
    Chain, LogletParams, ProviderConfiguration, ProviderKind, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId, RecordCache};
use restate_types::replicated_loglet::ReplicatedLogletParams;

use super::loglet::ReplicatedLoglet;
use super::metric_definitions;
use super::network::RequestPump;
use super::rpc_routers::{LogServersRpc, SequencersRpc};
use crate::Error;
use crate::loglet::{Loglet, LogletProvider, LogletProviderFactory, OperationError};
use crate::providers::replicated_loglet::error::ReplicatedLogletError;
use crate::providers::replicated_loglet::loglet::FindTailFlags;
use crate::providers::replicated_loglet::tasks::PeriodicTailChecker;

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
                let is_local_sequencer = loglet.is_sequencer_local();
                let key_value = entry.insert(Arc::new(loglet));

                let loglet = Arc::downgrade(key_value.value());
                // the periodic tail checker depends on whether we are a sequencer node or not.
                // For non-sequencer nodes, the period impacts the max lag of our read
                // streams' view of tail. For sequencers, we only need this to do periodic
                // releases/check-seals.
                let (duration, opts) = if is_local_sequencer {
                    (
                        Configuration::pinned()
                            .bifrost
                            .replicated_loglet
                            .sequencer_inactivity_timeout
                            .into(),
                        FindTailFlags::ForceSealCheck,
                    )
                } else {
                    (Duration::from_secs(2), FindTailFlags::Default)
                };
                let _ = TaskCenter::spawn(
                    TaskKind::BifrostBackgroundLowPriority,
                    "periodic-tail-checker",
                    PeriodicTailChecker::run(loglet_id, loglet, duration, opts),
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

        // use the last loglet if it was replicated as a source for preferred nodes to reduce data
        // scatter for this log.
        let mut preferred_nodes = match chain {
            Some(chain) if chain.tail().config.kind == ProviderKind::Replicated => {
                let tail = chain.tail();
                // Json serde
                let params =
                    ReplicatedLogletParams::deserialize_from(tail.config.params.as_bytes())
                        .map_err(|e| {
                            ReplicatedLogletError::LogletParamsParsingError(log_id, tail.index(), e)
                        })?;
                params.nodeset
            }
            _ => NodeSet::new(),
        };

        let new_segment_index = chain
            .map(|chain| chain.tail_index().next())
            .unwrap_or(SegmentIndex::OLDEST);

        let my_node = my_node_id();
        // If we are a log-server, it should be preferred.
        if Configuration::pinned().roles().contains(Role::LogServer) {
            preferred_nodes.insert(my_node);
        }

        let opts = NodeSetSelectorOptions::new(u32::from(log_id) as u64)
            .with_target_size(defaults.target_nodeset_size)
            .with_preferred_nodes(&preferred_nodes)
            .with_top_priority_node(my_node);

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        let selection = NodeSetSelector::select(
            &nodes_config,
            &defaults.replication_property,
            logserver_candidate_filter,
            |_, config| {
                matches!(
                    config.log_server_config.storage_state,
                    StorageState::ReadWrite
                )
            },
            opts,
        );

        match selection {
            Ok(nodeset) => {
                debug_assert!(nodeset.len() >= defaults.replication_property.num_copies() as usize);
                if defaults.replication_property.num_copies() > 1
                    && nodeset.len() == defaults.replication_property.num_copies() as usize
                {
                    warn!(
                        ?log_id,
                        replication = %defaults.replication_property,
                        generated_nodeset_size = nodeset.len(),
                        "The number of writeable log-servers is too small for the configured \
                        replication, there will be no fault-tolerance until you add more nodes."
                    );
                }
                let new_params = ReplicatedLogletParams {
                    loglet_id: LogletId::new(log_id, new_segment_index),
                    sequencer: my_node,
                    replication: defaults.replication_property.clone(),
                    nodeset,
                };

                let new_params = new_params
                    .serialize()
                    .expect("LogletParams serde is infallible");
                Ok(LogletParams::from(new_params))
            }
            Err(err) => {
                warn!(?log_id, "Cannot select node-set for log: {err}");
                Err(OperationError::retryable(err))
            }
        }
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}

pub fn logserver_candidate_filter(_node_id: PlainNodeId, config: &NodeConfig) -> bool {
    // Important note: we check if the server has role=log-server when storage_state is
    // provisioning because all nodes get provisioning storage by default, we only care about
    // log-servers so we avoid adding other nodes in the nodeset. In the case of read-write, we
    // don't check the role to not accidentally consider those nodes as non-logservers even if
    // the role was removed by mistake (although some protection should be added for this)
    match config.log_server_config.storage_state {
        StorageState::ReadWrite => true,
        // Why is this being commented out?
        // Just being conservative to avoid polluting nodesets with nodes that might have started
        // and crashed and never became log-servers. If enough nodes in this state were added to
        // nodesets, we might never be able to seal those loglets unless we actually start those
        // nodes.
        // The origin of allowing those nodes to be in new nodesets came from the need to
        // swap log-servers with new ones on rolling upgrades (N5 starts up to replace N1 for instance).
        // This requires that we drain N1 (marking it read-only) and start up N5. New nodesets
        // after this point should not include N1 and will include N5. For now, I prefer to
        // constraint this until we have the full story on how those operations will be
        // coordinated.
        //
        // StorageState::Provisioning if config.has_role(Role::LogServer) => true,
        // explicit match to make it clear that we are excluding nodes with the following states,
        // any new states added will force the compiler to fail
        StorageState::Provisioning
        | StorageState::Disabled
        | StorageState::ReadOnly
        | StorageState::DataLoss => false,
    }
}
