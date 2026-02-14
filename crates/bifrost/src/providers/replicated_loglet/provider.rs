// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use restate_core::network::{
    BackPressureMode, Buffered, MessageRouterBuilder, Networking, TransportConnect,
};
use restate_core::{Metadata, TaskCenter, TaskCenterFutureExt, TaskKind, my_node_id};
use restate_types::config::Configuration;
use restate_types::logs::metadata::{
    Chain, LogletParams, ProviderConfiguration, ProviderKind, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId, RecordCache};
use restate_types::net::replicated_loglet::{SequencerDataService, SequencerMetaService};
use restate_types::nodes_config::{Role, StorageState};
use restate_types::replicated_loglet::{ReplicatedLogletParams, logserver_candidate_filter};
use restate_types::replication::{
    NodeSet, NodeSetChecker, NodeSetSelector, NodeSetSelectorOptions, ReplicationProperty,
};

use super::loglet::ReplicatedLoglet;
use super::metric_definitions;
use super::network::{SequencerDataRpcHandler, SequencerInfoRpcHandler};
use crate::Error;
use crate::loglet::{Improvement, Loglet, LogletProvider, LogletProviderFactory, OperationError};
use crate::providers::replicated_loglet::error::ReplicatedLogletError;
use crate::providers::replicated_loglet::loglet::FindTailFlags;
use crate::providers::replicated_loglet::tasks::PeriodicTailChecker;

pub struct Factory<T> {
    networking: Networking<T>,
    data_request_pump: Buffered<SequencerDataService>,
    info_request_pump: Buffered<SequencerMetaService>,
    record_cache: RecordCache,
}

impl<T: TransportConnect> Factory<T> {
    pub fn new(
        networking: Networking<T>,
        record_cache: RecordCache,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        // Handling Sequencer(s) incoming data requests
        let data_pool = TaskCenter::with_current(|tc| {
            tc.memory_controller().create_pool(
                // NOTE: This is a shared pool with log-server store data path
                "log-server-data",
                || Configuration::pinned().log_server.data_service_memory_limit,
            )
        });
        let data_request_pump = router_builder
            .register_buffered_service_with_pool(data_pool, BackPressureMode::PushBack);
        // Sequencer meta uses the default shared pool.
        let info_request_pump =
            router_builder.register_buffered_service(BackPressureMode::PushBack);

        Self {
            networking,
            data_request_pump,
            info_request_pump,
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
            self.networking,
            self.record_cache,
        ));

        // run the request pump. The request pump handles/routes incoming messages to our
        // locally hosted sequencers.
        self.data_request_pump.start(
            TaskKind::NetworkMessageHandler,
            "sequencer-data-ingress",
            SequencerDataRpcHandler::new(provider.clone()),
        )?;

        self.info_request_pump.start(
            TaskKind::NetworkMessageHandler,
            "sequencer-info-ingress",
            SequencerInfoRpcHandler::new(provider.clone()),
        )?;

        Ok(provider)
    }
}

pub(super) struct ReplicatedLogletProvider<T> {
    active_loglets: DashMap<(LogId, SegmentIndex), Arc<ReplicatedLoglet<T>>>,
    networking: Networking<T>,
    record_cache: RecordCache,
}

impl<T: TransportConnect> ReplicatedLogletProvider<T> {
    fn new(networking: Networking<T>, record_cache: RecordCache) -> Self {
        Self {
            active_loglets: Default::default(),
            networking,
            record_cache,
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

    fn may_improve_params(
        &self,
        log_id: LogId,
        current_params: &LogletParams,
        defaults: &ProviderConfiguration,
    ) -> Result<Improvement, OperationError> {
        let ProviderConfiguration::Replicated(defaults) = defaults else {
            panic!("ProviderConfiguration::Replicated is expected");
        };

        let current_params = ReplicatedLogletParams::deserialize_from(current_params.as_bytes())
            .map_err(|e| {
                ReplicatedLogletError::LogletParamsParsingError(
                    log_id,
                    0.into(), /* dummy index */
                    e,
                )
            })?;

        let mut preferred_nodes = current_params.nodeset.clone();

        let my_node = my_node_id();

        // improvement to apply the replication property
        if current_params.replication != defaults.replication_property {
            return Ok(Improvement::Possible {
                reason: format!(
                    "replication can change from {} to {}",
                    current_params.replication, defaults.replication_property
                ),
            });
        }

        // improvement by moving the sequencer to this node
        if current_params.sequencer != my_node {
            return Ok(Improvement::Possible {
                reason: format!(
                    "sequencer can move from {} to {}",
                    current_params.sequencer, my_node
                ),
            });
        }

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

        let new_nodeset = selection.map_err(OperationError::retryable)?;

        let mut node_set_checker =
            NodeSetChecker::new(&new_nodeset, &nodes_config, &defaults.replication_property);
        node_set_checker.fill_with(true);

        // check that the new node set fulfills the replication property
        if !node_set_checker.check_write_quorum(|attr| *attr) {
            // we couldn't find a nodeset that fulfills the desired replication property
            return Ok(Improvement::None);
        }

        if new_nodeset.len() < current_params.nodeset.len() {
            // a bigger nodeset is a better nodeset, we reject a smaller offer
            return Ok(Improvement::None);
        }
        // if it's identical, just shuffled around, then no, do nothing.
        if current_params.nodeset.is_subset(&new_nodeset)
            && new_nodeset.len() == current_params.nodeset.len()
        {
            return Ok(Improvement::None);
        }

        Ok(Improvement::Possible {
            reason: format!(
                "nodeset update from {} to {}",
                current_params.nodeset, new_nodeset
            ),
        })
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
        let mut preferred_nodes = if let Some(chain) = chain
            && let Some(tail) = chain.non_special_tail()
            && tail.config.kind == ProviderKind::Replicated
        {
            // Json serde
            let params = ReplicatedLogletParams::deserialize_from(tail.config.params.as_bytes())
                .map_err(|e| {
                    ReplicatedLogletError::LogletParamsParsingError(log_id, tail.index(), e)
                })?;
            params.nodeset
        } else {
            NodeSet::new()
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
                let mut node_set_checker =
                    NodeSetChecker::new(&nodeset, &nodes_config, &defaults.replication_property);
                node_set_checker.fill_with(true);

                // check that the new node set fulfills the replication property
                if !node_set_checker.check_write_quorum(|attr| *attr) {
                    // we couldn't find a nodeset that fulfills the desired replication property
                    return Err(OperationError::terminal(InsufficientNodesError(
                        defaults.replication_property.clone(),
                    )));
                }

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
            Err(err) => Err(OperationError::terminal(err)),
        }
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        let mut tasks = JoinSet::new();
        // Drain and seal loglets with local sequencers
        for loglet in &self.active_loglets {
            let loglet = loglet.clone();
            tasks
                .build_task()
                .name("shutdown-loglet")
                .spawn(async move { loglet.shutdown().await }.in_current_tc())
                .expect("to spawn loglet shutdown");
        }

        let _ = tasks.join_all().await;
        info!("All sequencers were stopped");
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("not enough candidate nodes to form a node set that fulfills the replication property {0}")]
pub struct InsufficientNodesError(ReplicationProperty);
