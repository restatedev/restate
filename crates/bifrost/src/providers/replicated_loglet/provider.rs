// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::sync::Arc;
use std::time::Duration;

use ahash::HashMap;
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use restate_core::network::{
    BackPressureMode, Buffered, MessageRouterBuilder, Networking, TransportConnect,
};
use restate_core::{Metadata, TaskCenter, TaskCenterFutureExt, TaskKind, my_node_id};
use restate_types::config::Configuration;
use restate_types::locality::LocationScope;
use restate_types::logs::metadata::{
    Chain, LogletParams, Logs, ProviderConfiguration, ProviderKind, ReplicatedLogletConfig,
    SegmentIndex,
};
use restate_types::logs::{LogId, LogletId, RecordCache};
use restate_types::net::replicated_loglet::{SequencerDataService, SequencerMetaService};
use restate_types::nodes_config::{NodesConfiguration, Role, StorageState};
use restate_types::replicated_loglet::{ReplicatedLogletParams, logserver_candidate_filter};
use restate_types::replication::{
    NodeSelectorError, NodeSet, NodeSetChecker, NodeSetSelector, NodeSetSelectorOptions,
    ReplicationProperty, extend_top_n_load_balanced,
};
use restate_types::{PlainNodeId, Version, Versioned};

use super::loglet::ReplicatedLoglet;
use super::metric_definitions;
use super::network::{SequencerDataRpcHandler, SequencerInfoRpcHandler};
use crate::Error;
use crate::loglet::{Improvement, Loglet, LogletProvider, LogletProviderFactory, OperationError};
use crate::providers::replicated_loglet::error::ReplicatedLogletError;
use crate::providers::replicated_loglet::loglet::FindTailFlags;
use crate::providers::replicated_loglet::tasks::PeriodicTailChecker;

// Pick from the top HRW candidates to keep placement deterministic and stable,
// while still allowing the least-loaded nearby candidate to win.
const BALANCED_PLACEMENT_TOP_N: usize = 3;

fn nodeset_load_range(
    candidates: &[PlainNodeId],
    mut load: impl FnMut(PlainNodeId) -> usize,
) -> usize {
    let Some(first) = candidates.first().copied() else {
        return 0;
    };

    let mut min = load(first);
    let mut max = min;
    for node_id in candidates.iter().copied().skip(1) {
        let load = load(node_id);
        min = min.min(load);
        max = max.max(load);
    }

    max - min
}

fn balanced_nodeset_rebalance_improvement(
    candidates: &[PlainNodeId],
    current_nodeset: &NodeSet,
    proposed_nodeset: &NodeSet,
    current_loads: &HashMap<PlainNodeId, usize>,
    acceptable_range: usize,
) -> Option<(usize, usize)> {
    if current_nodeset.is_equivalent(proposed_nodeset) {
        return None;
    }

    let current_range = nodeset_load_range(candidates, |node_id| {
        current_loads.get(&node_id).copied().unwrap_or_default()
    });
    if current_range <= acceptable_range {
        return None;
    }

    let proposed_range = nodeset_load_range(candidates, |node_id| {
        let mut load = current_loads.get(&node_id).copied().unwrap_or_default();
        if current_nodeset.contains(node_id) && !proposed_nodeset.contains(node_id) {
            load = load.saturating_sub(1);
        }
        if proposed_nodeset.contains(node_id) && !current_nodeset.contains(node_id) {
            load += 1;
        }
        load
    });

    (proposed_range < current_range).then_some((current_range, proposed_range))
}

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
                || {
                    Configuration::pinned()
                        .log_server
                        .rocksdb_data_memtables_budget()
                },
            )
        });
        let data_request_pump =
            router_builder.register_buffered_service_with_pool(data_pool, BackPressureMode::Lossy);
        // Sequencer meta uses the default shared pool.
        let info_request_pump = router_builder.register_buffered_service(BackPressureMode::Lossy);

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
    nodeset_load_cache: Mutex<NodesetLoadCache>,
}

#[derive(Debug)]
struct NodesetLoadCache {
    logs_version: Version,
    loads: HashMap<PlainNodeId, usize>,
}

impl Default for NodesetLoadCache {
    fn default() -> Self {
        Self {
            logs_version: Version::INVALID,
            loads: HashMap::default(),
        }
    }
}

impl<T: TransportConnect> ReplicatedLogletProvider<T> {
    fn new(networking: Networking<T>, record_cache: RecordCache) -> Self {
        Self {
            active_loglets: Default::default(),
            networking,
            record_cache,
            nodeset_load_cache: Mutex::default(),
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

    fn experimental_balanced_placement_enabled() -> bool {
        Configuration::pinned()
            .common
            .experimental_placement_strategy
            .is_balanced_v2()
    }

    fn experimental_rebalances_when_healthy() -> bool {
        Configuration::pinned()
            .common
            .experimental_placement_rebalance_mode
            .rebalances_when_healthy()
    }

    fn supports_balanced_placement(replication: &ReplicationProperty) -> bool {
        replication.copies_at_scope(LocationScope::Region).is_none()
            && replication.copies_at_scope(LocationScope::Zone).is_none()
    }

    fn target_nodeset_size(
        replication: &ReplicationProperty,
        requested_size: u32,
        candidates: usize,
    ) -> Option<usize> {
        let replication_factor = replication.num_copies() as usize;
        // Keep the legacy target-size lower bound: 2r-1 for quorum overlap, and
        // r+1 so rf=1 still gets one spare when there is capacity.
        let target_size = cmp::max(requested_size as usize, replication_factor * 2 - 1);
        let target_size = cmp::max(replication_factor + 1, target_size);
        let target_size = target_size.min(candidates);

        (target_size >= replication_factor).then_some(target_size)
    }

    fn writable_log_server_candidates(nodes_config: &NodesConfiguration) -> Vec<PlainNodeId> {
        nodes_config
            .iter()
            .filter(|(node_id, node_config)| {
                logserver_candidate_filter(*node_id, node_config)
                    && matches!(
                        node_config.log_server_config.storage_state,
                        StorageState::ReadWrite
                    )
            })
            .map(|(node_id, _)| node_id)
            .collect()
    }

    fn compute_nodeset_loads(logs: &Logs) -> HashMap<PlainNodeId, usize> {
        let mut loads = HashMap::<PlainNodeId, usize>::default();

        for (log_id, chain) in logs.iter() {
            let tail = chain.tail();
            if tail.config.kind != ProviderKind::Replicated {
                continue;
            }

            let params = match ReplicatedLogletParams::deserialize_from(
                tail.config.params.as_bytes(),
            ) {
                Ok(params) => params,
                Err(err) => {
                    warn!(
                        %log_id,
                        segment_index = %tail.index(),
                        %err,
                        "Ignoring replicated loglet with invalid params while computing nodeset loads"
                    );
                    continue;
                }
            };

            for node_id in params.nodeset.iter().copied() {
                *loads.entry(node_id).or_default() += 1;
            }
        }

        loads
    }

    fn current_nodeset_loads(
        &self,
        replaced_nodeset: Option<&NodeSet>,
    ) -> HashMap<PlainNodeId, usize> {
        let logs = Metadata::with_current(|m| m.logs_snapshot());
        let logs_version = logs.version();

        let mut loads = {
            let mut cache = self.nodeset_load_cache.lock();
            if cache.logs_version != logs_version {
                cache.loads = Self::compute_nodeset_loads(&logs);
                cache.logs_version = logs_version;
            }
            cache.loads.clone()
        };

        if let Some(replaced_nodeset) = replaced_nodeset {
            for node_id in replaced_nodeset.iter() {
                if let Some(load) = loads.get_mut(node_id) {
                    *load = load.saturating_sub(1);
                }
            }
        }

        loads
    }

    fn select_nodeset(
        &self,
        log_id: LogId,
        defaults: &ReplicatedLogletConfig,
        nodes_config: &NodesConfiguration,
        preferred_nodes: &NodeSet,
        replaced_nodeset: Option<&NodeSet>,
    ) -> Result<NodeSet, NodeSelectorError> {
        let my_node = my_node_id().as_plain();

        if Self::experimental_balanced_placement_enabled() {
            if Self::supports_balanced_placement(&defaults.replication_property) {
                return self.select_balanced_nodeset(
                    log_id,
                    defaults,
                    nodes_config,
                    my_node,
                    preferred_nodes,
                    replaced_nodeset,
                );
            }

            warn!(
                replication = %defaults.replication_property,
                "Experimental balanced replicated-loglet nodeset placement only supports flat replication; falling back to legacy placement"
            );
        }

        let opts = NodeSetSelectorOptions::new(u32::from(log_id) as u64)
            .with_target_size(defaults.target_nodeset_size)
            .with_preferred_nodes(preferred_nodes)
            .with_top_priority_node(my_node);

        NodeSetSelector::select(
            nodes_config,
            &defaults.replication_property,
            logserver_candidate_filter,
            |_, config| {
                matches!(
                    config.log_server_config.storage_state,
                    StorageState::ReadWrite
                )
            },
            opts,
        )
    }

    fn select_balanced_nodeset(
        &self,
        log_id: LogId,
        defaults: &ReplicatedLogletConfig,
        nodes_config: &NodesConfiguration,
        my_node: PlainNodeId,
        preferred_nodes: &NodeSet,
        replaced_nodeset: Option<&NodeSet>,
    ) -> Result<NodeSet, NodeSelectorError> {
        let candidates = Self::writable_log_server_candidates(nodes_config);
        let Some(target_size) = Self::target_nodeset_size(
            &defaults.replication_property,
            defaults.target_nodeset_size.as_u32(),
            candidates.len(),
        ) else {
            return Err(NodeSelectorError::InsufficientNodes {
                nodeset_len: candidates.len(),
                replication_factor: defaults.replication_property.num_copies(),
            });
        };

        let rebalance = Self::experimental_rebalances_when_healthy();
        let mut selected = if rebalance {
            NodeSet::new()
        } else {
            preferred_nodes.clone()
        };
        // Keep the sequencer colocated with a writable log-server when possible.
        selected.insert(my_node);

        let loads = self.current_nodeset_loads(replaced_nodeset);

        if !rebalance {
            selected.retain(|node_id| candidates.contains(node_id));
            if selected.len() >= target_size {
                return Ok(selected);
            }
        }

        let candidates_len = candidates.len();
        extend_top_n_load_balanced(
            candidates,
            u32::from(log_id) as u64,
            target_size,
            selected,
            BALANCED_PLACEMENT_TOP_N,
            |node_id| loads.get(&node_id).copied().unwrap_or_default(),
        )
        .ok_or(NodeSelectorError::InsufficientNodes {
            nodeset_len: candidates_len,
            replication_factor: defaults.replication_property.num_copies(),
        })
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

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        let balanced_nodeset_rebalance = Self::experimental_balanced_placement_enabled()
            && Self::experimental_rebalances_when_healthy()
            && Self::supports_balanced_placement(&defaults.replication_property);
        let balanced_nodeset_rebalance_candidates =
            balanced_nodeset_rebalance.then(|| Self::writable_log_server_candidates(&nodes_config));
        let new_nodeset = self
            .select_nodeset(
                log_id,
                defaults,
                &nodes_config,
                &preferred_nodes,
                Some(&current_params.nodeset),
            )
            .map_err(OperationError::retryable)?;

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
        if current_params.nodeset.is_equivalent(&new_nodeset) {
            return Ok(Improvement::None);
        }

        let nodeset_range_improvement = if let Some(candidates) =
            balanced_nodeset_rebalance_candidates.as_deref()
            && let Some(target_size) = Self::target_nodeset_size(
                &defaults.replication_property,
                defaults.target_nodeset_size.as_u32(),
                candidates.len(),
            )
            && current_params.nodeset.len() >= target_size
            && current_params
                .nodeset
                .iter()
                .all(|node_id| candidates.contains(node_id))
        {
            let current_loads = self.current_nodeset_loads(None);
            let Some(improvement) = balanced_nodeset_rebalance_improvement(
                candidates,
                &current_params.nodeset,
                &new_nodeset,
                &current_loads,
                target_size,
            ) else {
                return Ok(Improvement::None);
            };
            Some(improvement)
        } else {
            None
        };

        Ok(Improvement::Possible {
            reason: if let Some((current_range, proposed_range)) = nodeset_range_improvement {
                format!(
                    "nodeset update from {} to {} reduces nodeset load range from {} to {}",
                    current_params.nodeset, new_nodeset, current_range, proposed_range
                )
            } else {
                format!(
                    "nodeset update from {} to {}",
                    current_params.nodeset, new_nodeset
                )
            },
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

        // Use the last loglet if it was replicated as a source for preferred nodes to reduce data
        // scatter for this log.
        let replaced_nodeset = if let Some(chain) = chain
            && let Some(tail) = chain.non_special_tail()
            && tail.config.kind == ProviderKind::Replicated
        {
            // Json serde
            let params = ReplicatedLogletParams::deserialize_from(tail.config.params.as_bytes())
                .map_err(|e| {
                    ReplicatedLogletError::LogletParamsParsingError(log_id, tail.index(), e)
                })?;
            Some(params.nodeset)
        } else {
            None
        };
        let mut preferred_nodes = replaced_nodeset.clone().unwrap_or_else(NodeSet::new);

        let new_segment_index = chain
            .map(|chain| chain.tail_index().next())
            .unwrap_or(SegmentIndex::OLDEST);

        let my_node = my_node_id();
        // If we are a log-server, it should be preferred.
        if Configuration::pinned().roles().contains(Role::LogServer) {
            preferred_nodes.insert(my_node);
        }

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        let selection = self.select_nodeset(
            log_id,
            defaults,
            &nodes_config,
            &preferred_nodes,
            replaced_nodeset.as_ref(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u32) -> PlainNodeId {
        PlainNodeId::new(id)
    }

    fn loads(values: impl IntoIterator<Item = (u32, usize)>) -> HashMap<PlainNodeId, usize> {
        values
            .into_iter()
            .map(|(node_id, load)| (node(node_id), load))
            .collect()
    }

    #[test]
    fn balanced_nodeset_rebalance_requires_material_range_improvement() {
        let candidates = [1, 2, 3, 4, 5].map(node);
        let current_nodeset = NodeSet::from([1, 2, 3]);
        let proposed_nodeset = NodeSet::from([1, 4, 5]);

        let already_fair = loads([(1, 59), (2, 57), (3, 58), (4, 56), (5, 58)]);
        assert_eq!(
            None,
            balanced_nodeset_rebalance_improvement(
                &candidates,
                &current_nodeset,
                &proposed_nodeset,
                &already_fair,
                3,
            )
        );

        let skewed = loads([(1, 96), (2, 96), (3, 96), (4, 0), (5, 0)]);
        assert_eq!(
            Some((96, 95)),
            balanced_nodeset_rebalance_improvement(
                &candidates,
                &current_nodeset,
                &proposed_nodeset,
                &skewed,
                3,
            )
        );

        let worse = loads([(1, 10), (2, 10), (3, 0), (4, 0), (5, 0)]);
        assert_eq!(
            None,
            balanced_nodeset_rebalance_improvement(
                &candidates,
                &NodeSet::from([3, 4, 5]),
                &NodeSet::from([1, 2, 3]),
                &worse,
                3,
            )
        );
    }
}
