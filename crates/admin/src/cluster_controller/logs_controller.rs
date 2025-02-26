// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::future;
use futures::never::Never;
use rand::prelude::IteratorRandom;
use rand::rng;
use std::collections::HashMap;
use std::iter;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, enabled, error, info, trace_span, Instrument, Level};

use restate_bifrost::loglet::FindTailAttr;
use restate_bifrost::{Bifrost, Error as BifrostError};
use restate_core::metadata_store::{Precondition, WriteError};
use restate_core::{
    Metadata, MetadataKind, MetadataWriter, ShutdownError, TargetVersion, TaskCenterFutureExt,
};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::live::Pinned;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{
    Chain, LogletConfig, LogletParams, Logs, LogsConfiguration, ProviderConfiguration,
    ProviderKind, ReplicatedLogletConfig, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId, Lsn, TailState};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, StorageState};
use restate_types::partition_table::PartitionTable;
use restate_types::replicated_loglet::{EffectiveNodeSet, ReplicatedLogletParams};
use restate_types::replication::{NodeSet, NodeSetSelector, NodeSetSelectorOptions};
use restate_types::retries::{RetryIter, RetryPolicy};
use restate_types::{logs, GenerationalNodeId, NodeId, PlainNodeId, Version, Versioned};

use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use crate::cluster_controller::scheduler;

type Result<T, E = LogsControllerError> = std::result::Result<T, E>;

const FALLBACK_MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum LogsControllerError {
    #[error("failed creating logs: {0}")]
    LogsBuilder(#[from] logs::builder::BuilderError),
    #[error("failed creating loglet params from loglet configuration: {0}")]
    ConfigurationToLogletParams(GenericError),
    #[error("failed creating loglet configuration from loglet params: {0}")]
    LogletParamsToConfiguration(GenericError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// Node set selector hints for the [`LogsController`].
pub trait NodeSetSelectorHints {
    /// A specific [`NodeId`] where the sequencer should run.
    fn preferred_sequencer(&self, log_id: &LogId) -> Option<NodeId>;
}

impl<T: NodeSetSelectorHints> NodeSetSelectorHints for &T {
    fn preferred_sequencer(&self, log_id: &LogId) -> Option<NodeId> {
        (*self).preferred_sequencer(log_id)
    }
}

/// States of a log managed by the [`LogsController`].
///
/// If a log does not have an entry in the [`Logs`] configuration, then it starts in state
/// [`LogState::Provisioning`]. Once the controller finds enough resources to create the initial
/// segment configuration, it moves it to [`LogState::Available`].
///
/// A log in state [`LogState::Available`] is running. Once the controller realizes that a log
/// needs reconfiguration (e.g. sequencer node being dead, risk of losing write-/f-majority quorum),
/// it moves to [`LogState::Sealing`].
///
/// A log in state [`LogState::Sealing`] can only move to [`LogState::Sealed`] once the log has
/// been sealed.
///
/// A log in state [`LogState::Sealed`] needs new resources to create a new segment configuration.
/// The controller monitors the cluster and moves to [`LogState::Available`] once a new
/// configuration could be created.
#[derive(Debug)]
enum LogState {
    /// Log is not yet part of the [`Logs`] configuration. Controller should look for resources to
    /// create the initial segment configuration.
    Provisioning { log_id: LogId },
    /// Log has an open tail segment and can be written to. Log is periodically monitored to check
    /// whether it needs to be reconfigured.
    Available {
        // This field is only optional to enable moving out of an Available state when transitioning
        // into another state.
        configuration: Option<LogletConfiguration>,
        // Currently, the log controller is only interested in the last segment of a log
        segment_index: SegmentIndex,
    },
    /// Log's tail segment is being sealed.
    Sealing {
        // This field is only optional to enable moving out of a Sealing state when transitioning
        // into another state.
        configuration: Option<LogletConfiguration>,
        // Currently, the log controller is only interested in the last segment of a log
        segment_index: SegmentIndex,
    },
    /// Log's tail segment has been sealed. Waiting for available resources to create a new segment
    /// configuration.
    Sealed {
        // Configuration of the sealed segment
        configuration: LogletConfiguration,
        // Currently, the log controller is only interested in the last segment of a log
        segment_index: SegmentIndex,
        // Lsn of sealed segment
        seal_lsn: Lsn,
    },
}

impl LogState {
    fn try_transition_to_sealed(&mut self, segment_index_to_seal: SegmentIndex, seal_lsn: Lsn) {
        match self {
            LogState::Provisioning { .. } => {}
            LogState::Available {
                configuration,
                segment_index,
            } => {
                // filter out sealed transitions that are not meant for our segment
                if *segment_index == segment_index_to_seal {
                    *self = LogState::Sealed {
                        configuration: configuration
                            .take()
                            .expect("previous configuration must be present"),
                        segment_index: segment_index_to_seal,
                        seal_lsn,
                    }
                } else {
                    debug!(
                        "Ignoring sealing because our state is at segment {} while the seal is for segment {}. Current state is LogState::Available, we are not transitioning to LogState::Sealed",
                        segment_index,
                        segment_index_to_seal,
                    );
                }
            }
            LogState::Sealing {
                configuration,
                segment_index,
            } => {
                // filter out sealed transitions that are not meant for our segment
                if *segment_index == segment_index_to_seal {
                    *self = LogState::Sealed {
                        configuration: configuration
                            .take()
                            .expect("previous configuration must be present"),
                        segment_index: segment_index_to_seal,
                        seal_lsn,
                    }
                } else {
                    debug!(
                        "Ignoring sealing because our state is at segment {} while the seal is for segment {}. Current state is LogState::Sealing",
                        segment_index,
                        segment_index_to_seal,
                    );
                }
            }
            LogState::Sealed { .. } => {}
        }
    }

    /// Checks whether the current segment requires reconfiguration and, therefore, needs to be
    /// sealed. The method returns [`true`] if the state was moved to sealing.
    fn try_transition_to_sealing(
        &mut self,
        log_id: LogId,
        nodes_config: &NodesConfiguration,
        logs_configuration: &LogsConfiguration,
        observed_cluster_state: &ObservedClusterState,
    ) -> bool {
        match self {
            // We can only move from Available to Sealing
            LogState::Available {
                configuration,
                segment_index,
            } => {
                if configuration
                    .as_ref()
                    .expect("configuration must be present")
                    .requires_reconfiguration(
                        log_id,
                        nodes_config,
                        logs_configuration,
                        observed_cluster_state,
                    )
                {
                    *self = LogState::Sealing {
                        configuration: configuration.take(),
                        segment_index: *segment_index,
                    };

                    true
                } else {
                    false
                }
            }
            LogState::Provisioning { .. } | LogState::Sealing { .. } | LogState::Sealed { .. } => {
                false
            }
        }
    }

    fn try_provisioning(
        &mut self,
        logs_configuration: &LogsConfiguration,
        observed_cluster_state: &ObservedClusterState,
        log_builder: &mut LogsBuilder,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<()> {
        match self {
            LogState::Provisioning { log_id } => {
                if log_builder.chain(*log_id).is_some() {
                    // already configured
                    return Ok(());
                }

                if let Some(loglet_configuration) = try_provisioning(
                    *log_id,
                    logs_configuration,
                    observed_cluster_state,
                    node_set_selector_hints,
                ) {
                    let chain = Chain::new(
                        loglet_configuration.as_provider(),
                        loglet_configuration.to_loglet_params()?,
                    );
                    log_builder
                        .add_log(*log_id, chain)
                        .expect("a provisioning log should not have a logs entry");

                    debug!(%log_id, "Provisioned log with configuration '{:?}'", loglet_configuration);

                    *self = LogState::Available {
                        segment_index: SegmentIndex::default(),
                        configuration: Some(loglet_configuration),
                    };
                }
            }
            // nothing to do :-)
            LogState::Sealing { .. } | LogState::Sealed { .. } | LogState::Available { .. } => {
                return Ok(())
            }
        };

        Ok(())
    }

    fn try_reconfiguring<F, G>(
        &mut self,
        log_id: LogId,
        logs_configuration: &LogsConfiguration,
        observed_cluster_state: &ObservedClusterState,
        mut append_segment: F,
        preferred_sequencer_hint: G,
    ) -> Result<()>
    where
        F: FnMut(
            Lsn,
            ProviderKind,
            LogletParams,
        ) -> Result<SegmentIndex, logs::builder::BuilderError>,
        G: Fn() -> Option<NodeId>,
    {
        match self {
            // We can only reconfigure if we are in state Sealed
            LogState::Sealed {
                seal_lsn,
                configuration,
                segment_index,
            } => {
                if let Some(loglet_configuration) = configuration.try_reconfiguring(
                    log_id,
                    *segment_index,
                    logs_configuration,
                    observed_cluster_state,
                    preferred_sequencer_hint(),
                ) {
                    let segment_index = append_segment(
                        *seal_lsn,
                        loglet_configuration.as_provider(),
                        loglet_configuration.to_loglet_params()?,
                    )?;

                    *self = LogState::Available {
                        segment_index,
                        configuration: Some(loglet_configuration),
                    };
                }
            }
            // nothing to do :-)
            LogState::Provisioning { .. }
            | LogState::Sealing { .. }
            | LogState::Available { .. } => return Ok(()),
        };

        Ok(())
    }

    fn current_segment_index(&self) -> Option<SegmentIndex> {
        match self {
            LogState::Provisioning { .. } => None,
            LogState::Available { segment_index, .. } => Some(*segment_index),
            LogState::Sealing { segment_index, .. } => Some(*segment_index),
            LogState::Sealed { segment_index, .. } => Some(*segment_index),
        }
    }
}

/// Try provisioning a new log for the given [`ProviderKind`] based on the observed cluster state.
/// If this is possible, then this function returns some [`LogletConfiguration`].
fn try_provisioning(
    log_id: LogId,
    logs_configuration: &LogsConfiguration,
    observed_cluster_state: &ObservedClusterState,
    node_set_selector_hints: impl NodeSetSelectorHints,
) -> Option<LogletConfiguration> {
    match logs_configuration.default_provider {
        ProviderConfiguration::Local => {
            let log_id = LogletId::new(log_id, SegmentIndex::OLDEST);
            Some(LogletConfiguration::Local(log_id.into()))
        }
        #[cfg(any(test, feature = "memory-loglet"))]
        ProviderConfiguration::InMemory => {
            let log_id = LogletId::new(log_id, SegmentIndex::OLDEST);
            Some(LogletConfiguration::Memory(log_id.into()))
        }
        #[cfg(feature = "replicated-loglet")]
        ProviderConfiguration::Replicated(ref config) => build_new_replicated_loglet_configuration(
            log_id,
            config,
            LogletId::new(log_id, SegmentIndex::OLDEST),
            &Metadata::with_current(|m| m.nodes_config_ref()),
            observed_cluster_state,
            None,
            node_set_selector_hints.preferred_sequencer(&log_id),
        )
        .map(LogletConfiguration::Replicated),
    }
}

#[cfg(feature = "replicated-loglet")]
fn logserver_writeable_node_filter(
    observed_cluster_state: &ObservedClusterState,
) -> impl Fn(PlainNodeId, &NodeConfig) -> bool + '_ {
    |node_id: PlainNodeId, config: &NodeConfig| {
        matches!(
            config.log_server_config.storage_state,
            StorageState::ReadWrite
        ) && observed_cluster_state.is_node_alive(node_id)
    }
}
/// Build a new segment configuration for a replicated loglet based on the observed cluster state
/// and the previous configuration.
#[cfg(feature = "replicated-loglet")]
pub fn build_new_replicated_loglet_configuration(
    log_id: LogId,
    replicated_loglet_config: &ReplicatedLogletConfig,
    loglet_id: LogletId,
    nodes_config: &NodesConfiguration,
    observed_cluster_state: &ObservedClusterState,
    preferred_nodes: Option<&NodeSet>,
    preferred_sequencer: Option<NodeId>,
) -> Option<ReplicatedLogletParams> {
    use restate_types::replication::{NodeSetSelector, NodeSetSelectorOptions};
    use tracing::warn;

    let mut rng = rng();

    let replication = replicated_loglet_config.replication_property.clone();

    let &sequencer = preferred_sequencer
        .and_then(|node_id| {
            // map to a known alive node
            observed_cluster_state.alive_nodes.get(&node_id.id())
        })
        .or_else(|| {
            // we can place the sequencer on any alive node
            observed_cluster_state.alive_nodes.values().choose(&mut rng)
        })?;

    let opts = NodeSetSelectorOptions::new(u32::from(log_id) as u64)
        .with_target_size(replicated_loglet_config.target_nodeset_size)
        .with_preferred_nodes_opt(preferred_nodes)
        .with_top_priority_node(sequencer);

    let selection = NodeSetSelector::select(
        nodes_config,
        &replication,
        restate_bifrost::providers::replicated_loglet::logserver_candidate_filter,
        logserver_writeable_node_filter(observed_cluster_state),
        opts,
    );

    match selection {
        Ok(nodeset) => {
            // todo(asoli): here is the right place to do additional validation and reject the nodeset if it
            // fails to meet some safety margin. For now, we'll accept the nodeset as it meets the
            // minimum replication requirement only.
            debug_assert!(nodeset.len() >= replication.num_copies() as usize);
            if replication.num_copies() > 1 && nodeset.len() == replication.num_copies() as usize {
                warn!(
                    ?log_id,
                    %replication,
                    generated_nodeset_size = nodeset.len(),
                    "The number of writeable log-servers is too small for the configured \
                    replication, there will be no fault-tolerance until you add more nodes."
                );
            }
            Some(ReplicatedLogletParams {
                loglet_id,
                sequencer,
                replication,
                nodeset,
            })
        }
        Err(err) => {
            warn!(?log_id, "Cannot select node-set for log: {err}");
            None
        }
    }
}

/// Representation of supported loglet configuration types.
#[derive(Debug)]
enum LogletConfiguration {
    #[cfg(feature = "replicated-loglet")]
    Replicated(ReplicatedLogletParams),
    Local(u64),
    #[cfg(any(test, feature = "memory-loglet"))]
    Memory(u64),
}

impl LogletConfiguration {
    fn as_provider(&self) -> ProviderKind {
        match self {
            #[cfg(feature = "replicated-loglet")]
            LogletConfiguration::Replicated(_) => ProviderKind::Replicated,
            LogletConfiguration::Local(_) => ProviderKind::Local,
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => ProviderKind::InMemory,
        }
    }

    fn requires_reconfiguration(
        &self,
        log_id: LogId,
        nodes_config: &NodesConfiguration,
        logs_configuration: &LogsConfiguration,
        observed_cluster_state: &ObservedClusterState,
    ) -> bool {
        match (self, &logs_configuration.default_provider) {
            #[cfg(any(test, feature = "memory-loglet"))]
            (Self::Memory(_), ProviderConfiguration::InMemory) => false,
            (Self::Local(_), ProviderConfiguration::Local) => false,
            #[cfg(feature = "replicated-loglet")]
            (Self::Replicated(params), ProviderConfiguration::Replicated(config)) => {
                let sequencer_change_required = !observed_cluster_state
                    .is_node_alive(params.sequencer)
                    && !observed_cluster_state.alive_nodes.is_empty();

                if sequencer_change_required {
                    debug!(
                        %log_id,
                        loglet_id = %params.loglet_id,
                        "Replicated loglet requires a sequencer change, existing sequencer {} is presumed dead",
                        params.sequencer
                    );
                    return true;
                }

                let opts = NodeSetSelectorOptions::new(u32::from(log_id) as u64)
                    .with_target_size(config.target_nodeset_size)
                    .with_preferred_nodes(&params.nodeset)
                    // current sequencer
                    .with_top_priority_node(params.sequencer);

                let Ok(selection) = NodeSetSelector::select(
                    nodes_config,
                    &params.replication,
                    restate_bifrost::providers::replicated_loglet::logserver_candidate_filter,
                    logserver_writeable_node_filter(observed_cluster_state),
                    opts,
                ) else {
                    // if selection fails, we won't be in a better position if we changed the nodeset
                    // from existing one, so let's keep things as is.
                    return false;
                };

                if params.replication != config.replication_property {
                    // replication property has changed, we need to reconfigure.
                    debug!(
                        %log_id,
                        loglet_id = %params.loglet_id,
                        current_replication = %params.replication,
                        new_replication = %config.replication_property,
                        "Replicated loglet default replication has changed, will attempt reconfiguration"
                    );
                    return true;
                }

                // todo 1: This is an over-simplifying check, ideally we'd want to see if the new nodeset
                // improves our safety-margin (fault-tolerance) or not by running nodeset checker with higher
                // replication on every scope, or any other reasonable metric that let us decide
                // whether it's worth the reconfiguration or not.

                // todo 2: We should check the current segment for sealability, otherwise we might propose
                //  reconfiguration when we are virtually certain to get stuck!
                let effective_nodeset = EffectiveNodeSet::new(params.nodeset.clone(), nodes_config);
                if selection != *effective_nodeset {
                    debug!(
                        %log_id,
                        loglet_id = %params.loglet_id,
                        original_nodeset = %effective_nodeset,
                        potential_nodeset = %selection,
                        "Replicated loglet nodeset can be improved, will attempt reconfiguration"
                    );
                    return true;
                }

                false
            }
            (x, y @ ProviderConfiguration::Replicated(_)) => {
                debug!(
                    %log_id,
                    "Changing bifrost provider from {} to {}, will attempt reconfiguration",
                    x.as_provider(), y.kind(),
                );
                true
            }
            (x, y) => {
                debug!(
                    %log_id,
                    "Changing bifrost provider from {} to {} is not supported at the moment. Ignoring reconfiguration request",
                    x.as_provider(), y.kind(),
                );
                false
            }
        }
    }

    fn to_loglet_params(&self) -> Result<LogletParams> {
        Ok(match self {
            #[cfg(feature = "replicated-loglet")]
            LogletConfiguration::Replicated(configuration) => LogletParams::from(
                configuration
                    .serialize()
                    .map_err(|err| LogsControllerError::ConfigurationToLogletParams(err.into()))?,
            ),
            LogletConfiguration::Local(id) => LogletParams::from(id.to_string()),
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(id) => LogletParams::from(id.to_string()),
        })
    }

    fn try_reconfiguring(
        &self,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        logs_configuration: &LogsConfiguration,
        observed_cluster_state: &ObservedClusterState,
        preferred_sequencer: Option<NodeId>,
    ) -> Option<LogletConfiguration> {
        let next_loglet_id = LogletId::new(log_id, last_segment_index.next());

        match logs_configuration.default_provider {
            #[cfg(any(test, feature = "memory-loglet"))]
            ProviderConfiguration::InMemory => {
                Some(LogletConfiguration::Memory(next_loglet_id.into()))
            }
            ProviderConfiguration::Local => Some(LogletConfiguration::Local(next_loglet_id.into())),
            #[cfg(feature = "replicated-loglet")]
            ProviderConfiguration::Replicated(ref config) => {
                let previous_params = match self {
                    Self::Replicated(previous_params) => Some(previous_params),
                    _ => None,
                };

                build_new_replicated_loglet_configuration(
                    log_id,
                    config,
                    next_loglet_id,
                    &Metadata::with_current(|m| m.nodes_config_ref()),
                    observed_cluster_state,
                    previous_params.map(|params| &params.nodeset),
                    preferred_sequencer,
                )
                .map(LogletConfiguration::Replicated)
            }
        }
    }

    fn node_set_iter(&self) -> impl Iterator<Item = &PlainNodeId> {
        match self {
            #[cfg(feature = "replicated-loglet")]
            LogletConfiguration::Replicated(configuration) => {
                itertools::Either::Left(configuration.nodeset.iter())
            }
            LogletConfiguration::Local(_) => itertools::Either::Right(iter::empty()),
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => itertools::Either::Right(iter::empty()),
        }
    }

    fn sequencer_node(&self) -> Option<GenerationalNodeId> {
        match self {
            #[cfg(feature = "replicated-loglet")]
            LogletConfiguration::Replicated(configuration) => Some(configuration.sequencer),
            LogletConfiguration::Local(_) => None,
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => None,
        }
    }
}

impl TryFrom<&LogletConfig> for LogletConfiguration {
    type Error = GenericError;

    fn try_from(value: &LogletConfig) -> Result<Self, Self::Error> {
        match value.kind {
            ProviderKind::Local => Ok(LogletConfiguration::Local(value.params.parse()?)),
            #[cfg(any(test, feature = "memory-loglet"))]
            ProviderKind::InMemory => Ok(LogletConfiguration::Memory(value.params.parse()?)),
            #[cfg(feature = "replicated-loglet")]
            ProviderKind::Replicated => {
                ReplicatedLogletParams::deserialize_from(value.params.as_bytes())
                    .map(LogletConfiguration::Replicated)
                    .map_err(Into::into)
            }
        }
    }
}

/// Effects used by [`LogsControllerInner`] to signal required actions to the [`LogsController`]. Often those
/// effects require an asynchronous operation whose result is reported back via an [`Event`].
enum Effect {
    /// Write [`Logs`] to the metadata store if the previous version matches
    WriteLogs {
        logs: Arc<Logs>,
        previous_version: Version,
        debounce: Option<RetryIter<'static>>,
    },
    /// Seal the given segment of the given [`LogId`].
    Seal {
        log_id: LogId,
        segment_index: SegmentIndex,
        debounce: Option<RetryIter<'static>>,
    },
    FindLogsTail,
}

/// Events that the [`LogsControllerInner`] needs to react to. The events can originate from previous [`Effect`]
/// or from an external source.
enum Event {
    /// Successfully written the [`Logs`] configuration with the given version
    WriteLogsSucceeded(Version),
    /// Failed writing `logs`
    WriteLogsFailed {
        logs: Arc<Logs>,
        previous_version: Version,
        debounce: Option<RetryIter<'static>>,
    },
    /// Found a newer [`Logs`] version.
    NewLogs,
    /// Sealing of the given log segment succeeded
    SealSucceeded {
        log_id: LogId,
        segment_index: SegmentIndex,
        seal_lsn: Lsn,
    },
    /// Sealing of the given log segment failed
    SealFailed {
        log_id: LogId,
        segment_index: SegmentIndex,
        debounce: Option<RetryIter<'static>>,
    },
    LogsTailUpdates {
        updates: LogsTailUpdates,
    },
}

/// The inner logs controller is responsible for provisioning, monitoring, sealing and reconfiguring
/// of logs. It does so by reacting to [`Event`], [`Logs`], and [`PartitionTable`] changes. To drive
/// the internal log state machines forward, it emits [`Effect`].
///
/// The underlying idea is that [`Logs`] and [`PartitionTable`] hold the ground truth about the
/// current state of the logs and which logs to provision. Whenever a newer logs or partition table
/// is found, the logs controller will reset itself accordingly. Partitions that don't have a
/// corresponding log in logs, will be provisioned.
///
/// Since logs contains the ground truth, any log state change that implies a change in [`Logs`]
/// (e.g. reconfiguration, initial provisioning) results into a write to the metadata store. Since
/// there can be multiple writes happening concurrently from different logs controllers, we can only
/// assume that our [`LogsControllerInner::logs_state`] is valid after we see our write succeed. If
/// a different logs controller writes first, then we have to reset our internal state. To avoid the
/// complexity of handling multiple in-flight logs writes, the controller waits until the
/// in-flight write completes or a newer log is detected.
struct LogsControllerInner {
    logs_state: HashMap<LogId, LogState>,
    logs_write_in_progress: Option<Version>,

    // We are storing the logs explicitly (not relying on Metadata::current()) because we need a fixed
    // snapshot to keep logs_state in sync.
    current_logs: Arc<Logs>,
    retry_policy: RetryPolicy,
}

impl LogsControllerInner {
    fn new(
        current_logs: Arc<Logs>,
        retry_policy: RetryPolicy,
    ) -> Result<Self, LogsControllerError> {
        let mut logs_state = HashMap::with_capacity(current_logs.num_logs());
        Self::update_logs_state(&mut logs_state, current_logs.as_ref())?;

        Ok(Self {
            current_logs,
            logs_state,
            logs_write_in_progress: None,
            retry_policy,
        })
    }

    fn on_observed_cluster_state_update(
        &mut self,
        nodes_config: &NodesConfiguration,
        observed_cluster_state: &ObservedClusterState,
        effects: &mut Vec<Effect>,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<()> {
        // we don't do concurrent updates to avoid complexity
        if self.logs_write_in_progress.is_some() {
            return Ok(());
        }

        self.seal_logs(nodes_config, observed_cluster_state, effects);

        let mut builder = self.current_logs.deref().clone().into_builder();
        self.provision_logs(
            observed_cluster_state,
            &mut builder,
            &node_set_selector_hints,
        )?;
        self.reconfigure_logs(
            observed_cluster_state,
            &mut builder,
            node_set_selector_hints,
        )?;

        if let Some(logs) = builder.build_if_modified() {
            if enabled!(Level::TRACE) {
                debug!(?logs, "Proposing new log chain version {}", logs.version());
            } else {
                debug!("Proposing new log chain version {}", logs.version());
            }

            self.logs_write_in_progress = Some(logs.version());
            let logs = Arc::new(logs);
            effects.push(Effect::WriteLogs {
                logs: Arc::clone(&logs),
                previous_version: self.current_logs.version(),
                debounce: None,
            });
            // we already update the current logs but will wait until we learn whether the update
            // was successful or not. If not, then we'll reset our internal state wrt the new
            // logs version.
            self.current_logs = logs;
        }

        Ok(())
    }

    fn seal_logs(
        &mut self,
        nodes_config: &NodesConfiguration,
        observed_cluster_state: &ObservedClusterState,
        effects: &mut Vec<Effect>,
    ) {
        for (log_id, log_state) in &mut self.logs_state {
            if log_state.try_transition_to_sealing(
                *log_id,
                nodes_config,
                self.current_logs.configuration(),
                observed_cluster_state,
            ) {
                effects.push(Effect::Seal {
                    log_id: *log_id,
                    segment_index: log_state
                        .current_segment_index()
                        .expect("expect a valid segment"),
                    debounce: None,
                })
            }
        }
    }

    fn provision_logs(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        logs_builder: &mut LogsBuilder,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<()> {
        for log_state in self.logs_state.values_mut() {
            log_state.try_provisioning(
                self.current_logs.configuration(),
                observed_cluster_state,
                logs_builder,
                &node_set_selector_hints,
            )?;
        }

        Ok(())
    }

    fn reconfigure_logs(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        logs_builder: &mut LogsBuilder,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<()> {
        for (log_id, log_state) in &mut self.logs_state {
            log_state.try_reconfiguring(
                *log_id,
                self.current_logs.configuration(),
                observed_cluster_state,
                |seal_lsn, provider_kind, loglet_params| {
                    let mut chain_builder = logs_builder
                        .chain(*log_id)
                        .expect("Log with '{log_id}' should be present");

                    chain_builder.append_segment(seal_lsn, provider_kind, loglet_params)
                },
                || node_set_selector_hints.preferred_sequencer(log_id),
            )?;
        }

        Ok(())
    }

    fn on_event(
        &mut self,
        event: Event,
        effects: &mut Vec<Effect>,
        metadata_writer: &mut MetadataWriter,
    ) -> Result<()> {
        match event {
            Event::WriteLogsSucceeded(version) => {
                self.on_logs_written(version);
                metadata_writer.submit(self.current_logs.clone());
            }
            Event::WriteLogsFailed {
                logs,
                previous_version: expected_version,
                debounce,
            } => {
                // Filter out out-dated log write attempts
                if Some(logs.version()) == self.logs_write_in_progress {
                    // todo what if it doesn't work again? Maybe stepping down as the leader
                    effects.push(Effect::WriteLogs {
                        logs,
                        previous_version: expected_version,
                        debounce: debounce.or_else(|| Some(self.retry_policy.clone().into_iter())),
                    });
                }
            }
            Event::NewLogs => {
                self.on_logs_update(Metadata::with_current(|m| m.logs_ref()), effects)?;
            }
            Event::SealSucceeded {
                log_id,
                segment_index,
                seal_lsn,
            } => {
                self.on_logs_sealed(log_id, segment_index, seal_lsn);
            }
            Event::SealFailed {
                log_id,
                segment_index,
                debounce,
            } => {
                if matches!(self.logs_state.get(&log_id), Some(LogState::Sealing { segment_index: current_segment_index, ..}) if segment_index == *current_segment_index)
                {
                    // todo what if it doesn't work again? Maybe stepping down as the leader
                    effects.push(Effect::Seal {
                        log_id,
                        segment_index,
                        debounce: debounce.or_else(|| Some(self.retry_policy.clone().into_iter())),
                    })
                }
            }
            Event::LogsTailUpdates { updates } => {
                self.on_logs_tail_updates(&updates);
            }
        }

        Ok(())
    }

    fn on_logs_written(&mut self, version: Version) {
        // filter out, outdated writes
        if Some(version) == self.logs_write_in_progress {
            self.logs_write_in_progress = None;
        }
    }

    fn on_logs_update(&mut self, logs: Pinned<Logs>, effects: &mut Vec<Effect>) -> Result<()> {
        // rebuild the internal state if we receive a newer logs or one with a version we were
        // supposed to write (race condition)
        if logs.version() > self.current_logs.version()
            || Some(logs.version()) == self.logs_write_in_progress
        {
            self.logs_write_in_progress = None;
            self.current_logs = logs.into_arc();

            // only keep provisioning logs since they are derived from the partition table
            self.logs_state
                .retain(|_, state| matches!(state, LogState::Provisioning { .. }));

            Self::update_logs_state(&mut self.logs_state, self.current_logs.as_ref())?;

            // check whether we have a pending seal operation for any of the logs
            effects.push(Effect::FindLogsTail);
        }

        Ok(())
    }

    fn update_logs_state(
        logs_state: &mut HashMap<LogId, LogState>,
        logs: &Logs,
    ) -> Result<(), LogsControllerError> {
        for (log_id, chain) in logs.iter() {
            let tail = chain.tail();

            if let Some(seal_lsn) = tail.tail_lsn {
                // sealed tail segment
                logs_state.insert(
                    *log_id,
                    LogState::Sealed {
                        configuration: tail
                            .config
                            .try_into()
                            .map_err(LogsControllerError::LogletParamsToConfiguration)?,
                        segment_index: tail.index(),
                        seal_lsn,
                    },
                );
            } else {
                // open tail segment
                logs_state.insert(
                    *log_id,
                    LogState::Available {
                        configuration: Some(
                            tail.config
                                .try_into()
                                .map_err(LogsControllerError::LogletParamsToConfiguration)?,
                        ),
                        segment_index: tail.index(),
                    },
                );
            }
        }

        Ok(())
    }

    fn on_logs_tail_updates(&mut self, updates: &LogsTailUpdates) {
        for (log_id, update) in updates {
            let TailState::Sealed(seal_lsn) = update.tail else {
                // if tail is open, we don't have to do anything
                // because in all cases, the `LogState` here is
                // the one that should remain in effect
                continue;
            };

            let Some(state) = self.logs_state.get_mut(log_id) else {
                continue;
            };

            state.try_transition_to_sealed(update.segment_index, seal_lsn);
        }
    }

    fn on_partition_table_update(&mut self, partition_table: &PartitionTable) {
        // update the provisioning logs
        for (partition_id, _) in partition_table.partitions() {
            self.logs_state
                .entry((*partition_id).into())
                .or_insert_with(|| {
                    let log_id = (*partition_id).into();
                    LogState::Provisioning { log_id }
                });
        }
    }

    fn on_logs_sealed(&mut self, log_id: LogId, segment_index: SegmentIndex, seal_lsn: Lsn) {
        if let Some(logs_state) = self.logs_state.get_mut(&log_id) {
            logs_state.try_transition_to_sealed(segment_index, seal_lsn);
        }
    }
}

pub struct LogTailUpdate {
    segment_index: SegmentIndex,
    tail: TailState,
}

pub type LogsTailUpdates = HashMap<LogId, LogTailUpdate>;

/// Runs the inner logs controller and processes the [`Effect`].
pub struct LogsController {
    effects: Option<Vec<Effect>>,
    inner: LogsControllerInner,
    bifrost: Bifrost,
    metadata_writer: MetadataWriter,
    async_operations: JoinSet<Event>,
    find_logs_tail_semaphore: Arc<Semaphore>,
}

impl LogsController {
    pub fn new(
        bifrost: Bifrost,
        metadata_writer: MetadataWriter,
    ) -> Result<Self, LogsControllerError> {
        //todo(azmy): make configurable
        let retry_policy = RetryPolicy::exponential(
            Duration::from_millis(10),
            2.0,
            Some(15),
            Some(Duration::from_secs(5)),
        );

        let mut this = Self {
            effects: Some(Vec::new()),
            inner: LogsControllerInner::new(
                Metadata::with_current(|m| m.logs_snapshot()),
                retry_policy,
            )?,
            bifrost,
            metadata_writer,
            async_operations: JoinSet::default(),
            find_logs_tail_semaphore: Arc::new(Semaphore::new(1)),
        };

        this.find_logs_tail();
        Ok(this)
    }

    pub fn find_logs_tail(&mut self) {
        // none
        let Ok(permit) = self.find_logs_tail_semaphore.clone().try_acquire_owned() else {
            // already a find tail job is running
            return;
        };

        let logs = Arc::clone(&self.inner.current_logs);
        let bifrost = self.bifrost.clone();
        let find_tail = async move {
            let updates = future::join_all(logs.iter().map(|(log_id, chain)| {
                let log_id = *log_id;
                let tail_segment = chain.tail();
                let bifrost = bifrost.clone();

                async move {
                    let writeable_loglet = match bifrost.admin().writeable_loglet(log_id).await {
                        Ok(loglet) => loglet,
                        Err(BifrostError::Shutdown(_)) => return None,
                        Err(err) => {
                            debug!(error=%err, %log_id, segment_index=%tail_segment.index(), "Failed to find writable loglet");
                            return None;
                        }
                    };

                    if writeable_loglet.segment_index() != tail_segment.index() {
                        // writable segment in bifrost is probably ahead of our snapshot.
                        // then there is probably a new metadata update that will fix this
                        // for now we just ignore this segment
                        debug!(%log_id, segment_index=%tail_segment.index(), "Segment is not tail segment, skip finding tail");
                        return None;
                    }

                    debug!(%log_id, segment_index=%writeable_loglet.segment_index(), "Attempting to find tail for loglet");
                    let found_tail = match writeable_loglet.find_tail(FindTailAttr::Durable).await {
                        Ok(tail) => tail,
                        Err(err) => {
                            debug!(error=%err, %log_id, segment_index=%writeable_loglet.segment_index(), "Failed to find tail for loglet");
                            return None;
                        }
                    };

                    Some((log_id, LogTailUpdate {
                        segment_index: writeable_loglet.segment_index(),
                        tail: found_tail,
                    }))
                }
            })).await.into_iter().flatten().collect();

            // we explicitly drop the permit here to make sure
            // it's moved to the future closure
            drop(permit);
            Event::LogsTailUpdates { updates }
        };

        self.async_operations.spawn(
            find_tail
                .log_slow_after(
                    Duration::from_secs(3),
                    tracing::Level::INFO,
                    "Determining the tail status for all logs",
                )
                .with_overdue(Duration::from_secs(15), tracing::Level::WARN)
                .instrument(trace_span!("scheduled-find-tail"))
                .in_current_tc(),
        );
    }

    pub fn on_observed_cluster_state_update(
        &mut self,
        nodes_config: &NodesConfiguration,
        observed_cluster_state: &ObservedClusterState,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<(), anyhow::Error> {
        self.inner.on_observed_cluster_state_update(
            nodes_config,
            observed_cluster_state,
            self.effects.as_mut().expect("to be present"),
            node_set_selector_hints,
        )?;
        self.apply_effects();

        Ok(())
    }

    pub fn on_partition_table_update(&mut self, partition_table: &PartitionTable) {
        self.inner.on_partition_table_update(partition_table);
    }

    pub fn on_logs_update(&mut self, logs: Pinned<Logs>) -> Result<()> {
        self.inner
            .on_logs_update(logs, self.effects.as_mut().expect("to be present"))?;
        self.apply_effects();

        Ok(())
    }

    fn apply_effects(&mut self) {
        let mut effects = self.effects.take().expect("to be present");
        for effect in effects.drain(..) {
            match effect {
                Effect::WriteLogs {
                    logs,
                    previous_version,
                    debounce,
                } => {
                    self.write_logs(previous_version, logs, debounce);
                }
                Effect::Seal {
                    log_id,
                    segment_index,
                    debounce,
                } => {
                    self.seal_log(log_id, segment_index, debounce);
                }
                Effect::FindLogsTail => {
                    self.find_logs_tail();
                }
            }
        }

        self.effects = Some(effects);
    }

    fn write_logs(
        &mut self,
        previous_version: Version,
        logs: Arc<Logs>,
        mut debounce: Option<RetryIter<'static>>,
    ) {
        let metadata_writer = self.metadata_writer.clone();

        self.async_operations.spawn(async move {
                if let Some(debounce) = &mut debounce {
                    let delay = debounce.next().unwrap_or(FALLBACK_MAX_RETRY_DELAY);
                    debug!(?delay, %previous_version, "Wait before attempting to write the log chain metadata");
                    tokio::time::sleep(delay).await;
                }

                if let Err(err) = metadata_writer.metadata_store_client()
                    .put(
                        BIFROST_CONFIG_KEY.clone(),
                        logs.deref(),
                        Precondition::MatchesVersion(previous_version),
                    )
                    .await
                {
                    return match err {
                        WriteError::FailedPrecondition(err) => {
                            info!(%err, "Detected a concurrent modification to the log chain");
                            // Perhaps we already have a newer version, if not, fetch latest.
                            let _ = Metadata::current().sync(MetadataKind::Logs, TargetVersion::Version(previous_version.next())).await;
                            Event::NewLogs
                        }
                        err => {
                            info!(%err, "Failed writing the new log chain {} to metadata store, will retry", logs.version());
                            Event::WriteLogsFailed {
                                logs,
                                previous_version,
                                debounce,
                            }
                        }
                    };
                }

                // we don't update metadata with the new logs here because it would cause a race
                // condition with the outer on_logs_updated signal

                let version = logs.version();
                Event::WriteLogsSucceeded(version)
        }.in_current_tc());
    }

    fn seal_log(
        &mut self,
        log_id: LogId,
        segment_index: SegmentIndex,
        mut debounce: Option<RetryIter<'static>>,
    ) {
        let bifrost = self.bifrost.clone();

        self.async_operations.spawn(
            async move {
                let start = tokio::time::Instant::now();
                if let Some(debounce) = &mut debounce {
                    let delay = debounce.next().unwrap_or(FALLBACK_MAX_RETRY_DELAY);
                    debug!(?delay, %log_id, %segment_index, "Wait before attempting to seal loglet");
                    tokio::time::sleep(delay).await;
                }

                match bifrost.admin().seal(log_id, segment_index).await {
                    Ok(sealed_segment) => {
                        debug!(
                            %log_id,
                            %segment_index,
                            "Loglet has been sealed in {:?}, stable tail is {}",
                            start.elapsed(),
                            sealed_segment.tail.offset(),
                        );
                        Event::SealSucceeded {
                            log_id,
                            segment_index,
                            seal_lsn: sealed_segment.tail.offset(),
                        }
                    }
                    Err(_) => {
                        Event::SealFailed {
                            log_id,
                            segment_index,
                            debounce,
                        }
                    }
                }
            }
            .in_current_tc(),
        );
    }

    pub async fn run_async_operations(&mut self) -> Result<Never> {
        loop {
            if self.async_operations.is_empty() {
                futures::future::pending().await
            } else {
                let event = self
                    .async_operations
                    .join_next()
                    .await
                    .expect("should not be empty")
                    .expect("async operations must not panic");
                self.inner.on_event(
                    event,
                    self.effects.as_mut().expect("to be present"),
                    &mut self.metadata_writer,
                )?;
                self.apply_effects();
            }
        }
    }
}

/// Placement hints for the [`scheduler::Scheduler`] based on the logs configuration
#[derive(derive_more::From)]
pub struct LogsBasedPartitionProcessorPlacementHints<'a> {
    logs_controller: &'a LogsController,
}

impl scheduler::PartitionProcessorPlacementHints for LogsBasedPartitionProcessorPlacementHints<'_> {
    fn preferred_nodes(&self, partition_id: &PartitionId) -> impl Iterator<Item = &PlainNodeId> {
        let log_id = LogId::from(*partition_id);

        self.logs_controller
            .inner
            .logs_state
            .get(&log_id)
            .and_then(|log_state| match log_state {
                LogState::Available { configuration, .. } => configuration
                    .as_ref()
                    .map(|configuration| itertools::Either::Left(configuration.node_set_iter())),
                LogState::Sealing { .. }
                | LogState::Sealed { .. }
                | LogState::Provisioning { .. } => None,
            })
            .unwrap_or_else(|| itertools::Either::Right(iter::empty()))
    }

    fn preferred_leader(&self, partition_id: &PartitionId) -> Option<PlainNodeId> {
        let log_id = LogId::from(*partition_id);

        self.logs_controller
            .inner
            .logs_state
            .get(&log_id)
            .and_then(|log_state| match log_state {
                LogState::Available { configuration, .. } => {
                    configuration.as_ref().and_then(|configuration| {
                        configuration
                            .sequencer_node()
                            .map(GenerationalNodeId::as_plain)
                    })
                }
                LogState::Sealing { .. }
                | LogState::Sealed { .. }
                | LogState::Provisioning { .. } => None,
            })
    }
}

#[cfg(test)]
pub mod tests {
    use std::num::NonZeroU8;

    use enumset::{enum_set, EnumSet};
    use googletest::prelude::*;

    use restate_types::locality::NodeLocation;
    use restate_types::logs::metadata::{
        LogsConfiguration, NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig,
    };
    use restate_types::logs::{LogId, LogletId};
    use restate_types::nodes_config::{
        LogServerConfig, MetadataServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
    };
    use restate_types::replicated_loglet::ReplicatedLogletParams;
    use restate_types::replication::{NodeSet, ReplicationProperty};
    use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};

    use crate::cluster_controller::logs_controller::{
        build_new_replicated_loglet_configuration, LogletConfiguration,
    };
    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;

    #[derive(Debug, Default, Clone)]
    pub struct MockNodes {
        pub nodes_config: NodesConfiguration,
        pub observed_state: ObservedClusterState,
    }

    impl MockNodes {
        pub fn builder() -> MockNodeBuilder {
            MockNodeBuilder {
                nodes_config: NodesConfiguration::new(1.into(), "mock-cluster".to_owned()),
                observed_state: ObservedClusterState::default(),
            }
        }

        pub fn add_dedicated_log_server_node(&mut self, node_id: impl Into<PlainNodeId>) {
            self.add_node(node_id, enum_set!(Role::LogServer), StorageState::ReadWrite)
        }

        pub fn set_storage_state(
            &mut self,
            node_id: impl Into<PlainNodeId>,
            storage_state: StorageState,
        ) {
            let node_id = node_id.into();
            let mut node_config = self.nodes_config.find_node_by_id(node_id).unwrap().clone();
            node_config.log_server_config.storage_state = storage_state;

            self.nodes_config.upsert_node(node_config);
        }

        pub fn add_node(
            &mut self,
            node_id: impl Into<PlainNodeId>,
            roles: EnumSet<Role>,
            storage_state: StorageState,
        ) {
            let node_id = node_id.into();
            self.observed_state.dead_nodes.remove(&node_id);
            self.observed_state
                .alive_nodes
                .insert(node_id, node_id.with_generation(1));
            self.nodes_config
                .upsert_node(node(node_id, roles, storage_state));
        }

        pub fn kill_node(&mut self, node_id: impl Into<PlainNodeId>) {
            let node_id = node_id.into();
            assert!(
                self.observed_state.alive_nodes.remove(&node_id).is_some(),
                "node not found"
            );
            self.observed_state.dead_nodes.insert(node_id);
        }

        pub fn kill_nodes<const N: usize>(&mut self, ids: [impl Into<PlainNodeId>; N]) {
            for id in ids {
                self.kill_node(id);
            }
        }

        pub fn revive_node(&mut self, (node_id, generation): (u32, u32)) {
            let id = node_id.into();
            assert!(self.observed_state.dead_nodes.remove(&id), "node not found");
            self.observed_state
                .alive_nodes
                .insert(id, id.with_generation(generation));
        }

        pub fn revive_nodes<const N: usize>(&mut self, ids: [(u32, u32); N]) {
            for (node_id, generation) in ids {
                self.revive_node((node_id, generation));
            }
        }
    }

    pub struct MockNodeBuilder {
        nodes_config: NodesConfiguration,
        observed_state: ObservedClusterState,
    }

    impl MockNodeBuilder {
        pub fn with_node(
            mut self,
            node_id: u32,
            roles: impl Into<EnumSet<Role>>,
            storage_state: StorageState,
        ) -> Self {
            let id = node_id.into();
            self.nodes_config
                .upsert_node(node(node_id, roles.into(), storage_state));
            self.observed_state
                .alive_nodes
                .insert(id, id.with_generation(1));

            self
        }

        #[allow(dead_code)]
        pub fn with_dead_node(
            mut self,
            node_id: u32,
            roles: EnumSet<Role>,
            storage_state: StorageState,
        ) -> Self {
            let id = node_id.into();
            self.nodes_config
                .upsert_node(node(node_id, roles, storage_state));
            self.observed_state.dead_nodes.insert(id);

            self
        }

        pub fn with_nodes<const N: usize>(
            mut self,
            ids: [u32; N],
            roles: EnumSet<Role>,
            state: StorageState,
        ) -> Self {
            for id in ids {
                self = self.with_node(id, roles, state);
            }
            self
        }

        pub fn dead_nodes<const N: usize>(mut self, ids: [u32; N]) -> Self {
            for id in ids {
                let id = id.into();
                self.observed_state.alive_nodes.remove(&id);
                self.observed_state.dead_nodes.insert(id);
            }
            self
        }

        pub fn with_dedicated_admin_node(self, id: u32) -> Self {
            self.with_nodes(
                [id],
                enum_set!(Role::Admin | Role::MetadataServer),
                StorageState::Disabled,
            )
        }

        pub fn with_all_roles_node(self, id: u32) -> Self {
            self.with_nodes([id], EnumSet::all(), StorageState::ReadWrite)
        }

        pub fn with_dedicated_worker_nodes<const N: usize>(self, ids: [u32; N]) -> Self {
            self.with_nodes(ids, enum_set!(Role::Worker), StorageState::Disabled)
        }

        pub fn with_dedicated_log_server_nodes<const N: usize>(self, ids: [u32; N]) -> Self {
            self.with_nodes(ids, enum_set!(Role::LogServer), StorageState::ReadWrite)
        }

        pub fn build(self) -> MockNodes {
            MockNodes {
                nodes_config: self.nodes_config,
                observed_state: self.observed_state,
            }
        }
    }

    fn logs_configuration(replication_factor: u8) -> LogsConfiguration {
        LogsConfiguration {
            default_provider: ProviderConfiguration::Replicated(ReplicatedLogletConfig {
                target_nodeset_size: NodeSetSize::default(),
                replication_property: ReplicationProperty::new(
                    NonZeroU8::new(replication_factor).expect("must be non zero"),
                ),
            }),
        }
    }

    pub fn node(
        node_id: impl Into<PlainNodeId>,
        roles: EnumSet<Role>,
        storage_state: StorageState,
    ) -> NodeConfig {
        let node_id = node_id.into();
        NodeConfig::new(
            format!("node-{}", u32::from(node_id)),
            node_id.with_generation(1),
            NodeLocation::default(),
            format!("https://node-{}", u32::from(node_id))
                .parse()
                .unwrap(),
            roles,
            LogServerConfig { storage_state },
            MetadataServerConfig::default(),
        )
    }

    #[test]
    fn loglet_requires_reconfiguration() {
        const LOG_ID: LogId = LogId::new(10);
        let mut nodes = MockNodes::builder()
            .with_all_roles_node(0)
            .with_dead_node(
                1,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            )
            .with_all_roles_node(2)
            .build();

        let seq_n0 = ReplicatedLogletParams {
            loglet_id: LogletId::from(1),
            sequencer: GenerationalNodeId::new(0, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            nodeset: NodeSet::from([0, 1, 2]),
        };

        let logs_config = logs_configuration(2);

        let sequencer_replacement = LogletConfiguration::Replicated(ReplicatedLogletParams {
            sequencer: GenerationalNodeId::new(1, 1),
            ..seq_n0.clone()
        });

        assert!(sequencer_replacement.requires_reconfiguration(
            LOG_ID,
            &nodes.nodes_config,
            &logs_config,
            &nodes.observed_state
        ));

        let params = LogletConfiguration::Replicated(seq_n0.clone());
        assert!(
            !params.requires_reconfiguration(
                LOG_ID,
                &nodes.nodes_config,
                &logs_config,
                &nodes.observed_state
            ),
            "we should not reconfigure when we can't improve the nodeset"
        );

        // nodeset has already 3 nodes which is sufficient to lose 1 domain, but we consider node 1 unwriteable because it's dead
        // so we should see request to add 3 to _expand_ the nodeset
        // reconfiguration even if the cluster is bigger
        nodes.add_dedicated_log_server_node(3);
        assert!(
            params.requires_reconfiguration(
                LOG_ID,
                &nodes.nodes_config,
                &logs_config,
                &nodes.observed_state
            ),
            "nodeset improvement is needed as the nodeset has 1 dead node and we have other nodes to choose from (namely N3)"
        );

        let ProviderConfiguration::Replicated(ref replicated_loglet_config) =
            logs_config.default_provider
        else {
            unreachable!()
        };

        // in this test we don't care whether the sequencer stays on N0 or not, so let's keep it
        // there.
        let config = build_new_replicated_loglet_configuration(
            LOG_ID,
            replicated_loglet_config,
            seq_n0.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&seq_n0.nodeset),
            Some(seq_n0.sequencer.into()),
        )
        .unwrap();
        assert_eq!(config.sequencer, seq_n0.sequencer, "no change in sequencer");
        assert_eq!(
            config.nodeset,
            NodeSet::from([0, 1, 2, 3]),
            "picks writeable log servers for nodeset"
        );

        // if node1 became read-only, we need to reconfigure and now we will reduce the nodeset
        // size to avoid writes to this node being picked up in future nodesets since read-only
        // means that we don't want this node to be in new nodesets.
        nodes.set_storage_state(1, StorageState::ReadOnly);
        assert!(
            params.requires_reconfiguration(
                LOG_ID,
                &nodes.nodes_config,
                &logs_config,
                &nodes.observed_state
            ),
            "new nodeset is suggested as N1 became read-only which makes it no a candidate anymore"
        );

        let config = build_new_replicated_loglet_configuration(
            LOG_ID,
            replicated_loglet_config,
            seq_n0.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&seq_n0.nodeset),
            Some(seq_n0.sequencer.into()),
        )
        .unwrap();
        assert_eq!(config.sequencer, seq_n0.sequencer, "no change in sequencer");
        assert_eq!(
            config.nodeset,
            NodeSet::from([0, 2, 3]),
            "picks writeable log servers for nodeset"
        );
    }

    #[test]
    fn bootstrap_and_reconfigure_replicated_loglet() {
        const LOG_ID: LogId = LogId::new(10);
        let mut nodes = MockNodes::builder()
            .with_dedicated_admin_node(0)
            .with_dedicated_log_server_nodes([1, 2, 3, 6])
            .with_dedicated_worker_nodes([4, 5])
            .dead_nodes([6]) // N6 starts out offline
            .build();

        let logs_config = logs_configuration(2);

        let ProviderConfiguration::Replicated(ref replicated_loglet_config) =
            logs_config.default_provider
        else {
            unreachable!()
        };

        let initial = build_new_replicated_loglet_configuration(
            LOG_ID,
            replicated_loglet_config,
            LogletId::from(1),
            &nodes.nodes_config,
            &nodes.observed_state,
            None,
            Some(NodeId::new_plain(5)),
        )
        .unwrap();

        // 6 is picked even that it's dead because we have enough alive+writeable nodes to form a
        //   good nodeset but we need to include dead nodes that may come back to life to reduce
        //   load skew
        assert_eq!(initial.nodeset, NodeSet::from([1, 2, 3, 6]));
        assert_eq!(initial.sequencer, GenerationalNodeId::new(5, 1));
        assert_eq!(
            initial.replication,
            replicated_loglet_config.replication_property
        );

        // with the previous sequencer, we expect no change to the nodeset
        // but a new sequencer to be chosen
        nodes.kill_nodes([2, 5]);

        let config = build_new_replicated_loglet_configuration(
            LOG_ID,
            replicated_loglet_config,
            initial.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&initial.nodeset),
            Some(initial.sequencer.into()),
        )
        .unwrap();

        assert_eq!(
            config.nodeset,
            NodeSet::from([1, 2, 3, 6]),
            "leave the loglet alone when nodeset can't be improved"
        );
        assert_ne!(
            config.sequencer,
            GenerationalNodeId::new(5, 1),
            "sequencer must change to a live node"
        );
        let sequencer_g2 = config.sequencer;

        nodes.revive_node((2, 2)); // N2 comes back

        let mut config = build_new_replicated_loglet_configuration(
            LOG_ID,
            replicated_loglet_config,
            config.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&config.nodeset),
            Some(config.sequencer.into()),
        )
        .unwrap();

        assert_eq!(config.nodeset, NodeSet::from([1, 2, 3, 6]));
        assert_eq!(config.sequencer, sequencer_g2); // unchanged

        nodes.kill_nodes([1, 2]); // we can no longer pick a nodeset that meets the replication requirement
        config.sequencer = GenerationalNodeId::from((5.into(), 1)); // pretend N5 (which is now dead) was the sequencer

        // the loglet should be reconfigured to use a new live node as its sequencer, keeping its nodeset
        let new_sequencer_config = build_new_replicated_loglet_configuration(
            LOG_ID,
            replicated_loglet_config,
            config.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&config.nodeset),
            Some(NodeId::Generational(config.sequencer)),
        )
        .unwrap();

        // who's alive now? only 3 as a log-server
        // although we have too many dead nodes, we are still able to choose nodes for the nodeset
        // because cluster_state is only advisory here, as long as those nodes are not marked
        // ReadOnly, we'll pick them up. sequencer. Why did 1 go away? it's because we can't form a
        // good writeable nodeset anyway so we fallback to normal nodeset size from all candidates.
        assert_eq!(new_sequencer_config.nodeset, NodeSet::from([2, 3, 6]));
        // new sequencer must be one of [0,3,4] as those are the only life nodes left
        assert_that!(
            new_sequencer_config.sequencer,
            any![
                eq(GenerationalNodeId::new(0, 1)),
                eq(GenerationalNodeId::new(3, 1)),
                eq(GenerationalNodeId::new(4, 1))
            ]
        );

        nodes.revive_nodes([(1, 2), (6, 2)]);

        let config = build_new_replicated_loglet_configuration(
            LOG_ID,
            replicated_loglet_config,
            config.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&config.nodeset),
            Some(config.sequencer.into()),
        )
        .unwrap();

        // ... then we can form a healthy nodeset that meets the replication requirement with
        // sufficient writeable and alive nodes again
        assert_eq!(config.nodeset, NodeSet::from([1, 2, 3, 6]));
    }
}
