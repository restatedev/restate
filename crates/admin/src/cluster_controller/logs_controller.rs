// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod nodeset_selection;

use futures::never::Never;
use rand::prelude::IteratorRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::iter;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, trace, trace_span, Instrument};
use xxhash_rust::xxh3::Xxh3Builder;

use restate_bifrost::{Bifrost, BifrostAdmin, Error as BifrostError};
use restate_core::metadata_store::{
    retry_on_network_error, MetadataStoreClient, Precondition, ReadWriteError, WriteError,
};
use restate_core::{Metadata, MetadataWriter, ShutdownError, TaskCenterFutureExt};
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::live::Pinned;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{
    Chain, DefaultProvider, LogletConfig, LogletParams, Logs, LogsConfiguration,
    NodeSetSelectionStrategy, ProviderKind, ReplicatedLogletConfig, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId, Lsn, TailState};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::retries::{RetryIter, RetryPolicy};
use restate_types::{logs, GenerationalNodeId, NodeId, PlainNodeId, Version, Versioned};

use crate::cluster_controller::logs_controller::nodeset_selection::{
    NodeSelectionError, NodeSetSelector,
};
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use crate::cluster_controller::scheduler;

type Result<T, E = LogsControllerError> = std::result::Result<T, E>;

const FALLBACK_MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum LogsControllerError {
    #[error("failed writing to the metadata store: {0}")]
    MetadataStore(#[from] ReadWriteError),
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
                }
            }
            LogState::Sealed { .. } => {}
        }
    }

    /// Checks whether the current segment requires reconfiguration and, therefore, needs to be
    /// sealed. The method returns [`true`] if the state was moved to sealing.
    fn try_transition_to_sealing(
        &mut self,
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
                ..
            } => {
                if let Some(loglet_configuration) = configuration.try_reconfiguring(
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
        DefaultProvider::Local => {
            let log_id = LogletId::new(log_id, SegmentIndex::OLDEST);
            Some(LogletConfiguration::Local(log_id.into()))
        }
        #[cfg(any(test, feature = "memory-loglet"))]
        DefaultProvider::InMemory => {
            let log_id = LogletId::new(log_id, SegmentIndex::OLDEST);
            Some(LogletConfiguration::Memory(log_id.into()))
        }
        #[cfg(feature = "replicated-loglet")]
        DefaultProvider::Replicated(ref config) => build_new_replicated_loglet_configuration(
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

/// Build a new segment configuration for a replicated loglet based on the observed cluster state
/// and the previous configuration.
#[cfg(feature = "replicated-loglet")]
pub fn build_new_replicated_loglet_configuration(
    replicated_loglet_config: &ReplicatedLogletConfig,
    loglet_id: LogletId,
    nodes_config: &NodesConfiguration,
    observed_cluster_state: &ObservedClusterState,
    previous_params: Option<&ReplicatedLogletParams>,
    preferred_sequencer: Option<NodeId>,
) -> Option<ReplicatedLogletParams> {
    let mut rng = thread_rng();

    let replication = replicated_loglet_config.replication_property.clone();
    let strategy = replicated_loglet_config.nodeset_selection_strategy;

    let preferred_nodes = previous_params
        .map(|p| p.nodeset.clone())
        .unwrap_or_default();

    let &sequencer = preferred_sequencer
        .and_then(|node_id| {
            // map to a known alive node
            observed_cluster_state.alive_nodes.get(&node_id.id())
        })
        .or_else(|| {
            // we can place the sequencer on any alive node
            observed_cluster_state.alive_nodes.values().choose(&mut rng)
        })?;

    let selection = NodeSetSelector::new(nodes_config, observed_cluster_state).select(
        strategy,
        &replication,
        &mut rng,
        &preferred_nodes,
    );

    match selection {
        Ok(nodeset) => Some(ReplicatedLogletParams {
            loglet_id,
            sequencer,
            replication,
            nodeset,
        }),
        Err(NodeSelectionError::InsufficientWriteableNodes) => {
            debug!(
                ?loglet_id,
                "Insufficient writeable nodes to select new nodeset for replicated loglet"
            );

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
    fn loglet_id(&self) -> LogletId {
        match self {
            Self::Local(id) => LogletId::from(*id),
            #[cfg(any(test, feature = "memory-loglet"))]
            Self::Memory(id) => LogletId::from(*id),
            #[cfg(feature = "replicated-loglet")]
            Self::Replicated(params) => params.loglet_id,
        }
    }

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
        nodes_config: &NodesConfiguration,
        logs_configuration: &LogsConfiguration,
        observed_cluster_state: &ObservedClusterState,
    ) -> bool {
        match (self, &logs_configuration.default_provider) {
            #[cfg(any(test, feature = "memory-loglet"))]
            (Self::Memory(_), DefaultProvider::InMemory) => false,
            (Self::Local(_), DefaultProvider::Local) => false,
            #[cfg(feature = "replicated-loglet")]
            (Self::Replicated(params), DefaultProvider::Replicated(config)) => {
                let sequencer_change_required = !observed_cluster_state
                    .is_node_alive(params.sequencer)
                    && !observed_cluster_state.alive_nodes.is_empty();

                if sequencer_change_required {
                    debug!(
                        loglet_id = ?params.loglet_id,
                        "Replicated loglet requires a sequencer change, existing sequencer {} is presumed dead",
                        params.sequencer
                    );
                }

                let nodeset_improvement_possible =
                    NodeSetSelector::new(nodes_config, observed_cluster_state).can_improve(
                        &params.nodeset,
                        NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
                        &config.replication_property,
                    );

                if nodeset_improvement_possible {
                    debug!(
                        loglet_id = ?params.loglet_id,
                        "Replicated loglet nodeset can be improved, will attempt reconfiguration"
                    );
                }

                sequencer_change_required || nodeset_improvement_possible
            }
            _ => {
                debug!(
                    "Changing provider type is not supporter at the moment. Ignoring reconfigure"
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
        logs_configuration: &LogsConfiguration,
        observed_cluster_state: &ObservedClusterState,
        preferred_sequencer: Option<NodeId>,
    ) -> Option<LogletConfiguration> {
        let loglet_id = self.loglet_id();

        match logs_configuration.default_provider {
            #[cfg(any(test, feature = "memory-loglet"))]
            DefaultProvider::InMemory => Some(LogletConfiguration::Memory(loglet_id.next().into())),
            DefaultProvider::Local => Some(LogletConfiguration::Local(loglet_id.next().into())),
            #[cfg(feature = "replicated-loglet")]
            DefaultProvider::Replicated(ref config) => {
                let previous_params = match self {
                    Self::Replicated(previous_params) => Some(previous_params),
                    _ => None,
                };

                build_new_replicated_loglet_configuration(
                    config,
                    loglet_id.next(),
                    &Metadata::with_current(|m| m.nodes_config_ref()),
                    observed_cluster_state,
                    previous_params,
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
    logs_state: HashMap<LogId, LogState, Xxh3Builder>,
    logs_write_in_progress: Option<Version>,

    // We are storing the logs explicitly (not relying on Metadata::current()) because we need a fixed
    // snapshot to keep logs_state in sync.
    current_logs: Arc<Logs>,
    retry_policy: RetryPolicy,
}

impl LogsControllerInner {
    fn new(configuration: LogsConfiguration, retry_policy: RetryPolicy) -> Self {
        Self {
            current_logs: Arc::new(Logs::with_logs_configuration(configuration)),
            logs_state: HashMap::with_hasher(Xxh3Builder::default()),
            logs_write_in_progress: None,
            retry_policy,
        }
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
            debug!("Proposing new logs configuration: {logs:?}");
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
                self.on_logs_update(Metadata::with_current(|m| m.logs_ref()))?;
                effects.push(Effect::FindLogsTail);
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

    fn on_logs_update(&mut self, logs: Pinned<Logs>) -> Result<()> {
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

            for (log_id, chain) in self.current_logs.iter() {
                let tail = chain.tail();

                if let Some(seal_lsn) = tail.tail_lsn {
                    // sealed tail segment
                    self.logs_state.insert(
                        *log_id,
                        LogState::Sealed {
                            configuration: tail.config.try_into().map_err(|err| {
                                LogsControllerError::LogletParamsToConfiguration(err)
                            })?,
                            segment_index: tail.index(),
                            seal_lsn,
                        },
                    );
                } else {
                    // open tail segment
                    self.logs_state.insert(
                        *log_id,
                        LogState::Available {
                            configuration: Some(tail.config.try_into().map_err(|err| {
                                LogsControllerError::LogletParamsToConfiguration(err)
                            })?),
                            segment_index: tail.index(),
                        },
                    );
                }
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
    metadata_store_client: MetadataStoreClient,
    metadata_writer: MetadataWriter,
    async_operations: JoinSet<Event>,
    find_logs_tail_semaphore: Arc<Semaphore>,
}

impl LogsController {
    pub async fn init(
        configuration: &Configuration,
        bifrost: Bifrost,
        metadata_store_client: MetadataStoreClient,
        metadata_writer: MetadataWriter,
    ) -> Result<Self> {
        // obtain the latest logs or init it with an empty logs variant
        let logs = retry_on_network_error(
            configuration.common.network_error_retry_policy.clone(),
            || {
                metadata_store_client.get_or_insert(BIFROST_CONFIG_KEY.clone(), || {
                    Logs::from_configuration(configuration)
                })
            },
        )
        .await?;

        let logs_configuration = logs.configuration().clone();
        metadata_writer.update(Arc::new(logs)).await?;

        //todo(azmy): make configurable
        let retry_policy = RetryPolicy::exponential(
            Duration::from_millis(10),
            2.0,
            Some(15),
            Some(Duration::from_secs(5)),
        );

        let mut this = Self {
            effects: Some(Vec::new()),
            inner: LogsControllerInner::new(logs_configuration, retry_policy),
            bifrost,
            metadata_store_client,
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
        let metadata_store_client = self.metadata_store_client.clone();
        let metadata_writer = self.metadata_writer.clone();
        let find_tail = async move {
            let bifrost_admin =
                BifrostAdmin::new(&bifrost, &metadata_writer, &metadata_store_client);

            let mut updates = LogsTailUpdates::default();
            for (log_id, chain) in logs.iter() {
                let tail_segment = chain.tail();

                let writable_loglet = match bifrost_admin.writeable_loglet(*log_id).await {
                    Ok(loglet) => loglet,
                    Err(BifrostError::Shutdown(_)) => break,
                    Err(err) => {
                        debug!(error=%err, %log_id, segment_index=%tail_segment.index(), "Failed to find writable loglet");
                        continue;
                    }
                };

                if writable_loglet.segment_index() != tail_segment.index() {
                    // writable segment in bifrost is probably ahead of our snapshot.
                    // then there is probably a new metadata update that will fix this
                    // for now we just ignore this segment
                    trace!(%log_id, segment_index=%tail_segment.index(), "Segment is not tail segment, skip finding tail");
                    continue;
                }

                let found_tail = match writable_loglet.find_tail().await {
                    Ok(tail) => tail,
                    Err(err) => {
                        debug!(error=%err, %log_id, segment_index=%tail_segment.index(), "Failed to find tail for loglet");
                        continue;
                    }
                };

                // send message
                let update = LogTailUpdate {
                    segment_index: writable_loglet.segment_index(),
                    tail: found_tail,
                };

                updates.insert(*log_id, update);
            }
            // we explicitly drop the permit here to make sure
            // it's moved to the future closure
            drop(permit);
            Event::LogsTailUpdates { updates }
        };

        self.async_operations.spawn(
            find_tail
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
        self.inner.on_logs_update(logs)
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
        let metadata_store_client = self.metadata_store_client.clone();
        let metadata_writer = self.metadata_writer.clone();

        self.async_operations.spawn(async move {
                if let Some(debounce) = &mut debounce {
                    let delay = debounce.next().unwrap_or(FALLBACK_MAX_RETRY_DELAY);
                    debug!(?delay, %previous_version, "Wait before attempting to write logs");
                    tokio::time::sleep(delay).await;
                }

                if let Err(err) = metadata_store_client
                    .put(
                        BIFROST_CONFIG_KEY.clone(),
                        logs.deref(),
                        Precondition::MatchesVersion(previous_version),
                    )
                    .await
                {
                    return match err {
                        WriteError::FailedPrecondition(_) => {
                            debug!("Detected a concurrent modification of logs. Fetching the latest logs now.");
                            // There was a concurrent modification of the logs. Fetch the latest version.
                            match metadata_store_client
                                .get::<Logs>(BIFROST_CONFIG_KEY.clone())
                                .await
                            {
                                Ok(result) => {
                                    let logs = result.expect("should be present");
                                    // we are only failing if we are shutting down
                                    let _ = metadata_writer.update(Arc::new(logs)).await;
                                    Event::NewLogs
                                }
                                Err(err) => {
                                    debug!("failed committing new logs: {err}");
                                    Event::WriteLogsFailed {
                                        logs,
                                        previous_version,
                                        debounce,
                                    }
                                }
                            }
                        }
                        err => {
                            debug!("failed writing new logs: {err}");
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
        let metadata_store_client = self.metadata_store_client.clone();
        let metadata_writer = self.metadata_writer.clone();

        self.async_operations.spawn(
            async move {
                if let Some(debounce) = &mut debounce {
                    let delay = debounce.next().unwrap_or(FALLBACK_MAX_RETRY_DELAY);
                    debug!(?delay, %log_id, %segment_index, "Wait before attempting to seal log");
                    tokio::time::sleep(delay).await;
                }

                let bifrost_admin =
                    BifrostAdmin::new(&bifrost, &metadata_writer, &metadata_store_client);

                match bifrost_admin.seal(log_id, segment_index).await {
                    Ok(sealed_segment) => {
                        if sealed_segment.tail.is_sealed() {
                            Event::SealSucceeded {
                                log_id,
                                segment_index,
                                seal_lsn: sealed_segment.tail.offset(),
                            }
                        } else {
                            Event::SealFailed {
                                log_id,
                                segment_index,
                                debounce,
                            }
                        }
                    }
                    Err(err) => {
                        debug!(%log_id, %segment_index, "failed sealing loglet: {err}");
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

impl<'a> scheduler::PartitionProcessorPlacementHints
    for LogsBasedPartitionProcessorPlacementHints<'a>
{
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
    use restate_types::logs::metadata::{
        DefaultProvider, LogsConfiguration, NodeSetSelectionStrategy, ReplicatedLogletConfig,
    };
    use restate_types::logs::LogletId;
    use restate_types::nodes_config::{
        LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
    };
    use restate_types::replicated_loglet::{NodeSet, ReplicatedLogletParams, ReplicationProperty};
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

        pub fn add_dedicated_log_server_node(&mut self, id: u32) {
            self.add_node(id, enum_set!(Role::LogServer), StorageState::ReadWrite)
        }

        pub fn add_node(
            &mut self,
            node_id: u32,
            roles: EnumSet<Role>,
            storage_state: StorageState,
        ) {
            let id = node_id.into();
            self.observed_state.dead_nodes.remove(&id);
            self.observed_state
                .alive_nodes
                .insert(id, id.with_generation(1));
            self.nodes_config
                .upsert_node(node(node_id, roles, storage_state));
        }

        pub fn kill_node(&mut self, node_id: u32) {
            let id = node_id.into();
            assert!(
                self.observed_state.alive_nodes.remove(&id).is_some(),
                "node not found"
            );
            self.observed_state.dead_nodes.insert(id);
        }

        pub fn kill_nodes<const N: usize>(&mut self, ids: [u32; N]) {
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
            roles: EnumSet<Role>,
            storage_state: StorageState,
        ) -> Self {
            let id = node_id.into();
            self.nodes_config
                .upsert_node(node(node_id, roles, storage_state));
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
                enum_set!(Role::Admin | Role::MetadataStore),
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

        pub fn with_mixed_server_nodes<const N: usize>(self, ids: [u32; N]) -> Self {
            self.with_nodes(
                ids,
                enum_set!(Role::Worker | Role::LogServer),
                StorageState::ReadWrite,
            )
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
            default_provider: DefaultProvider::Replicated(ReplicatedLogletConfig {
                replication_property: ReplicationProperty::new(
                    NonZeroU8::new(replication_factor).expect("must be non zero"),
                ),
                nodeset_selection_strategy: NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            }),
        }
    }

    pub fn node(id: u32, roles: EnumSet<Role>, storage_state: StorageState) -> NodeConfig {
        NodeConfig::new(
            format!("node-{id}"),
            PlainNodeId::from(id).with_generation(1),
            format!("https://node-{id}").parse().unwrap(),
            roles,
            LogServerConfig { storage_state },
        )
    }

    #[test]
    fn loglet_requires_reconfiguration() {
        let mut nodes = MockNodes::builder()
            .with_all_roles_node(0)
            .with_dead_node(
                1,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            )
            .with_node(
                2,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            )
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
            &nodes.nodes_config,
            &logs_config,
            &nodes.observed_state
        ));

        let params = LogletConfiguration::Replicated(seq_n0.clone());
        assert!(
            !params.requires_reconfiguration(
                &nodes.nodes_config,
                &logs_config,
                &nodes.observed_state
            ),
            "we should not reconfigure when we can't improve the nodeset"
        );

        nodes.add_dedicated_log_server_node(3);
        assert!(
            params.requires_reconfiguration(
                &nodes.nodes_config,
                &logs_config,
                &nodes.observed_state
            ),
            "we should be able to go to [N0, N2, N3] from the previous configuration"
        );

        // this _should_ be false, as we likely can't seal the [N0, N1, N2] loglet segment with nodes N1 and N2 dead.
        // in the future, we'll make the logs controller smarter about this type of thing
        nodes.add_dedicated_log_server_node(4);
        nodes.kill_node(2);
        assert!(params.requires_reconfiguration(
            &nodes.nodes_config,
            &logs_config,
            &nodes.observed_state
        ));

        let DefaultProvider::Replicated(ref replicated_loglet_config) =
            logs_config.default_provider
        else {
            unreachable!()
        };

        let config = build_new_replicated_loglet_configuration(
            replicated_loglet_config,
            seq_n0.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&seq_n0),
            Some(seq_n0.sequencer.into()),
        )
        .unwrap();
        assert_eq!(config.sequencer, seq_n0.sequencer, "no change in sequencer");
        assert_eq!(
            config.nodeset,
            NodeSet::from([0, 3, 4]),
            "picks healthy log servers for nodeset"
        );
    }

    #[test]
    fn bootstrap_and_reconfigure_replicated_loglet() {
        let mut nodes = MockNodes::builder()
            .with_dedicated_admin_node(0)
            .with_dedicated_log_server_nodes([1, 2, 3, 6])
            .with_dedicated_worker_nodes([4, 5])
            .dead_nodes([6]) // N6 starts out offline
            .build();

        let logs_config = logs_configuration(2);

        let DefaultProvider::Replicated(ref replicated_loglet_config) =
            logs_config.default_provider
        else {
            unreachable!()
        };

        let initial = build_new_replicated_loglet_configuration(
            replicated_loglet_config,
            LogletId::from(1),
            &nodes.nodes_config,
            &nodes.observed_state,
            None,
            Some(NodeId::new_plain(5)),
        )
        .unwrap();

        assert_eq!(initial.nodeset, NodeSet::from([1, 2, 3]));
        assert_eq!(initial.sequencer, GenerationalNodeId::new(5, 1));
        assert_eq!(
            initial.replication,
            replicated_loglet_config.replication_property
        );

        // with one log server down (out of 3) and the previous sequencer, we expect no
        // change to the nodeset but a new sequencer to be chosen
        nodes.kill_nodes([2, 5]);

        let config = build_new_replicated_loglet_configuration(
            replicated_loglet_config,
            initial.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&initial),
            Some(initial.sequencer.into()),
        )
        .unwrap();

        assert_eq!(
            config.nodeset,
            NodeSet::from([1, 2, 3]),
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
            replicated_loglet_config,
            config.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&config),
            Some(config.sequencer.into()),
        )
        .unwrap();

        assert_eq!(config.nodeset, NodeSet::from([1, 2, 3]));
        assert_eq!(config.sequencer, sequencer_g2); // unchanged

        nodes.kill_nodes([1, 2]); // we can no longer pick a nodeset that meets the replication requirement
        config.sequencer = GenerationalNodeId::from((5.into(), 1)); // pretend N5 (which is now dead) was the sequencer

        // the loglet should be reconfigured to use a new live node as its sequencer, keeping its nodeset
        let new_sequencer_config = build_new_replicated_loglet_configuration(
            replicated_loglet_config,
            config.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&config),
            Some(NodeId::Generational(config.sequencer)),
        );

        assert_eq!(
            None, new_sequencer_config,
            "not enough alive node to satisfy minimum number of nodes"
        );

        // N1 comes back and N6 is added, which was not previously in the loglet's nodeset
        nodes.revive_nodes([(1, 2), (6, 2)]);

        let config = build_new_replicated_loglet_configuration(
            replicated_loglet_config,
            config.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&config),
            Some(config.sequencer.into()),
        )
        .unwrap();

        // ... then we can form a healthy nodeset that meets the replication requirement
        assert_eq!(config.nodeset, NodeSet::from([1, 3, 6]));

        nodes.revive_node((2, 3)); // N2 comes back _again_

        let config = build_new_replicated_loglet_configuration(
            replicated_loglet_config,
            config.loglet_id,
            &nodes.nodes_config,
            &nodes.observed_state,
            Some(&config),
            Some(config.sequencer.into()),
        )
        .unwrap();

        // we can now further expand the nodeset to exceed the 2f + 1 fault tolerance
        assert_eq!(config.nodeset, NodeSet::from([1, 2, 3, 6]));
    }
}
