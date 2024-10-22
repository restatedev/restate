// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::prelude::IteratorRandom;
use rand::{thread_rng, RngCore};
use std::collections::HashMap;
use std::iter;
use std::num::NonZeroU8;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, trace, trace_span, Instrument};
use xxhash_rust::xxh3::Xxh3Builder;

use restate_bifrost::{Bifrost, BifrostAdmin, Error as BifrostError};
use restate_core::metadata_store::{
    retry_on_network_error, MetadataStoreClient, Precondition, ReadWriteError, WriteError,
};
use restate_core::{metadata, task_center, Metadata, MetadataWriter, ShutdownError};
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::live::Pinned;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{
    Chain, LogletConfig, LogletParams, Logs, ProviderKind, SegmentIndex,
};
use restate_types::logs::{LogId, Lsn, TailState};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::nodes_config::Role;
use restate_types::partition_table::PartitionTable;
use restate_types::replicated_loglet::{
    NodeSet, ReplicatedLogletId, ReplicatedLogletParams, ReplicationProperty,
};
use restate_types::retries::{RetryIter, RetryPolicy};
use restate_types::{logs, GenerationalNodeId, NodeId, PlainNodeId, Version, Versioned};

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
    Provisioning {
        log_id: LogId,
        provider_kind: ProviderKind,
    },
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
    fn try_transition_to_sealing(&mut self, observed_cluster_state: &ObservedClusterState) -> bool {
        match self {
            // We can only move from Available to Sealing
            LogState::Available {
                configuration,
                segment_index,
            } => {
                if configuration
                    .as_ref()
                    .expect("configuration must be present")
                    .requires_reconfiguration(observed_cluster_state)
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
        observed_cluster_state: &ObservedClusterState,
        log_builder: &mut LogsBuilder,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<()> {
        match self {
            LogState::Provisioning {
                provider_kind,
                log_id,
            } => {
                if let Some(loglet_configuration) = try_provisioning(
                    *log_id,
                    *provider_kind,
                    observed_cluster_state,
                    node_set_selector_hints,
                ) {
                    let chain =
                        Chain::new(*provider_kind, loglet_configuration.to_loglet_params()?);
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
                if let Some(loglet_configuration) = configuration
                    .try_reconfiguring(observed_cluster_state, preferred_sequencer_hint())
                {
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
    provider_kind: ProviderKind,
    observed_cluster_state: &ObservedClusterState,
    node_set_selector_hints: impl NodeSetSelectorHints,
) -> Option<LogletConfiguration> {
    match provider_kind {
        ProviderKind::Local => {
            let log_id = thread_rng().next_u64();
            Some(LogletConfiguration::Local(log_id))
        }
        #[cfg(any(test, feature = "memory-loglet"))]
        ProviderKind::InMemory => {
            let log_id = thread_rng().next_u64();
            Some(LogletConfiguration::Memory(log_id))
        }
        #[cfg(feature = "replicated-loglet")]
        ProviderKind::Replicated => build_new_replicated_loglet_configuration(
            ReplicatedLogletId::new(log_id, SegmentIndex::OLDEST),
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
fn build_new_replicated_loglet_configuration(
    loglet_id: ReplicatedLogletId,
    observed_cluster_state: &ObservedClusterState,
    previous_configuration: Option<&ReplicatedLogletParams>,
    preferred_sequencer: Option<NodeId>,
) -> Option<ReplicatedLogletParams> {
    let mut rng = thread_rng();
    // todo make min nodeset size configurable, respect roles, respect StorageState, etc.
    let nodes_config = metadata().nodes_config_ref();

    let log_servers: Vec<_> = observed_cluster_state
        .alive_nodes
        .values()
        .filter(|node| {
            nodes_config
                .find_node_by_id(**node)
                .ok()
                .is_some_and(|config| config.has_role(Role::LogServer))
        })
        .collect();

    let sequencer = preferred_sequencer
        .and_then(|node_id| {
            // map to a known alive node
            observed_cluster_state.alive_nodes.get(&node_id.id())
        })
        .or_else(|| {
            // we can place the sequencer on any alive node
            observed_cluster_state.alive_nodes.values().choose(&mut rng)
        });

    if log_servers.len() >= 3 {
        let replication = ReplicationProperty::new(NonZeroU8::new(2).expect("to be valid"));
        let mut nodeset = NodeSet::empty();

        for node in &log_servers {
            nodeset.insert(node.as_plain());
        }

        Some(ReplicatedLogletParams {
            loglet_id,
            sequencer: *sequencer.expect("one node must exist"),
            replication,
            nodeset,
            write_set: None,
        })
    } else if let Some(sequencer) = sequencer {
        previous_configuration.cloned().map(|mut configuration| {
            configuration.loglet_id = loglet_id;
            configuration.sequencer = *sequencer;
            configuration
        })
    } else {
        None
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

    fn requires_reconfiguration(&self, observed_cluster_state: &ObservedClusterState) -> bool {
        match self {
            #[cfg(feature = "replicated-loglet")]
            LogletConfiguration::Replicated(configuration) => {
                // todo check also whether we can improve the nodeset based on the observed cluster state
                !observed_cluster_state.is_node_alive(configuration.sequencer)
            }
            LogletConfiguration::Local(_) => false,
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => false,
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
        observed_cluster_state: &ObservedClusterState,
        preferred_sequencer: Option<NodeId>,
    ) -> Option<LogletConfiguration> {
        match self {
            #[cfg(feature = "replicated-loglet")]
            LogletConfiguration::Replicated(configuration) => {
                build_new_replicated_loglet_configuration(
                    configuration.loglet_id.next(),
                    observed_cluster_state,
                    Some(configuration),
                    preferred_sequencer,
                )
                .map(LogletConfiguration::Replicated)
            }
            LogletConfiguration::Local(_) => {
                let loglet_id = thread_rng().next_u64();
                Some(LogletConfiguration::Local(loglet_id))
            }
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => {
                let loglet_id = thread_rng().next_u64();
                Some(LogletConfiguration::Memory(loglet_id))
            }
        }
    }

    fn node_set_iter(&self) -> impl Iterator<Item = &PlainNodeId> {
        match self {
            LogletConfiguration::Replicated(configuration) => {
                itertools::Either::Left(configuration.nodeset.iter())
            }
            LogletConfiguration::Local(_) => itertools::Either::Right(iter::empty()),
            LogletConfiguration::Memory(_) => itertools::Either::Right(iter::empty()),
        }
    }

    fn sequencer_node(&self) -> Option<GenerationalNodeId> {
        match self {
            LogletConfiguration::Replicated(configuration) => Some(configuration.sequencer),
            LogletConfiguration::Local(_) => None,
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
/// complexity of handling multiple in-flight logs writes, the controller waits until the an
/// in-flight write completes or a newer log is detected.
struct LogsControllerInner {
    // todo make configurable
    default_provider: ProviderKind,

    logs_state: HashMap<LogId, LogState, Xxh3Builder>,
    logs_write_in_progress: Option<Version>,

    // We are storing the logs explicitly (not relying on metadata()) because we need a fixed
    // snapshot to keep logs_state in sync.
    current_logs: Arc<Logs>,
    retry_policy: RetryPolicy,
}

impl LogsControllerInner {
    fn new(
        logs: Pinned<Logs>,
        partition_table: &PartitionTable,
        default_provider: ProviderKind,
        retry_policy: RetryPolicy,
    ) -> Result<Self> {
        let mut this = Self {
            default_provider,
            current_logs: Arc::new(Logs::default()),
            logs_state: HashMap::with_hasher(Xxh3Builder::default()),
            logs_write_in_progress: None,
            retry_policy,
        };

        this.on_logs_update(logs)?;
        this.on_partition_table_update(partition_table);
        Ok(this)
    }

    fn on_observed_cluster_state_update(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        effects: &mut Vec<Effect>,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<()> {
        // we don't do concurrent updates to avoid complexity
        if self.logs_write_in_progress.is_none() {
            self.seal_logs(observed_cluster_state, effects);

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
                // we already update the current logs but will wait until we learn whether the write
                // was successful or not. If not, then we'll reset our internal state wrt the new
                // logs version.
                self.current_logs = logs;
            }
        }

        Ok(())
    }

    fn seal_logs(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        effects: &mut Vec<Effect>,
    ) {
        for (log_id, log_state) in &mut self.logs_state {
            if log_state.try_transition_to_sealing(observed_cluster_state) {
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
                observed_cluster_state,
                |seal_lsn, provider_kind, loglet_params| {
                    let mut chain_builder = logs_builder
                        .chain(log_id)
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
        metadata: &Metadata,
        metadata_writer: &mut MetadataWriter,
    ) -> Result<()> {
        match event {
            Event::WriteLogsSucceeded(version) => {
                self.on_logs_written(version);
                // todo prevent clone by adding support for passing in Arcs
                metadata_writer.submit(self.current_logs.deref().clone());
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
                self.on_logs_update(metadata.logs_ref())?;
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
                    debug!(%log_id, "Try provisioning log with provider '{}'", self.default_provider);
                    LogState::Provisioning {
                        log_id,
                        provider_kind: self.default_provider,
                    }
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
    metadata: Metadata,
    metadata_writer: MetadataWriter,
    async_operations: JoinSet<Event>,
    find_logs_tail_semaphore: Arc<Semaphore>,
}

impl LogsController {
    pub async fn init(
        configuration: &Configuration,
        metadata: Metadata,
        bifrost: Bifrost,
        metadata_store_client: MetadataStoreClient,
        metadata_writer: MetadataWriter,
    ) -> Result<Self> {
        // obtain the latest logs or init it with an empty logs variant
        let logs = retry_on_network_error(
            configuration.common.network_error_retry_policy.clone(),
            || metadata_store_client.get_or_insert(BIFROST_CONFIG_KEY.clone(), Logs::default),
        )
        .await?;
        metadata_writer.update(logs).await?;

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
                metadata.logs_ref(),
                metadata.partition_table_ref().as_ref(),
                configuration.bifrost.default_provider,
                retry_policy,
            )?,
            metadata,
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
                    // then there is probably a new metadata update that will gonna fix this
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
            // we explicity drop the permit here to make sure
            // it's moved to the future closure
            drop(permit);
            Event::LogsTailUpdates { updates }
        };

        let tc = task_center();
        self.async_operations.spawn(async move {
            tc.run_in_scope(
                "log-controller-refresh-tail",
                None,
                find_tail.instrument(trace_span!("scheduled-find-tail")),
            )
            .await
        });
    }

    pub fn on_observed_cluster_state_update(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        node_set_selector_hints: impl NodeSetSelectorHints,
    ) -> Result<(), anyhow::Error> {
        self.inner.on_observed_cluster_state_update(
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
        let tc = task_center().clone();
        let metadata_store_client = self.metadata_store_client.clone();
        let metadata_writer = self.metadata_writer.clone();

        self.async_operations.spawn(async move {
            tc.run_in_scope("logs-controller-write-logs", None, async {
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
                                    let _ = metadata_writer.update(logs).await;
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
                                debounce
                            }
                        }
                    };
                }

                // we don't update metadata with the new logs here because it would cause a race
                // condition with the outer on_logs_updated signal

                let version = logs.version();
                Event::WriteLogsSucceeded(version)
            })
            .await
        });
    }

    fn seal_log(
        &mut self,
        log_id: LogId,
        segment_index: SegmentIndex,
        mut debounce: Option<RetryIter<'static>>,
    ) {
        let tc = task_center().clone();
        let bifrost = self.bifrost.clone();
        let metadata_store_client = self.metadata_store_client.clone();
        let metadata_writer = self.metadata_writer.clone();

        self.async_operations.spawn(async move {
            tc.run_in_scope("logs-controller-seal-log", None, async {
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
            })
            .await
        });
    }

    pub async fn run_async_operations(&mut self) -> Result<()> {
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
                    &self.metadata,
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
