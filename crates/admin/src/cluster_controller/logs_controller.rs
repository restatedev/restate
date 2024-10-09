// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cluster_controller::scheduler::ObservedClusterState;
use rand::{thread_rng, RngCore};
use restate_bifrost::{Bifrost, BifrostAdmin};
use restate_core::metadata_store::{MetadataStoreClient, Precondition, WriteError};
use restate_core::{task_center, Metadata, MetadataWriter};
use restate_types::logs::builder::{ChainBuilder, LogsBuilder};
use restate_types::logs::metadata::{
    Chain, LogletConfig, LogletParams, Logs, ProviderKind, SegmentIndex,
};
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::partition_table::PartitionTable;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::{Version, Versioned};
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::debug;

enum LogsState {
    Provisioning {
        log_id: LogId,
        provider_kind: ProviderKind,
    },
    Running {
        configuration: Option<LogletConfiguration>,
        segment_index: SegmentIndex,
    },
    Sealing {
        configuration: Option<LogletConfiguration>,
        segment_index: SegmentIndex,
    },
    Sealed {
        configuration: LogletConfiguration,
        segment_index: SegmentIndex,
        seal_lsn: Lsn,
    },
}

impl LogsState {
    fn try_seal(&mut self, segment_index_to_seal: SegmentIndex, seal_lsn: Lsn) {
        match self {
            LogsState::Provisioning { .. } => {}
            LogsState::Running {
                configuration,
                segment_index,
            } => {
                if *segment_index == segment_index_to_seal {
                    *self = LogsState::Sealed {
                        configuration: configuration
                            .take()
                            .expect("requires previous configuration"),
                        segment_index: segment_index_to_seal,
                        seal_lsn,
                    }
                }
            }
            LogsState::Sealing {
                configuration,
                segment_index,
            } => {
                if *segment_index == segment_index_to_seal {
                    *self = LogsState::Sealed {
                        configuration: configuration
                            .take()
                            .expect("requires previous configuration"),
                        segment_index: segment_index_to_seal,
                        seal_lsn,
                    }
                }
            }
            LogsState::Sealed { .. } => {}
        }
    }

    fn try_sealing(&mut self) {
        match self {
            LogsState::Provisioning { .. } => {}
            LogsState::Running {
                configuration,
                segment_index,
            } => {
                *self = LogsState::Sealing {
                    configuration: configuration.take(),
                    segment_index: *segment_index,
                }
            }
            LogsState::Sealing { .. } => {}
            LogsState::Sealed { .. } => {}
        }
    }

    fn try_provisioning(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        log_builder: &mut LogsBuilder,
    ) -> Result<(), anyhow::Error> {
        match self {
            LogsState::Provisioning {
                provider_kind,
                log_id,
            } => {
                if let Some(loglet_configuration) =
                    try_provisioning(*provider_kind, observed_cluster_state)
                {
                    let chain =
                        Chain::new(*provider_kind, loglet_configuration.to_loglet_params()?);
                    log_builder
                        .add_log(*log_id, chain)
                        .expect("a provisioning log should not have a logs entry");

                    *self = LogsState::Running {
                        segment_index: SegmentIndex::default(),
                        configuration: Some(loglet_configuration),
                    };
                }
            }
            // nothing to do :-)
            LogsState::Sealing { .. } | LogsState::Sealed { .. } | LogsState::Running { .. } => {
                return Ok(())
            }
        };

        Ok(())
    }

    fn try_reconfiguring(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        chain_builder: &mut ChainBuilder,
    ) -> Result<(), anyhow::Error> {
        match self {
            LogsState::Sealed {
                seal_lsn,
                configuration,
                ..
            } => {
                if let Some(loglet_configuration) =
                    configuration.try_reconfiguring(observed_cluster_state)
                {
                    let segment_index = chain_builder.append_segment(
                        *seal_lsn,
                        loglet_configuration.as_provider(),
                        loglet_configuration.to_loglet_params()?,
                    )?;

                    *self = LogsState::Running {
                        segment_index,
                        configuration: Some(loglet_configuration),
                    };
                }
            }
            // nothing to do :-)
            LogsState::Provisioning { .. }
            | LogsState::Sealing { .. }
            | LogsState::Running { .. } => return Ok(()),
        };

        Ok(())
    }

    fn requires_reconfiguration(&self, observed_cluster_state: &ObservedClusterState) -> bool {
        match self {
            LogsState::Provisioning { .. }
            | LogsState::Sealing { .. }
            | LogsState::Sealed { .. } => false,
            LogsState::Running { configuration, .. } => configuration
                .as_ref()
                .expect("configuration must be present")
                .requires_reconfiguration(observed_cluster_state),
        }
    }

    fn current_segment(&self) -> Option<SegmentIndex> {
        match self {
            LogsState::Provisioning { .. } => None,
            LogsState::Running { segment_index, .. } => Some(*segment_index),
            LogsState::Sealing { segment_index, .. } => Some(*segment_index),
            LogsState::Sealed { segment_index, .. } => Some(*segment_index),
        }
    }
}

fn try_provisioning(
    provider_kind: ProviderKind,
    observed_cluster_state: &ObservedClusterState,
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
        ProviderKind::Replicated => find_new_replicated_loglet_configuration(observed_cluster_state, None)
            .map(LogletConfiguration::Replicated),
    }
}

fn find_new_replicated_loglet_configuration(
    observed_cluster_state: &ObservedClusterState,
    previous_configuration: Option<&ReplicatedLogletParams>,
) -> Option<ReplicatedLogletParams> {
    // todo: calculate new configuration for replicated loglet based on observed cluster state
    previous_configuration.cloned()
}

enum LogletConfiguration {
    Replicated(ReplicatedLogletParams),
    Local(u64),
    #[cfg(any(test, feature = "memory-loglet"))]
    Memory(u64),
}

impl LogletConfiguration {
    fn as_provider(&self) -> ProviderKind {
        match self {
            LogletConfiguration::Replicated(_) => ProviderKind::Replicated,
            LogletConfiguration::Local(_) => ProviderKind::Local,
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => ProviderKind::InMemory,
        }
    }

    fn requires_reconfiguration(&self, observed_cluster_state: &ObservedClusterState) -> bool {
        match self {
            LogletConfiguration::Replicated(configuration) => {
                // todo check also whether we can improve the nodeset based on the observed cluster state
                observed_cluster_state
                    .dead_nodes
                    .contains(&configuration.sequencer.as_plain())
            }
            LogletConfiguration::Local(_) => false,
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => false,
        }
    }

    fn to_loglet_params(&self) -> anyhow::Result<LogletParams> {
        Ok(match self {
            LogletConfiguration::Replicated(configuration) => {
                LogletParams::from(configuration.serialize()?)
            }
            LogletConfiguration::Local(id) => LogletParams::from(id.to_string()),
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(id) => LogletParams::from(id.to_string()),
        })
    }

    fn try_reconfiguring(
        &self,
        observed_cluster_state: &ObservedClusterState,
    ) -> Option<LogletConfiguration> {
        let previous_configuration = match self {
            LogletConfiguration::Replicated(configuration) => Some(configuration),
            LogletConfiguration::Local(_) => {
                let loglet_id = rand::thread_rng().next_u64();
                return Some(LogletConfiguration::Local(loglet_id));
            }
            #[cfg(any(test, feature = "memory-loglet"))]
            LogletConfiguration::Memory(_) => {
                let loglet_id = rand::thread_rng().next_u64();
                return Some(LogletConfiguration::Memory(loglet_id));
            }
        };

        if let Some(new_configuration) =
            find_new_replicated_loglet_configuration(observed_cluster_state, previous_configuration)
        {
            Some(LogletConfiguration::Replicated(new_configuration))
        } else {
            None
        }
    }
}

impl TryFrom<&LogletConfig> for LogletConfiguration {
    type Error = anyhow::Error;

    fn try_from(value: &LogletConfig) -> Result<Self, Self::Error> {
        match value.kind {
            ProviderKind::Local => Ok(LogletConfiguration::Local(value.params.parse()?)),
            #[cfg(any(test, feature = "memory-loglet"))]
            ProviderKind::InMemory => Ok(LogletConfiguration::Memory(value.params.parse()?)),
            ProviderKind::Replicated => {
                ReplicatedLogletParams::deserialize_from(value.params.as_bytes())
                    .map(LogletConfiguration::Replicated)
                    .map_err(Into::into)
            }
        }
    }
}

enum Effect {
    CommitLogs {
        logs: Logs,
        expected_version: Version,
    },
    Seal {
        log_id: LogId,
        segment_index: SegmentIndex,
    },
}

enum Event {
    LogsCommitSucceeded(Version),
    LogsCommitFailed {
        logs: Logs,
        expected_version: Version,
    },
    NewLogs(Logs),
    Sealed {
        log_id: LogId,
        segment_index: SegmentIndex,
        seal_lsn: Lsn,
    },
    SealFailed {
        log_id: LogId,
        segment_index: SegmentIndex,
    },
}

struct Inner {
    default_provider: ProviderKind,
    logs: Logs,
    logs_state: BTreeMap<LogId, LogsState>,
    logs_commit_in_progress: Option<Version>,
}

impl Inner {
    fn new(logs: Logs, partition_table: &PartitionTable) -> Result<Self, anyhow::Error> {
        let mut this = Self {
            default_provider: ProviderKind::Local,
            logs: Logs::default(),
            logs_state: BTreeMap::default(),
            logs_commit_in_progress: None,
        };

        this.on_new_logs(logs, partition_table)?;
        Ok(this)
    }

    fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        effects: &mut Vec<Effect>,
    ) -> Result<(), anyhow::Error> {
        // we don't do concurrent updates
        if self.logs_commit_in_progress.is_none() {
            self.seal_logs(observed_cluster_state, effects);

            let mut builder = self.logs.clone().into_builder();
            self.provision_logs(observed_cluster_state, &mut builder)?;
            self.reconfigure_logs(observed_cluster_state, &mut builder)?;

            if let Some(logs) = builder.build_if_modified() {
                self.logs_commit_in_progress = Some(logs.version());
                effects.push(Effect::CommitLogs {
                    logs: logs.clone(),
                    expected_version: self.logs.version(),
                });
                self.logs = logs;
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
            if log_state.requires_reconfiguration(observed_cluster_state) {
                log_state.try_sealing();
                effects.push(Effect::Seal {
                    log_id: *log_id,
                    segment_index: log_state.current_segment().expect("expect a valid segment"),
                })
            }
        }
    }

    fn provision_logs(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        logs_builder: &mut LogsBuilder,
    ) -> Result<(), anyhow::Error> {
        for (log_state) in self.logs_state.values_mut() {
            log_state.try_provisioning(observed_cluster_state, logs_builder)?;
        }

        Ok(())
    }

    fn reconfigure_logs(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        logs_builder: &mut LogsBuilder,
    ) -> Result<(), anyhow::Error> {
        for (log_id, log_state) in &mut self.logs_state {
            let mut chain_builder = logs_builder.chain(log_id).expect("should be present");
            log_state.try_reconfiguring(observed_cluster_state, &mut chain_builder)?;
        }

        Ok(())
    }

    fn on_events(
        &mut self,
        events: impl Iterator<Item = Event>,
        effects: &mut Vec<Effect>,
        partition_table: &PartitionTable,
    ) -> Result<(), anyhow::Error> {
        for event in events {
            self.on_event(event, effects, partition_table)?;
        }

        Ok(())
    }

    fn on_event(&mut self, event: Event, effects: &mut Vec<Effect>, partition_table: &PartitionTable) -> Result<(), anyhow::Error> {
        match event {
            Event::LogsCommitSucceeded(version) => {
                self.on_logs_committed(version);
            }
            Event::NewLogs(logs) => {
                self.on_new_logs(logs, partition_table)?;
            }
            Event::Sealed {
                log_id,
                segment_index,
                seal_lsn,
            } => {
                self.on_sealed_log(log_id, segment_index, seal_lsn);
            }
            Event::LogsCommitFailed {
                logs,
                expected_version,
            } => {
                if Some(logs.version()) == self.logs_commit_in_progress {
                    // todo what if it doesn't work again? Maybe stepping down as the leader
                    effects.push(Effect::CommitLogs {
                        logs,
                        expected_version,
                    });
                }
            }
            Event::SealFailed {
                log_id,
                segment_index,
            } => {
                // todo what if it doesn't work again? Maybe stepping down as the leader
                effects.push(Effect::Seal {
                    log_id,
                    segment_index,
                })
            }
        }

        Ok(())
    }

    fn on_logs_committed(&mut self, version: Version) {
        // filter out, outdated commits
        if Some(version) == self.logs_commit_in_progress {
            self.logs_commit_in_progress = None;
        }
    }

    fn on_new_logs(
        &mut self,
        logs: Logs,
        partition_table: &PartitionTable,
    ) -> Result<(), anyhow::Error> {
        // rebuild the internal state if we receive a newer logs or one with a version we were
        // supposed to commit (race condition)
        if logs.version() > self.logs.version()
            || Some(logs.version()) == self.logs_commit_in_progress
        {
            self.clear();
            self.logs = logs;

            for (log_id, chain) in self.logs.iter() {
                let tail = chain.tail();

                if let Some(seal_lsn) = tail.tail_lsn {
                    self.logs_state.insert(
                        *log_id,
                        LogsState::Sealed {
                            configuration: tail.config.try_into()?,
                            segment_index: tail.index(),
                            seal_lsn,
                        },
                    );
                } else {
                    // open tail segment
                    self.logs_state.insert(
                        *log_id,
                        LogsState::Running {
                            configuration: Some(tail.config.try_into()?),
                            segment_index: tail.index(),
                        },
                    );
                }
            }

            // update the provisioning logs
            for (partition_id, _) in partition_table.partitions() {
                self.logs_state
                    .entry((*partition_id).into())
                    .or_insert_with(|| LogsState::Provisioning {
                        log_id: (*partition_id).into(),
                        provider_kind: self.default_provider,
                    });
            }
        }

        Ok(())
    }

    fn on_sealed_log(&mut self, log_id: LogId, segment_index: SegmentIndex, seal_lsn: Lsn) {
        if let Some(logs_state) = self.logs_state.get_mut(&log_id) {
            logs_state.try_seal(segment_index, seal_lsn);
        }
    }

    fn clear(&mut self) {
        self.logs_commit_in_progress = None;
        self.logs_state.clear();
    }
}

pub struct LogsController {
    effects: Option<Vec<Effect>>,
    inner: Inner,
    metadata: Metadata,
    bifrost: Bifrost,
    metadata_store_client: MetadataStoreClient,
    metadata_writer: MetadataWriter,
    async_operations: JoinSet<Event>,
}

impl LogsController {
    pub fn new(
        metadata: Metadata,
        bifrost: Bifrost,
        metadata_store_client: MetadataStoreClient,
        metadata_writer: MetadataWriter,
    ) -> anyhow::Result<Self> {
        // todo get rid of clone by using Arc<Logs>
        let logs = metadata.logs_snapshot().deref().clone();
        Ok(Self {
            effects: Some(Vec::new()),
            inner: Inner::new(logs, metadata.partition_table_ref().as_ref())?,
            metadata,
            bifrost,
            metadata_store_client,
            metadata_writer,
            async_operations: JoinSet::default(),
        })
    }

    pub fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
    ) -> Result<(), anyhow::Error> {
        self.inner.on_observed_cluster_state(
            observed_cluster_state,
            self.effects.as_mut().expect("to be present"),
        )?;
        self.apply_effects();

        Ok(())
    }

    fn apply_effects(&mut self) {
        let mut effects = self.effects.take().expect("to be present");
        for effect in effects.drain(..) {
            match effect {
                Effect::CommitLogs {
                    logs,
                    expected_version,
                } => {
                    self.commit_logs(expected_version, logs);
                }
                Effect::Seal {
                    log_id,
                    segment_index,
                } => {
                    self.seal_log(log_id, segment_index);
                }
            }
        }

        self.effects = Some(effects);
    }

    fn commit_logs(&mut self, expected_version: Version, logs: Logs) {
        let tc = task_center().clone();
        let metadata_store_client = self.metadata_store_client.clone();
        let metadata_writer = self.metadata_writer.clone();
        self.async_operations.spawn_local(async move {
            tc.run_in_scope("commit-logs", None, async {
                if let Err(err) = metadata_store_client
                    .put(
                        BIFROST_CONFIG_KEY.clone(),
                        &logs,
                        Precondition::MatchesVersion(expected_version),
                    )
                    .await
                {
                    return match err {
                        WriteError::FailedPrecondition(_) => {
                            // There was a concurrent modification of the logs. Fetch the latest version.
                            match metadata_store_client
                                .get::<Logs>(BIFROST_CONFIG_KEY.clone())
                                .await
                            {
                                Ok(result) => {
                                    let logs = result.expect("should be present");
                                    // we are only failing if we are shutting down
                                    let _ = metadata_writer.update(logs.clone()).await;
                                    Event::NewLogs(logs)
                                }
                                Err(err) => {
                                    debug!("failed committing new logs: {err}");
                                    Event::LogsCommitFailed {
                                        logs,
                                        expected_version,
                                    }
                                }
                            }
                        }
                        err => {
                            debug!("failed committing new logs: {err}");
                            Event::LogsCommitFailed {
                                logs,
                                expected_version,
                            }
                        }
                    };
                }

                let version = logs.version();
                // we are only failing if we are shutting down
                let _ = metadata_writer.update(logs).await;

                Event::LogsCommitSucceeded(version)
            })
            .await
        });
    }

    fn seal_log(&mut self, log_id: LogId, segment_index: SegmentIndex) {
        let tc = task_center().clone();
        let bifrost = self.bifrost.clone();
        let metadata_store_client = self.metadata_store_client.clone();
        let metadata_writer = self.metadata_writer.clone();
        self.async_operations.spawn_local(async move {
            tc.run_in_scope("seal-log", None, async {
                let bifrost_admin =
                    BifrostAdmin::new(&bifrost, &metadata_writer, &metadata_store_client);

                match bifrost_admin.seal(log_id, segment_index).await {
                    Ok(tail_state) => {
                        if tail_state.is_sealed() {
                            Event::Sealed {
                                log_id,
                                segment_index,
                                seal_lsn: tail_state.offset(),
                            }
                        } else {
                            Event::SealFailed {
                                log_id,
                                segment_index,
                            }
                        }
                    }
                    Err(err) => {
                        debug!("failed to seal loglet: {err}");
                        Event::SealFailed {
                            log_id,
                            segment_index,
                        }
                    }
                }
            })
            .await
        });
    }

    pub async fn run_async_operations(&mut self) -> Result<(), anyhow::Error> {
        loop {
            if self.async_operations.is_empty() {
                futures::future::pending().await
            } else {
                let event = self
                    .async_operations
                    .join_next()
                    .await
                    .expect("should not be empty")?;
                self.inner
                    .on_event(event, self.effects.as_mut().expect("to be present"), self.metadata.partition_table_ref().as_ref())?;
                self.apply_effects();
            }
        }
    }
}
