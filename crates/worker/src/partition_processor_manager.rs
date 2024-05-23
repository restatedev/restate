// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::time::Duration;

use anyhow::Context;
use futures::future::OptionFuture;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use restate_core::network::NetworkSender;
use restate_core::TaskCenter;
use restate_network::rpc_router::{RpcError, RpcRouter};
use restate_node_protocol::partition_processor_manager::GetProcessorsState;
use restate_node_protocol::partition_processor_manager::ProcessorsStateResponse;
use restate_node_protocol::RpcMessage;
use restate_types::processors::{PartitionProcessorStatus, RunMode};
use tokio::sync::{mpsc, watch};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};

use restate_bifrost::Bifrost;
use restate_core::network::MessageRouterBuilder;
use restate_core::worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};
use restate_core::{cancellation_watcher, Metadata, ShutdownError, TaskId, TaskKind};
use restate_invoker_impl::InvokerHandle;
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_network::Networking;
use restate_node_protocol::cluster_controller::AttachRequest;
use restate_node_protocol::cluster_controller::{Action, AttachResponse};
use restate_node_protocol::MessageEnvelope;
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_types::arc_util::{ArcSwapExt, Updateable};
use restate_types::config::{
    Configuration, StorageOptions, UpdateableConfiguration, WorkerOptions,
};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::logs::{LogId, Lsn, Payload, SequenceNumber};
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command as WalCommand, Destination, Envelope, Header, Source};

use crate::partition::storage::invoker::InvokerStorageReader;
use crate::partition::storage::PartitionStorage;
use crate::partition::PartitionProcessorControlCommand;
use crate::PartitionProcessor;

pub struct PartitionProcessorManager {
    task_center: TaskCenter,
    updateable_config: UpdateableConfiguration,
    running_partition_processors: BTreeMap<PartitionId, State>,

    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    partition_store_manager: PartitionStoreManager,
    attach_router: RpcRouter<AttachRequest, Networking>,
    incoming_get_state: BoxStream<'static, MessageEnvelope<GetProcessorsState>>,
    networking: Networking,
    bifrost: Bifrost,
    invoker_handle: InvokerHandle<InvokerStorageReader<PartitionStore>>,
    rx: mpsc::Receiver<ProcessorsManagerCommand>,
    tx: mpsc::Sender<ProcessorsManagerCommand>,
    latest_attach_response: Option<(GenerationalNodeId, AttachResponse)>,

    persisted_lsns_rx: Option<watch::Receiver<BTreeMap<PartitionId, Lsn>>>,
}

#[derive(Debug, thiserror::Error)]
enum AttachError {
    #[error("No cluster controller found in nodes configuration")]
    NoClusterController,
    #[error(transparent)]
    ShutdownError(#[from] ShutdownError),
}

struct State {
    _created_at: MillisSinceEpoch,
    _key_range: RangeInclusive<PartitionKey>,
    _control_tx: mpsc::Sender<PartitionProcessorControlCommand>,
    watch_rx: watch::Receiver<PartitionProcessorStatus>,
    _task_id: TaskId,
}

impl PartitionProcessorManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_center: TaskCenter,
        updateable_config: UpdateableConfiguration,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        partition_store_manager: PartitionStoreManager,
        router_builder: &mut MessageRouterBuilder,
        networking: Networking,
        bifrost: Bifrost,
        invoker_handle: InvokerHandle<InvokerStorageReader<PartitionStore>>,
    ) -> Self {
        let attach_router = RpcRouter::new(networking.clone(), router_builder);
        let incoming_get_state = router_builder.subscribe_to_stream(2);

        let (tx, rx) = mpsc::channel(updateable_config.load().worker.internal_queue_length());
        Self {
            task_center,
            updateable_config,
            running_partition_processors: BTreeMap::default(),
            metadata,
            metadata_store_client,
            partition_store_manager,
            incoming_get_state,
            networking,
            bifrost,
            invoker_handle,
            attach_router,
            rx,
            tx,
            latest_attach_response: None,
            persisted_lsns_rx: None,
        }
    }

    pub fn handle(&self) -> ProcessorsManagerHandle {
        ProcessorsManagerHandle::new(self.tx.clone())
    }

    async fn attach(&mut self) -> Result<MessageEnvelope<AttachResponse>, AttachError> {
        loop {
            // We try to get the admin node on every retry since it might change between retries.
            let admin_node = self
                .metadata
                .nodes_config()
                .get_admin_node()
                .ok_or(AttachError::NoClusterController)?
                .current_generation;

            info!(
                "Attempting to attach to cluster controller '{}'",
                admin_node
            );
            if admin_node == self.metadata.my_node_id() {
                // If this node is running the cluster controller, we need to wait a little to give cluster
                // controller time to start up. This is only done to reduce the chances of observing
                // connection errors in log. Such logs are benign since we retry, but it's still not nice
                // to print, specially in a single-node setup.
                info!( "This node is the cluster controller, giving cluster controller service 500ms to start");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            match self
                .attach_router
                .call(admin_node.into(), &AttachRequest::default())
                .await
            {
                Ok(response) => return Ok(response),
                Err(RpcError::Shutdown(e)) => return Err(AttachError::ShutdownError(e)),
                Err(e) => {
                    warn!(
                        "Failed to send attach message to cluster controller: {}, retrying....",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        // Initial attach
        let response = tokio::time::timeout(Duration::from_secs(5), self.attach())
            .await
            .context("Timeout waiting to attach to a cluster controller")??;

        let (from, msg) = response.split();
        // We ignore errors due to shutdown
        let _ = self.apply_plan(&msg.actions);
        self.latest_attach_response = Some((from, msg));
        info!("Plan applied from attaching to controller {}", from);

        let (persisted_lsns_tx, persisted_lsns_rx) = watch::channel(BTreeMap::default());
        self.persisted_lsns_rx = Some(persisted_lsns_rx);

        let watchdog = PersistedLogLsnWatchdog::new(
            self.updateable_config
                .clone()
                .map_as_updateable_owned(|config| &config.worker.storage),
            self.partition_store_manager.clone(),
            persisted_lsns_tx,
        );
        self.task_center.spawn_child(
            TaskKind::Watchdog,
            "persisted-lsn-watchdog",
            None,
            watchdog.run(),
        )?;

        loop {
            tokio::select! {
                Some(command) = self.rx.recv() => {
                    self.on_command(command);
                    debug!("PartitionProcessorManager shutting down");
                }
                Some(get_state) = self.incoming_get_state.next() => {
                    self.on_get_state(get_state);
                }
              _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    fn on_get_state(&self, get_state_msg: MessageEnvelope<GetProcessorsState>) {
        let (from, msg) = get_state_msg.split();
        let persisted_lsns = self.persisted_lsns_rx.as_ref().map(|w| w.borrow());

        // For all running partitions, collect state, enrich it, and send it back.
        let state: BTreeMap<PartitionId, PartitionProcessorStatus> = self
            .running_partition_processors
            .iter()
            .map(|(partition_id, state)| {
                let mut status = state.watch_rx.borrow().clone();
                status.last_persisted_log_lsn = persisted_lsns
                    .as_ref()
                    .and_then(|lsns| lsns.get(partition_id).cloned());
                (*partition_id, status)
            })
            .collect();

        let response = ProcessorsStateResponse {
            request_id: msg.correlation_id(),
            state,
        };
        let networking = self.networking.clone();
        // ignore shutdown errors.
        let _ = self.task_center.spawn(
            restate_core::TaskKind::Disposable,
            "get-processors-state-response",
            None,
            async move { Ok(networking.send(from.into(), &response).await?) },
        );
    }

    fn on_command(&mut self, command: ProcessorsManagerCommand) {
        use ProcessorsManagerCommand::*;
        match command {
            GetLivePartitions(sender) => {
                let live_partitions = self.running_partition_processors.keys().cloned().collect();
                let _ = sender.send(live_partitions);
            }
        }
    }

    pub fn apply_plan(&mut self, actions: &[Action]) -> Result<(), ShutdownError> {
        let config = self.updateable_config.pinned();
        let options = &config.worker;

        for action in actions {
            match action {
                Action::RunPartition(action) => {
                    #[allow(clippy::map_entry)]
                    if !self
                        .running_partition_processors
                        .contains_key(&action.partition_id)
                    {
                        let (control_tx, control_rx) = mpsc::channel(2);
                        let status = PartitionProcessorStatus::new(action.mode);
                        let (watch_tx, watch_rx) = watch::channel(status.clone());

                        let _task_id = self.spawn_partition_processor(
                            options,
                            action.partition_id,
                            action.key_range_inclusive.clone().into(),
                            status,
                            control_rx,
                            watch_tx,
                        )?;
                        let state = State {
                            _created_at: MillisSinceEpoch::now(),
                            _key_range: action.key_range_inclusive.clone().into(),
                            _task_id,
                            _control_tx: control_tx,
                            watch_rx,
                        };
                        self.running_partition_processors
                            .insert(action.partition_id, state);
                    } else {
                        debug!(
                            "Partition processor for partition id '{}' is already running.",
                            action.partition_id
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn spawn_partition_processor(
        &self,
        options: &WorkerOptions,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
        status: PartitionProcessorStatus,
        control_rx: mpsc::Receiver<PartitionProcessorControlCommand>,
        watch_tx: watch::Sender<PartitionProcessorStatus>,
    ) -> Result<TaskId, ShutdownError> {
        let planned_mode = status.planned_mode;
        let processor = PartitionProcessor::new(
            partition_id,
            key_range.clone(),
            status,
            options.num_timers_in_memory_limit(),
            options.internal_queue_length(),
            control_rx,
            watch_tx,
            self.invoker_handle.clone(),
        );
        let networking = self.networking.clone();
        let mut bifrost = self.bifrost.clone();
        let metadata_store_client = self.metadata_store_client.clone();
        let node_id = self.metadata.my_node_id();

        self.task_center.spawn_child(
            TaskKind::PartitionProcessor,
            "partition-processor",
            Some(processor.partition_id),
            {
                let storage_manager = self.partition_store_manager.clone();
                let options = options.clone();
                async move {
                    let partition_store = storage_manager
                        .open_partition_store(
                            partition_id,
                            key_range.clone(),
                            OpenMode::CreateIfMissing,
                            &options.storage.rocksdb,
                        )
                        .await?;

                    if planned_mode == RunMode::Leader {
                        Self::claim_leadership(
                            &mut bifrost,
                            metadata_store_client,
                            partition_id,
                            key_range,
                            node_id,
                        )
                        .await?;
                    }

                    processor.run(networking, bifrost, partition_store).await
                }
            },
        )
    }

    async fn claim_leadership(
        bifrost: &mut Bifrost,
        metadata_store_client: MetadataStoreClient,
        partition_id: PartitionId,
        partition_range: RangeInclusive<PartitionKey>,
        node_id: GenerationalNodeId,
    ) -> anyhow::Result<()> {
        let leader_epoch =
            Self::obtain_next_epoch(metadata_store_client, partition_id, node_id).await?;

        Self::announce_leadership(
            bifrost,
            node_id,
            partition_id,
            partition_range,
            leader_epoch,
        )
        .await?;

        Ok(())
    }

    async fn obtain_next_epoch(
        metadata_store_client: MetadataStoreClient,
        partition_id: PartitionId,
        node_id: GenerationalNodeId,
    ) -> Result<LeaderEpoch, ReadModifyWriteError> {
        let epoch: EpochMetadata = metadata_store_client
            .read_modify_write(partition_processor_epoch_key(partition_id), |epoch| {
                let next_epoch = epoch
                    .map(|epoch: EpochMetadata| epoch.claim_leadership(node_id, partition_id))
                    .unwrap_or_else(|| EpochMetadata::new(node_id, partition_id));

                Ok(next_epoch)
            })
            .await?;
        Ok(epoch.epoch())
    }

    async fn announce_leadership(
        bifrost: &mut Bifrost,
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        leader_epoch: LeaderEpoch,
    ) -> anyhow::Result<()> {
        let header = Header {
            dest: Destination::Processor {
                partition_key: *partition_key_range.start(),
                dedup: None,
            },
            source: Source::ControlPlane {},
        };

        let envelope = Envelope::new(
            header,
            WalCommand::AnnounceLeader(AnnounceLeader {
                node_id,
                leader_epoch,
            }),
        );
        let payload = Payload::new(envelope.to_bytes()?);

        bifrost
            .append(LogId::from(partition_id), payload)
            .await
            .context("failed to write AnnounceLeader record to bifrost")?;

        Ok(())
    }
}

/// Monitors the persisted log lsns and notifies the partition processor manager about it. The
/// current approach requires flushing the memtables to make sure that data has been persisted.
/// An alternative approach could be to register an event listener on flush events and using
/// table properties to retrieve the flushed log lsn. However, this requires that we update our
/// RocksDB binding to expose event listeners and table properties :-(
struct PersistedLogLsnWatchdog {
    configuration: Box<dyn Updateable<StorageOptions> + Send + Sync + 'static>,
    partition_store_manager: PartitionStoreManager,
    watch_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,
    persisted_lsns: BTreeMap<PartitionId, Lsn>,
    persist_lsn_interval: Option<time::Interval>,
    persist_lsn_threshold: Lsn,
}

impl PersistedLogLsnWatchdog {
    fn new(
        mut configuration: impl Updateable<StorageOptions> + Send + Sync + 'static,
        partition_store_manager: PartitionStoreManager,
        watch_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,
    ) -> Self {
        let options = configuration.load();

        let (persist_lsn_interval, persist_lsn_threshold) = Self::create_persist_lsn(options);

        PersistedLogLsnWatchdog {
            configuration: Box::new(configuration),
            partition_store_manager,
            watch_tx,
            persisted_lsns: BTreeMap::default(),
            persist_lsn_interval,
            persist_lsn_threshold,
        }
    }

    fn create_persist_lsn(options: &StorageOptions) -> (Option<time::Interval>, Lsn) {
        let persist_lsn_interval = options.persist_lsn_interval.map(|duration| {
            let mut interval = time::interval(duration.into());
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        });

        let persist_lsn_threshold = Lsn::from(options.persist_lsn_threshold);

        (persist_lsn_interval, persist_lsn_threshold)
    }

    async fn run(mut self) -> anyhow::Result<()> {
        debug!("Start running persisted lsn watchdog");

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let mut config_watcher = Configuration::watcher();

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                },
                _ = OptionFuture::from(self.persist_lsn_interval.as_mut().map(|interval| interval.tick())) => {
                    let result = self.update_persisted_lsns().await;

                    if let Err(err) = result {
                        warn!("Failed updating the persisted applied lsns. This might prevent the log from being trimmed: {err}");
                    }
                }
                _ = config_watcher.changed() => {
                    self.on_config_update();
                }
            }
        }

        debug!("Stop persisted lsn watchdog");
        Ok(())
    }

    fn on_config_update(&mut self) {
        debug!("Updating the persisted log lsn watchdog");
        let options = self.configuration.load();

        (self.persist_lsn_interval, self.persist_lsn_threshold) = Self::create_persist_lsn(options);
    }

    async fn update_persisted_lsns(&mut self) -> Result<(), StorageError> {
        let partition_stores = self
            .partition_store_manager
            .get_all_partition_stores()
            .await;

        let mut new_persisted_lsns = BTreeMap::new();
        let mut modified = false;

        for partition_store in partition_stores {
            let partition_id = partition_store.partition_id();
            let mut partition_storage = PartitionStorage::from(partition_store);

            let applied_lsn = partition_storage.load_applied_lsn().await?;

            if let Some(applied_lsn) = applied_lsn {
                let previously_applied_lsn = self
                    .persisted_lsns
                    .get(&partition_id)
                    .cloned()
                    .unwrap_or(Lsn::INVALID);

                // only flush if there was some activity compared to the last check
                if applied_lsn >= previously_applied_lsn + self.persist_lsn_threshold {
                    // since we cannot be sure that we have read the applied lsn from disk, we need
                    // to flush the memtables to be sure that it is persisted
                    trace!(partition_id = %partition_id, "Flush partition store to persist applied lsn");
                    let partition_store = partition_storage.into_inner();
                    partition_store.flush_memtables(true).await?;
                    new_persisted_lsns.insert(partition_id, applied_lsn);
                    modified = true;
                } else {
                    new_persisted_lsns.insert(partition_id, previously_applied_lsn);
                }
            }
        }

        if modified {
            self.persisted_lsns = new_persisted_lsns.clone();
            // ignore send failures which should only occur during shutdown
            let _ = self.watch_tx.send(new_persisted_lsns);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::storage::PartitionStorage;
    use crate::partition_processor_manager::PersistedLogLsnWatchdog;
    use restate_core::{TaskKind, TestCoreEnv};
    use restate_partition_store::{OpenMode, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_types::arc_util::Constant;
    use restate_types::config::{CommonOptions, RocksDbOptions, StorageOptions};
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::logs::{Lsn, SequenceNumber};
    use std::collections::BTreeMap;
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use test_log::test;
    use tokio::sync::watch;
    use tokio::time::Instant;

    #[test(tokio::test(start_paused = true))]
    async fn persisted_log_lsn_watchdog_detects_applied_lsns() -> anyhow::Result<()> {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let storage_options = StorageOptions::default();
        let rocksdb_options = RocksDbOptions::default();

        node_env.tc.run_in_scope_sync("db-manager-init", None, || {
            RocksDbManager::init(Constant::new(CommonOptions::default()))
        });

        let all_partition_keys = RangeInclusive::new(0, PartitionKey::MAX);
        let partition_store_manager = PartitionStoreManager::create(
            Constant::new(storage_options.clone()),
            Constant::new(rocksdb_options.clone()),
            &[(PartitionId::MIN, all_partition_keys.clone())],
        )
        .await?;

        let partition_store = partition_store_manager
            .open_partition_store(
                PartitionId::MIN,
                all_partition_keys,
                OpenMode::CreateIfMissing,
                &rocksdb_options,
            )
            .await
            .expect("partition store present");

        let (watch_tx, mut watch_rx) = watch::channel(BTreeMap::default());

        let watchdog = PersistedLogLsnWatchdog::new(
            Constant::new(storage_options.clone()),
            partition_store_manager.clone(),
            watch_tx,
        );

        let now = Instant::now();

        node_env.tc.spawn(
            TaskKind::Watchdog,
            "persiste-log-lsn-test",
            None,
            watchdog.run(),
        )?;

        assert!(
            tokio::time::timeout(Duration::from_secs(1), watch_rx.changed())
                .await
                .is_err()
        );
        let mut partition_storage = PartitionStorage::from(partition_store);
        let mut txn = partition_storage.create_transaction();
        let lsn = Lsn::OLDEST + Lsn::from(storage_options.persist_lsn_threshold);
        txn.store_applied_lsn(lsn).await?;
        txn.commit().await?;

        watch_rx.changed().await?;
        assert_eq!(watch_rx.borrow().get(&PartitionId::MIN), Some(&lsn));
        let persist_lsn_interval: Duration = storage_options
            .persist_lsn_interval
            .expect("should be enabled")
            .into();
        assert!(now.elapsed() >= persist_lsn_interval);

        // we are short by one to hit the persist lsn threshold
        let next_lsn = lsn.prev() + Lsn::from(storage_options.persist_lsn_threshold);
        let mut txn = partition_storage.create_transaction();
        txn.store_applied_lsn(next_lsn).await?;
        txn.commit().await?;

        // await the persist lsn interval so that we have a chance to see the update
        tokio::time::sleep(persist_lsn_interval).await;

        // we should not receive a new notification because we haven't reached the threshold yet
        assert!(
            tokio::time::timeout(Duration::from_secs(1), watch_rx.changed())
                .await
                .is_err()
        );

        let next_persisted_lsn = next_lsn + Lsn::from(1);
        let mut txn = partition_storage.create_transaction();
        txn.store_applied_lsn(next_persisted_lsn).await?;
        txn.commit().await?;

        watch_rx.changed().await?;
        assert_eq!(
            watch_rx.borrow().get(&PartitionId::MIN),
            Some(&next_persisted_lsn)
        );

        Ok(())
    }
}
