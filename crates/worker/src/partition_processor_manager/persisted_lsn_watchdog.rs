// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use futures::future::OptionFuture;
use tokio::sync::watch;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use restate_core::cancellation_watcher;
use restate_partition_store::PartitionStoreManager;
use restate_storage_api::fsm_table::ReadOnlyFsmTable;
use restate_storage_api::StorageError;
use restate_types::config::{Configuration, StorageOptions};
use restate_types::identifiers::PartitionId;
use restate_types::live::LiveLoad;
use restate_types::logs::{Lsn, SequenceNumber};

/// Monitors the persisted log lsns and notifies the partition processor manager about it. The
/// current approach requires flushing the memtables to make sure that data has been persisted.
/// An alternative approach could be to register an event listener on flush events and using
/// table properties to retrieve the flushed log lsn. However, this requires that we update our
/// RocksDB binding to expose event listeners and table properties :-(
pub struct PersistedLogLsnWatchdog {
    configuration: Box<dyn LiveLoad<StorageOptions> + Send + Sync + 'static>,
    partition_store_manager: PartitionStoreManager,
    watch_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,
    persisted_lsns: BTreeMap<PartitionId, Lsn>,
    persist_lsn_interval: Option<time::Interval>,
    persist_lsn_threshold: Lsn,
}

impl PersistedLogLsnWatchdog {
    pub fn new(
        mut configuration: impl LiveLoad<StorageOptions> + Send + Sync + 'static,
        partition_store_manager: PartitionStoreManager,
        watch_tx: watch::Sender<BTreeMap<PartitionId, Lsn>>,
    ) -> Self {
        let options = configuration.live_load();

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
        let persist_lsn_interval = options.persist_lsn_interval().map(|duration| {
            let mut interval = time::interval(duration);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        });

        (
            persist_lsn_interval,
            Lsn::from(options.persist_lsn_threshold),
        )
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!("Start running persisted lsn watchdog");

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let mut config_watcher = Configuration::watcher();

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                },
                Some(_) = OptionFuture::from(self.persist_lsn_interval.as_mut().map(|interval| interval.tick())) => {
                     if let Err(err) = self.update_persisted_lsns().await {
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
        let options = self.configuration.live_load();

        (self.persist_lsn_interval, self.persist_lsn_threshold) = Self::create_persist_lsn(options);
    }

    async fn update_persisted_lsns(&mut self) -> Result<(), StorageError> {
        let partition_stores = self
            .partition_store_manager
            .get_all_partition_stores()
            .await;

        let mut new_persisted_lsns = BTreeMap::new();
        let mut modified = false;

        for mut partition_store in partition_stores {
            let partition_id = partition_store.partition_id();

            let applied_lsn = partition_store.get_applied_lsn().await?;

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
                    info!(
                        partition_id = %partition_id,
                        applied_lsn = %applied_lsn,
                        "Flush partition store to persist applied lsn"
                    );
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
    use crate::partition_processor_manager::persisted_lsn_watchdog::PersistedLogLsnWatchdog;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv};
    use restate_partition_store::{OpenMode, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::fsm_table::FsmTable;
    use restate_storage_api::Transaction;
    use restate_types::config::{CommonOptions, RocksDbOptions, StorageOptions};
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::live::Constant;
    use restate_types::logs::{Lsn, SequenceNumber};
    use std::collections::BTreeMap;
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use test_log::test;
    use tokio::sync::watch;
    use tokio::time::Instant;

    #[test(restate_core::test(start_paused = true))]
    async fn persisted_log_lsn_watchdog_detects_applied_lsns() -> anyhow::Result<()> {
        let _node_env = TestCoreEnv::create_with_single_node(1, 1).await;
        let storage_options = StorageOptions::default();
        let rocksdb_options = RocksDbOptions::default();

        RocksDbManager::init(Constant::new(CommonOptions::default()));

        let all_partition_keys = RangeInclusive::new(0, PartitionKey::MAX);
        let partition_store_manager = PartitionStoreManager::create(
            Constant::new(storage_options.clone()).boxed(),
            Constant::new(rocksdb_options.clone()).boxed(),
            &[(PartitionId::MIN, all_partition_keys.clone())],
        )
        .await?;

        let mut partition_store = partition_store_manager
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

        TaskCenter::spawn(TaskKind::Watchdog, "persisted-log-lsn-test", watchdog.run())?;

        assert!(
            tokio::time::timeout(Duration::from_secs(1), watch_rx.changed())
                .await
                .is_err()
        );
        let mut txn = partition_store.transaction();
        let lsn = Lsn::OLDEST + Lsn::from(storage_options.persist_lsn_threshold);
        txn.put_applied_lsn(lsn).await.unwrap();
        txn.commit().await?;

        watch_rx.changed().await?;
        assert_eq!(watch_rx.borrow().get(&PartitionId::MIN), Some(&lsn));
        let persist_lsn_interval: Duration = storage_options
            .persist_lsn_interval()
            .expect("should be enabled");
        assert!(now.elapsed() >= persist_lsn_interval);

        // we are short by one to hit the persist lsn threshold
        let next_lsn = lsn.prev() + Lsn::from(storage_options.persist_lsn_threshold);
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(next_lsn).await.unwrap();
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
        let mut txn = partition_store.transaction();
        txn.put_applied_lsn(next_persisted_lsn).await.unwrap();
        txn.commit().await?;

        watch_rx.changed().await?;
        assert_eq!(
            watch_rx.borrow().get(&PartitionId::MIN),
            Some(&next_persisted_lsn)
        );

        RocksDbManager::get().shutdown().await;

        Ok(())
    }
}
