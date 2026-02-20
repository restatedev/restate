// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Network processing for log-server
//!
//! We maintain a stream per message type for fine-grain per-message-type control over the queue
//! depth, head-of-line blocking issues, and priority of consumption.
//!
//! Both data and metadata services use memory-metered channels for admission control.
//! When memory limits are exceeded, incoming messages are rejected with load shedding
//! rather than blocking the sender. This prevents memory ballooning when downstream
//! processing (e.g., RocksDB writes) is stalling.
use std::collections::hash_map;

use ahash::HashMap;
use anyhow::Context;
use tokio::task::JoinSet;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::trace;

use restate_core::network::{
    BackPressureMode, ControlServiceShards, MessageRouterBuilder, ShardControlMessage, Sharded,
};
use restate_core::{TaskCenter, cancellation_token};
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::logs::LogletId;
use restate_types::net::log_server::*;
use restate_types::nodes_config::StorageState;
use restate_types::protobuf::common::LogServerStatus;

use crate::loglet_worker::{LogletWorker, LogletWorkerHandle};
use crate::logstore::LogStore;
use crate::metadata::LogletStateMap;

type LogletWorkerMap = HashMap<LogletId, LogletWorkerHandle>;

pub struct RequestPump {
    data_svc: Sharded<LogServerDataService>,
    meta_svc: Sharded<LogServerMetaService>,
}

impl RequestPump {
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        // Data service uses a dedicated memory pool with configurable capacity.
        // When memory is exhausted, requests are rejected immediately (load shedding)
        // rather than queuing which could cause unbounded memory growth.
        let data_pool = TaskCenter::with_current(|tc| {
            tc.memory_controller().create_pool("log-server-data", || {
                Configuration::pinned().log_server.data_service_memory_limit
            })
        });

        let data_svc = router_builder
            .register_sharded_service_with_pool(data_pool, BackPressureMode::PushBack);
        // Meta service uses the default shared memory pool.
        let meta_svc = router_builder.register_sharded_service(BackPressureMode::Lossy);

        Self { data_svc, meta_svc }
    }

    /// Starts the main processing loop, exits on error or shutdown.
    pub async fn run<S>(
        self,
        health_status: HealthStatus<LogServerStatus>,
        log_store: S,
        state_map: LogletStateMap,
        _storage_state: StorageState,
    ) -> anyhow::Result<()>
    where
        S: LogStore + Clone + Sync + Send + 'static,
    {
        let RequestPump {
            data_svc, meta_svc, ..
        } = self;

        let cancel_token = cancellation_token();

        let mut loglet_workers = HashMap::default();

        let (mut data_svc, data_shards) = data_svc.start();
        let (mut meta_svc, meta_shards) = meta_svc.start();
        health_status.update(LogServerStatus::Ready);

        // We only block on loading loglet state from logstore, if this proves to be a bottle-neck (it
        // shouldn't) then we can offload this operation to a background task.
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    // stop accepting messages
                    drop(data_svc);
                    drop(meta_svc);
                    health_status.update(LogServerStatus::Stopping);
                    break;
                }
                Some(ShardControlMessage::RegisterSortCode {sort_code, decision}) = meta_svc.next() => {
                    let loglet_id = LogletId::from(sort_code);
                    trace!("Register shard on info svc for loglet={loglet_id}");
                    // find the worker or create one.
                    let handle = Self::find_or_create_worker(
                        loglet_id,
                        &log_store,
                        &state_map,
                        &mut loglet_workers,
                        &data_shards,
                        &meta_shards,
                    ).await?;
                    decision.accept(handle.meta_tx());
                }
                Some(ShardControlMessage::RegisterSortCode {sort_code, decision}) = data_svc.next() => {
                    let loglet_id = LogletId::from(sort_code);
                    trace!("Register shard on data svc for loglet={loglet_id}");
                    // find the worker or create one.
                    let handle = Self::find_or_create_worker(
                        loglet_id,
                        &log_store,
                        &state_map,
                        &mut loglet_workers,
                        &data_shards,
                        &meta_shards,
                    ).await?;
                    decision.accept(handle.data_tx());
                }
            }
        }

        // shutdown all workers.
        Self::shutdown(loglet_workers).await;
        health_status.update(LogServerStatus::Unknown);
        Ok(())
    }

    pub async fn shutdown(loglet_workers: LogletWorkerMap) {
        // stop all writers
        let mut tasks = JoinSet::new();
        for (_, task) in loglet_workers {
            tasks.spawn(task.drain());
        }
        // await all tasks to shutdown
        let _ = tasks.join_all().await;
        trace!("All loglet workers have terminated");
    }

    async fn find_or_create_worker<'a, S: LogStore>(
        loglet_id: LogletId,
        log_store: &S,
        state_map: &LogletStateMap,
        loglet_workers: &'a mut LogletWorkerMap,
        data_shards: &'a ControlServiceShards<LogServerDataService>,
        meta_shards: &'a ControlServiceShards<LogServerMetaService>,
    ) -> anyhow::Result<&'a LogletWorkerHandle> {
        if let hash_map::Entry::Vacant(e) = loglet_workers.entry(loglet_id) {
            let state = state_map
                .get_or_load(loglet_id, log_store)
                .await
                .context("cannot load loglet state map from logstore")?;
            let handle = LogletWorker::start(loglet_id, log_store.clone(), state.clone())?;

            data_shards.force_register_sort_code(loglet_id.into(), handle.data_tx());
            meta_shards.force_register_sort_code(loglet_id.into(), handle.meta_tx());

            e.insert(handle);
        }
        Ok(loglet_workers.get(&loglet_id).unwrap())
    }
}
