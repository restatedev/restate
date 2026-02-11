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

use ahash::{HashMap, HashMapExt};
use anyhow::Context;
use tokio::task::JoinSet;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::trace;

use restate_core::network::{BackPressureMode, MessageRouterBuilder, ServiceReceiver};
use restate_core::{TaskCenter, cancellation_token};
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::logs::LogletId;
use restate_types::net::log_server::*;
use restate_types::nodes_config::StorageState;
use restate_types::protobuf::common::LogServerStatus;

use crate::loglet_worker::{LogletWorker, LogletWorkerHandle};
use crate::logstore::LogStore;
use crate::metadata::LogletStateMap;

const DEFAULT_WRITERS_CAPACITY: usize = 128;

type LogletWorkerMap = HashMap<LogletId, LogletWorkerHandle>;

pub struct RequestPump {
    data_svc_rx: ServiceReceiver<LogServerDataService>,
    info_svc_rx: ServiceReceiver<LogServerMetaService>,
}

impl RequestPump {
    pub fn new(
        mut configuration: Live<Configuration>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let config = configuration.live_load();

        // Data service uses a dedicated memory pool with configurable capacity.
        // When memory is exhausted, requests are rejected immediately (load shedding)
        // rather than queuing which could cause unbounded memory growth.
        let data_pool = TaskCenter::with_current(|tc| {
            tc.memory_controller().create_pool(
                "log-server-data",
                config.log_server.data_service_memory_limit,
                |pool| {
                    pool.set_capacity(Configuration::pinned().log_server.data_service_memory_limit);
                },
            )
        });

        // todo(asoli): Consider making this push-back
        let data_svc_rx =
            router_builder.register_service_with_pool(data_pool, BackPressureMode::Lossy);
        // Meta service uses the default shared memory pool.
        let info_svc_rx = router_builder.register_service(BackPressureMode::Lossy);

        Self {
            data_svc_rx,
            info_svc_rx,
        }
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
            data_svc_rx,
            info_svc_rx,
            ..
        } = self;

        let cancel_token = cancellation_token();

        let mut loglet_workers = HashMap::with_capacity(DEFAULT_WRITERS_CAPACITY);

        let mut data_svc_rx = data_svc_rx.start();
        let mut info_svc_rx = info_svc_rx.start();
        health_status.update(LogServerStatus::Ready);

        // We need to dispatch this work to the right loglet worker as quickly as possible
        // to avoid blocking the message handler.
        //
        // We only block on loading loglet state from logstore, if this proves to be a bottle-neck (it
        // shouldn't) then we can offload this operation to a background task.
        loop {
            // Ordered by priority of message types
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    // stop accepting messages
                    drop(data_svc_rx);
                    drop(info_svc_rx);
                    health_status.update(LogServerStatus::Stopping);
                    break;
                }
                Some(op) = info_svc_rx.next() => {
                    // all requests are sorted by sort-code (V2 fabric)
                    // messages without sort-code will be ignored.
                    let Some(sort_code) = op.sort_code() else {
                        trace!("Received log-server message {} without sort-code, ignoring", op.msg_type());
                        continue;
                    };
                    let loglet_id = LogletId::from(sort_code);
                    // find the worker or create one.
                    // enqueue.
                    let worker = Self::find_or_create_worker(
                        loglet_id,
                        &log_store,
                        &state_map,
                        &mut loglet_workers,
                    ).await?;
                    worker.enqueue_info_msg(op);
                }
                Some(op) = data_svc_rx.next() => {
                    // all requests are sorted by sort-code (V2 fabric)
                    // messages without sort-code will be ignored.
                    let Some(sort_code) = op.sort_code() else {
                        trace!("Received log-server message {} without sort-code, ignoring", op.msg_type());
                        continue;
                    };
                    let loglet_id = LogletId::from(sort_code);
                    // find the worker or create one.
                    // enqueue.
                    let worker = Self::find_or_create_worker(
                        loglet_id,
                        &log_store,
                        &state_map,
                        &mut loglet_workers,
                    ).await?;
                    worker.enqueue_data_msg(op);
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
    ) -> anyhow::Result<&'a LogletWorkerHandle> {
        if let hash_map::Entry::Vacant(e) = loglet_workers.entry(loglet_id) {
            let state = state_map
                .get_or_load(loglet_id, log_store)
                .await
                .context("cannot load loglet state map from logstore")?;
            let handle = LogletWorker::start(loglet_id, log_store.clone(), state.clone())?;
            e.insert(handle);
        }

        Ok(loglet_workers.get(&loglet_id).unwrap())
    }
}
