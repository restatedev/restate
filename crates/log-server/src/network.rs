// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use std::collections::{hash_map, HashMap};
use std::pin::Pin;

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::Stream;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{debug, trace};
use xxhash_rust::xxh3::Xxh3Builder;

use restate_core::network::{Incoming, MessageRouterBuilder, NetworkSender};
use restate_core::{cancellation_watcher, Metadata, TaskCenter};
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::net::log_server::{Release, Released, Store, Stored};
use restate_types::nodes_config::StorageState;
use restate_types::replicated_loglet::ReplicatedLogletId;

use crate::loglet_worker::{LogletWorker, LogletWorkerHandle};
use crate::logstore::LogStore;
use crate::metadata::{GlobalTailTrackerMap, LogletStateMap};

const DEFAULT_WRITERS_CAPACITY: usize = 128;

type MessageStream<T> = Pin<Box<dyn Stream<Item = Incoming<T>> + Send + Sync + 'static>>;

type LogletWorkerMap = HashMap<ReplicatedLogletId, LogletWorkerHandle, Xxh3Builder>;

pub struct RequestPump {
    task_center: TaskCenter,
    _metadata: Metadata,
    _configuration: Live<Configuration>,
    store_stream: MessageStream<Store>,
    release_stream: MessageStream<Release>,
}

impl RequestPump {
    pub fn new(
        task_center: TaskCenter,
        metadata: Metadata,
        mut configuration: Live<Configuration>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let queue_length = configuration
            .live_load()
            .log_server
            .incoming_network_queue_length
            .into();
        // We divide requests into two priority categories.
        let store_stream = router_builder.subscribe_to_stream(queue_length);
        let release_stream = router_builder.subscribe_to_stream(queue_length);
        Self {
            task_center,
            _metadata: metadata,
            _configuration: configuration,
            store_stream,
            release_stream,
        }
    }

    /// Starts the main processing loop, exits on error or shutdown.
    pub async fn run<N: NetworkSender + 'static, S>(
        self,
        _networking: N,
        log_store: S,
        _storage_state: StorageState,
    ) -> anyhow::Result<()>
    where
        S: LogStore + Clone + Sync + Send + 'static,
    {
        let RequestPump {
            task_center,
            mut store_stream,
            mut release_stream,
            ..
        } = self;

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let global_tail_tracker = GlobalTailTrackerMap::default();

        // optimization to fetch all known loglets from disk
        let mut state_map = LogletStateMap::load_all(&log_store)
            .await
            .context("cannot load loglet state map from logstore")?;

        let mut loglet_workers =
            HashMap::with_capacity_and_hasher(DEFAULT_WRITERS_CAPACITY, Xxh3Builder::default());

        // We need to dispatch this work to the right loglet worker as quickly as possible
        // to avoid blocking the message handler.
        //
        // We only block on loading loglet state from logstore, if this proves to be a bottle-neck (it
        // shouldn't) then we can offload this operation to a background task.
        loop {
            // Ordered by priority of message types
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    // stop accepting messages
                    drop(store_stream);
                    drop(release_stream);
                    // shutdown all workers.
                    Self::shutdown(loglet_workers).await;
                    return Ok(());
                }
                Some(release) = release_stream.next() => {
                    // find the worker or create one.
                    // enqueue.
                    let worker = Self::find_or_create_worker(
                        release.loglet_id,
                        &log_store,
                        &task_center,
                        &global_tail_tracker,
                        &mut state_map,
                        &mut loglet_workers,
                    ).await?;
                    Self::on_release(worker, release);
                }
                Some(store) = store_stream.next() => {
                    // find the worker or create one.
                    // enqueue.
                    let worker = Self::find_or_create_worker(
                        store.loglet_id,
                        &log_store,
                        &task_center,
                        &global_tail_tracker,
                        &mut state_map,
                        &mut loglet_workers,
                    ).await?;
                    Self::on_store(worker, store);
                }
            }
        }
    }

    pub async fn shutdown(loglet_workers: LogletWorkerMap) {
        // stop all writers
        let mut tasks = FuturesUnordered::new();
        for (_, task) in loglet_workers {
            tasks.push(task.cancel());
        }
        // await all tasks to shutdown
        while tasks.next().await.is_some() {}
        trace!("All loglet workers have terminated");
    }

    fn on_store(worker: &LogletWorkerHandle, msg: Incoming<Store>) {
        if let Err(msg) = worker.enqueue_store(msg) {
            // worker has crashed or shutdown in progress. Notify the sender and drop the message.
            if let Err(e) = msg.try_respond_rpc(Stored::empty()) {
                debug!(?e.source, peer = %msg.peer(), "Failed to respond to Store message with status Disabled due to peer channel capacity being full");
            }
        }
    }

    fn on_release(worker: &LogletWorkerHandle, msg: Incoming<Release>) {
        if let Err(msg) = worker.enqueue_release(msg) {
            // worker has crashed or shutdown in progress. Notify the sender and drop the message.
            if let Err(e) = msg.try_respond_rpc(Released::empty()) {
                debug!(?e.source, peer = %msg.peer(), "Failed to respond to Release message with status Disabled due to peer channel capacity being full");
            }
        }
    }

    async fn find_or_create_worker<'a, S: LogStore>(
        loglet_id: ReplicatedLogletId,
        log_store: &S,
        task_center: &TaskCenter,
        global_tail_tracker: &GlobalTailTrackerMap,
        state_map: &mut LogletStateMap,
        loglet_workers: &'a mut LogletWorkerMap,
    ) -> anyhow::Result<&'a LogletWorkerHandle> {
        if let hash_map::Entry::Vacant(e) = loglet_workers.entry(loglet_id) {
            let state = state_map
                .get_or_load(loglet_id, log_store)
                .await
                .context("cannot load loglet state map from logstore")?;
            let handle = LogletWorker::start(
                task_center,
                loglet_id,
                log_store.clone(),
                state.clone(),
                global_tail_tracker.get_tracker(loglet_id),
            )?;
            e.insert(handle);
        }

        Ok(loglet_workers.get(&loglet_id).unwrap())
    }
}
