// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use restate_core::network::Incoming;
use restate_core::{cancellation_watcher, ShutdownError, TaskCenter, TaskHandle, TaskKind};
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::log_server::{Release, Seal, Sealed, Status, Store, StoreFlags, Stored};
use restate_types::replicated_loglet::ReplicatedLogletId;
use restate_types::GenerationalNodeId;

use crate::logstore::{AsyncToken, LogStore};
use crate::metadata::{GlobalTailTracker, LogletState};

/// A loglet worker
///
/// The design of the store flow assumes that sequencer will send records in the order we can
/// accept then (per-connection) to reduce complexity on the log-server side. This means that
/// the log-server does not need to re-order or buffer records in memory.
/// Records will be rejected if:
///   1) Record offset > local tail
///   2) Or, Record offset > known_global_tail
pub struct LogletWorkerHandle {
    store_tx: mpsc::UnboundedSender<Incoming<Store>>,
    release_tx: mpsc::UnboundedSender<Incoming<Release>>,
    seal_tx: mpsc::UnboundedSender<Incoming<Seal>>,
    tc_handle: TaskHandle<()>,
}

impl LogletWorkerHandle {
    pub fn cancel(self) -> TaskHandle<()> {
        self.tc_handle.cancel();
        self.tc_handle
    }

    pub fn enqueue_store(&self, msg: Incoming<Store>) -> Result<(), Incoming<Store>> {
        self.store_tx.send(msg).map_err(|e| e.0)?;
        Ok(())
    }

    pub fn enqueue_release(&self, msg: Incoming<Release>) -> Result<(), Incoming<Release>> {
        self.release_tx.send(msg).map_err(|e| e.0)?;
        Ok(())
    }

    pub fn enqueue_seal(&self, msg: Incoming<Seal>) -> Result<(), Incoming<Seal>> {
        self.seal_tx.send(msg).map_err(|e| e.0)?;
        Ok(())
    }
}

pub struct LogletWorker<S> {
    loglet_id: ReplicatedLogletId,
    log_store: S,
    loglet_state: LogletState,
    global_tail_tracker: GlobalTailTracker,
}

impl<S: LogStore> LogletWorker<S> {
    pub fn start(
        task_center: &TaskCenter,
        loglet_id: ReplicatedLogletId,
        log_store: S,
        loglet_state: LogletState,
        global_tail_tracker: GlobalTailTracker,
    ) -> Result<LogletWorkerHandle, ShutdownError> {
        let writer = Self {
            loglet_id,
            log_store,
            loglet_state,
            global_tail_tracker,
        };

        let (store_tx, store_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::unbounded_channel();
        let (seal_tx, seal_rx) = mpsc::unbounded_channel();
        let tc_handle = task_center.spawn_unmanaged(
            TaskKind::LogletWriter,
            "loglet-worker",
            None,
            writer.run(store_rx, release_rx, seal_rx),
        )?;
        Ok(LogletWorkerHandle {
            store_tx,
            release_tx,
            seal_tx,
            tc_handle,
        })
    }

    async fn run(
        mut self,
        mut store_rx: mpsc::UnboundedReceiver<Incoming<Store>>,
        mut release_rx: mpsc::UnboundedReceiver<Incoming<Release>>,
        mut seal_rx: mpsc::UnboundedReceiver<Incoming<Seal>>,
    ) {
        // The worker is the sole writer to this loglet's local-tail so it's safe to maintain a moving
        // local tail view and serialize changes to logstore as long as we send them in the correct
        // order.
        let mut sealing_in_progress = false;
        let mut staging_local_tail = self.loglet_state.local_tail().offset();
        let mut global_tail_subscriber = self.global_tail_tracker.subscribe();
        let mut known_global_tail = *global_tail_subscriber.borrow_and_update();
        let mut in_flight_stores = FuturesUnordered::new();
        let mut in_flight_network_sends = FuturesUnordered::new();
        let mut waiting_for_seal = FuturesUnordered::new();
        let mut in_flight_seal = std::pin::pin!(OptionFuture::default());
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    // todo: consider a draining shutdown if needed
                    // this might include sending notifications of shutdown to allow graceful
                    // handoff
                    debug!(loglet_id = %self.loglet_id, "Loglet writer shutting down");
                    return;
                }
                Some(_) = in_flight_stores.next() => {}
                // The in-flight seal (if any)
                Some(Ok(_)) = &mut in_flight_seal => {
                    sealing_in_progress = false;
                    self.loglet_state.get_tail_watch().notify_seal();
                    debug!(loglet_id = %self.loglet_id, "Loglet is sealed");
                    in_flight_seal.set(None.into());
                }
                // The set of requests waiting for seal to complete
                Some(_) = waiting_for_seal.next() => {}
                // The set of network sends waiting to complete
                Some(_) = in_flight_network_sends.next() => {}
                // todo: consider removing if no external changes will happen to known_global_tail
                Ok(_) = global_tail_subscriber.changed() => {
                    // makes sure we don't ever see a backward's view
                    known_global_tail = known_global_tail.max(*global_tail_subscriber.borrow_and_update());
                }
                // RELEASE
                Some(msg) = release_rx.recv() => {
                    self.global_tail_tracker.maybe_update(msg.known_global_tail);
                    known_global_tail = known_global_tail.max(msg.known_global_tail);
                }
                Some(msg) = seal_rx.recv() => {
                    // this message might be telling us about a higher `known_global_tail`
                    self.global_tail_tracker.maybe_update(msg.known_global_tail);
                    known_global_tail = known_global_tail.max(msg.known_global_tail);
                    // If we have a seal operation in-flight, we'd want this request to wait for
                    // seal to happen
                    let response = msg.prepare_response(Sealed::empty());
                    let tail_watcher = self.loglet_state.get_tail_watch();
                    waiting_for_seal.push(async move {
                        let seal_watcher = tail_watcher.wait_for_seal();
                        if seal_watcher.await.is_ok() {
                            let msg = Sealed::new(&tail_watcher.get()).with_status(Status::Ok);
                            let response = response.map(|_| msg);
                            // send the response over the network
                            let _ = response.send().await;
                        }
                    });
                    let seal_token = self.process_seal(msg.into_body(), &mut sealing_in_progress).await;
                    if let Some(seal_token) = seal_token {
                        in_flight_seal.set(Some(seal_token).into());
                    }

                }
                // STORE
                Some(msg) = store_rx.recv() => {
                    // this message might be telling us about a higher `known_global_tail`
                    self.global_tail_tracker.maybe_update(msg.known_global_tail);
                    known_global_tail = known_global_tail.max(msg.known_global_tail);
                    let next_ok_offset = std::cmp::max(staging_local_tail, known_global_tail );
                    let response =
                    msg.prepare_response(Stored::empty());
                    let peer = msg.peer();
                    let (status, maybe_store_token) = self.process_store(peer, msg.into_body(), &mut staging_local_tail, next_ok_offset, &sealing_in_progress).await;
                    // if this store is complete, the last committed is updated to this value.
                    let future_last_committed = staging_local_tail;
                    if let Some(store_token) = maybe_store_token {
                        // in-flight store...
                        let local_tail_watch = self.loglet_state.get_tail_watch();
                        in_flight_stores.push(async move {
                            // wait for log store to finish
                            let res = store_token.await;
                            match res {
                                Ok(_) => {
                                    // advance local-tail
                                    local_tail_watch.notify_offset_update(future_last_committed);
                                    // ignoring the error if we couldn't send the response
                                    let msg = Stored::new(&local_tail_watch.get()).with_status(status);
                                    let response = response.map(|_| msg);
                                    // send the response over the network
                                    let _ = response.send().await;
                                }
                                Err(e) => {
                                    // log-store in failsafe mode and cannot process stores anymore.
                                    warn!(?e, "Log-store is in failsafe mode, dropping store");
                                    let response = response.map(|msg| msg.with_status(Status::Disabled));
                                    let _ = response.send().await;
                                }
                            }
                        });
                    } else {
                        // we didn't store, let's respond immediately with status
                        let msg = Stored::new(&self.loglet_state.local_tail()).with_status(status);
                        in_flight_network_sends.push(async move {
                            let response = response.map(|_| msg);
                            // ignore send errors.
                            let _ = response.send().await;
                        });
                    }
                }
            }
        }
    }

    async fn process_store(
        &mut self,
        peer: GenerationalNodeId,
        body: Store,
        staging_local_tail: &mut LogletOffset,
        next_ok_offset: LogletOffset,
        sealing_in_progress: &bool,
    ) -> (Status, Option<AsyncToken>) {
        // Is this a sealed loglet?
        if !body.flags.contains(StoreFlags::IgnoreSeal) && self.loglet_state.is_sealed() {
            return (Status::Sealed, None);
        }

        // even if ignore-seal is set, we must wait for the in-flight seal before we can accept
        // writes.
        if *sealing_in_progress {
            return (Status::Sealing, None);
        }

        // We have been holding this record for too long.
        if body.expired() {
            return (Status::Dropped, None);
        }
        let Some(last_offset) = body.last_offset() else {
            // too many records
            return (Status::Malformed, None);
        };

        // Invalid offset cannot be accepted
        if body.first_offset == LogletOffset::INVALID {
            // invalid offset
            return (Status::Malformed, None);
        };

        // if sequencer is known, reject writes that refer to a different sequencer
        let known_sequencer = self.loglet_state.sequencer();
        if known_sequencer.is_some_and(|s| s != &body.sequencer) {
            return (Status::SequencerMismatch, None);
        }

        // Are we writing an older record than local-tail, this must be from the sequencer.
        if body.first_offset < next_ok_offset && peer != body.sequencer {
            return (Status::SequencerMismatch, None);
        }

        if body.flags.contains(StoreFlags::IgnoreSeal) {
            // store happens anyway.
            todo!("repair stores are not implemented yet")
        }

        if body.first_offset != next_ok_offset {
            // We can only accept writes coming in order. We don't support buffering out-of-order
            // writes.
            debug!(
                loglet_id = %self.loglet_id,
                "Can only accept writes coming in order, next_ok={} msg.first_offset={}",
                next_ok_offset, body.first_offset
            );
            return (Status::OutOfBounds, None);
        }

        let set_sequncer_in_metadata = if known_sequencer.is_none() {
            self.loglet_state.set_sequencer(body.sequencer)
        } else {
            // sequencer is already known, no need to store it in log-store's metadata
            false
        };
        // send store to log-store. Only push-back when log-store's batching capacity is
        // exhausted.
        match self
            .log_store
            .enqueue_store(body, set_sequncer_in_metadata)
            .await
        {
            Ok(store_token) => {
                *staging_local_tail = last_offset.next();
                (Status::Ok, Some(store_token))
            }
            Err(_) => {
                // shutting down. log-store is disabled
                (Status::Disabled, None)
            }
        }
    }

    async fn process_seal(
        &mut self,
        body: Seal,
        sealing_in_progress: &mut bool,
    ) -> Option<AsyncToken> {
        // Is this a sealed loglet?
        if self.loglet_state.is_sealed() {
            *sealing_in_progress = false;
            return None;
        }

        *sealing_in_progress = true;

        match self.log_store.enqueue_seal(body).await {
            Ok(store_token) => Some(store_token),
            Err(_) => {
                // Note that this fail-safe status is in-fact non-recoverable
                // Meanwhile seal-waiters will continue to wait indefinitely.
                //
                // shutting down. log-store is disabled
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::prelude::*;
    use test_log::test;

    use restate_core::network::Connection;
    use restate_core::{TaskCenter, TaskCenterBuilder};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::live::Live;
    use restate_types::logs::Record;
    use restate_types::net::codec::MessageBodyExt;
    use restate_types::net::CURRENT_PROTOCOL_VERSION;
    use restate_types::replicated_loglet::ReplicatedLogletId;

    use crate::metadata::{GlobalTailTrackerMap, LogletStateMap};
    use crate::rocksdb_logstore::{RocksDbLogStore, RocksDbLogStoreBuilder};
    use crate::setup_panic_handler;

    use super::LogletWorker;

    async fn setup() -> Result<(TaskCenter, RocksDbLogStore)> {
        setup_panic_handler();
        let tc = TaskCenterBuilder::default_for_tests().build()?;
        let config = Live::from_value(Configuration::default());
        let common_rocks_opts = config.clone().map(|c| &c.common);
        let log_store = tc
            .run_in_scope("test-setup", None, async {
                RocksDbManager::init(common_rocks_opts);
                // create logstore.
                let builder = RocksDbLogStoreBuilder::create(
                    config.clone().map(|c| &c.log_server).boxed(),
                    config.map(|c| &c.log_server.rocksdb).boxed(),
                )
                .await?;
                let log_store = builder.start(&tc).await?;
                Result::Ok(log_store)
            })
            .await?;
        Ok((tc, log_store))
    }

    #[test(tokio::test(start_paused = true))]
    async fn test_simple_store_flow() -> Result<()> {
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const LOGLET: ReplicatedLogletId = ReplicatedLogletId::new(1);

        let (tc, log_store) = setup().await?;
        let mut loglet_state_map = LogletStateMap::default();
        let global_tail_tracker = GlobalTailTrackerMap::default();
        let (net_tx, mut net_rx) = mpsc::channel(10);
        let connection = Connection::new_fake(SEQUENCER, CURRENT_PROTOCOL_VERSION, net_tx);

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(
            &tc,
            LOGLET,
            log_store,
            loglet_state,
            global_tail_tracker.get_tracker(LOGLET),
        )?;

        let payloads = vec![
            Record::from("a sample record".to_owned()),
            Record::from("another record").to_owned(),
        ];

        // offsets 1, 2
        let msg1 = Store {
            loglet_id: LOGLET,
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            known_global_tail: LogletOffset::INVALID,
            first_offset: LogletOffset::OLDEST,
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        // offsets 3, 4
        let msg2 = Store {
            loglet_id: LOGLET,
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            known_global_tail: LogletOffset::INVALID,
            first_offset: LogletOffset::new(3),
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        let msg1 = Incoming::for_testing(&connection, msg1, None);
        let msg2 = Incoming::for_testing(&connection, msg2, None);
        let msg1_id = msg1.msg_id();
        let msg2_id = msg2.msg_id();

        // pipelined writes
        worker.enqueue_store(msg1).unwrap();
        worker.enqueue_store(msg2).unwrap();
        // wait for response (in test-env, it's safe to assume that responses will arrive in order)
        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), eq(msg1_id));
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));

        // response 2
        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), eq(msg2_id));
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(5)));

        tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;

        Ok(())
    }

    #[test(tokio::test(start_paused = true))]
    async fn test_store_and_seal() -> Result<()> {
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const LOGLET: ReplicatedLogletId = ReplicatedLogletId::new(1);

        let (tc, log_store) = setup().await?;
        let mut loglet_state_map = LogletStateMap::default();
        let global_tail_tracker = GlobalTailTrackerMap::default();
        let (net_tx, mut net_rx) = mpsc::channel(10);
        let connection = Connection::new_fake(SEQUENCER, CURRENT_PROTOCOL_VERSION, net_tx);

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(
            &tc,
            LOGLET,
            log_store,
            loglet_state,
            global_tail_tracker.get_tracker(LOGLET),
        )?;

        let payloads = vec![
            Record::from("a sample record".to_owned()),
            Record::from("another record").to_owned(),
        ];

        // offsets 1, 2
        let msg1 = Store {
            loglet_id: LOGLET,
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            known_global_tail: LogletOffset::INVALID,
            first_offset: LogletOffset::OLDEST,
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        let seal1 = Seal {
            known_global_tail: LogletOffset::INVALID,
            loglet_id: LOGLET,
            sequencer: SEQUENCER,
        };

        let seal2 = Seal {
            known_global_tail: LogletOffset::INVALID,
            loglet_id: LOGLET,
            sequencer: SEQUENCER,
        };

        // offsets 3, 4
        let msg2 = Store {
            loglet_id: LOGLET,
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            known_global_tail: LogletOffset::INVALID,
            first_offset: LogletOffset::new(3),
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        let msg1 = Incoming::for_testing(&connection, msg1, None);
        let seal1 = Incoming::for_testing(&connection, seal1, None);
        let seal2 = Incoming::for_testing(&connection, seal2, None);
        let msg2 = Incoming::for_testing(&connection, msg2, None);
        let msg1_id = msg1.msg_id();
        let seal1_id = seal1.msg_id();
        let seal2_id = seal2.msg_id();
        let msg2_id = msg2.msg_id();

        worker.enqueue_store(msg1).unwrap();
        // first store is successful
        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), eq(msg1_id));
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));
        worker.enqueue_seal(seal1).unwrap();
        // should latch onto existing seal
        worker.enqueue_seal(seal2).unwrap();
        // seal takes precedence, but it gets processed in the background. This store is likely to
        // observe Status::Sealing
        worker.enqueue_store(msg2).unwrap();
        // sealing
        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), eq(msg2_id));
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Sealing));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));
        // seal responses can come at any order, but we'll consume waiters queue before we process
        // store messages.
        // sealed
        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), any!(eq(seal1_id), eq(seal2_id)));
        let sealed: Sealed = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(sealed.status, eq(Status::Ok));
        assert_that!(sealed.local_tail, eq(LogletOffset::new(3)));

        // sealed2
        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), any!(eq(seal1_id), eq(seal2_id)));
        let sealed: Sealed = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(sealed.status, eq(Status::Ok));
        assert_that!(sealed.local_tail, eq(LogletOffset::new(3)));

        // try another store
        let msg3 = Store {
            loglet_id: LOGLET,
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            known_global_tail: LogletOffset::new(3),
            first_offset: LogletOffset::new(3),
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };
        let msg3 = Incoming::for_testing(&connection, msg3, None);
        let msg3_id = msg3.msg_id();
        worker.enqueue_store(msg3).unwrap();
        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), eq(msg3_id));
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Sealed));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));

        tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;

        Ok(())
    }
}
