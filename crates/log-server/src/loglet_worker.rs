// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use tracing::{debug, trace, trace_span, warn, Instrument};

use restate_core::network::Incoming;
use restate_core::{cancellation_watcher, ShutdownError, TaskCenter, TaskHandle, TaskKind};
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::log_server::*;
use restate_types::replicated_loglet::ReplicatedLogletId;
use restate_types::GenerationalNodeId;

use crate::logstore::{AsyncToken, LogStore};
use crate::metadata::LogletState;

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
    get_loglet_info_tx: mpsc::UnboundedSender<Incoming<GetLogletInfo>>,
    get_records_tx: mpsc::UnboundedSender<Incoming<GetRecords>>,
    trim_tx: mpsc::UnboundedSender<Incoming<Trim>>,
    wait_for_tail_tx: mpsc::UnboundedSender<Incoming<WaitForTail>>,
    get_digest_tx: mpsc::UnboundedSender<Incoming<GetDigest>>,
    tc_handle: TaskHandle<()>,
}

impl LogletWorkerHandle {
    pub fn cancel(self) -> TaskHandle<()> {
        self.tc_handle.cancel();
        self.tc_handle
    }

    pub fn enqueue_get_digest(&self, msg: Incoming<GetDigest>) -> Result<(), Incoming<GetDigest>> {
        self.get_digest_tx.send(msg).map_err(|e| e.0)
    }

    pub fn enqueue_wait_for_tail(
        &self,
        msg: Incoming<WaitForTail>,
    ) -> Result<(), Incoming<WaitForTail>> {
        self.wait_for_tail_tx.send(msg).map_err(|e| e.0)
    }

    pub fn enqueue_store(&self, msg: Incoming<Store>) -> Result<(), Incoming<Store>> {
        self.store_tx.send(msg).map_err(|e| e.0)
    }

    pub fn enqueue_release(&self, msg: Incoming<Release>) -> Result<(), Incoming<Release>> {
        self.release_tx.send(msg).map_err(|e| e.0)
    }

    pub fn enqueue_seal(&self, msg: Incoming<Seal>) -> Result<(), Incoming<Seal>> {
        self.seal_tx.send(msg).map_err(|e| e.0)
    }

    pub fn enqueue_get_loglet_info(
        &self,
        msg: Incoming<GetLogletInfo>,
    ) -> Result<(), Incoming<GetLogletInfo>> {
        self.get_loglet_info_tx.send(msg).map_err(|e| e.0)
    }

    pub fn enqueue_get_records(
        &self,
        msg: Incoming<GetRecords>,
    ) -> Result<(), Incoming<GetRecords>> {
        self.get_records_tx.send(msg).map_err(|e| e.0)
    }

    pub fn enqueue_trim(&self, msg: Incoming<Trim>) -> Result<(), Incoming<Trim>> {
        self.trim_tx.send(msg).map_err(|e| e.0)
    }
}

pub struct LogletWorker<S> {
    loglet_id: ReplicatedLogletId,
    log_store: S,
    loglet_state: LogletState,
}

impl<S: LogStore> LogletWorker<S> {
    pub fn start(
        loglet_id: ReplicatedLogletId,
        log_store: S,
        loglet_state: LogletState,
    ) -> Result<LogletWorkerHandle, ShutdownError> {
        let writer = Self {
            loglet_id,
            log_store,
            loglet_state,
        };

        let (store_tx, store_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::unbounded_channel();
        let (seal_tx, seal_rx) = mpsc::unbounded_channel();
        let (get_loglet_info_tx, get_loglet_info_rx) = mpsc::unbounded_channel();
        let (get_records_tx, get_records_rx) = mpsc::unbounded_channel();
        let (trim_tx, trim_rx) = mpsc::unbounded_channel();
        let (wait_for_tail_tx, wait_for_tail_rx) = mpsc::unbounded_channel();
        let (get_digest_tx, get_digest_rx) = mpsc::unbounded_channel();
        // todo
        let tc_handle = TaskCenter::spawn_unmanaged(
            TaskKind::LogletWriter,
            "loglet-worker",
            writer.run(
                store_rx,
                release_rx,
                seal_rx,
                get_loglet_info_rx,
                get_records_rx,
                trim_rx,
                wait_for_tail_rx,
                get_digest_rx,
            ),
        )?;
        Ok(LogletWorkerHandle {
            store_tx,
            release_tx,
            seal_tx,
            get_loglet_info_tx,
            get_records_tx,
            trim_tx,
            wait_for_tail_tx,
            get_digest_tx,
            tc_handle,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn run(
        mut self,
        mut store_rx: mpsc::UnboundedReceiver<Incoming<Store>>,
        mut release_rx: mpsc::UnboundedReceiver<Incoming<Release>>,
        mut seal_rx: mpsc::UnboundedReceiver<Incoming<Seal>>,
        mut get_loglet_info_rx: mpsc::UnboundedReceiver<Incoming<GetLogletInfo>>,
        mut get_records_rx: mpsc::UnboundedReceiver<Incoming<GetRecords>>,
        mut trim_rx: mpsc::UnboundedReceiver<Incoming<Trim>>,
        mut wait_for_tail_rx: mpsc::UnboundedReceiver<Incoming<WaitForTail>>,
        mut get_digest_rx: mpsc::UnboundedReceiver<Incoming<GetDigest>>,
    ) {
        // The worker is the sole writer to this loglet's local-tail so it's safe to maintain a moving
        // local tail view and serialize changes to logstore as long as we send them in the correct
        // order.
        let mut sealing_in_progress = false;
        let mut staging_local_tail = self.loglet_state.local_tail().offset();
        let mut in_flight_stores = FuturesUnordered::new();
        let mut in_flight_network_sends = FuturesUnordered::new();
        let mut waiting_for_seal = FuturesUnordered::new();
        let mut in_flight_seal = std::pin::pin!(OptionFuture::default());
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        loop {
            tokio::select! {
                // todo(asoli): Benchmark on diverse workload to determine if biased causes
                // starvation.
                biased;
                _ = &mut shutdown => {
                    // todo: consider a draining shutdown if needed
                    // this might include sending notifications of shutdown to allow graceful
                    // handoff
                    debug!(loglet_id = %self.loglet_id, "Loglet writer shutting down");
                    return;
                }
                // GET_DIGEST
                Some(msg) = get_digest_rx.recv() => {
                    self.loglet_state.notify_known_global_tail(msg.body().header.known_global_tail);
                    // digest responses are spawned as tasks
                    self.process_get_digest(msg);
                }
                Some(_) = in_flight_stores.next() => {}
                // The in-flight seal (if any)
                Some(Ok(_)) = &mut in_flight_seal => {
                    sealing_in_progress = false;
                    self.loglet_state.get_local_tail_watch().notify_seal();
                    debug!(loglet_id = %self.loglet_id, "Loglet is sealed");
                    in_flight_seal.set(None.into());
                }
                // The set of requests waiting for seal to complete
                Some(_) = waiting_for_seal.next() => {}
                // The set of network sends waiting to complete
                Some(_) = in_flight_network_sends.next() => {}
                // WAIT_FOR_TAIL
                Some(msg) = wait_for_tail_rx.recv() => {
                    self.loglet_state.notify_known_global_tail(msg.body().header.known_global_tail);
                    // responses are spawned as disposable tasks
                    self.process_wait_for_tail(msg);
                }
                // RELEASE
                Some(msg) = release_rx.recv() => {
                    self.loglet_state.notify_known_global_tail(msg.body().header.known_global_tail);
                }
                Some(msg) = seal_rx.recv() => {
                    let (reciprocal, msg) = msg.split();
                    // this message might be telling us about a higher `known_global_tail`
                    self.loglet_state.notify_known_global_tail(msg.header.known_global_tail);
                    // If we have a seal operation in-flight, we'd want this request to wait for
                    // seal to happen
                    let tail_watcher = self.loglet_state.get_local_tail_watch();
                    let global_tail = self.loglet_state.get_global_tail_tracker();
                    waiting_for_seal.push(async move {
                        let seal_watcher = tail_watcher.wait_for_seal();
                        if seal_watcher.await.is_ok() {
                            let body = Sealed::new(*tail_watcher.get(), global_tail.get()).with_status(Status::Ok);
                            let response = reciprocal.prepare(body);
                            // send the response over the network
                            let _ = response.send().await;
                        }
                    });
                    let seal_token = self.process_seal(msg, &mut sealing_in_progress).await;
                    if let Some(seal_token) = seal_token {
                        in_flight_seal.set(Some(seal_token).into());
                    }

                }
                // GET_LOGLET_INFO
                Some(msg) = get_loglet_info_rx.recv() => {
                    self.loglet_state.notify_known_global_tail(msg.body().header.known_global_tail);
                    // drop response if connection is lost/congested
                    let peer = msg.peer();
                    if let Err(e) = msg.to_rpc_response(LogletInfo::new(self.loglet_state.local_tail(), self.loglet_state.trim_point(), self.loglet_state.known_global_tail())).try_send() {
                        debug!(?e.source, peer = %peer, "Failed to respond to GetLogletInfo message due to peer channel capacity being full");
                    } else {
                        tracing::trace!(%peer, %self.loglet_id, local_tail = ?self.loglet_state.local_tail(), known_global_tail = %self.loglet_state.known_global_tail(), "GetLogletInfo response");
                    }
                }
                // GET_RECORDS
                Some(msg) = get_records_rx.recv() => {
                    self.loglet_state.notify_known_global_tail(msg.body().header.known_global_tail);
                    // read responses are spawned as disposable tasks
                    self.process_get_records(msg);
                }
                // TRIM
                Some(msg) = trim_rx.recv() => {
                    self.loglet_state.notify_known_global_tail(msg.body().header.known_global_tail);
                    self.process_trim(msg);
                }
                // STORE
                Some(mut msg) = store_rx.recv() => {
                    let span = trace_span!("LogServer: store");
                    msg.follow_from_sender_for(&span);

                    let (reciprocal, msg) = msg.split();
                    // this message might be telling us about a higher `known_global_tail`
                    self.loglet_state.notify_known_global_tail(msg.header.known_global_tail);
                    let next_ok_offset = std::cmp::max(staging_local_tail, self.loglet_state.known_global_tail());
                    let peer = reciprocal.peer();
                    let (status, maybe_store_token) = self.process_store(peer, msg, &mut staging_local_tail, next_ok_offset, &sealing_in_progress).await;
                    // if this store is complete, the last committed is updated to this value.
                    let future_last_committed = staging_local_tail;
                    if let Some(store_token) = maybe_store_token {
                        // in-flight store...
                        let local_tail_watch = self.loglet_state.get_local_tail_watch();
                        let global_tail = self.loglet_state.get_global_tail_tracker();
                        in_flight_stores.push(async move {
                            // wait for log store to finish
                            let res = store_token.await;
                            match res {
                                Ok(_) => {
                                    // advance local-tail
                                    local_tail_watch.notify_offset_update(future_last_committed);
                                    // ignoring the error if we couldn't send the response
                                    let msg = Stored::new(*local_tail_watch.get(), global_tail.get()).with_status(status);
                                    let response = reciprocal.prepare(msg);
                                    // send the response over the network
                                    let _ = response.send().await;
                                }
                                Err(e) => {
                                    // log-store in failsafe mode and cannot process stores anymore.
                                    warn!(?e, "Log-store is in failsafe mode, dropping store");
                                    let _ = reciprocal.prepare(Stored::empty()).send().await;
                                }
                            }
                        }.instrument(span));
                    } else {
                        // we didn't store, let's respond immediately with status
                        let msg = Stored::new(self.loglet_state.local_tail(), self.loglet_state.known_global_tail()).with_status(status);
                        let response = reciprocal.prepare(msg);
                        in_flight_network_sends.push(async move {
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

        if body.payloads.is_empty() {
            // Can't store zero records
            return (Status::Malformed, None);
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
        if body.first_offset < next_ok_offset
            && peer != body.sequencer
            // not a repair store.
            && !body.flags.contains(StoreFlags::IgnoreSeal)
        {
            return (Status::SequencerMismatch, None);
        }

        if body.flags.contains(StoreFlags::IgnoreSeal) {
            // We must be sealed (sanity check)
            // Accept repair store only on sealed loglet.
            if !self.loglet_state.is_sealed() {
                warn!(
                    loglet_id = %self.loglet_id,
                    %peer,
                    first_offset = %body.first_offset,
                    "Ignoring repair store on unsealed loglet, repair should only happen on sealed loglets"
                );
                return (Status::Malformed, None);
            }
            trace!(
                loglet_id = %self.loglet_id,
                %peer,
                first_offset = %body.first_offset,
                "Admitting a repair store for sealed loglet to restore replication"
            );
        }

        if body.first_offset > next_ok_offset {
            // We can only accept writes coming in order. We don't support buffering out-of-order
            // writes.
            debug!(
                loglet_id = %self.loglet_id,
                first_offset = %body.first_offset,
                %peer,
                "Can only accept writes coming in order, next_ok={}",
                next_ok_offset,
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
                *staging_local_tail = std::cmp::max(*staging_local_tail, last_offset.next());
                (Status::Ok, Some(store_token))
            }
            Err(_) => {
                // shutting down. log-store is disabled
                (Status::Disabled, None)
            }
        }
    }

    fn process_wait_for_tail(&mut self, msg: Incoming<WaitForTail>) {
        let loglet_state = self.loglet_state.clone();
        // fails on shutdown, in this case, we ignore the request
        let _ = TaskCenter::spawn(TaskKind::Disposable, "logserver-tail-monitor", async move {
            let (reciprocal, msg) = msg.split();
            let local_tail_watch = loglet_state.get_local_tail_watch();
            // If shutdown happened, this task will be disposed of and we won't send
            // the response.
            match msg.query {
                TailUpdateQuery::LocalTail(target_offset) => {
                    local_tail_watch
                        .wait_for_offset_or_seal(target_offset)
                        .await?;
                }
                TailUpdateQuery::GlobalTail(target_global_tail) => {
                    let global_tail_tracker = loglet_state.get_global_tail_tracker();
                    tokio::select! {
                        res = global_tail_tracker.wait_for_offset(target_global_tail) => { res.map(|_|()) },
                        // Are we locally sealed?
                        res = local_tail_watch.wait_for_seal() => { res },
                    }?;
                }
                TailUpdateQuery::LocalOrGlobal(target_offset) => {
                    let global_tail_tracker = loglet_state.get_global_tail_tracker();
                    tokio::select! {
                        res = global_tail_tracker.wait_for_offset(target_offset) => { res.map(|_|()) },
                        res = local_tail_watch.wait_for_offset_or_seal(target_offset) => { res.map(|_|()) },
                    }?;
                }
            };

            let update =
                TailUpdated::new(loglet_state.local_tail(), loglet_state.known_global_tail());
            let _ = reciprocal.prepare(update).send().await;
            Ok(())
        });
    }

    fn process_get_records(&mut self, msg: Incoming<GetRecords>) {
        let log_store = self.log_store.clone();
        let loglet_state = self.loglet_state.clone();
        // fails on shutdown, in this case, we ignore the request
        let _ = TaskCenter::spawn(TaskKind::Disposable, "logserver-get-records", async move {
            let (reciprocal, msg) = msg.split();
            let from_offset = msg.from_offset;
            // validate that from_offset <= to_offset
            if msg.from_offset > msg.to_offset {
                let response =
                    reciprocal.prepare(Records::empty(from_offset).with_status(Status::Malformed));
                // ship the response to the original connection
                let _ = response.send().await;
                return Ok(());
            }
            let records = match log_store.read_records(msg, &loglet_state).await {
                Ok(records) => records,
                Err(_) => Records::new(
                    loglet_state.local_tail(),
                    loglet_state.known_global_tail(),
                    from_offset,
                )
                .with_status(Status::Disabled),
            };
            // ship the response to the original connection
            let _ = reciprocal.prepare(records).send().await;
            Ok(())
        });
    }

    fn process_get_digest(&mut self, msg: Incoming<GetDigest>) {
        let log_store = self.log_store.clone();
        let loglet_state = self.loglet_state.clone();
        // fails on shutdown, in this case, we ignore the request
        let _ = TaskCenter::spawn(TaskKind::Disposable, "logserver-get-digest", async move {
            let (reciprocal, msg) = msg.split();
            // validation. Note that to_offset is inclusive.
            if msg.from_offset > msg.to_offset {
                let response = reciprocal.prepare(Digest::empty().with_status(Status::Malformed));
                // ship the response to the original connection
                let _ = response.send().await;
                return Ok(());
            }
            let digest = match log_store.get_records_digest(msg, &loglet_state).await {
                Ok(digest) => digest,
                Err(_) => Digest::new(
                    loglet_state.local_tail(),
                    loglet_state.known_global_tail(),
                    Default::default(),
                )
                .with_status(Status::Disabled),
            };
            // ship the response to the original connection
            let _ = reciprocal.prepare(digest).send().await;
            Ok(())
        });
    }

    fn process_trim(&mut self, msg: Incoming<Trim>) {
        // When trimming, we eagerly update the in-memory view of the trim-point _before_ we
        // perform the trim on the log-store since it's safer to over report the trim-point than
        // under report.
        //
        // fails on shutdown, in this case, we ignore the request
        let mut loglet_state = self.loglet_state.clone();
        let log_store = self.log_store.clone();
        let _ = TaskCenter::spawn(TaskKind::Disposable, "logserver-trim", async move {
            let loglet_id = msg.body().header.loglet_id;
            let new_trim_point = msg.body().trim_point;
            // cannot trim beyond the global known tail (if known) or the local_tail whichever is higher.
            let local_tail = loglet_state.local_tail();
            let known_global_tail = loglet_state.known_global_tail();
            let high_watermark = known_global_tail.max(local_tail.offset());
            if new_trim_point < LogletOffset::OLDEST || new_trim_point >= high_watermark {
                let _ = msg
                    .to_rpc_response(
                        Trimmed::new(loglet_state.local_tail(), known_global_tail)
                            .with_status(Status::Malformed),
                    )
                    .send()
                    .await;
                return Ok(());
            }

            let (reciprocal, mut msg) = msg.split();
            // The trim point cannot be at or exceed the local_tail, we clip to the
            // local_tail-1 if that's the case.
            msg.trim_point = msg.trim_point.min(local_tail.offset().prev());

            let body = if loglet_state.update_trim_point(msg.trim_point) {
                match log_store.enqueue_trim(msg).await?.await {
                    Ok(_) => {
                        Trimmed::new(loglet_state.local_tail(), loglet_state.known_global_tail())
                            .with_status(Status::Ok)
                    }
                    Err(_) => {
                        warn!(
                            %loglet_id,
                            "Log-store is disabled, and its trim-point will falsely be reported as {} since we couldn't commit that to the log-store. Trim-point will be correct after restart.",
                            new_trim_point
                        );
                        Trimmed::new(loglet_state.local_tail(), loglet_state.known_global_tail())
                            .with_status(Status::Disabled)
                    }
                }
            } else {
                // it's already trimmed
                Trimmed::new(loglet_state.local_tail(), loglet_state.known_global_tail())
            };

            // ship the response to the original connection
            let _ = reciprocal.prepare(body).send().await;
            Ok(())
        });
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
    use std::sync::Arc;

    use super::*;
    use googletest::prelude::*;
    use test_log::test;

    use restate_core::network::OwnedConnection;
    use restate_core::{MetadataBuilder, TaskCenter};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::live::Live;
    use restate_types::logs::{KeyFilter, Keys, Record, RecordCache};
    use restate_types::net::codec::MessageBodyExt;
    use restate_types::net::CURRENT_PROTOCOL_VERSION;
    use restate_types::replicated_loglet::ReplicatedLogletId;

    use crate::metadata::LogletStateMap;
    use crate::rocksdb_logstore::{RocksDbLogStore, RocksDbLogStoreBuilder};

    use super::LogletWorker;

    async fn setup() -> Result<RocksDbLogStore> {
        let config = Live::from_value(Configuration::default());
        let common_rocks_opts = config.clone().map(|c| &c.common);
        RocksDbManager::init(common_rocks_opts);
        let metadata_builder = MetadataBuilder::default();
        assert!(TaskCenter::try_set_global_metadata(
            metadata_builder.to_metadata()
        ));
        // create logstore.
        let builder = RocksDbLogStoreBuilder::create(
            config.clone().map(|c| &c.log_server).boxed(),
            config.map(|c| &c.log_server.rocksdb).boxed(),
            RecordCache::new(1_000_000),
        )
        .await?;
        Ok(builder.start(Default::default()).await?)
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_simple_store_flow() -> Result<()> {
        let log_store = setup().await?;
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const LOGLET: ReplicatedLogletId = ReplicatedLogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();
        let (net_tx, mut net_rx) = mpsc::channel(10);
        let connection = OwnedConnection::new_fake(SEQUENCER, CURRENT_PROTOCOL_VERSION, net_tx);

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(LOGLET, log_store, loglet_state)?;

        let payloads: Arc<[Record]> = vec![
            Record::from("a sample record"),
            Record::from("another record"),
        ]
        .into();

        // offsets 1, 2
        let msg1 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::OLDEST,
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        // offsets 3, 4
        let msg2 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(3),
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        let msg1 = Incoming::for_testing(connection.downgrade(), msg1, None);
        let msg2 = Incoming::for_testing(connection.downgrade(), msg2, None);
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

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_store_and_seal() -> Result<()> {
        let log_store = setup().await?;
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const LOGLET: ReplicatedLogletId = ReplicatedLogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();
        let (net_tx, mut net_rx) = mpsc::channel(10);
        let connection = OwnedConnection::new_fake(SEQUENCER, CURRENT_PROTOCOL_VERSION, net_tx);

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(LOGLET, log_store, loglet_state)?;

        let payloads: Arc<[Record]> = vec![
            Record::from("a sample record"),
            Record::from("another record"),
        ]
        .into();

        // offsets 1, 2
        let msg1 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::OLDEST,
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        let seal1 = Seal {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            sequencer: SEQUENCER,
        };

        let seal2 = Seal {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            sequencer: SEQUENCER,
        };

        // offsets 3, 4
        let msg2 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(3),
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        let msg1 = Incoming::for_testing(connection.downgrade(), msg1, None);
        let seal1 = Incoming::for_testing(connection.downgrade(), seal1, None);
        let seal2 = Incoming::for_testing(connection.downgrade(), seal2, None);
        let msg2 = Incoming::for_testing(connection.downgrade(), msg2, None);
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
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(3)),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(3),
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };
        let msg3 = Incoming::for_testing(connection.downgrade(), msg3, None);
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

        // GetLogletInfo
        // offsets 3, 4
        let msg = GetLogletInfo {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
        };
        let msg = Incoming::for_testing(connection.downgrade(), msg, None);
        let msg_id = msg.msg_id();
        worker.enqueue_get_loglet_info(msg).unwrap();

        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), eq(msg_id));
        let info: LogletInfo = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(info.status, eq(Status::Ok));
        assert_that!(info.local_tail, eq(LogletOffset::new(3)));
        assert_that!(info.trim_point, eq(LogletOffset::INVALID));
        assert_that!(info.sealed, eq(true));

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_repair_store() -> Result<()> {
        let log_store = setup().await?;
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const PEER: GenerationalNodeId = GenerationalNodeId::new(2, 2);
        const LOGLET: ReplicatedLogletId = ReplicatedLogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();
        let (net_tx, mut net_rx) = mpsc::channel(10);
        let connection = OwnedConnection::new_fake(SEQUENCER, CURRENT_PROTOCOL_VERSION, net_tx);

        let (peer_net_tx, mut peer_net_rx) = mpsc::channel(10);
        let repair_connection =
            OwnedConnection::new_fake(PEER, CURRENT_PROTOCOL_VERSION, peer_net_tx);

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(LOGLET, log_store, loglet_state)?;

        let payloads: Arc<[Record]> = vec![
            Record::from("a sample record"),
            Record::from("another record"),
        ]
        .into();

        // offsets 1, 2
        let msg1 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::OLDEST,
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        // offsets 10, 11
        let msg2 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(10),
            flags: StoreFlags::empty(),
            payloads: payloads.clone(),
        };

        let seal1 = Seal {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            sequencer: SEQUENCER,
        };

        // 5, 6
        let repair_message_before_local_tail = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(5),
            flags: StoreFlags::IgnoreSeal,
            payloads: payloads.clone(),
        };

        // 16, 17
        let repair_message_after_local_tail = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(16)),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(16),
            flags: StoreFlags::IgnoreSeal,
            payloads: payloads.clone(),
        };

        let msg1 = Incoming::for_testing(connection.downgrade(), msg1, None);
        let msg2 = Incoming::for_testing(connection.downgrade(), msg2, None);
        let repair1 = Incoming::for_testing(
            repair_connection.downgrade(),
            repair_message_before_local_tail,
            None,
        );
        let repair2 = Incoming::for_testing(
            repair_connection.downgrade(),
            repair_message_after_local_tail,
            None,
        );
        let seal1 = Incoming::for_testing(connection.downgrade(), seal1, None);

        worker.enqueue_store(msg1).unwrap();
        worker.enqueue_store(msg2).unwrap();
        // first store is successful
        let response = net_rx.recv().await.unwrap();
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.sealed, eq(false));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));

        // 10, 11
        let response = net_rx.recv().await.unwrap();
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.sealed, eq(false));
        assert_that!(stored.local_tail, eq(LogletOffset::new(12)));

        worker.enqueue_seal(seal1).unwrap();
        // seal responses can come at any order, but we'll consume waiters queue before we process
        // store messages.
        // sealed
        let response = net_rx.recv().await.unwrap();
        let sealed: Sealed = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(sealed.status, eq(Status::Ok));
        assert_that!(sealed.local_tail, eq(LogletOffset::new(12)));

        // repair store (before local tail, local tail won't move)
        worker.enqueue_store(repair1).unwrap();
        let response = peer_net_rx.recv().await.unwrap();
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(12)));

        worker.enqueue_store(repair2).unwrap();
        let response = peer_net_rx.recv().await.unwrap();
        let stored: Stored = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(18)));

        // GetLogletInfo
        // offsets 3, 4
        let msg = GetLogletInfo {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
        };
        let msg = Incoming::for_testing(connection.downgrade(), msg, None);
        let msg_id = msg.msg_id();
        worker.enqueue_get_loglet_info(msg).unwrap();

        let response = net_rx.recv().await.unwrap();
        let header = response.header.unwrap();
        assert_that!(header.in_response_to(), eq(msg_id));
        let info: LogletInfo = response
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(info.status, eq(Status::Ok));
        assert_that!(info.local_tail, eq(LogletOffset::new(18)));
        assert_that!(info.trim_point, eq(LogletOffset::INVALID));
        assert_that!(info.sealed, eq(true));
        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_simple_get_records_flow() -> Result<()> {
        let log_store = setup().await?;
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const LOGLET: ReplicatedLogletId = ReplicatedLogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();
        let (net_tx, mut net_rx) = mpsc::channel(10);
        let connection = OwnedConnection::new_fake(SEQUENCER, CURRENT_PROTOCOL_VERSION, net_tx);

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(LOGLET, log_store, loglet_state)?;

        // Populate the log-store with some records (..,2,..,5,..,10, 11)
        // Note: dots mean we don't have records at those globally committed offsets.
        worker
            .enqueue_store(Incoming::for_testing(
                connection.downgrade(),
                Store {
                    // faking that offset=1 is released
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(2)),
                    timeout_at: None,
                    sequencer: SEQUENCER,
                    known_archived: LogletOffset::INVALID,
                    first_offset: LogletOffset::new(2),
                    flags: StoreFlags::empty(),
                    payloads: vec![Record::from("record2")].into(),
                },
                None,
            ))
            .unwrap();

        worker
            .enqueue_store(Incoming::for_testing(
                connection.downgrade(),
                Store {
                    // faking that offset=4 is released
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(5)),
                    timeout_at: None,
                    sequencer: SEQUENCER,
                    known_archived: LogletOffset::INVALID,
                    first_offset: LogletOffset::new(5),
                    flags: StoreFlags::empty(),
                    payloads: vec![Record::from(("record5", Keys::Single(11)))].into(),
                },
                None,
            ))
            .unwrap();

        worker
            .enqueue_store(Incoming::for_testing(
                connection.downgrade(),
                Store {
                    // faking that offset=9 is released
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                    timeout_at: None,
                    sequencer: SEQUENCER,
                    known_archived: LogletOffset::INVALID,
                    first_offset: LogletOffset::new(10),
                    flags: StoreFlags::empty(),
                    payloads: vec![Record::from("record10"), Record::from("record11")].into(),
                },
                None,
            ))
            .unwrap();

        // Wait for stores to complete.
        for _ in 0..3 {
            let stored: Stored = net_rx
                .recv()
                .await
                .unwrap()
                .body
                .unwrap()
                .try_decode(connection.protocol_version())?;
            assert_that!(stored.status, eq(Status::Ok));
        }

        // We expect to see [2, 5]. No trim gaps, no filtered gaps.
        worker
            .enqueue_get_records(Incoming::for_testing(
                connection.downgrade(),
                GetRecords {
                    // faking that offset=9 is released
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                    filter: KeyFilter::Any,
                    // no memory limits
                    total_limit_in_bytes: None,
                    from_offset: LogletOffset::new(1),
                    to_offset: LogletOffset::new(7),
                },
                None,
            ))
            .unwrap();

        let mut records: Records = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(records.status, eq(Status::Ok));
        assert_that!(records.local_tail, eq(LogletOffset::new(12)));
        assert_that!(records.sealed, eq(false));
        assert_that!(records.next_offset, eq(LogletOffset::new(8)));
        assert_that!(records.records.len(), eq(2));
        // pop in reverse order
        for i in [5, 2] {
            let (offset, record) = records.records.pop().unwrap();
            assert_that!(offset, eq(LogletOffset::from(i)));
            assert_that!(record.is_data(), eq(true));
            let data = record.try_unwrap_data().unwrap();
            let original: String = data.decode().unwrap();
            assert_that!(original, eq(format!("record{i}")));
        }

        // We expect to see [2, FILTERED(5), 10, 11]. No trim gaps.
        worker
            .enqueue_get_records(Incoming::for_testing(
                connection.downgrade(),
                GetRecords {
                    // INVALID can be used when we don't have a reasonable value to pass in.
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
                    // no memory limits
                    total_limit_in_bytes: None,
                    filter: KeyFilter::Within(0..=5),
                    from_offset: LogletOffset::new(1),
                    // to a point beyond local tail
                    to_offset: LogletOffset::new(100),
                },
                None,
            ))
            .unwrap();

        let mut records: Records = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(records.status, eq(Status::Ok));
        assert_that!(records.local_tail, eq(LogletOffset::new(12)));
        assert_that!(records.next_offset, eq(LogletOffset::new(12)));
        assert_that!(records.sealed, eq(false));
        assert_that!(records.records.len(), eq(4));
        // pop() returns records in reverse order
        for i in [11, 10, 5, 2] {
            let (offset, record) = records.records.pop().unwrap();
            assert_that!(offset, eq(LogletOffset::from(i)));
            if i == 5 {
                // this one is filtered
                assert_that!(record.is_filtered_gap(), eq(true));
                let gap = record.try_unwrap_filtered_gap().unwrap();
                assert_that!(gap.to, eq(LogletOffset::new(5)));
            } else {
                assert_that!(record.is_data(), eq(true));
                let data = record.try_unwrap_data().unwrap();
                let original: String = data.decode().unwrap();
                assert_that!(original, eq(format!("record{i}")));
            }
        }

        // Apply memory limits (2 bytes) should always see the first real record.
        // We expect to see [FILTERED(5), 10]. (11 is not returend due to budget)
        worker
            .enqueue_get_records(Incoming::for_testing(
                connection.downgrade(),
                GetRecords {
                    // INVALID can be used when we don't have a reasonable value to pass in.
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
                    // no memory limits
                    total_limit_in_bytes: Some(2),
                    filter: KeyFilter::Within(0..=5),
                    from_offset: LogletOffset::new(4),
                    // to a point beyond local tail
                    to_offset: LogletOffset::new(100),
                },
                None,
            ))
            .unwrap();

        let mut records: Records = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(records.status, eq(Status::Ok));
        assert_that!(records.local_tail, eq(LogletOffset::new(12)));
        assert_that!(records.next_offset, eq(LogletOffset::new(11)));
        assert_that!(records.sealed, eq(false));
        assert_that!(records.records.len(), eq(2));
        // pop() returns records in reverse order
        for i in [10, 5] {
            let (offset, record) = records.records.pop().unwrap();
            assert_that!(offset, eq(LogletOffset::from(i)));
            if i == 5 {
                // this one is filtered
                assert_that!(record.is_filtered_gap(), eq(true));
                let gap = record.try_unwrap_filtered_gap().unwrap();
                assert_that!(gap.to, eq(LogletOffset::new(5)));
            } else {
                assert_that!(record.is_data(), eq(true));
                let data = record.try_unwrap_data().unwrap();
                let original: String = data.decode().unwrap();
                assert_that!(original, eq(format!("record{i}")));
            }
        }

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_trim_basics() -> Result<()> {
        let log_store = setup().await?;
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const LOGLET: ReplicatedLogletId = ReplicatedLogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();
        let (net_tx, mut net_rx) = mpsc::channel(10);
        let connection = OwnedConnection::new_fake(SEQUENCER, CURRENT_PROTOCOL_VERSION, net_tx);

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(LOGLET, log_store.clone(), loglet_state.clone())?;

        assert_that!(loglet_state.trim_point(), eq(LogletOffset::INVALID));
        assert_that!(loglet_state.local_tail().offset(), eq(LogletOffset::OLDEST));
        // The loglet has no knowledge of global commits, it shouldn't accept trims.
        worker
            .enqueue_trim(Incoming::for_testing(
                connection.downgrade(),
                Trim {
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::OLDEST),
                    trim_point: LogletOffset::OLDEST,
                },
                None,
            ))
            .unwrap();

        let trimmed: Trimmed = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(trimmed.status, eq(Status::Malformed));
        assert_that!(trimmed.local_tail, eq(LogletOffset::OLDEST));
        assert_that!(trimmed.sealed, eq(false));

        // The loglet has knowledge of global tail of 10, it should accept trims up to 9 but it
        // won't move trim point beyond its local tail.
        worker
            .enqueue_trim(Incoming::for_testing(
                connection.downgrade(),
                Trim {
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                    trim_point: LogletOffset::new(9),
                },
                None,
            ))
            .unwrap();

        let trimmed: Trimmed = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(trimmed.status, eq(Status::Ok));
        assert_that!(trimmed.local_tail, eq(LogletOffset::OLDEST));
        assert_that!(trimmed.sealed, eq(false));

        // let's store some records at offsets (5, 6)
        worker
            .enqueue_store(Incoming::for_testing(
                connection.downgrade(),
                Store {
                    // faking that offset=9 is released
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                    timeout_at: None,
                    sequencer: SEQUENCER,
                    known_archived: LogletOffset::INVALID,
                    first_offset: LogletOffset::new(5),
                    flags: StoreFlags::empty(),
                    payloads: vec![Record::from("record5"), Record::from("record6")].into(),
                },
                None,
            ))
            .unwrap();
        let stored: Stored = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(7)));

        // trim to 5
        worker
            .enqueue_trim(Incoming::for_testing(
                connection.downgrade(),
                Trim {
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                    trim_point: LogletOffset::new(5),
                },
                None,
            ))
            .unwrap();

        let trimmed: Trimmed = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(trimmed.status, eq(Status::Ok));
        assert_that!(trimmed.local_tail, eq(LogletOffset::new(7)));
        assert_that!(trimmed.sealed, eq(false));

        // Attempt to read. We expect to see a trim gap (1->5, 6 (data-record))
        worker
            .enqueue_get_records(Incoming::for_testing(
                connection.downgrade(),
                GetRecords {
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
                    total_limit_in_bytes: None,
                    filter: KeyFilter::Any,
                    from_offset: LogletOffset::OLDEST,
                    // to a point beyond local tail
                    to_offset: LogletOffset::new(100),
                },
                None,
            ))
            .unwrap();

        let mut records: Records = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(records.status, eq(Status::Ok));
        assert_that!(records.local_tail, eq(LogletOffset::new(7)));
        assert_that!(records.next_offset, eq(LogletOffset::new(7)));
        assert_that!(records.sealed, eq(false));
        assert_that!(records.records.len(), eq(2));
        // pop() returns records in reverse order
        for i in [6, 1] {
            let (offset, record) = records.records.pop().unwrap();
            assert_that!(offset, eq(LogletOffset::from(i)));
            if i == 1 {
                // this one is a trim gap
                assert_that!(record.is_trim_gap(), eq(true));
                let gap = record.try_unwrap_trim_gap().unwrap();
                assert_that!(gap.to, eq(LogletOffset::new(5)));
            } else {
                assert_that!(record.is_data(), eq(true));
                let data = record.try_unwrap_data().unwrap();
                let original: String = data.decode().unwrap();
                assert_that!(original, eq(format!("record{i}")));
            }
        }

        // trim everything
        worker
            .enqueue_trim(Incoming::for_testing(
                connection.downgrade(),
                Trim {
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                    trim_point: LogletOffset::new(9),
                },
                None,
            ))
            .unwrap();

        let trimmed: Trimmed = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(trimmed.status, eq(Status::Ok));
        assert_that!(trimmed.local_tail, eq(LogletOffset::new(7)));
        assert_that!(trimmed.sealed, eq(false));

        // Attempt to read again. We expect to see a trim gap (1->6)
        worker
            .enqueue_get_records(Incoming::for_testing(
                connection.downgrade(),
                GetRecords {
                    header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
                    total_limit_in_bytes: None,
                    filter: KeyFilter::Any,
                    from_offset: LogletOffset::OLDEST,
                    // to a point beyond local tail
                    to_offset: LogletOffset::new(100),
                },
                None,
            ))
            .unwrap();

        let mut records: Records = net_rx
            .recv()
            .await
            .unwrap()
            .body
            .unwrap()
            .try_decode(connection.protocol_version())?;
        assert_that!(records.status, eq(Status::Ok));
        assert_that!(records.local_tail, eq(LogletOffset::new(7)));
        assert_that!(records.next_offset, eq(LogletOffset::new(7)));
        assert_that!(records.sealed, eq(false));
        assert_that!(records.records.len(), eq(1));
        let (offset, record) = records.records.pop().unwrap();
        assert_that!(offset, eq(LogletOffset::from(1)));
        assert_that!(record.is_trim_gap(), eq(true));
        let gap = record.try_unwrap_trim_gap().unwrap();
        assert_that!(gap.to, eq(LogletOffset::new(6)));

        // Make sure that we can load the local-tail correctly when loading the loglet_state
        let loglet_state_map = LogletStateMap::default();
        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        assert_that!(loglet_state.trim_point(), eq(LogletOffset::new(6)));
        assert_that!(loglet_state.local_tail().offset(), eq(LogletOffset::new(7)));

        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
