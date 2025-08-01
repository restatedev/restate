// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use futures::future::OptionFuture;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, error, instrument, trace, trace_span, warn};

use restate_core::network::{Incoming, Rpc, ServiceMessage, Verdict};
use restate_core::task_center::TaskGuard;
use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_token};
use restate_types::GenerationalNodeId;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber};
use restate_types::net::{RpcRequest, UnaryMessage, log_server::*};

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
    data_svc_tx: mpsc::UnboundedSender<ServiceMessage<LogServerDataService>>,
    info_svc_tx: mpsc::UnboundedSender<ServiceMessage<LogServerMetaService>>,

    loglet_guard: TaskGuard<()>,
}

impl LogletWorkerHandle {
    pub async fn drain(self) -> Result<(), ShutdownError> {
        self.loglet_guard.cancel_and_wait().await
    }

    pub fn enqueue_data_msg(&self, op: ServiceMessage<LogServerDataService>) {
        let _ = self.data_svc_tx.send(op);
    }

    pub fn enqueue_info_msg(&self, op: ServiceMessage<LogServerMetaService>) {
        let _ = self.info_svc_tx.send(op);
    }
}

pub struct LogletWorker<S> {
    loglet_id: LogletId,
    log_store: S,
    loglet_state: LogletState,
}

impl<S: LogStore> LogletWorker<S> {
    pub fn start(
        loglet_id: LogletId,
        log_store: S,
        loglet_state: LogletState,
    ) -> Result<LogletWorkerHandle, ShutdownError> {
        let writer = Self {
            loglet_id,
            log_store,
            loglet_state,
        };

        let (data_svc_tx, data_svc_rx) = mpsc::unbounded_channel();
        let (info_svc_tx, info_svc_rx) = mpsc::unbounded_channel();

        let loglet_guard = TaskCenter::spawn_unmanaged(
            TaskKind::LogletWriter,
            "loglet-worker",
            writer.run(data_svc_rx, info_svc_rx),
        )?
        .into_guard();
        Ok(LogletWorkerHandle {
            data_svc_tx,
            info_svc_tx,
            loglet_guard,
        })
    }

    async fn run(
        mut self,
        mut data_svc_rx: mpsc::UnboundedReceiver<ServiceMessage<LogServerDataService>>,
        mut info_svc_rx: mpsc::UnboundedReceiver<ServiceMessage<LogServerMetaService>>,
    ) {
        // The worker is the sole writer to this loglet's local-tail so it's safe to maintain a moving
        // local tail view and serialize changes to logstore as long as we send them in the correct
        // order.
        let mut sealing_in_progress = false;
        let mut staging_local_tail = self.loglet_state.local_tail().offset();
        let mut in_flight_stores = JoinSet::new();
        let mut waiting_for_seal = JoinSet::new();
        let mut in_flight_seal = std::pin::pin!(OptionFuture::default());
        let cancel_token = cancellation_token();
        let mut draining = false;

        loop {
            tokio::select! {
                // todo(asoli): Benchmark on diverse workload to determine if biased causes
                // starvation.
                biased;
                    // Draining flag ensures that this branch is disabled after draining
                    // is started. If we don't do this, the loop will be stuck in this branch
                    // since cancelled() will always be `Poll::Ready`.
                _ = cancel_token.cancelled(), if !draining => {
                    draining = true;
                    data_svc_rx.close();
                    info_svc_rx.close();
                    trace!(loglet_id = %self.loglet_id, "Loglet worker shutting down");
                }
                // The in-flight seal (if any)
                Some(res) = &mut in_flight_seal => {
                    sealing_in_progress = false;
                    in_flight_seal.set(None.into());
                    if let Err(e) = res {
                        error!(loglet_id = %self.loglet_id, ?e, "Rocksdb writer terminated before we can seal the loglet, terminating the loglet worker");
                        in_flight_seal.set(None.into());
                        break;
                    } else {
                        self.loglet_state.get_local_tail_watch().notify_seal();
                        debug!(loglet_id = %self.loglet_id, "Loglet is now sealed on this log-server node");
                    }
                }
                // The set of requests waiting for seal to complete
                Some(_) = waiting_for_seal.join_next() => {}
                // LogServiceInfoService
                Some(msg) = info_svc_rx.recv() => {
                    self.process_info_svc_op(msg, &mut sealing_in_progress, &mut in_flight_seal, &mut waiting_for_seal).await;
                }
                Some(_) = in_flight_stores.join_next() => {}
                // LogServiceDataService
                Some(msg) = data_svc_rx.recv() => {
                    self.process_data_svc_op(msg, &sealing_in_progress, &mut staging_local_tail, &mut in_flight_stores).await;
                }
                else =>  {
                    break;
                }
            }
        }

        // draining in-flight operations
        drop(data_svc_rx);
        drop(info_svc_rx);
        tracing::debug!(loglet_id = %self.loglet_id, "Draining loglet worker");
        loop {
            tokio::select! {
                Some(res) = &mut in_flight_seal => {
                    in_flight_seal.set(None.into());
                    if res.is_ok() {
                        self.loglet_state.get_local_tail_watch().notify_seal();
                        debug!(loglet_id = %self.loglet_id, "Loglet is now sealed on this log-server node");
                    }
                }
                Some(_) = in_flight_stores.join_next() => {}
                Some(_) = waiting_for_seal.join_next() => {}
                else => break,
            }
        }
        tracing::debug!(loglet_id = %self.loglet_id, "loglet worker drained");
    }

    async fn process_data_svc_op(
        &mut self,
        msg: ServiceMessage<LogServerDataService>,
        sealing_in_progress: &bool,
        staging_local_tail: &mut LogletOffset,
        in_flight_stores: &mut JoinSet<()>,
    ) {
        match msg {
            // GET_RECORDS
            ServiceMessage::Rpc(msg) if msg.msg_type() == GetRecords::TYPE => {
                self.process_get_records(msg.into_typed());
            }
            // STORE
            ServiceMessage::Rpc(message) if msg.msg_type() == Store::TYPE => {
                let mut msg = message.into_typed::<Store>();
                let span = trace_span!("LogServer: store");
                msg.follow_from_sender_for(&span);
                let peer = msg.peer();

                let (reciprocal, msg) = msg.split();
                let first_offset = msg.first_offset;
                // this message might be telling us about a higher `known_global_tail`
                self.loglet_state
                    .notify_known_global_tail(msg.header.known_global_tail);
                let next_ok_offset =
                    std::cmp::max(*staging_local_tail, self.loglet_state.known_global_tail());
                let (status, maybe_store_token) = self
                    .process_store(
                        peer,
                        msg,
                        staging_local_tail,
                        next_ok_offset,
                        sealing_in_progress,
                    )
                    .await;
                // if this store is complete, the last committed is updated to this value.
                let future_last_committed = *staging_local_tail;
                if let Some(store_token) = maybe_store_token {
                    // in-flight store...
                    let local_tail_watch = self.loglet_state.get_local_tail_watch();
                    let global_tail = self.loglet_state.get_global_tail_tracker();
                    let loglet_id = self.loglet_id;
                    in_flight_stores.spawn(Box::pin(
                        async move {
                            // wait for log store to finish
                            let res = store_token.await;
                            trace!(%loglet_id, %first_offset, "Store completed; responding to sequencer");
                            match res {
                                Ok(_) => {
                                    // advance local-tail
                                    local_tail_watch.notify_offset_update(future_last_committed);
                                    // ignoring the error if we couldn't send the response
                                    let msg =
                                        Stored::new(*local_tail_watch.get(), global_tail.get())
                                            .with_status(status);
                                    reciprocal.send(msg);
                                }
                                Err(e) => {
                                    // log-store in failsafe mode and cannot process stores anymore.
                                    warn!(?e, "Log-store is in failsafe mode, dropping store");
                                    reciprocal.send(Stored::empty());
                                }
                            }
                        }
                        .instrument(span),
                    ));
                } else {
                    // we didn't store, let's respond immediately with status
                    let msg = Stored::new(
                        self.loglet_state.local_tail(),
                        self.loglet_state.known_global_tail(),
                    )
                    .with_status(status);
                    reciprocal.send(msg);
                }
            }
            msg => msg.fail(Verdict::MessageUnrecognized),
        }
    }
    async fn process_info_svc_op(
        &mut self,
        msg: ServiceMessage<LogServerMetaService>,
        sealing_in_progress: &mut bool,
        in_flight_seal: &mut Pin<&mut OptionFuture<AsyncToken>>,
        waiting_for_seal: &mut JoinSet<()>,
    ) {
        match msg {
            // RELEASE
            ServiceMessage::Unary(msg) if msg.msg_type() == Release::TYPE => {
                let release = msg.into_typed::<Release>().into_body();
                self.loglet_state
                    .notify_known_global_tail(release.header.known_global_tail);
            }
            // GET_DIGEST
            ServiceMessage::Rpc(msg) if msg.msg_type() == GetDigest::TYPE => {
                self.process_get_digest(msg.into_typed());
            }
            // GET_LOGLET_INFO
            ServiceMessage::Rpc(msg) if msg.msg_type() == GetLogletInfo::TYPE => {
                let msg = msg.into_typed::<GetLogletInfo>();
                let peer = msg.peer();
                let (reciprocal, msg) = msg.split();
                self.loglet_state
                    .notify_known_global_tail(msg.header.known_global_tail);
                // drop response if connection is lost/congested
                reciprocal.send(LogletInfo::new(
                    self.loglet_state.local_tail(),
                    self.loglet_state.trim_point(),
                    self.loglet_state.known_global_tail(),
                ));
                tracing::trace!(%peer, %self.loglet_id, local_tail = ?self.loglet_state.local_tail(), known_global_tail = %self.loglet_state.known_global_tail(), "GetLogletInfo response");
            }
            // WAIT_FOR_TAIL
            ServiceMessage::Rpc(msg) if msg.msg_type() == WaitForTail::TYPE => {
                self.process_wait_for_tail(msg.into_typed());
            }
            // SEAL
            ServiceMessage::Rpc(msg) if msg.msg_type() == Seal::TYPE => {
                let message = msg.into_typed::<Seal>();
                let (reciprocal, msg) = message.split();
                // this message might be telling us about a higher `known_global_tail`
                self.loglet_state
                    .notify_known_global_tail(msg.header.known_global_tail);
                // If we have a seal operation in-flight, we'd want this request to wait for
                // seal to happen
                let tail_watcher = self.loglet_state.get_local_tail_watch();
                let global_tail = self.loglet_state.get_global_tail_tracker();
                waiting_for_seal.spawn(Box::pin(async move {
                    let seal_watcher = tail_watcher.wait_for_seal();
                    if seal_watcher.await.is_ok() {
                        let body = Sealed::new(*tail_watcher.get(), global_tail.get())
                            .with_status(Status::Ok);
                        // send the response over the network
                        reciprocal.send(body);
                    }
                }));
                let seal_token = self.process_seal(msg, sealing_in_progress).await;
                if let Some(seal_token) = seal_token {
                    in_flight_seal.set(Some(seal_token).into());
                }
            }
            // TRIM
            ServiceMessage::Rpc(msg) if msg.msg_type() == Trim::TYPE => {
                self.process_trim(msg.into_typed());
            }
            msg => msg.fail(Verdict::MessageUnrecognized),
        }
    }

    #[instrument(level = "debug", skip_all, fields(loglet_id = %self.loglet_id, first_offset = %body.first_offset, %peer))]
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
            trace!("Admitting a repair store loglet to restore replication");
        }

        if body.first_offset > next_ok_offset {
            // We can only accept writes coming in order. We don't support buffering out-of-order
            // writes.
            debug!(
                "Can only accept writes coming in order, next_ok={}",
                next_ok_offset,
            );
            return (Status::OutOfBounds, None);
        }

        let set_sequencer_in_metadata = if known_sequencer.is_none() {
            self.loglet_state.set_sequencer(body.sequencer)
        } else {
            // sequencer is already known, no need to store it in log-store's metadata
            false
        };
        // send store to log-store. Only push-back when log-store's batching capacity is
        // exhausted.
        match self
            .log_store
            .enqueue_store(body, set_sequencer_in_metadata)
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

    fn process_wait_for_tail(&mut self, msg: Incoming<Rpc<WaitForTail>>) {
        let (reciprocal, msg) = msg.split();
        self.loglet_state
            .notify_known_global_tail(msg.header.known_global_tail);

        let loglet_state = self.loglet_state.clone();
        // fails on shutdown, in this case, we ignore the request
        let _ = TaskCenter::spawn(TaskKind::Disposable, "logserver-tail-monitor", async move {
            let local_tail_watch = loglet_state.get_local_tail_watch();
            // If shutdown happened, this task will be disposed of and we won't send
            // the response.
            match msg.query {
                TailUpdateQuery::Unknown => {
                    reciprocal.send(TailUpdated::empty().with_status(Status::Malformed));
                    return Ok(());
                }
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
                        res = local_tail_watch.wait_for_seal() => { res.map_err(|_| ShutdownError) },
                    }?;
                }
                TailUpdateQuery::LocalOrGlobal(target_offset) => {
                    let global_tail_tracker = loglet_state.get_global_tail_tracker();
                    tokio::select! {
                        res = global_tail_tracker.wait_for_offset(target_offset) => { res.map(|_|()).map_err(|_| ShutdownError) },
                        res = local_tail_watch.wait_for_offset_or_seal(target_offset) => { res.map(|_|()).map_err(|_| ShutdownError) },
                    }?;
                }
            };

            let update =
                TailUpdated::new(loglet_state.local_tail(), loglet_state.known_global_tail());
            reciprocal.send(update);
            Ok(())
        });
    }

    fn process_get_records(&mut self, msg: Incoming<Rpc<GetRecords>>) {
        let (reciprocal, msg) = msg.split();
        self.loglet_state
            .notify_known_global_tail(msg.header.known_global_tail);

        let log_store = self.log_store.clone();
        let loglet_state = self.loglet_state.clone();
        let from_offset = msg.from_offset;
        // validate that from_offset <= to_offset
        if msg.from_offset > msg.to_offset {
            reciprocal.send(Records::empty(from_offset).with_status(Status::Malformed));
            return;
        }
        // fails on shutdown, in this case, we ignore the request
        let _ = TaskCenter::spawn_unmanaged(
            TaskKind::Disposable,
            "logserver-get-records",
            async move {
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
                reciprocal.send(records);
            },
        );
    }

    fn process_get_digest(&mut self, msg: Incoming<Rpc<GetDigest>>) {
        let (reciprocal, msg) = msg.split();
        self.loglet_state
            .notify_known_global_tail(msg.header.known_global_tail);

        let log_store = self.log_store.clone();
        let loglet_state = self.loglet_state.clone();
        // validation. Note that to_offset is inclusive.
        if msg.from_offset > msg.to_offset {
            reciprocal.send(Digest::empty().with_status(Status::Malformed));
            return;
        }
        // fails on shutdown, in this case, we ignore the request
        let _ =
            TaskCenter::spawn_unmanaged(TaskKind::Disposable, "logserver-get-digest", async move {
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
                reciprocal.send(digest);
            });
    }

    fn process_trim(&mut self, msg: Incoming<Rpc<Trim>>) {
        let (reciprocal, mut msg) = msg.split();
        self.loglet_state
            .notify_known_global_tail(msg.header.known_global_tail);
        // When trimming, we eagerly update the in-memory view of the trim-point _before_ we
        // perform the trim on the log-store since it's safer to over report the trim-point than
        // under report.
        let loglet_id = msg.header.loglet_id;
        let new_trim_point = msg.trim_point;
        let mut loglet_state = self.loglet_state.clone();
        let local_tail = loglet_state.local_tail();
        let known_global_tail = loglet_state.known_global_tail();
        let high_watermark = known_global_tail.max(local_tail.offset());
        // cannot trim beyond the global known tail (if known) or the local_tail whichever is higher.
        if new_trim_point < LogletOffset::OLDEST || new_trim_point >= high_watermark {
            reciprocal.send(
                Trimmed::new(loglet_state.local_tail(), known_global_tail)
                    .with_status(Status::Malformed),
            );
            return;
        }

        // fails on shutdown, in this case, we ignore the request
        let log_store = self.log_store.clone();
        let _ = TaskCenter::spawn_child(TaskKind::Disposable, "logserver-trim", async move {
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
            reciprocal.send(body);
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

        #[allow(clippy::manual_ok_err)]
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

    use restate_core::{MetadataBuilder, TaskCenter};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::live::{Constant, LiveLoadExt};
    use restate_types::logs::{KeyFilter, Keys, Record, RecordCache};

    use crate::metadata::LogletStateMap;
    use crate::rocksdb_logstore::{RocksDbLogStore, RocksDbLogStoreBuilder};

    use super::LogletWorker;

    async fn setup() -> Result<RocksDbLogStore> {
        let config = Constant::new(Configuration::default());
        let common_rocks_opts = config.clone().map(|c| &c.common);
        RocksDbManager::init(common_rocks_opts);
        let metadata_builder = MetadataBuilder::default();
        assert!(TaskCenter::try_set_global_metadata(
            metadata_builder.to_metadata()
        ));
        // create logstore.
        let builder = RocksDbLogStoreBuilder::create(
            config.map(|config| &config.log_server),
            RecordCache::new(1_000_000),
        )
        .await?;
        Ok(builder.start(Default::default()).await?)
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_simple_store_flow() -> Result<()> {
        let log_store = setup().await?;
        const SEQUENCER: GenerationalNodeId = GenerationalNodeId::new(1, 1);
        const LOGLET: LogletId = LogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();

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
            payloads: payloads.clone().into(),
        };

        // offsets 3, 4
        let msg2 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(3),
            flags: StoreFlags::empty(),
            payloads: payloads.clone().into(),
        };

        let (msg1, msg1_reply) =
            ServiceMessage::fake_rpc(msg1, Some(LOGLET.into()), SEQUENCER, None);
        let (msg2, msg2_reply) =
            ServiceMessage::fake_rpc(msg2, Some(LOGLET.into()), SEQUENCER, None);

        // pipelined writes
        worker.enqueue_data_msg(msg1);
        worker.enqueue_data_msg(msg2);
        // wait for response (in test-env, it's safe to assume that responses will arrive in order)
        let stored = msg1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));

        let stored = msg2_reply.await?;
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
        const LOGLET: LogletId = LogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();

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
            payloads: payloads.clone().into(),
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
            payloads: payloads.clone().into(),
        };

        let (msg1, msg1_reply) =
            ServiceMessage::fake_rpc(msg1, Some(LOGLET.into()), SEQUENCER, None);
        let (seal1, seal1_reply) =
            ServiceMessage::fake_rpc(seal1, Some(LOGLET.into()), SEQUENCER, None);
        let (seal2, seal2_reply) =
            ServiceMessage::fake_rpc(seal2, Some(LOGLET.into()), SEQUENCER, None);
        let (msg2, msg2_reply) =
            ServiceMessage::fake_rpc(msg2, Some(LOGLET.into()), SEQUENCER, None);

        worker.enqueue_data_msg(msg1);
        // first store is successful
        let stored = msg1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));
        worker.enqueue_info_msg(seal1);
        // should latch onto existing seal
        worker.enqueue_info_msg(seal2);
        // seal takes precedence, but it gets processed in the background. This store is likely to
        // observe Status::Sealing.
        worker.enqueue_data_msg(msg2);
        // sealing
        let stored = msg2_reply.await?;
        assert_that!(stored.status, eq(Status::Sealing));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));
        // seal responses can come at any order, but we'll consume waiters queue before we process
        // store messages.
        // sealed
        let sealed = seal1_reply.await?;
        assert_that!(sealed.status, eq(Status::Ok));
        assert_that!(sealed.local_tail, eq(LogletOffset::new(3)));

        // sealed2
        let sealed = seal2_reply.await?;
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
            payloads: payloads.clone().into(),
        };
        let (msg3, msg3_reply) =
            ServiceMessage::fake_rpc(msg3, Some(LOGLET.into()), SEQUENCER, None);
        worker.enqueue_data_msg(msg3);
        let stored = msg3_reply.await?;
        assert_that!(stored.status, eq(Status::Sealed));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));

        // GetLogletInfo
        // offsets 3, 4
        let msg = GetLogletInfo {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
        };
        let (msg, msg_reply) = ServiceMessage::fake_rpc(msg, Some(LOGLET.into()), SEQUENCER, None);
        worker.enqueue_info_msg(msg);

        let info = msg_reply.await?;
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
        const LOGLET: LogletId = LogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();

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
            payloads: payloads.clone().into(),
        };

        // offsets 10, 11
        let msg2 = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(10),
            flags: StoreFlags::empty(),
            payloads: payloads.clone().into(),
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
            payloads: payloads.clone().into(),
        };

        // 16, 17
        let repair_message_after_local_tail = Store {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(16)),
            timeout_at: None,
            sequencer: SEQUENCER,
            known_archived: LogletOffset::INVALID,
            first_offset: LogletOffset::new(16),
            flags: StoreFlags::IgnoreSeal,
            payloads: payloads.clone().into(),
        };

        let (msg1, msg1_reply) =
            ServiceMessage::fake_rpc(msg1, Some(LOGLET.into()), SEQUENCER, None);

        let (msg2, msg2_reply) =
            ServiceMessage::fake_rpc(msg2, Some(LOGLET.into()), SEQUENCER, None);

        let (repair1, repair1_reply) = ServiceMessage::fake_rpc(
            repair_message_before_local_tail,
            Some(LOGLET.into()),
            PEER,
            None,
        );

        let (repair2, repair2_reply) = ServiceMessage::fake_rpc(
            repair_message_after_local_tail,
            Some(LOGLET.into()),
            PEER,
            None,
        );

        let (seal1, seal1_reply) =
            ServiceMessage::fake_rpc(seal1, Some(LOGLET.into()), SEQUENCER, None);

        worker.enqueue_data_msg(msg1);
        worker.enqueue_data_msg(msg2);
        // first store is successful
        let stored = msg1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.sealed, eq(false));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));

        // 10, 11
        let stored = msg2_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.sealed, eq(false));
        assert_that!(stored.local_tail, eq(LogletOffset::new(12)));

        worker.enqueue_info_msg(seal1);
        // seal responses can come at any order, but we'll consume waiters queue before we process
        // store messages.
        // sealed
        let sealed = seal1_reply.await?;
        assert_that!(sealed.status, eq(Status::Ok));
        assert_that!(sealed.local_tail, eq(LogletOffset::new(12)));

        // repair store (before local tail, local tail won't move)
        worker.enqueue_data_msg(repair1);
        let stored: Stored = repair1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(12)));

        worker.enqueue_data_msg(repair2);
        let stored: Stored = repair2_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(18)));

        // GetLogletInfo
        // offsets 3, 4
        let msg = GetLogletInfo {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
        };
        let (msg, msg_reply) = ServiceMessage::fake_rpc(msg, Some(LOGLET.into()), SEQUENCER, None);
        worker.enqueue_info_msg(msg);

        let info = msg_reply.await?;
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
        const LOGLET: LogletId = LogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(LOGLET, log_store, loglet_state)?;

        // Populate the log-store with some records (..,2,..,5,..,10, 11)
        // Note: dots mean we don't have records at those globally committed offsets.
        let mut stores = JoinSet::new();
        let (store, store_reply) = ServiceMessage::fake_rpc(
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
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );
        worker.enqueue_data_msg(store);
        stores.spawn(store_reply);

        let (store, store_reply) = ServiceMessage::fake_rpc(
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
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );
        worker.enqueue_data_msg(store);
        stores.spawn(store_reply);

        let (store, store_reply) = ServiceMessage::fake_rpc(
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
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );
        worker.enqueue_data_msg(store);
        stores.spawn(store_reply);

        // Wait for stores to complete.
        while let Some(stored) = stores.join_next().await {
            let stored = stored.unwrap().unwrap();
            assert_that!(stored.status, eq(Status::Ok));
        }

        // We expect to see [2, 5]. No trim gaps, no filtered gaps.
        let (get_records, get_records_reply) = ServiceMessage::fake_rpc(
            GetRecords {
                // faking that offset=9 is released
                header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                filter: KeyFilter::Any,
                // no memory limits
                total_limit_in_bytes: None,
                from_offset: LogletOffset::new(1),
                to_offset: LogletOffset::new(7),
            },
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_data_msg(get_records);

        let mut records: Records = get_records_reply.await?;
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
        let (get_records, get_records_reply) = ServiceMessage::fake_rpc(
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
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_data_msg(get_records);

        let mut records: Records = get_records_reply.await?;
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
        // We expect to see [FILTERED(5), 10]. (11 is not returned due to budget)
        let (get_records, get_records_reply) = ServiceMessage::fake_rpc(
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
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_data_msg(get_records);

        let mut records: Records = get_records_reply.await?;
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
        const LOGLET: LogletId = LogletId::new_unchecked(1);
        let loglet_state_map = LogletStateMap::default();

        let loglet_state = loglet_state_map.get_or_load(LOGLET, &log_store).await?;
        let worker = LogletWorker::start(LOGLET, log_store.clone(), loglet_state.clone())?;

        assert_that!(loglet_state.trim_point(), eq(LogletOffset::INVALID));
        assert_that!(loglet_state.local_tail().offset(), eq(LogletOffset::OLDEST));
        // The loglet has no knowledge of global commits, it shouldn't accept trims.
        let (msg, msg_reply) = ServiceMessage::fake_rpc(
            Trim {
                header: LogServerRequestHeader::new(LOGLET, LogletOffset::OLDEST),
                trim_point: LogletOffset::OLDEST,
            },
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );
        worker.enqueue_info_msg(msg);

        let trimmed: Trimmed = msg_reply.await?;
        assert_that!(trimmed.status, eq(Status::Malformed));
        assert_that!(trimmed.local_tail, eq(LogletOffset::OLDEST));
        assert_that!(trimmed.sealed, eq(false));

        // The loglet has knowledge of global tail of 10, it should accept trims up to 9 but it
        // won't move trim point beyond its local tail.
        let (msg, msg_reply) = ServiceMessage::fake_rpc(
            Trim {
                header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                trim_point: LogletOffset::new(9),
            },
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );
        worker.enqueue_info_msg(msg);

        let trimmed: Trimmed = msg_reply.await?;
        assert_that!(trimmed.status, eq(Status::Ok));
        assert_that!(trimmed.local_tail, eq(LogletOffset::OLDEST));
        assert_that!(trimmed.sealed, eq(false));

        // let's store some records at offsets (5, 6)
        let (msg, msg_reply) = ServiceMessage::fake_rpc(
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
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_data_msg(msg);
        let stored: Stored = msg_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(7)));

        // trim to 5
        let (msg, msg_reply) = ServiceMessage::fake_rpc(
            Trim {
                header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                trim_point: LogletOffset::new(5),
            },
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_info_msg(msg);

        let trimmed: Trimmed = msg_reply.await?;
        assert_that!(trimmed.status, eq(Status::Ok));
        assert_that!(trimmed.local_tail, eq(LogletOffset::new(7)));
        assert_that!(trimmed.sealed, eq(false));

        // Attempt to read. We expect to see a trim gap (1->5, 6 (data-record))
        let (msg, msg_reply) = ServiceMessage::fake_rpc(
            GetRecords {
                header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
                total_limit_in_bytes: None,
                filter: KeyFilter::Any,
                from_offset: LogletOffset::OLDEST,
                // to a point beyond local tail
                to_offset: LogletOffset::new(100),
            },
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_data_msg(msg);

        let mut records: Records = msg_reply.await?;
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
        let (msg, msg_reply) = ServiceMessage::fake_rpc(
            Trim {
                header: LogServerRequestHeader::new(LOGLET, LogletOffset::new(10)),
                trim_point: LogletOffset::new(9),
            },
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_info_msg(msg);

        let trimmed: Trimmed = msg_reply.await?;
        assert_that!(trimmed.status, eq(Status::Ok));
        assert_that!(trimmed.local_tail, eq(LogletOffset::new(7)));
        assert_that!(trimmed.sealed, eq(false));

        // Attempt to read again. We expect to see a trim gap (1->6)
        let (msg, msg_reply) = ServiceMessage::fake_rpc(
            GetRecords {
                header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
                total_limit_in_bytes: None,
                filter: KeyFilter::Any,
                from_offset: LogletOffset::OLDEST,
                // to a point beyond local tail
                to_offset: LogletOffset::new(100),
            },
            Some(LOGLET.into()),
            SEQUENCER,
            None,
        );

        worker.enqueue_data_msg(msg);

        let mut records: Records = msg_reply.await?;
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
