// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use metrics::counter;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, info, trace, warn};

use restate_clock::WallClock;
use restate_clock::time::MillisSinceEpoch;
use restate_core::network::{
    Incoming, Oneshot, Reciprocal, Rpc, ServiceMessage, ServiceStream, ShardSender, Verdict,
};
use restate_core::task_center::TaskGuard;
use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_token};
use restate_futures_util::waiter_queue::WaiterQueue;
use restate_memory::EstimatedMemorySize;
use restate_serde_util::ByteCount;
use restate_types::GenerationalNodeId;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber, TailState};
use restate_types::net::{RpcRequest, UnaryMessage, log_server::*};
use restate_types::retries::with_jitter;

use crate::logstore::{LogStore, LogletWriter, WriteDisableReason};
use crate::metadata::{IntrospectLogletWorker, LogletState, LogletWorkerState};
use crate::metric_definitions::{
    LOG_SERVER_LOGLET_STARTED, LOG_SERVER_LOGLET_STOPPED, LOG_SERVER_STORE_BYTES,
    LOG_SERVER_STORE_RECORDS,
};
use crate::tasks::{
    OnComplete, SealStorageTask, StoreStorageTask, SyncGlobalTailStorageTask, TrimStorageTask,
};

/// A loglet worker
///
/// The design of the store flow assumes that sequencer will send records in the order we can
/// accept then (per-connection) to reduce complexity on the log-server side. This means that
/// the log-server does not need to re-order or buffer records in memory.
/// Records will be rejected if:
///   1) Record offset > local tail
///   2) Or, Record offset > known_global_tail
pub struct LogletWorkerHandle {
    meta_tx: ShardSender<LogServerMetaService>,
    data_tx: ShardSender<LogServerDataService>,
    introspection_tx: mpsc::Sender<IntrospectLogletWorker>,
    loglet_guard: TaskGuard<()>,
}

impl LogletWorkerHandle {
    pub async fn drain(self) -> Result<(), ShutdownError> {
        self.loglet_guard.cancel_and_wait().await
    }

    pub fn data_tx(&self) -> ShardSender<LogServerDataService> {
        self.data_tx.clone()
    }

    pub fn meta_tx(&self) -> ShardSender<LogServerMetaService> {
        self.meta_tx.clone()
    }

    pub fn introspection_tx(&self) -> &mpsc::Sender<IntrospectLogletWorker> {
        &self.introspection_tx
    }
}

pub struct LogletWorker<S: LogStore> {
    loglet_id: LogletId,
    log_store: S,
    writer: S::Writer,
    loglet_state: LogletState,
    seal_enqueued: bool,
    accepting_writes: bool,
    // The worker is the sole writer to this loglet's local-tail so it's safe
    // to maintain a moving local tail view and serialize changes to logstore
    // as long as we send them in the correct order.
    staging_local_tail: LogletOffset,
    global_tail: LogletOffset,
    known_sequencer: Option<GenerationalNodeId>,
    /// Wait queues for rpc reciprocals
    /// Pending responders — drained when watches advance.
    pending: PendingWaiters,
    last_request: MillisSinceEpoch,
    last_periodically_synced_global_tail: LogletOffset,
}

impl<S: LogStore> LogletWorker<S> {
    pub fn start(
        loglet_id: LogletId,
        log_store: S,
        loglet_state: LogletState,
    ) -> Result<LogletWorkerHandle, ShutdownError> {
        counter!(LOG_SERVER_LOGLET_STARTED).increment(1);
        let writer = log_store.new_loglet_writer(loglet_id, &loglet_state);
        let known_sequencer = loglet_state.sequencer().copied();
        let staging_local_tail = loglet_state.local_tail().offset();
        let global_tail = loglet_state.known_global_tail();
        let worker = Self {
            loglet_id,
            log_store,
            writer,
            loglet_state,
            staging_local_tail,
            global_tail,
            known_sequencer,
            seal_enqueued: false,
            // we assume that we are accepting writes until we observe that writes are disabled.
            accepting_writes: true,
            pending: PendingWaiters::default(),
            last_request: WallClock::recent_ms(),
            last_periodically_synced_global_tail: global_tail,
        };

        let (data_tx, data_rx) = ShardSender::new();
        let (meta_tx, meta_rx) = ShardSender::new();
        let (introspection_tx, introspection_rx) = mpsc::channel(10);

        let loglet_guard = TaskCenter::spawn_unmanaged(
            TaskKind::LogletWorker,
            "loglet-worker",
            worker.run(data_rx, meta_rx, introspection_rx),
        )?
        .into_guard();
        Ok(LogletWorkerHandle {
            data_tx,
            meta_tx,
            introspection_tx,
            loglet_guard,
        })
    }

    async fn run(
        mut self,
        mut data_rx: ServiceStream<LogServerDataService>,
        mut meta_rx: ServiceStream<LogServerMetaService>,
        mut introspection_rx: mpsc::Receiver<IntrospectLogletWorker>,
    ) {
        let mut local_tail_stream = WatchStream::new(self.loglet_state.subscribe_local_tail());
        let mut global_tail_stream = WatchStream::new(self.loglet_state.subscribe_global_tail());
        let log_store_state = self.log_store.state().clone();

        let cancel_token = cancellation_token();
        let mut write_disable_fut = std::pin::pin!(log_store_state.wait_disabled());

        let mut check_interval = tokio::time::interval(with_jitter(Duration::from_secs(10), 0.3));
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                // Draining flag ensures that this branch is disabled after draining
                // is started. If we don't do this, the loop will be stuck in this branch
                // since cancelled() will always be `Poll::Ready`.
                _ = cancel_token.cancelled() => {
                    data_rx.close();
                    meta_rx.close();
                    info!(
                        loglet_id = %self.loglet_id,
                        "Loglet worker shutting down. Will drain {} data and {} meta messages and {} pending rpcs",
                        data_rx.len(),
                        meta_rx.len(),
                        self.pending.len(),
                    );
                    // drain and terminate
                    break;
                }
                Some(cmd) = introspection_rx.recv() => {
                    self.on_introspection_command(cmd);
                }
                // LogStore has disabled writes — drain pending waiters (resolving any that
                // succeeded before the failure), but continue serving reads and meta ops.
                reason = &mut write_disable_fut, if self.accepting_writes => {
                    self.on_writes_disabled(reason);
                }
                // Meta service messages
                Some(msg) = meta_rx.next() => {
                    self.process_info_svc_op(msg);
                }
                Some(current_tail) = local_tail_stream.next() => {
                    self.on_local_tail_change(current_tail);
                }
                Some(global_tail) = global_tail_stream.next(), if self.pending.has_waiting_for_global_tail_updates() => {
                    self.on_global_tail_change(global_tail);
                }
                // Data service messages
                Some(msg) = data_rx.next() => {
                    self.process_data_svc_op(msg);
                }
                _ = check_interval.tick() => {
                    if self.is_quiescent() {
                        debug!(loglet_id = %self.loglet_id, "Loglet worker became quiescent, will terminate");
                        data_rx.close();
                        meta_rx.close();
                        break;
                    } else {
                        self.persist_global_tail();
                    }

                }

            }
        }

        debug!(loglet_id = %self.loglet_id, "Draining the rpc channels");

        // Draining the RPC channels first
        loop {
            tokio::select! {
                // Meta service messages
                Some(msg) = meta_rx.next() => {
                    self.process_info_svc_op(msg);
                }
                // Data service messages
                Some(msg) = data_rx.next() => {
                    self.process_data_svc_op(msg);
                }
                else => {
                    break;
                }
            }
        }

        debug!(loglet_id = %self.loglet_id, "Waiting for loglet worker to finish");
        loop {
            tokio::select! {
                Some(cmd) = introspection_rx.recv(), if self.pending.has_pending_seal() => {
                    self.on_introspection_command(cmd);
                }
                // LogStore has disabled writes — drain pending waiters (resolving any that
                // succeeded before the failure), but continue serving reads and meta ops.
                reason = &mut write_disable_fut, if self.accepting_writes && self.pending.has_pending_seal() => {
                    self.on_writes_disabled(reason);
                    break;
                }
                // We only enable this branch for seals. We will not wait for "wait-for-tail" rpcs at drain time.
                Some(current_tail) = local_tail_stream.next(), if self.pending.has_pending_seal() => {
                    self.on_local_tail_change(current_tail);
                }
                _ = tokio::time::sleep(Duration::from_secs(20)), if self.pending.has_pending_seal() => {
                    info!(loglet_id = %self.loglet_id, "Loglet worker (local-tail: {}, staging-local-tail: {}, global-tail: {}) still waiting for {}",
                        self.loglet_state.local_tail(),
                        self.staging_local_tail,
                        self.global_tail,
                        self.pending.debug_string()
                    );
                }
                else => {
                    break;
                }
            }
        }

        // If we still have anyone waiting for tail and we are sealed. We should let them know.
        // This step is important in case we are quiescent and have received wait-for-tail requests
        // during the drain loop above. Since we are quiescent (sealed), we'd want to notify
        // those waiting that we are sealed before we stop the worker.
        self.on_local_tail_change(self.loglet_state.local_tail());
        self.persist_global_tail();
        counter!(LOG_SERVER_LOGLET_STOPPED).increment(1);

        info!(loglet_id = %self.loglet_id, "loglet worker stopped");
    }

    fn on_local_tail_change(&mut self, new_tail: TailState<LogletOffset>) {
        if new_tail.is_sealed() && self.seal_enqueued {
            self.seal_enqueued = false;
            self.staging_local_tail = new_tail.offset();
        }
        self.pending
            .on_local_tail_change(new_tail, self.global_tail);
    }

    fn on_global_tail_change(&mut self, new_global: LogletOffset) {
        self.pending
            .on_global_tail_change(self.loglet_state.local_tail(), new_global);
    }

    fn on_introspection_command(&mut self, cmd: IntrospectLogletWorker) {
        match cmd {
            IntrospectLogletWorker::GetState(tx) => {
                let _ = tx.send(LogletWorkerState {
                    staging_local_tail: self.staging_local_tail,
                    accepting_writes: self.accepting_writes,
                    seal_enqueued: self.seal_enqueued,
                    pending_seals: self.pending.seals.len() as u32,
                    pending_tail_waiters: (self.pending.local_tail_waiters.len()
                        + self.pending.global_tail_waiters.len()
                        + self.pending.local_or_global_tail_waiters.len())
                        as u32,
                    last_request_at: self.last_request,
                });
            }
        }
    }

    fn is_quiescent(&self) -> bool {
        !self.seal_enqueued
            && self.pending.is_empty()
            && self.loglet_state.is_sealed()
            && self.last_request.elapsed() >= Duration::from_secs(60)
    }

    /// Returns the updated or the existing global tail
    fn update_global_tail(&mut self, new_tail: LogletOffset) -> LogletOffset {
        if new_tail > self.global_tail {
            self.global_tail = new_tail;
            self.loglet_state.notify_known_global_tail(new_tail);
            new_tail
        } else {
            self.global_tail
        }
    }

    fn on_writes_disabled(&mut self, reason: &WriteDisableReason) {
        warn!(loglet_id = %self.loglet_id, %reason, "LogStore writes disabled: {reason}");
        // Switched to fail-safe mode. If we have pending rpcs, let's flush
        // them and don't check this branch again.
        self.accepting_writes = false;

        let current_tail = self.loglet_state.local_tail();
        self.pending
            .on_global_tail_change(current_tail, self.global_tail);
        self.pending
            .on_local_tail_change(current_tail, self.global_tail);
        // Transition to read-only mode.
        // Reset staging to match durable state. We know that no more writes will be durable after
        // the current tail.
        self.staging_local_tail = current_tail.offset();
        self.seal_enqueued = false;

        // Drain all waiting for seal
        self.pending
            .drain_waiting_seal(current_tail, self.global_tail, Status::Disabled);

        // We expect no more local-tail changes to happen, close all pending wait-for-local-tail
        // Since global tail can still change, we keep those who are waiting for global tail changes.
        self.pending
            .drain_local_tail_waiters(current_tail, self.global_tail, Status::Disabled);

        // Do not accept any more writes.
        self.writer.close();
    }

    // ---- Message processing ----

    fn process_data_svc_op(&mut self, msg: ServiceMessage<LogServerDataService>) {
        self.last_request = WallClock::recent_ms();
        match msg {
            // GET_RECORDS
            ServiceMessage::Rpc(msg) if msg.msg_type() == GetRecords::TYPE => {
                self.process_get_records(msg.into_typed());
            }
            // STORE
            ServiceMessage::Rpc(message) if msg.msg_type() == Store::TYPE => {
                self.process_store(message.into_typed::<Store>());
            }
            msg => msg.fail(Verdict::MessageUnrecognized),
        }
    }

    fn process_info_svc_op(&mut self, msg: ServiceMessage<LogServerMetaService>) {
        self.last_request = WallClock::recent_ms();
        match msg {
            // RELEASE
            ServiceMessage::Unary(msg) if msg.msg_type() == Release::TYPE => {
                let release = msg.into_typed::<Release>().into_body();
                self.update_global_tail(release.header.known_global_tail);
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
                let known_global_tail = self.update_global_tail(msg.header.known_global_tail);
                reciprocal.send(LogletInfo::new(
                    self.loglet_state.local_tail(),
                    self.loglet_state.trim_point(),
                    known_global_tail,
                ));
                trace!(%peer, %self.loglet_id, local_tail = ?self.loglet_state.local_tail(), %known_global_tail, "GetLogletInfo response");
            }
            // WAIT_FOR_TAIL
            ServiceMessage::Rpc(msg) if msg.msg_type() == WaitForTail::TYPE => {
                self.process_wait_for_tail(msg.into_typed());
            }
            // SEAL
            ServiceMessage::Rpc(msg) if msg.msg_type() == Seal::TYPE => {
                self.process_seal(msg.into_typed());
            }
            // TRIM
            ServiceMessage::Rpc(msg) if msg.msg_type() == Trim::TYPE => {
                self.process_trim(msg.into_typed());
            }
            msg => msg.fail(Verdict::MessageUnrecognized),
        }
    }

    fn process_store(&mut self, msg: Incoming<Rpc<Store>>) {
        let local_tail = self.loglet_state.local_tail();

        let peer = msg.peer();
        let (reciprocal, body, memory) = msg.split_with_reservation();
        let mut task = StoreStorageTask::new(self.loglet_id, memory, reciprocal);
        let known_global_tail = self.update_global_tail(body.header.known_global_tail);
        let is_repair = body.flags.contains(StoreFlags::IgnoreSeal);
        trace!(
            loglet_id = %self.loglet_id,
            "Processing store request: from_offset: {}, to_offset: {:?}, global_tail: {}, size={}",
            body.first_offset,
            body.last_offset(),
            body.header.known_global_tail,
            ByteCount::from(body.estimated_encode_size())
        );

        let count = body.payloads.len() as u64;
        let bytes = body.payloads.estimated_memory_size() as u64;

        let Some(last_offset) = body.last_offset() else {
            update_store_stats(count, bytes, "malformed");
            task.on_complete(local_tail, known_global_tail, Status::Malformed);
            return;
        };

        if body.payloads.is_empty() || body.first_offset == LogletOffset::INVALID {
            update_store_stats(count, bytes, "malformed");
            task.on_complete(local_tail, known_global_tail, Status::Malformed);
            return;
        }

        // Is this a sealed loglet?
        if !is_repair && local_tail.is_sealed() {
            update_store_stats(count, bytes, "sealed");
            task.on_complete(local_tail, known_global_tail, Status::Sealed);
            return;
        }

        if !self.accepting_writes {
            update_store_stats(count, bytes, "disabled");
            task.on_complete(local_tail, known_global_tail, Status::Disabled);
            return;
        }

        // Reject writes that refer to a different sequencer.
        if self.known_sequencer.is_some_and(|s| s != body.sequencer) {
            update_store_stats(count, bytes, "malformed");
            task.on_complete(local_tail, known_global_tail, Status::SequencerMismatch);
            return;
        }
        let next_ok_offset = std::cmp::max(self.staging_local_tail, known_global_tail);

        // Are we writing an older record than local-tail, this must be from the sequencer.
        if body.first_offset < next_ok_offset
            && peer != body.sequencer
            && !body.flags.contains(StoreFlags::IgnoreSeal)
        {
            update_store_stats(count, bytes, "malformed");
            task.on_complete(local_tail, known_global_tail, Status::SequencerMismatch);
            return;
        }

        if body.first_offset > next_ok_offset {
            debug!(
                loglet_id = %self.loglet_id,
                "Can only accept writes coming in order, next_ok={}",
                next_ok_offset,
            );
            update_store_stats(count, bytes, "malformed");
            task.on_complete(local_tail, known_global_tail, Status::OutOfBounds);
            return;
        }

        // We have been holding this record for too long.
        if body.expired() {
            update_store_stats(count, bytes, "expired");
            task.on_complete(local_tail, known_global_tail, Status::Dropped);
            return;
        }

        // Full duplicate fast-path: if the entire store range has already been
        // accepted, skip the write entirely. Not applicable to repair stores
        // because they could be replicating records we never had (behind our
        // local tail doesn't mean we have them).
        if !is_repair {
            // Full duplicate fast-path: if the entire store range has already been
            // accepted, skip the write entirely. Not applicable to repair stores
            // because they could be replicating records we never had (behind our
            // local tail doesn't mean we have them).
            if local_tail.offset() > last_offset {
                // Already durable — respond immediately.
                task.on_complete(local_tail, known_global_tail, Status::Ok);
                return;
            } else if self.staging_local_tail > last_offset {
                // in flight, latch. Also, respond with stored if the global tail has already
                // moved beyond this store which indicates that this is an extra store.
                //
                // Note: that tail waiters watch the "tail", hence the `next()`.
                task.release_memory();
                self.pending
                    .local_or_global_tail_waiters
                    .push(last_offset.next(), Box::new(task));
                return;
            }
        }

        // Even if ignore-seal is set, we must wait for the in-flight seal before
        // we can accept writes. Once the seal is durably committed (is_sealed()
        // returns true), repair stores (IgnoreSeal) can proceed — the "sealing in
        // progress" state is over.
        if self.seal_enqueued && !local_tail.is_sealed() {
            if is_repair {
                // Why we can't accept these repair writes while seal is enqueued?
                // Because the seal might revert the staging_local_tail to a smaller value
                // and the staging_local_tail was the basis of accepting this as a repair.
                update_store_stats(count, bytes, "sealing");
                task.on_complete(local_tail, known_global_tail, Status::Sealing);
                return;
            } else {
                // if it's a normal write, we know that it will be rejected but we don't need to go
                // through a retry, instead. We drop the reservation and let it wait for the seal
                // to complete. We fake this as _if_ the store was enqueued (but it's not)
                // but let the store wait for the seal to complete and it will be rejected with
                // Status::Sealed because we'll drain all stores > seal with Sealed status. If
                // the log-store failed (writes disabled) and the seal was never completed, the
                // store will be rejected with Status::Disabled.
                update_store_stats(count, bytes, "sealed");
                task.release_memory();
                self.pending
                    .local_or_global_tail_waiters
                    .push(last_offset.next(), Box::new(task));
                return;
            }
        }

        // Remember the sequencer if this is the first store we've seen.
        if self.known_sequencer.is_none() {
            self.known_sequencer = Some(body.sequencer);
            self.loglet_state.set_sequencer(body.sequencer);
            task.set_sequencer(body.sequencer);
        }

        // Send store to log-store. The writer advances the registered loglet's
        // tail watch after durable commit.
        if self
            .writer
            .enqueue_store(body.first_offset, last_offset, body.payloads, task)
        {
            // Advance staging_local_tail on successful enqueue so we don't advance it artificially
            self.staging_local_tail = std::cmp::max(self.staging_local_tail, last_offset.next());
            update_store_stats(count, bytes, if is_repair { "repair" } else { "ok" });
        } else {
            self.accepting_writes = false;
            update_store_stats(count, bytes, "disabled");
        }
    }

    fn process_wait_for_tail(&mut self, msg: Incoming<Rpc<WaitForTail>>) {
        let (reciprocal, msg) = msg.split();
        self.update_global_tail(msg.header.known_global_tail);

        let mut notify = NotifyTailUpdate {
            reply_to: Some(reciprocal),
        };

        let current_tail = self.loglet_state.local_tail();
        match msg.query {
            TailUpdateQuery::Unknown => {
                notify.on_complete(current_tail, self.global_tail, Status::Malformed);
            }
            TailUpdateQuery::LocalTail(target) => {
                if current_tail.offset() >= target || current_tail.is_sealed() {
                    notify.on_complete(current_tail, self.global_tail, Status::Ok);
                } else {
                    self.pending
                        .local_tail_waiters
                        .push(target, Box::new(notify));
                }
            }
            TailUpdateQuery::GlobalTail(target) => {
                if self.global_tail >= target || current_tail.is_sealed() {
                    notify.on_complete(
                        current_tail,
                        self.global_tail,
                        if current_tail.is_sealed() {
                            Status::Sealed
                        } else {
                            Status::Ok
                        },
                    );
                } else {
                    self.pending
                        .global_tail_waiters
                        .push(target, Box::new(notify));
                }
            }
            TailUpdateQuery::LocalOrGlobal(target) => {
                if current_tail.offset() >= target
                    || self.global_tail >= target
                    || current_tail.is_sealed()
                {
                    notify.on_complete(current_tail, self.global_tail, Status::Ok);
                } else {
                    self.pending
                        .local_or_global_tail_waiters
                        .push(target, Box::new(notify));
                }
            }
        }
    }

    fn process_get_records(&mut self, msg: Incoming<Rpc<GetRecords>>) {
        let (reciprocal, msg) = msg.split();
        self.update_global_tail(msg.header.known_global_tail);

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
        self.update_global_tail(msg.header.known_global_tail);

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
        let (reciprocal, msg) = msg.split();
        let known_global_tail = self.update_global_tail(msg.header.known_global_tail);

        let new_trim_point = msg.trim_point;
        let local_tail = self.loglet_state.local_tail();
        let high_watermark = known_global_tail.max(local_tail.offset());

        // cannot trim beyond the global known tail (if known) or the local_tail whichever is higher.
        if new_trim_point < LogletOffset::OLDEST || new_trim_point >= high_watermark {
            let mut task = TrimStorageTask::new(self.loglet_id, new_trim_point, reciprocal);
            task.on_complete(local_tail, known_global_tail, Status::Malformed);
            return;
        }

        // Clip the trim point so it doesn't exceed local_tail - 1.
        let clipped_trim_point = msg.trim_point.min(local_tail.offset().prev());
        let mut task = TrimStorageTask::new(self.loglet_id, clipped_trim_point, reciprocal);

        // If the durable trim point already covers this request, respond immediately.
        if self.loglet_state.trim_point() >= clipped_trim_point {
            task.on_complete(local_tail, known_global_tail, Status::Ok);
            return;
        }

        // Eagerly update the trim-point watch so readers see it immediately.
        // The RPC response waits for durability via the flush token.
        self.loglet_state.update_trim_point(clipped_trim_point);

        // Enqueue the trim. Wait for durability via flush token.
        self.writer.enqueue_trim(task);
    }

    /// Enqueues a seal if one hasn't been enqueued yet for this worker.
    /// The seal is fire-and-forget: the writer will advance the seal flag
    /// on the tail watch after durable commit. Callers (seal-waiters) are
    /// held in `pending_seals` and drained when `is_sealed()` becomes true.
    fn process_seal(&mut self, msg: Incoming<Rpc<Seal>>) {
        let (reciprocal, msg) = msg.split();
        let known_global_tail = self.update_global_tail(msg.header.known_global_tail);

        let mut task = SealStorageTask::new(
            self.loglet_id,
            self.loglet_state.get_local_tail_watch(),
            reciprocal,
        );

        // Already sealed (durably) or already enqueued — nothing to do.
        let local_tail = self.loglet_state.local_tail();
        if local_tail.is_sealed() {
            self.seal_enqueued = false;
            task.on_complete(local_tail, known_global_tail, Status::Ok);
            return;
        }

        // Join the seal wait list
        if self.seal_enqueued {
            self.pending.seals.push(task);
            return;
        }

        self.seal_enqueued = self.writer.enqueue_seal(task);
    }

    fn persist_global_tail(&mut self) {
        if !self.accepting_writes {
            return;
        }
        if self.global_tail > self.last_periodically_synced_global_tail {
            self.writer
                .set_known_global_tail(SyncGlobalTailStorageTask::new(
                    self.loglet_id,
                    self.global_tail,
                ));
            self.last_periodically_synced_global_tail = self.global_tail;
        }
    }
}

fn update_store_stats(count: u64, bytes: u64, status: &'static str) {
    counter!(LOG_SERVER_STORE_RECORDS, "status" => status).increment(count);
    counter!(LOG_SERVER_STORE_BYTES, "status" => status).increment(bytes);
}

/// Groups all pending waiter collections. Each tail-update variant gets its
/// own `WaiterQueue` keyed by `target` offset, enabling `drain_up_to` with a
/// single monotonic threshold per queue.
#[derive(Default)]
struct PendingWaiters {
    /// Repair stores — keyed by flush token. These cannot use local-tail
    /// because they may target offsets behind the current tail.
    seals: Vec<SealStorageTask>,
    /// Resolves when `local_tail >= target` OR sealed.
    local_tail_waiters: WaiterQueue<LogletOffset, Box<dyn OnComplete>>,
    // Resolves when `global_tail >= target` OR sealed.
    global_tail_waiters: WaiterQueue<LogletOffset, Box<dyn OnComplete>>,
    /// Resolves when `local_tail >= target` OR `global_tail >= target`.
    local_or_global_tail_waiters: WaiterQueue<LogletOffset, Box<dyn OnComplete>>,
}

struct NotifyTailUpdate {
    reply_to: Option<Reciprocal<Oneshot<TailUpdated>>>,
}

impl OnComplete for NotifyTailUpdate {
    fn on_complete(
        &mut self,
        local_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        status: Status,
    ) {
        if let Some(reply_to) = self.reply_to.take() {
            reply_to.send(TailUpdated::new(local_tail, global_tail).with_status(status));
        }
    }
}

impl PendingWaiters {
    pub fn is_empty(&self) -> bool {
        self.seals.is_empty()
            && self.local_tail_waiters.is_empty()
            && self.global_tail_waiters.is_empty()
            && self.local_or_global_tail_waiters.is_empty()
    }

    pub fn len(&self) -> usize {
        self.seals.len()
            + self.local_tail_waiters.len()
            + self.global_tail_waiters.len()
            + self.local_or_global_tail_waiters.len()
    }

    pub fn debug_string(&self) -> String {
        format!(
            "seals: {}, local_tail_waiters: {}, global_tail_waiters: {}, local_or_global_tail_waiters: {}",
            self.seals.len(),
            self.local_tail_waiters.len(),
            self.global_tail_waiters.len(),
            self.local_or_global_tail_waiters.len()
        )
    }

    pub fn has_waiting_for_global_tail_updates(&self) -> bool {
        !self.local_or_global_tail_waiters.is_empty() || !self.global_tail_waiters.is_empty()
    }

    pub fn has_pending_seal(&self) -> bool {
        !self.seals.is_empty()
    }

    /// Called when the local tail watch fires. Drains stores, seals, and
    /// tail-update waiters that depend on local tail or sealed state.
    pub fn on_local_tail_change(
        &mut self,
        current_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
    ) {
        // Drain seals if the loglet is now sealed.
        if current_tail.is_sealed() {
            for mut notify in self.seals.drain(..) {
                notify.on_complete(current_tail, global_tail, Status::Ok);
            }
            // Sealed resolves all local-tail and global-tail waiters.
            Self::drain_tail_waiters_all(
                current_tail,
                global_tail,
                &mut self.local_tail_waiters,
                Status::Sealed,
            );
            Self::drain_tail_waiters_all(
                current_tail,
                global_tail,
                &mut self.global_tail_waiters,
                Status::Sealed,
            );
            Self::drain_tail_waiters_all(
                current_tail,
                global_tail,
                &mut self.local_or_global_tail_waiters,
                Status::Sealed,
            );
        } else {
            self.local_tail_waiters
                .drain_up_to(current_tail.offset(), |mut notify| {
                    notify.on_complete(current_tail, global_tail, Status::Ok);
                });
            // Local-or-global waiters: resolve if local_tail >= target.
            self.local_or_global_tail_waiters
                .drain_up_to(current_tail.offset(), |mut notify| {
                    notify.on_complete(current_tail, global_tail, Status::Ok);
                });
        }
    }

    /// Called when the global tail watch fires.
    pub fn on_global_tail_change(
        &mut self,
        current_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
    ) {
        // Global-tail waiters: resolve if global_tail >= target.
        self.global_tail_waiters
            .drain_up_to(global_tail, |mut notify| {
                notify.on_complete(
                    current_tail,
                    global_tail,
                    if current_tail.is_sealed() {
                        Status::Sealed
                    } else {
                        Status::Ok
                    },
                )
            });

        // Local-or-global waiters: resolve if global_tail >= target.
        self.local_or_global_tail_waiters
            .drain_up_to(global_tail, |mut notify| {
                notify.on_complete(
                    current_tail,
                    global_tail,
                    if current_tail.is_sealed() {
                        Status::Sealed
                    } else {
                        Status::Ok
                    },
                )
            });
    }

    pub fn drain_local_tail_waiters(
        &mut self,
        current_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        status: Status,
    ) {
        Self::drain_tail_waiters_all(
            current_tail,
            global_tail,
            &mut self.local_tail_waiters,
            status,
        );
    }

    pub fn drain_waiting_seal(
        &mut self,
        current_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        status: Status,
    ) {
        for mut notify in self.seals.drain(..) {
            notify.on_complete(current_tail, global_tail, status);
        }
    }

    fn drain_tail_waiters_all(
        current_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        queue: &mut WaiterQueue<LogletOffset, Box<dyn OnComplete>>,
        status: Status,
    ) {
        queue.drain_all(|mut hook| {
            hook.on_complete(current_tail, global_tail, status);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::task::JoinSet;

    use super::*;
    use googletest::prelude::*;
    use test_log::test;

    use restate_core::{MetadataBuilder, TaskCenter};
    use restate_rocksdb::RocksDbManager;
    use restate_types::logs::{KeyFilter, Keys, Record};

    use crate::metadata::LogletStateMap;
    use crate::rocksdb_logstore::{RocksDbLogStore, RocksDbLogStoreBuilder};

    use super::LogletWorker;

    async fn setup() -> Result<RocksDbLogStore> {
        RocksDbManager::init();
        let metadata_builder = MetadataBuilder::default();
        assert!(TaskCenter::try_set_global_metadata(
            metadata_builder.to_metadata()
        ));
        // create logstore.
        let builder = RocksDbLogStoreBuilder::create().await?;
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
        worker.data_tx().send(msg1);
        worker.data_tx().send(msg2);
        // wait for response (in test-env, it's safe to assume that responses will arrive in order)
        // With the watch-based model, both stores may be batched together so the
        // local_tail in the first response may already reflect the second store.
        let stored = msg1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, ge(LogletOffset::new(3)));

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

        worker.data_tx().send(msg1);
        // first store is successful
        let stored = msg1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));
        worker.meta_tx().send(seal1);
        // should latch onto existing seal
        worker.meta_tx().send(seal2);
        // seal takes precedence, but it gets processed in the background. This store will
        // fail with Status::Sealed.
        worker.data_tx().send(msg2);
        // sealing
        let stored = msg2_reply.await?;
        assert_that!(stored.status, eq(Status::Sealed));
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
        worker.data_tx().send(msg3);
        let stored = msg3_reply.await?;
        assert_that!(stored.status, eq(Status::Sealed));
        assert_that!(stored.local_tail, eq(LogletOffset::new(3)));

        // GetLogletInfo
        // offsets 3, 4
        let msg = GetLogletInfo {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
        };
        let (msg, msg_reply) = ServiceMessage::fake_rpc(msg, Some(LOGLET.into()), SEQUENCER, None);
        worker.meta_tx().send(msg);

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

        worker.data_tx().send(msg1);
        worker.data_tx().send(msg2);
        // first store is successful. With the watch-based model, both stores may be
        // batched together so the local_tail may already reflect the second store.
        let stored = msg1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.sealed, eq(false));
        assert_that!(stored.local_tail, ge(LogletOffset::new(3)));

        // 10, 11
        let stored = msg2_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.sealed, eq(false));
        assert_that!(stored.local_tail, eq(LogletOffset::new(12)));

        worker.meta_tx().send(seal1);
        // seal responses can come at any order, but we'll consume waiters queue before we process
        // store messages.
        // sealed
        let sealed = seal1_reply.await?;
        assert_that!(sealed.status, eq(Status::Ok));
        assert_that!(sealed.local_tail, eq(LogletOffset::new(12)));

        // repair store (before local tail, local tail won't move)
        worker.data_tx().send(repair1);
        let stored: Stored = repair1_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(12)));

        worker.data_tx().send(repair2);
        let stored: Stored = repair2_reply.await?;
        assert_that!(stored.status, eq(Status::Ok));
        assert_that!(stored.local_tail, eq(LogletOffset::new(18)));

        // GetLogletInfo
        // offsets 3, 4
        let msg = GetLogletInfo {
            header: LogServerRequestHeader::new(LOGLET, LogletOffset::INVALID),
        };
        let (msg, msg_reply) = ServiceMessage::fake_rpc(msg, Some(LOGLET.into()), SEQUENCER, None);
        worker.meta_tx().send(msg);

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
        worker.data_tx().send(store);
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
        worker.data_tx().send(store);
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
        worker.data_tx().send(store);
        stores.spawn(store_reply);

        // Wait for stores to complete.
        while let Some(stored) = stores.join_next().await {
            let stored: Stored = stored.unwrap().unwrap();
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

        worker.data_tx().send(get_records);

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

        worker.data_tx().send(get_records);

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

        worker.data_tx().send(get_records);

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
        worker.meta_tx().send(msg);

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
        worker.meta_tx().send(msg);

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

        worker.data_tx().send(msg);
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

        worker.meta_tx().send(msg);

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

        worker.data_tx().send(msg);

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

        worker.meta_tx().send(msg);

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

        worker.data_tx().send(msg);

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
