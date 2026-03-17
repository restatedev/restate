// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::IoSlice;
use std::sync::Arc;

use ahash::{HashMap, HashSet};
use bytes::BytesMut;
use metrics::{Histogram, histogram};
use rocksdb::{BoundColumnFamily, WriteBatch};
use smallvec::{SmallVec, smallvec, smallvec_inline};
use tokio::sync::mpsc;
use tracing::{debug, error, trace};

use restate_core::{ShutdownError, TaskCenter, TaskKind};
use restate_memory::MemoryLease;
use restate_rocksdb::{IoMode, Priority, RocksDb, RocksError};
use restate_types::GenerationalNodeId;
use restate_types::config::{Configuration, LogServerOptions};
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber, TailState};
use restate_types::net::log_server::{Seal, Status, Store, StoreFlags, Trim};

use super::keys::{DataRecordKey, KeyPrefixKind, MetadataKey};
use super::record_format::DataRecordEncoder;
use super::{DATA_CF, METADATA_CF};

use crate::logstore::{LogStoreState, WriteDisableReason};
use crate::metadata::LogletState;
use crate::metric_definitions::LOG_SERVER_WRITE_BATCH_SIZE_BYTES;
use crate::tasks::{StoreStorageTask, TrimStorageTask, WriteStorageTask};

/// Commands sent to the [`LogStoreWriter`] over the mpsc channel.
enum LogStoreWriteCommand {
    /// A data/metadata write to RocksDB.
    Write(WriteCommand),
    /// Register a loglet's state so the writer can advance its tail watch.
    Register {
        loglet_id: LogletId,
        loglet_state: LogletState,
    },
    /// Unregister a loglet's state. Only removes the entry if the watch in the
    /// map is the same instance (identity check via `same_watch()`).
    Unregister {
        loglet_id: LogletId,
        loglet_state: LogletState,
    },
}

impl std::fmt::Debug for LogStoreWriteCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogStoreWriteCommand::Write(cmd) => {
                write!(f, "Write(")?;
                match &cmd.data_update {
                    Some(DataUpdate::StoreBatch { .. }) => write!(f, "Store")?,
                    Some(DataUpdate::TrimLogRecords { .. }) => write!(f, "Trim")?,
                    None if cmd.metadata_updates.is_empty() => write!(f, "Noop!")?,
                    None => {}
                }
                for update in &cmd.metadata_updates {
                    match update {
                        MetadataUpdate::Seal => write!(f, "+Seal")?,
                        MetadataUpdate::SetSequencer { .. } => write!(f, "+SetSequencer")?,
                        MetadataUpdate::UpdateTrimPoint { .. } => write!(f, "+UpdateTrimPoint")?,
                        MetadataUpdate::UpdateGlobalTail { .. } => write!(f, "+UpdateGlobalTail")?,
                        MetadataUpdate::UpdateLocalTail { .. } => write!(f, "+UpdateLocalTail")?,
                    }
                }
                write!(
                    f,
                    ", loglet={}, reservation={})",
                    cmd.loglet_id,
                    cmd.reservation.size(),
                )
            }
            LogStoreWriteCommand::Register {
                loglet_id,
                loglet_state,
            } => {
                let tail = loglet_state.local_tail();
                write!(
                    f,
                    "Register(loglet={loglet_id}, local_tail={tail}, global_tail={})",
                    loglet_state.known_global_tail(),
                )
            }
            LogStoreWriteCommand::Unregister {
                loglet_id,
                loglet_state,
            } => {
                let tail = loglet_state.local_tail();
                write!(
                    f,
                    "Unregister(loglet={loglet_id}, local_tail={tail}, global_tail={})",
                    loglet_state.known_global_tail(),
                )
            }
        }
    }
}

struct WriteCommand {
    /// A storage task that will be notified when the write is complete.
    task: Option<Box<dyn WriteStorageTask>>,
    loglet_id: LogletId,
    data_update: Option<DataUpdate>,
    // inlining size=2 to optimize for the common case of updating the global tail
    // along with another metadata update.
    metadata_updates: SmallVec<[MetadataUpdate; 2]>,
    /// Memory reservation held until the write is committed to RocksDB.
    /// This ensures backpressure from RocksDB stalls propagates to the network layer.
    reservation: MemoryLease,
}

impl Drop for WriteCommand {
    fn drop(&mut self) {
        if let Some(mut task) = self.task.take() {
            task.on_complete(
                TailState::invalid(),
                LogletOffset::INVALID,
                Status::Disabled,
            );
        }
    }
}

enum DataUpdate {
    StoreBatch { store_message: Store },
    TrimLogRecords { trim_point: LogletOffset },
}

enum MetadataUpdate {
    SetSequencer {
        sequencer: GenerationalNodeId,
    },
    UpdateTrimPoint {
        new_trim_point: LogletOffset,
    },
    UpdateGlobalTail {
        known_global_tail: LogletOffset,
    },
    /// Can be used to write the local-tail directly to metadata cf as of v1.8
    #[allow(dead_code)]
    UpdateLocalTail {
        new_local_tail: LogletOffset,
    },
    Seal,
}

pub(crate) struct LogStoreWriterBuilder {
    rocksdb: Arc<RocksDb>,
    /// Store-level state shared with loglet workers. The writer sets the
    /// disable reason on failsafe to unblock all store waiters.
    log_store_state: LogStoreState,
}

impl LogStoreWriterBuilder {
    pub(crate) fn new(rocksdb: Arc<RocksDb>, log_store_state: LogStoreState) -> Self {
        Self {
            rocksdb,
            log_store_state,
        }
    }

    /// Must be called from task_center context
    pub fn start(self) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        // The channel is bounded by the ability to acquire memory for those writes via the memory
        // pool.
        let (hi_pri_tx, mut hi_pri_rx) = mpsc::unbounded_channel();
        let (tx, mut rx) = mpsc::unbounded_channel();

        TaskCenter::spawn_unmanaged(
            TaskKind::SystemService,
            "log-server-rocksdb-writer",
            async move {
                let data_cf = self
                    .rocksdb
                    .inner()
                    .cf_handle(DATA_CF)
                    .expect("data cf exists");
                let metadata_cf = self
                    .rocksdb
                    .inner()
                    .cf_handle(METADATA_CF)
                    .expect("metadata cf exists");

                let mut writer = LogStoreWriter {
                    rocksdb: &self.rocksdb,
                    data_cf,
                    metadata_cf,
                    sealed_loglets: HashSet::default(),
                    state_map: HashMap::default(),
                    arena: BytesMut::default(),
                    log_store_state: self.log_store_state.clone(),
                    write_size_histogram: histogram!(LOG_SERVER_WRITE_BATCH_SIZE_BYTES),
                };
                debug!("Start running LogStoreWriter");
                let mut config = Configuration::live();
                let mut batch = Batch::default();

                loop {
                    tokio::select! {
                        biased;
                        Some(cmd)= hi_pri_rx.recv() => {
                            writer.handle_command(cmd, &mut batch);
                            // Opportunistically drain hi-pri commands.
                            // We don't send data through hi-pri channel, so we don't think
                            // too much about big the batch size is.
                            while let Ok(cmd) = hi_pri_rx.try_recv() {
                                writer.handle_command(cmd, &mut batch);
                            }
                            let config = &config.live_load().log_server;
                            if writer.commit(config, &mut batch).await {
                               continue;
                            } else {
                                // the store is disabled, will drop the rest of the commands.
                                break;
                            }
                        }
                        Some(cmd) = rx.recv() => {
                            writer.handle_command(cmd, &mut batch);
                        }
                        else => {
                            // both channels are closed, we are done.
                            break;
                        }
                    }

                    let config = &config.live_load().log_server;
                    // Opportunistically drain normal-pri commands.
                    while batch.size_in_bytes() < config.write_batch_bytes().as_usize()
                        && config
                            .write_batch_count
                            .is_none_or(|c| batch.len() < c.get())
                        && let Ok(cmd) = rx.try_recv()
                    {
                        writer.handle_command(cmd, &mut batch);
                    }

                    if !writer.commit(config, &mut batch).await {
                        // the store is disabled, will drop the rest of the commands.
                        break;
                    }
                }

                // Signal to everybody that writes are now disabled unless we're
                // disabled for other reasons already.
                if self.log_store_state.accepting_writes() {
                    // write the final batch
                    if !batch.is_empty() {
                        writer
                            .commit(&config.live_load().log_server, &mut batch)
                            .await;
                    }
                    debug!("LogStore writer shutdown complete");
                    self.log_store_state
                        .disable_writes(WriteDisableReason::Shutdown);
                } else {
                    // guaranteed that it's initialized
                    let reason = self.log_store_state.wait_disabled().await;
                    error!(
                        "Writes to log-server have been disabled until the node is manually restarted and \
                         the underlying reason has been resolved. Reason: {reason}",
                    );
                }
            },
        )?;
        Ok(RocksDbLogWriterHandle { tx, hi_pri_tx })
    }
}

struct Batch {
    memory: MemoryLease,
    write_batch: WriteBatch,
    tasks: Vec<(LogletId, Box<dyn WriteStorageTask>, Status)>,
    /// Per-loglet max committed offset for the current batch.
    max_offsets: HashMap<LogletId, LogletOffset>,
    /// Loglets that had a seal command in this batch.
    seals: HashSet<LogletId>,
    sync_write_is_required: bool,
}

impl Batch {
    /// Resets the batch for the next commit cycle.
    ///
    /// Note: `max_batch_token` is intentionally *not* cleared here. Tokens are
    /// monotonically increasing, so a stale value is harmless —
    /// [`TokenOwner::retire_through`] with an already-retired token is a no-op.
    /// The field is unconditionally overwritten by the next `Write` command
    /// before the next commit.
    fn clear(&mut self) {
        self.memory = MemoryLease::unlinked();
        self.tasks.clear();
        self.write_batch.clear();
        self.max_offsets.clear();
        self.seals.clear();
        self.sync_write_is_required = false;
    }

    fn is_empty(&self) -> bool {
        self.write_batch.is_empty()
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.write_batch.len()
    }

    fn size_in_bytes(&self) -> usize {
        self.write_batch.size_in_bytes()
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            memory: MemoryLease::unlinked(),
            tasks: Vec::default(),
            write_batch: WriteBatch::default(),
            max_offsets: HashMap::default(),
            seals: HashSet::default(),
            sync_write_is_required: false,
        }
    }
}

struct LogStoreWriter<'a> {
    rocksdb: &'a Arc<RocksDb>,
    data_cf: Arc<BoundColumnFamily<'a>>,
    metadata_cf: Arc<BoundColumnFamily<'a>>,
    /// The set of loglets that we have observed to be sealed while processing
    /// commands.
    sealed_loglets: HashSet<LogletId>,
    /// Registered loglet states. The writer uses these to advance tail watches
    /// after durable commit.
    state_map: HashMap<LogletId, LogletState>,
    arena: BytesMut,
    /// Store-level state shared with loglet workers. The writer sets the
    /// disable reason on failsafe to unblock all store waiters.
    log_store_state: LogStoreState,
    write_size_histogram: Histogram,
}

impl LogStoreWriter<'_> {
    fn handle_command(&mut self, command: LogStoreWriteCommand, batch: &mut Batch) {
        trace!("LogStoreWriter: {command:?}");
        match command {
            LogStoreWriteCommand::Register {
                loglet_id,
                loglet_state,
            } => {
                if loglet_state.is_sealed() {
                    self.sealed_loglets.insert(loglet_id);
                }
                self.state_map.insert(loglet_id, loglet_state);
            }
            LogStoreWriteCommand::Unregister {
                loglet_id,
                loglet_state,
            } => {
                // Only remove if the watch in the map is the same instance
                // as the one being unregistered (identity check). This prevents
                // a stale unregister from Worker A removing Worker B's fresh
                // registration.
                if let Some(existing) = self.state_map.get(&loglet_id)
                    && existing
                        .get_local_tail_watch()
                        .same_watch(&loglet_state.get_local_tail_watch())
                {
                    self.state_map.remove(&loglet_id);
                    self.sealed_loglets.remove(&loglet_id);
                }
            }
            LogStoreWriteCommand::Write(mut command) => {
                if let Some(task) = &mut command.task {
                    task.on_start();
                }
                for metadata_update in &command.metadata_updates {
                    // We won't write the seal again to the database, but we
                    // will still consider the Token for this operation as flushed.
                    if let MetadataUpdate::Seal = metadata_update
                        && !self.sealed_loglets.contains(&command.loglet_id)
                    {
                        // We'd like to always sync-write the seal
                        batch.sync_write_is_required = true;
                        // We immediately consider this loglet as sealed so we can
                        // drop the remaining stores in the batch.
                        self.sealed_loglets.insert(command.loglet_id);
                        // let's notify watchers
                        batch.seals.insert(command.loglet_id);
                    }
                    Self::update_metadata(
                        &self.metadata_cf,
                        &mut batch.write_batch,
                        command.loglet_id,
                        metadata_update,
                    )
                }

                let status = match command.data_update.take() {
                    Some(DataUpdate::StoreBatch { store_message }) => {
                        let loglet_id = store_message.header.loglet_id;
                        let last_offset = store_message.last_offset().expect("non-empty store");
                        // We will ignore non-repair stores that were received after
                        // the loglet has been sealed.
                        if !store_message.flags.contains(StoreFlags::IgnoreSeal)
                            && self.sealed_loglets.contains(&loglet_id)
                        {
                            trace!(
                                "Ignoring storing [{}..{last_offset}] for loglet {loglet_id} after it was sealed",
                                store_message.first_offset,
                            );
                            Status::Sealed
                        } else {
                            // Track the max committed offset per loglet in this batch.
                            // Multiple stores for the same loglet coalesce into a single
                            // notify_offset_update with the highest offset. The tail watch
                            // itself is monotonic (combine() ignores backward moves), so
                            // we only need to track the batch-local max here.
                            batch
                                .max_offsets
                                .entry(loglet_id)
                                .and_modify(|existing| {
                                    *existing = (*existing).max(last_offset);
                                })
                                .or_insert(last_offset);
                            Self::process_store_message(
                                store_message,
                                &self.data_cf,
                                &mut batch.write_batch,
                                &mut self.arena,
                            );
                            // Combine all memory reservations into one so we can drop them
                            // all at once cheaply.
                            batch.memory.merge(command.reservation.take());
                            Status::Ok
                        }
                    }
                    Some(DataUpdate::TrimLogRecords { trim_point }) => {
                        Self::trim_log_records(
                            &self.data_cf,
                            &mut batch.write_batch,
                            command.loglet_id,
                            trim_point,
                        );
                        Status::Ok
                    }
                    None => Status::Ok,
                };

                if let Some(task) = command.task.take() {
                    batch.tasks.push((command.loglet_id, task, status));
                }
            }
        }
    }

    fn process_store_message(
        store_message: Store,
        data_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        arena: &mut BytesMut,
    ) {
        let mut offset = store_message.first_offset;
        for payload in store_message.payloads.iter() {
            let key_bytes =
                DataRecordKey::new(store_message.header.loglet_id, offset).to_binary_array();
            let encoder = DataRecordEncoder::from(payload);
            let value_bytes = encoder.encode_to_disk_format(arena);
            // Shortcut: we know that the chain is 2 slices wide, todo is to introduce an
            // IoBufQueue that can be used in ropes of owned byte slices like this case.
            let dst = [
                IoSlice::new(value_bytes.first_ref()),
                IoSlice::new(value_bytes.last_ref()),
            ];
            write_batch.put_cf_vectored(data_cf, &[IoSlice::new(&key_bytes)], &dst);
            // advance the offset for the next record
            offset = offset.next();
        }
    }

    fn update_metadata(
        metadata_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        loglet_id: LogletId,
        update: &MetadataUpdate,
    ) {
        match update {
            MetadataUpdate::SetSequencer { sequencer } => {
                let key = MetadataKey::new(KeyPrefixKind::Sequencer, loglet_id).to_binary_array();
                let value = sequencer.to_binary_array();
                write_batch.put_cf(metadata_cf, key, value);
            }
            MetadataUpdate::UpdateTrimPoint { new_trim_point } => {
                let key = MetadataKey::new(KeyPrefixKind::TrimPoint, loglet_id).to_binary_array();
                let value = new_trim_point.to_binary_array();
                write_batch.merge_cf(metadata_cf, key, value);
            }
            MetadataUpdate::Seal => {
                let key = MetadataKey::new(KeyPrefixKind::Seal, loglet_id).to_binary_array();
                let now = chrono::Utc::now().to_rfc3339();
                write_batch.put_cf(metadata_cf, key, now);
            }
            MetadataUpdate::UpdateLocalTail { new_local_tail } => {
                let key = MetadataKey::new(KeyPrefixKind::LocalTail, loglet_id).to_binary_array();
                write_batch.put_cf(metadata_cf, key, new_local_tail.to_binary_array());
            }
            MetadataUpdate::UpdateGlobalTail { known_global_tail } => {
                let key = MetadataKey::new(KeyPrefixKind::LastKnownGlobalTail, loglet_id)
                    .to_binary_array();
                write_batch.put_cf(metadata_cf, key, known_global_tail.to_binary_array());
            }
        }
    }

    fn trim_log_records(
        data_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        loglet_id: LogletId,
        trim_point: LogletOffset,
    ) {
        // the upper bound is exclusive for range deletions, therefore we need to increase it
        let from_key = DataRecordKey::new(loglet_id, LogletOffset::OLDEST).to_binary_array();
        let to_key = DataRecordKey::new(loglet_id, trim_point.next()).to_binary_array();

        write_batch.delete_range_cf(data_cf, from_key, to_key);
    }

    /// Returns true if the write batch was committed successfully (no rocksdb errors).
    async fn commit(&mut self, opts: &LogServerOptions, batch: &mut Batch) -> bool {
        if batch.write_batch.is_empty() {
            // committing an empty batch is not an error
            batch.clear();
            return true;
        };

        let write_batch = std::mem::take(&mut batch.write_batch);

        self.write_size_histogram
            .record(write_batch.size_in_bytes() as f64);

        let mut write_opts = rocksdb::WriteOptions::new();
        if batch.sync_write_is_required {
            write_opts.disable_wal(false);
            write_opts.set_sync(true);
        } else {
            write_opts.disable_wal(opts.rocksdb.rocksdb_disable_wal());
            write_opts.set_sync(!opts.rocksdb_disable_wal_fsync());
        }

        // hint to rocksdb to insert the memtable position hint for the batch, our writes per batch
        // are mostly ordered.
        write_opts.set_memtable_insert_hint_per_batch(true);

        let io_mode = if opts.always_commit_in_background {
            IoMode::AlwaysBackground
        } else {
            IoMode::Default
        };

        let result = if opts.read_only {
            Err(RocksError::ReadOnly)
        } else {
            self.rocksdb
                .write_batch(
                    "loglet-write-batch",
                    Priority::High,
                    io_mode,
                    write_opts,
                    write_batch,
                )
                .await
        };

        match result {
            Ok(write_batch) => {
                // Reuse the batch for the next commit, we must clear it first. Otherwise,
                // the same batch will be re-appended in the next round.
                batch.write_batch = write_batch;

                // Advance tail watches — one notification per loglet with the max offset.
                for (loglet_id, offset) in batch.max_offsets.drain() {
                    if let Some(loglet_state) = self.state_map.get(&loglet_id) {
                        loglet_state
                            .local_tail_watch()
                            .notify(self.sealed_loglets.contains(&loglet_id), offset.next());
                    }
                }
                for loglet_id in batch.seals.drain() {
                    if let Some(loglet_state) = self.state_map.get(&loglet_id) {
                        loglet_state.notify_seal();
                    }
                }

                // Release memory back to the pool BEFORE retiring tasks. This is
                // intentional: waiters woken by `retire_through` can immediately
                // enqueue new writes that reuse the freed budget, improving
                // throughput by overlapping the next batch's ingestion with
                // the current notification phase.
                batch.memory = MemoryLease::unlinked();

                // Notify tasks
                for (loglet_id, mut task, status) in batch.tasks.drain(..) {
                    if let Some(loglet_state) = self.state_map.get(&loglet_id) {
                        task.on_complete(
                            loglet_state.local_tail(),
                            loglet_state.known_global_tail(),
                            status,
                        );
                    } else {
                        // Should not happen, but leaving a log as a smoking gun.
                        error!(
                            "Storage task completed after the loglet was unregistered from log-server writer."
                        );
                    }
                }

                batch.clear();
                true
            }
            Err(e) => {
                // Disable writes to unblock all store waiters and to prevent future writes until
                // the node has been manually restarted.
                error!("Cannot write to log-server database: {e}");
                let reason = if matches!(e, RocksError::ReadOnly) {
                    WriteDisableReason::Manual
                } else {
                    WriteDisableReason::Error(e.into())
                };

                self.log_store_state.disable_writes(reason);
                batch.clear();
                // Stops the writer
                false
            }
        }
    }
}

#[derive(Clone)]
pub struct RocksDbLogWriterHandle {
    tx: mpsc::UnboundedSender<LogStoreWriteCommand>,
    hi_pri_tx: mpsc::UnboundedSender<LogStoreWriteCommand>,
}

impl RocksDbLogWriterHandle {
    /// Registers a loglet's state with the writer so it can advance the tail
    /// watch after durable commits.
    pub fn register_loglet(&self, loglet_id: LogletId, loglet_state: LogletState) -> bool {
        self.hi_pri_tx
            .send(LogStoreWriteCommand::Register {
                loglet_id,
                loglet_state,
            })
            .is_ok()
    }

    /// Unregisters a loglet's state. Uses identity check (`same_watch()`) to
    /// prevent a stale unregister from removing a fresh registration.
    pub fn unregister_loglet(&self, loglet_id: LogletId, loglet_state: LogletState) -> bool {
        // Using the normal channel to ensure unregistration happens after processing all
        // commands/tasks
        self.tx
            .send(LogStoreWriteCommand::Unregister {
                loglet_id,
                loglet_state,
            })
            .is_ok()
    }

    pub fn enqueue_seal(&self, seal_message: Seal, known_global_tail: LogletOffset) -> bool {
        let metadata_updates = smallvec_inline![
            MetadataUpdate::UpdateGlobalTail { known_global_tail },
            MetadataUpdate::Seal
        ];

        self.hi_pri_tx
            .send(LogStoreWriteCommand::Write(WriteCommand {
                task: None,
                loglet_id: seal_message.header.loglet_id,
                data_update: None,
                metadata_updates,
                reservation: MemoryLease::unlinked(),
            }))
            .is_ok()
    }

    /// Enqueues a store batch for writing to RocksDB. The writer will derive
    /// the committed offset from the store message and advance the registered
    /// loglet's tail watch after the batch is durably committed.
    pub fn enqueue_put_records(
        &self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
        known_global_tail: LogletOffset,
        reservation: MemoryLease,
        task: StoreStorageTask,
    ) -> bool {
        let loglet_id = store_message.header.loglet_id;
        let mut metadata_updates = SmallVec::default();
        if set_sequencer_in_metadata {
            metadata_updates.push(MetadataUpdate::SetSequencer {
                sequencer: store_message.sequencer,
            });
        }
        metadata_updates.push(MetadataUpdate::UpdateGlobalTail { known_global_tail });
        trace!(loglet_id = %store_message.header.loglet_id, "Sending store ({}..{:?}) to writer", store_message.first_offset, store_message.last_offset());
        let data_update = DataUpdate::StoreBatch { store_message };
        self.send_command(WriteCommand {
            task: Some(Box::new(task)),
            loglet_id,
            data_update: Some(data_update),
            metadata_updates,
            reservation,
        })
    }

    pub fn enqueue_trim(
        &self,
        trim_message: Trim,
        known_global_tail: LogletOffset,
        task: TrimStorageTask,
    ) -> bool {
        let data_update = DataUpdate::TrimLogRecords {
            trim_point: trim_message.trim_point,
        };
        let metadata_updates = smallvec_inline![
            MetadataUpdate::UpdateGlobalTail { known_global_tail },
            MetadataUpdate::UpdateTrimPoint {
                new_trim_point: trim_message.trim_point,
            },
        ];

        self.send_command(WriteCommand {
            task: Some(Box::new(task)),
            loglet_id: trim_message.header.loglet_id,
            data_update: Some(data_update),
            metadata_updates,
            reservation: MemoryLease::unlinked(),
        })
    }

    pub fn enqueue_set_global_tail(&self, loglet_id: LogletId, known_global_tail: LogletOffset) {
        let metadata_updates = smallvec![MetadataUpdate::UpdateGlobalTail { known_global_tail }];
        let _ = self
            .hi_pri_tx
            .send(LogStoreWriteCommand::Write(WriteCommand {
                task: None,
                loglet_id,
                data_update: None,
                metadata_updates,
                reservation: MemoryLease::unlinked(),
            }));
    }

    fn send_command(&self, cmd: WriteCommand) -> bool {
        self.tx.send(LogStoreWriteCommand::Write(cmd)).is_ok()
    }
}
