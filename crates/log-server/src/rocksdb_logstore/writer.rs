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
use tokio::sync::mpsc;
use tracing::{debug, error, trace};

use restate_core::{ShutdownError, TaskCenter, TaskKind};
use restate_memory::MemoryLease;
use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::GenerationalNodeId;
use restate_types::config::{Configuration, LogServerOptions};
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber};
use restate_types::net::log_server::{Seal, Store, StoreFlags, Trim};

use super::keys::{DataRecordKey, KeyPrefixKind, MetadataKey};
use super::record_format::DataRecordEncoder;
use super::{DATA_CF, METADATA_CF};
use restate_futures_util::monotonic_token::{Token, TokenListener, TokenOwner, Tokens};

use crate::logstore::{Commit, LogStoreState, WriteDisableReason};
use crate::metadata::LogletState;
use crate::metric_definitions::LOG_SERVER_WRITE_BATCH_SIZE_BYTES;

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
                let kind = match (&cmd.data_update, &cmd.metadata_update) {
                    (Some(DataUpdate::StoreBatch { .. }), _) => "Store",
                    (Some(DataUpdate::TrimLogRecords { .. }), _) => "Trim",
                    (None, Some(MetadataUpdate::Seal)) => "Seal",
                    (None, Some(MetadataUpdate::SetSequencer { .. })) => "SetSequencer",
                    (None, Some(MetadataUpdate::UpdateTrimPoint { .. })) => "UpdateTrimPoint",
                    (None, None) => "Noop!",
                };
                write!(
                    f,
                    "Write({kind}, loglet={}, token={:?}, reservation={})",
                    cmd.loglet_id,
                    cmd.token,
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
    /// Monotonically increasing token assigned by the `TokenGenerator`. Used to
    /// notify the flush listener after durable commit.
    token: Token<Commit>,
    loglet_id: LogletId,
    data_update: Option<DataUpdate>,
    metadata_update: Option<MetadataUpdate>,
    /// Memory reservation held until the write is committed to RocksDB.
    /// This ensures backpressure from RocksDB stalls propagates to the network layer.
    reservation: MemoryLease,
}

impl WriteCommand {
    fn requires_sync_write(&self) -> bool {
        matches!(&self.metadata_update, Some(MetadataUpdate::Seal))
    }
}

enum DataUpdate {
    StoreBatch { store_message: Store },
    TrimLogRecords { trim_point: LogletOffset },
}

enum MetadataUpdate {
    SetSequencer { sequencer: GenerationalNodeId },
    UpdateTrimPoint { new_trim_point: LogletOffset },
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
        let token_owner = TokenOwner::new();
        let tokens = token_owner.new_tokens();

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
                    token_owner,
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
                    let config = &config.live_load().log_server;
                    // The assumption here is that most of the bytes will do into the data column
                    // family.
                    //
                    // todo(asoli): replace 4 with a shared const.
                    let target_batch_size_bytes = config.rocksdb_data_memtables_budget() / 4;
                    tokio::select! {
                        biased;
                        Some(cmd)= hi_pri_rx.recv() => {
                            writer.handle_command(cmd, &mut batch);
                        }
                        Some(cmd) = rx.recv() => {
                            writer.handle_command(cmd, &mut batch);
                        }
                        else => {
                            // both channels are closed, we are done.
                            break;
                        }
                    }

                    // Opportunistically drain hi-pri commands. Bounded to avoid
                    // starving the normal-pri channel when hi-pri bursts occur
                    // (e.g. many seals during reconfiguration).
                    const MAX_HI_PRI_DRAIN: usize = 256;
                    for _ in 0..MAX_HI_PRI_DRAIN {
                        // We don't send data through hi-pri channel, so we don't think
                        // too much about big the batch size is.
                        if let Ok(cmd) = hi_pri_rx.try_recv() {
                            writer.handle_command(cmd, &mut batch);
                        } else {
                            break;
                        }
                    }

                    // Opportunistically drain normal-pri commands. Bounded to
                    // a single memtable's worth of data to avoid excessive ballooning
                    // and overshooting the target budget.
                    while batch.size_in_bytes() < target_batch_size_bytes
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
                    // gauranteed that it's initialized
                    let reason = self.log_store_state.wait_disabled().await;
                    error!(
                        "Writes to log-server have been disabled until the node is manually restarted and \
                         the underlying reason has been resolved. Reason: {reason}",
                    );
                }
            },
        )?;
        Ok(RocksDbLogWriterHandle {
            tx,
            hi_pri_tx,
            tokens,
        })
    }
}

struct Batch {
    memory: MemoryLease,
    write_batch: WriteBatch,
    /// The highest token seen in the current batch. After a successful commit,
    /// we call `token_owner.notify_through(max_batch_token)` to unblock waiters.
    max_batch_token: Option<Token<Commit>>,
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
            write_batch: WriteBatch::default(),
            max_batch_token: None,
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
    token_owner: TokenOwner<Commit>,
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
    // Returns true if the store is accepting writes
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
            LogStoreWriteCommand::Write(command) => {
                // Track the highest token in this batch.
                batch.max_batch_token.replace(
                    batch
                        .max_batch_token
                        .map_or(command.token, |t| t.max(command.token)),
                );
                batch.sync_write_is_required |= command.requires_sync_write();

                if let Some(metadata_update) = &command.metadata_update {
                    // We won't write the seal again to the database, but we
                    // will still consider the Token for this operation as flushed.
                    if let MetadataUpdate::Seal = metadata_update
                        && !self.sealed_loglets.contains(&command.loglet_id)
                    {
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
                        command.metadata_update.unwrap(),
                    )
                }

                match command.data_update {
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
                            return;
                        }
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
                    }
                    Some(DataUpdate::TrimLogRecords { trim_point }) => {
                        Self::trim_log_records(
                            &self.data_cf,
                            &mut batch.write_batch,
                            command.loglet_id,
                            trim_point,
                        );
                    }
                    None => {}
                }

                // Combine all memory reservations into one so we can drop them
                // all at once cheaply.
                batch.memory.merge(command.reservation);
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
        update: MetadataUpdate,
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

        let result = self
            .rocksdb
            .write_batch(
                "loglet-write-batch",
                Priority::High,
                io_mode,
                write_opts,
                write_batch,
            )
            .await;

        match result {
            Ok(write_batch) => {
                // Reuse the batch for the next commit, we must clear it first. Otherwise,
                // the same batch will be re-appended in the next round.
                batch.write_batch = write_batch;
                // Reset the flag until we see a command that requires a sync write again.
                batch.sync_write_is_required = false;

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
                // Release memory back to the pool BEFORE retiring tokens. This is
                // intentional: waiters woken by `retire_through` can immediately
                // enqueue new writes that reuse the freed budget, improving
                // throughput by overlapping the next batch's ingestion with
                // the current notification phase.
                batch.clear();
                // Notify the flush listener that all tokens in this batch are durable.
                // This must happen AFTER watch updates so that when the listener
                // fires, loglet state already reflects the committed data.
                if let Some(token) = batch.max_batch_token {
                    self.token_owner.retire_through(token);
                }
                true
            }
            Err(e) => {
                // Disable writes to unblock all store waiters and to prevent future writes until
                // the node has been manually restarted.
                self.log_store_state
                    .disable_writes(WriteDisableReason::Error(e.into()));
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
    tokens: Tokens<Commit>,
}

impl RocksDbLogWriterHandle {
    pub fn commit_listener(&self) -> TokenListener<Commit> {
        self.tokens.new_listener()
    }
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
        self.hi_pri_tx
            .send(LogStoreWriteCommand::Unregister {
                loglet_id,
                loglet_state,
            })
            .is_ok()
    }

    pub fn enqueue_seal(&self, seal_message: Seal) -> Option<Token<Commit>> {
        let token = self.tokens.next();
        self.hi_pri_tx
            .send(LogStoreWriteCommand::Write(WriteCommand {
                token,
                loglet_id: seal_message.header.loglet_id,
                data_update: None,
                metadata_update: Some(MetadataUpdate::Seal),
                reservation: MemoryLease::unlinked(),
            }))
            .is_ok()
            .then_some(token)
    }

    /// Enqueues a store batch for writing to RocksDB. The writer will derive
    /// the committed offset from the store message and advance the registered
    /// loglet's tail watch after the batch is durably committed.
    pub fn enqueue_put_records(
        &self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
        reservation: MemoryLease,
    ) -> Option<Token<Commit>> {
        let loglet_id = store_message.header.loglet_id;
        let metadata_update = set_sequencer_in_metadata.then_some(MetadataUpdate::SetSequencer {
            sequencer: store_message.sequencer,
        });
        let token = self.tokens.next();
        trace!(loglet_id = %store_message.header.loglet_id, "Sending store ({}..{:?}) to writer at token {token:?}", store_message.first_offset, store_message.last_offset());
        let data_update = DataUpdate::StoreBatch { store_message };
        self.send_command(LogStoreWriteCommand::Write(WriteCommand {
            token,
            loglet_id,
            data_update: Some(data_update),
            metadata_update,
            reservation,
        }))
        .then_some(token)
    }

    pub fn enqueue_trim(&self, trim_message: Trim) -> Option<Token<Commit>> {
        let data_update = DataUpdate::TrimLogRecords {
            trim_point: trim_message.trim_point,
        };
        let metadata_update = Some(MetadataUpdate::UpdateTrimPoint {
            new_trim_point: trim_message.trim_point,
        });

        let token = self.tokens.next();
        self.send_command(LogStoreWriteCommand::Write(WriteCommand {
            token,
            loglet_id: trim_message.header.loglet_id,
            data_update: Some(data_update),
            metadata_update,
            reservation: MemoryLease::unlinked(),
        }))
        .then_some(token)
    }

    fn send_command(&self, command: LogStoreWriteCommand) -> bool {
        self.tx.send(command).is_ok()
    }
}
