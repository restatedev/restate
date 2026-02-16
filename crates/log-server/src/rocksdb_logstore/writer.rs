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
use std::time::Duration;

use bytes::BytesMut;
use metrics::{Histogram, histogram};
use rocksdb::{BoundColumnFamily, WriteBatch};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, trace, warn};

use restate_bifrost::loglet::OperationError;
use restate_core::{ShutdownError, TaskCenter, TaskKind};
use restate_memory::MemoryLease;
use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::GenerationalNodeId;
use restate_types::config::{Configuration, LogServerOptions};
use restate_types::health::HealthStatus;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber};
use restate_types::net::log_server::{Seal, Store, Trim};
use restate_types::protobuf::common::LogServerStatus;

use super::keys::{DataRecordKey, KeyPrefixKind, MetadataKey};
use super::record_format::DataRecordEncoder;
use super::{DATA_CF, METADATA_CF};
use crate::logstore::AsyncToken;
use crate::metric_definitions::LOG_SERVER_WRITE_BATCH_SIZE_BYTES;

type Ack = oneshot::Sender<Result<(), OperationError>>;

pub struct LogStoreWriteCommand {
    loglet_id: LogletId,
    data_update: Option<DataUpdate>,
    metadata_update: Option<MetadataUpdate>,
    ack: Option<Ack>,
    /// Memory reservation held until the write is committed to RocksDB.
    /// This ensures backpressure from RocksDB stalls propagates to the network layer.
    reservation: MemoryLease,
}

impl LogStoreWriteCommand {
    pub fn requires_sync_write(&self) -> bool {
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

pub(crate) struct LogStoreWriter {
    rocksdb: Arc<RocksDb>,
    batch_acks_buf: Vec<Ack>,
    arena: BytesMut,
    health_status: HealthStatus<LogServerStatus>,
    sync_write_is_required: bool,
    write_size_histogram: Histogram,
    /// Reusable write batch buffer.
    write_batch: Option<WriteBatch>,
}

impl LogStoreWriter {
    pub(crate) fn new(rocksdb: Arc<RocksDb>, health_status: HealthStatus<LogServerStatus>) -> Self {
        Self {
            rocksdb,
            batch_acks_buf: Vec::default(),
            arena: BytesMut::default(),
            health_status,
            sync_write_is_required: false,
            write_size_histogram: histogram!(LOG_SERVER_WRITE_BATCH_SIZE_BYTES),
            write_batch: None,
        }
    }

    /// Must be called from task_center context
    pub fn start(mut self) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        // The channel is bounded by the ability to acquire memory for those writes via the memory
        // pool.
        let (sender, mut receiver) = mpsc::unbounded_channel();

        TaskCenter::spawn_unmanaged(
            TaskKind::SystemService,
            "log-server-rocksdb-writer",
            async move {
                debug!("Start running LogStoreWriter");
                let mut config = Configuration::live();
                let mut buffer = Vec::default();
                let mut mem_relaim = tokio::time::interval(Duration::from_secs(60));
                loop {
                    let config = &config.live_load().log_server;
                    let batch_size = std::cmp::max(1, config.writer_batch_commit_count);
                    tokio::select! {
                        count = receiver.recv_many(&mut buffer, batch_size) => {
                            if count == 0 {
                                debug!("LogStore loglet writer task finished");
                                break;
                            } else {
                                self.handle_commands(config, &mut buffer).await;
                            }
                        }
                        _ = mem_relaim.tick() => {
                            // Drop old buffers to reclaim memory if previous batches were too
                            // large
                            self.arena = BytesMut::default();
                            self.write_batch = Some(WriteBatch::default());
                            buffer = Vec::default();
                        }
                    }
                }
            },
        )?;
        Ok(RocksDbLogWriterHandle { sender })
    }

    async fn handle_commands(
        &mut self,
        opts: &LogServerOptions,
        commands: &mut Vec<LogStoreWriteCommand>,
    ) {
        self.batch_acks_buf.clear();
        self.batch_acks_buf.reserve(commands.len());
        // Hold memory reservations until after commit to ensure backpressure from
        // RocksDB stalls propagates to the network layer.
        let mut all_memory: MemoryLease = MemoryLease::unlinked();
        let batch_acks = &mut self.batch_acks_buf;
        let arena = &mut self.arena;

        let write_batch = self.write_batch.get_or_insert_with(WriteBatch::default);

        {
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

            for command in commands.drain(..) {
                self.sync_write_is_required |= command.requires_sync_write();

                match command.data_update {
                    Some(DataUpdate::StoreBatch { store_message }) => {
                        Self::process_store_message(store_message, &data_cf, write_batch, arena)
                    }
                    Some(DataUpdate::TrimLogRecords { trim_point }) => {
                        Self::trim_log_records(&data_cf, write_batch, command.loglet_id, trim_point)
                    }
                    None => {}
                }

                if let Some(metadata_update) = command.metadata_update {
                    Self::update_metadata(
                        &metadata_cf,
                        write_batch,
                        command.loglet_id,
                        metadata_update,
                    )
                }

                if let Some(ack) = command.ack {
                    batch_acks.push(ack);
                }

                // combine all memory reservations into one so we can drop them all at once
                // cheaply.
                all_memory.merge(command.reservation);
            }
        }

        self.write_size_histogram
            .record(write_batch.size_in_bytes() as f64);
        self.commit(opts, all_memory).await;
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

    async fn commit(&mut self, opts: &LogServerOptions, reservation: MemoryLease) {
        // Take the batch out without allocating a replacement (Option::take leaves None).
        // Note that the write batch is wrapped in `Option` so we can move it out without
        // allocating a temporary replacement (as `std::mem::take` would in this particular
        // case).
        let Some(write_batch) = self.write_batch.take() else {
            return;
        };

        let mut write_opts = rocksdb::WriteOptions::new();
        if self.sync_write_is_required {
            write_opts.disable_wal(false);
            write_opts.set_sync(true);
        } else {
            write_opts.disable_wal(opts.rocksdb.rocksdb_disable_wal());
            write_opts.set_sync(!opts.rocksdb_disable_wal_fsync());
        }

        // Reset the flag until we see a command that requires a sync write again.
        self.sync_write_is_required = false;
        // hint to rocksdb to insert the memtable position hint for the batch, our writes per batch
        // are mostly ordered.
        write_opts.set_memtable_insert_hint_per_batch(true);

        let io_mode = if opts.always_commit_in_background {
            IoMode::AlwaysBackground
        } else {
            IoMode::Default
        };

        trace!(
            "Committing loglet current write batch: {} items",
            write_batch.len(),
        );
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

        // Reservations are dropped here, after the commit completes, potentially
        // before sending acks.
        drop(reservation);

        match result {
            Ok(mut write_batch) => {
                self.send_acks(Ok(()));
                // Reuse the batch for the next commit, we must clear it first. Otherwise,
                // the same batch will be re-appended in the next round.
                write_batch.clear();
                self.write_batch = Some(write_batch);
            }
            Err((e, write_batch)) => {
                // Recover the batch for reuse if possible (None only on ShutdownError).
                if let Some(mut write_batch) = write_batch {
                    write_batch.clear();
                    self.write_batch = Some(write_batch);
                }
                error!("Failed to commit write batch to rocksdb log-store: {}", e);
                self.health_status.update(LogServerStatus::Failsafe);
                self.send_acks(Err(OperationError::terminal(e)));
            }
        }
    }

    fn send_acks(&mut self, result: Result<(), OperationError>) {
        self.batch_acks_buf.drain(..).for_each(|a| {
            let _ = a.send(result.clone());
        });
    }
}

#[derive(Debug, Clone)]
pub struct RocksDbLogWriterHandle {
    sender: mpsc::UnboundedSender<LogStoreWriteCommand>,
}

impl RocksDbLogWriterHandle {
    pub fn enqueue_seal(&self, seal_message: Seal) -> Result<AsyncToken, OperationError> {
        let (ack, receiver) = oneshot::channel();
        self.send_command(LogStoreWriteCommand {
            loglet_id: seal_message.header.loglet_id,
            data_update: None,
            metadata_update: Some(MetadataUpdate::Seal),
            ack: Some(ack),
            reservation: MemoryLease::unlinked(),
        })?;
        Ok(AsyncToken::new(receiver))
    }

    pub fn enqueue_put_records(
        &self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
        reservation: MemoryLease,
    ) -> Result<AsyncToken, OperationError> {
        let (ack, receiver) = oneshot::channel();
        let loglet_id = store_message.header.loglet_id;
        let metadata_update = set_sequencer_in_metadata.then_some(MetadataUpdate::SetSequencer {
            sequencer: store_message.sequencer,
        });
        let data_update = DataUpdate::StoreBatch { store_message };

        self.send_command(LogStoreWriteCommand {
            loglet_id,
            data_update: Some(data_update),
            metadata_update,
            ack: Some(ack),
            reservation,
        })?;
        Ok(AsyncToken::new(receiver))
    }

    pub fn enqueue_trim(&self, trim_message: Trim) -> Result<AsyncToken, OperationError> {
        let (ack, receiver) = oneshot::channel();

        let data_update = DataUpdate::TrimLogRecords {
            trim_point: trim_message.trim_point,
        };
        let metadata_update = Some(MetadataUpdate::UpdateTrimPoint {
            new_trim_point: trim_message.trim_point,
        });

        self.send_command(LogStoreWriteCommand {
            loglet_id: trim_message.header.loglet_id,
            data_update: Some(data_update),
            metadata_update,
            ack: Some(ack),
            reservation: MemoryLease::unlinked(),
        })?;

        Ok(AsyncToken::new(receiver))
    }

    fn send_command(&self, command: LogStoreWriteCommand) -> Result<(), ShutdownError> {
        if let Err(e) = self.sender.send(command) {
            warn!(
                "log-server rocksdb writer task is gone, not accepting the record: {}",
                e
            );
            return Err(ShutdownError);
        }
        Ok(())
    }
}
