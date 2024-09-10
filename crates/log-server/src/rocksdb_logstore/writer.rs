// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove after scaffolding is complete
#![allow(unused)]

use std::sync::Arc;

use bytes::BytesMut;
use futures::StreamExt as FutureStreamExt;
use metrics::histogram;
use restate_types::net::log_server::{Seal, Store};
use restate_types::time::NanosSinceEpoch;
use restate_types::GenerationalNodeId;
use rocksdb::{BoundColumnFamily, WriteBatch};
use serde_with::TimestampNanoSeconds;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{debug, error, trace, warn};

use restate_bifrost::loglet::OperationError;
use restate_core::{cancellation_watcher, ShutdownError, TaskCenter, TaskKind};
use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::config::LogServerOptions;
use restate_types::live::BoxedLiveLoad;
use restate_types::logs::{LogletOffset, Record, SequenceNumber};
use restate_types::replicated_loglet::ReplicatedLogletId;

use super::keys::{DataRecordKey, KeyPrefixKind, MetadataKey};
use super::record_format::DataRecordEncoder;
use super::{DATA_CF, METADATA_CF};
use crate::logstore::AsyncToken;
use crate::metric_definitions::{
    LOG_SERVER_STORE, LOG_SERVER_WRITE_BATCH_COUNT, LOG_SERVER_WRITE_BATCH_SIZE_BYTES,
};

type Ack = oneshot::Sender<Result<(), OperationError>>;
type AckRecv = oneshot::Receiver<Result<(), OperationError>>;

const RECORD_SIZE_GUESS: usize = 4_096; // Estimate 4KiB per record
const INITIAL_SERDE_BUFFER_SIZE: usize = 16_384; // Initial capacity 16KiB

pub struct LogStoreWriteCommand {
    loglet_id: ReplicatedLogletId,
    data_update: Option<DataUpdate>,
    metadata_update: Option<MetadataUpdate>,
    ack: Option<Ack>,
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
    buffer: BytesMut,
    updateable_options: BoxedLiveLoad<LogServerOptions>,
}

impl LogStoreWriter {
    pub(crate) fn new(
        rocksdb: Arc<RocksDb>,
        updateable_options: BoxedLiveLoad<LogServerOptions>,
    ) -> Self {
        Self {
            rocksdb,
            batch_acks_buf: Vec::default(),
            buffer: BytesMut::with_capacity(INITIAL_SERDE_BUFFER_SIZE),
            updateable_options,
        }
    }

    /// Must be called from task_center context
    pub fn start(mut self, tc: &TaskCenter) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        // big enough to allow a second full batch to queue up while the existing one is being processed
        let batch_size = std::cmp::max(
            1,
            self.updateable_options
                .live_load()
                .writer_batch_commit_count,
        );
        // leave twice as much space in the the channel to ensure we can enqueue up-to a full batch in
        // the backlog while we process this one.
        let (sender, receiver) = mpsc::channel(batch_size * 2);

        tc.spawn_child(
            TaskKind::SystemService,
            "log-server-rocksdb-writer",
            None,
            async move {
                debug!("Start running LogStoreWriter");
                let mut opts = self.updateable_options.clone();
                let mut receiver =
                    std::pin::pin!(ReceiverStream::new(receiver).ready_chunks(batch_size));
                let mut cancel = std::pin::pin!(cancellation_watcher());

                loop {
                    tokio::select! {
                        biased;
                        _ = &mut cancel => {
                            break;
                        }
                        Some(cmds) = TokioStreamExt::next(&mut receiver) => {
                                self.handle_commands(opts.live_load(), cmds).await;
                        }
                    }
                }
                debug!("LogStore loglet writer task finished");
                Ok(())
            },
        )?;
        Ok(RocksDbLogWriterHandle { sender })
    }

    async fn handle_commands(
        &mut self,
        opts: &LogServerOptions,
        commands: Vec<LogStoreWriteCommand>,
    ) {
        let mut write_batch = WriteBatch::default();
        self.batch_acks_buf.clear();
        self.batch_acks_buf.reserve(commands.len());
        let batch_acks = &mut self.batch_acks_buf;
        let buffer = &mut self.buffer;
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

            for command in commands {
                match command.data_update {
                    Some(DataUpdate::StoreBatch { store_message }) => Self::process_store_message(
                        store_message,
                        &data_cf,
                        &mut write_batch,
                        buffer,
                    ),
                    Some(DataUpdate::TrimLogRecords { trim_point }) => Self::trim_log_records(
                        &data_cf,
                        &mut write_batch,
                        command.loglet_id,
                        trim_point,
                        buffer,
                    ),
                    None => {}
                }

                if let Some(metadata_update) = command.metadata_update {
                    Self::update_metadata(
                        &metadata_cf,
                        &mut write_batch,
                        command.loglet_id,
                        metadata_update,
                        buffer,
                    )
                }

                if let Some(ack) = command.ack {
                    batch_acks.push(ack);
                }
            }
        }

        histogram!(LOG_SERVER_WRITE_BATCH_SIZE_BYTES).record(write_batch.size_in_bytes() as f64);
        histogram!(LOG_SERVER_WRITE_BATCH_COUNT).record(write_batch.len() as f64);
        self.commit(opts, write_batch).await;
    }

    fn process_store_message(
        store_message: Store,
        data_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        buffer: &mut BytesMut,
    ) {
        buffer.reserve(store_message.estimated_encode_size());
        let mut offset = store_message.first_offset;
        for payload in store_message.payloads {
            let key_bytes =
                DataRecordKey::new(store_message.loglet_id, offset).encode_and_split(buffer);
            let value_bytes = DataRecordEncoder::from(payload).encode_to_disk_format(buffer);
            write_batch.put_cf(data_cf, key_bytes, value_bytes);
            // advance the offset for the next record
            offset = offset.next();
        }
    }

    fn update_metadata(
        metadata_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        loglet_id: ReplicatedLogletId,
        update: MetadataUpdate,
        buffer: &mut BytesMut,
    ) {
        match update {
            MetadataUpdate::SetSequencer { sequencer } => {
                let key =
                    MetadataKey::new(KeyPrefixKind::Sequencer, loglet_id).encode_and_split(buffer);
                let value = sequencer.encode_and_split(buffer);
                write_batch.put_cf(metadata_cf, key, value);
            }
            MetadataUpdate::UpdateTrimPoint { new_trim_point } => {
                let key =
                    MetadataKey::new(KeyPrefixKind::TrimPoint, loglet_id).encode_and_split(buffer);
                let value = new_trim_point.encode_and_split(buffer);
                write_batch.merge_cf(metadata_cf, key, value);
            }
            MetadataUpdate::Seal => {
                let key = MetadataKey::new(KeyPrefixKind::Seal, loglet_id).encode_and_split(buffer);
                let now = chrono::Utc::now().to_rfc3339();
                write_batch.put_cf(metadata_cf, key, now);
            }
        }
    }

    fn trim_log_records(
        data_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        loglet_id: ReplicatedLogletId,
        trim_point: LogletOffset,
        buffer: &mut BytesMut,
    ) {
        // the upper bound is exclusive for range deletions, therefore we need to increase it
        let from_key = DataRecordKey::new(loglet_id, LogletOffset::OLDEST).encode_and_split(buffer);
        let to_key = DataRecordKey::new(loglet_id, trim_point.next()).encode_and_split(buffer);

        write_batch.delete_range_cf(data_cf, from_key, to_key);
    }

    async fn commit(&mut self, opts: &LogServerOptions, write_batch: WriteBatch) {
        let mut write_opts = rocksdb::WriteOptions::new();
        write_opts.disable_wal(opts.rocksdb.rocksdb_disable_wal());
        write_opts.set_sync(!opts.rocksdb_disable_wal_fsync());

        trace!(
            "Committing loglet current write batch: {} items",
            write_batch.len(),
        );
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

        if let Err(e) = result {
            error!("Failed to commit write batch to rocksdb log-store: {}", e);
            self.send_acks(Err(OperationError::terminal(e)));
            return;
        }

        self.send_acks(Ok(()));
    }

    fn send_acks(&mut self, result: Result<(), OperationError>) {
        self.batch_acks_buf.drain(..).for_each(|a| {
            let _ = a.send(result.clone());
        });
    }
}

#[derive(Debug, Clone)]
pub struct RocksDbLogWriterHandle {
    sender: mpsc::Sender<LogStoreWriteCommand>,
}

impl RocksDbLogWriterHandle {
    pub async fn enqueue_seal(&self, seal_message: Seal) -> Result<AsyncToken, OperationError> {
        let (ack, receiver) = oneshot::channel();
        self.send_command(LogStoreWriteCommand {
            loglet_id: seal_message.loglet_id,
            data_update: None,
            metadata_update: Some(MetadataUpdate::Seal),
            ack: Some(ack),
        })
        .await?;
        Ok(AsyncToken::new(receiver))
    }

    pub async fn enqueue_put_records(
        &self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
    ) -> Result<AsyncToken, OperationError> {
        let (ack, receiver) = oneshot::channel();
        // Do not allow more than 65k records in a single batch!
        // this is validated by the store handler
        let last_offset_in_batch = store_message
            .last_offset()
            .expect("last_offset is within bounds");

        let loglet_id = store_message.loglet_id;
        let metadata_update = set_sequencer_in_metadata.then_some(MetadataUpdate::SetSequencer {
            sequencer: store_message.sequencer,
        });
        let data_update = DataUpdate::StoreBatch { store_message };

        self.send_command(LogStoreWriteCommand {
            loglet_id,
            data_update: Some(data_update),
            metadata_update,
            ack: Some(ack),
        })
        .await?;
        Ok(AsyncToken::new(receiver))
    }

    pub async fn enqueue_trim(
        &self,
        loglet_id: ReplicatedLogletId,
        trim_point: LogletOffset,
    ) -> Result<AsyncToken, OperationError> {
        let (ack, receiver) = oneshot::channel();

        let data_update = DataUpdate::TrimLogRecords { trim_point };
        let metadata_update = Some(MetadataUpdate::UpdateTrimPoint {
            new_trim_point: trim_point,
        });

        self.send_command(LogStoreWriteCommand {
            loglet_id,
            data_update: Some(data_update),
            metadata_update,
            ack: Some(ack),
        })
        .await?;

        Ok(AsyncToken::new(receiver))
    }

    async fn send_command(&self, command: LogStoreWriteCommand) -> Result<(), ShutdownError> {
        if let Err(e) = self.sender.send(command).await {
            warn!(
                "log-server rocksdb writer task is gone, not accepting the record: {}",
                e
            );
            return Err(ShutdownError);
        }
        Ok(())
    }
}
