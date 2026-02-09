// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::StreamExt as FutureStreamExt;
use metrics::histogram;
use rocksdb::{BoundColumnFamily, WriteBatch};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt as TokioStreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace, warn};

use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::config::LocalLogletOptions;
use restate_types::live::LiveLoad;
use restate_types::logs::{LogletOffset, Record, SequenceNumber};

use super::keys::{MetadataKey, MetadataKind, RecordKey};
use super::log_state::LogStateUpdates;
use super::log_store::{DATA_CF, METADATA_CF};
use super::metric_definitions::{
    BIFROST_LOCAL_WRITE_BATCH_COUNT, BIFROST_LOCAL_WRITE_BATCH_SIZE_BYTES,
};
use super::record_format::{FORMAT_FOR_NEW_APPENDS, encode_record_and_split};
use crate::loglet::OperationError;

type Ack = oneshot::Sender<Result<(), OperationError>>;
type AckRecv = oneshot::Receiver<Result<(), OperationError>>;

const RECORD_SIZE_GUESS: usize = 4_096; // Estimate 4KiB per record
const INITIAL_SERDE_BUFFER_SIZE: usize = 16_384; // Initial capacity 16KiB

pub struct LogStoreWriteCommand {
    loglet_id: u64,
    data_update: Option<DataUpdate>,
    log_state_updates: Option<LogStateUpdates>,
    ack: Option<Ack>,
}

enum DataUpdate {
    PutRecords {
        first_offset: LogletOffset,
        payloads: Arc<[Record]>,
    },
    TrimLog {
        old_trim_point: LogletOffset,
        new_trim_point: LogletOffset,
    },
}

pub(crate) struct LogStoreWriter {
    rocksdb: Arc<RocksDb>,
    batch_acks_buf: Vec<Ack>,
    buffer: BytesMut,
}

impl LogStoreWriter {
    pub(crate) fn new(rocksdb: Arc<RocksDb>) -> Self {
        Self {
            rocksdb,
            batch_acks_buf: Vec::default(),
            buffer: BytesMut::with_capacity(INITIAL_SERDE_BUFFER_SIZE),
        }
    }

    /// Must be called from task_center context
    pub fn start(
        mut self,
        mut updateable: impl LiveLoad<Live = LocalLogletOptions> + 'static,
    ) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        // big enough to allows a second full batch to queue up while the existing one is being processed
        let batch_size = std::cmp::max(1, updateable.live_load().writer_batch_commit_count);
        // leave twice as much space in the the channel to ensure we can enqueue up-to a full batch in
        // the backlog while we process this one.
        let (sender, receiver) = mpsc::channel(batch_size * 2);

        TaskCenter::spawn_child(
            TaskKind::LogletProvider,
            "local-loglet-writer",
            async move {
                debug!("Start running LogStoreWriter");
                let opts = updateable.live_load();
                let batch_size = std::cmp::max(1, opts.writer_batch_commit_count);
                let batch_duration: Duration = opts.writer_batch_commit_duration.into();
                // We don't want to use chunks_timeout if time-based batching is disabled, why?
                // because even if duration is zero, tokio's timer resolution is 1ms which means
                // that we will delay every batch by 1ms for no reason.
                let receiver = if batch_duration == Duration::ZERO {
                    ReceiverStream::new(receiver)
                        .ready_chunks(batch_size)
                        .boxed()
                } else {
                    ReceiverStream::new(receiver)
                        .chunks_timeout(batch_size, batch_duration)
                        .boxed()
                };
                tokio::pin!(receiver);

                loop {
                    tokio::select! {
                        biased;
                        _ = cancellation_watcher() => {
                            break;
                        }
                        Some(cmds) = TokioStreamExt::next(&mut receiver) => {
                                let opts = updateable.live_load();
                                self.handle_commands(opts, cmds).await;
                        }
                    }
                }
                debug!("Local loglet writer task finished");
                Ok(())
            },
        )?;
        Ok(RocksDbLogWriterHandle { sender })
    }

    async fn handle_commands(
        &mut self,
        opts: &LocalLogletOptions,
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
                    Some(DataUpdate::PutRecords {
                        first_offset,
                        payloads,
                    }) => Self::put_records(
                        &data_cf,
                        buffer,
                        &mut write_batch,
                        command.loglet_id,
                        first_offset,
                        payloads,
                    ),
                    Some(DataUpdate::TrimLog {
                        old_trim_point,
                        new_trim_point,
                    }) => Self::trim_log(
                        &data_cf,
                        buffer,
                        &mut write_batch,
                        command.loglet_id,
                        old_trim_point,
                        new_trim_point,
                    ),
                    None => {}
                }

                // todo: future optimization. pre-merge all updates within a batch before writing
                // the merge to rocksdb.
                if let Some(logstate_updates) = command.log_state_updates {
                    Self::update_log_state(
                        &metadata_cf,
                        &mut write_batch,
                        command.loglet_id,
                        logstate_updates,
                        buffer,
                    )
                }

                if let Some(ack) = command.ack {
                    batch_acks.push(ack);
                }
            }
        }

        histogram!(BIFROST_LOCAL_WRITE_BATCH_SIZE_BYTES).record(write_batch.size_in_bytes() as f64);
        histogram!(BIFROST_LOCAL_WRITE_BATCH_COUNT).record(write_batch.len() as f64);
        self.commit(opts, write_batch).await;
    }

    fn update_log_state(
        metadata_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        loglet_id: u64,
        updates: LogStateUpdates,
        buffer: &mut BytesMut,
    ) {
        let buffer = updates.encode_and_split(buffer).expect("encode");
        write_batch.merge_cf(
            metadata_cf,
            MetadataKey::new(loglet_id, MetadataKind::LogState).to_bytes(),
            buffer,
        );
    }

    fn put_records(
        data_cf: &Arc<BoundColumnFamily>,
        serde_buffer: &mut BytesMut,
        write_batch: &mut WriteBatch,
        id: u64,
        mut offset: LogletOffset,
        payloads: Arc<[Record]>,
    ) {
        serde_buffer.reserve(payloads.len() * RECORD_SIZE_GUESS);
        for payload in payloads.iter() {
            let key_bytes = RecordKey::new(id, offset).encode_and_split(serde_buffer);
            let value_bytes =
                encode_record_and_split(FORMAT_FOR_NEW_APPENDS, payload, serde_buffer);
            write_batch.put_cf(data_cf, key_bytes, value_bytes);
            // advance the offset for the next record
            offset = offset.next();
        }
    }

    fn trim_log(
        data_cf: &Arc<BoundColumnFamily>,
        serde_buffer: &mut BytesMut,
        write_batch: &mut WriteBatch,
        id: u64,
        old_trim_point: LogletOffset,
        new_trim_point: LogletOffset,
    ) {
        // the old trim point has already been removed on the previous trim operation
        let from_bytes = RecordKey::new(id, old_trim_point.next()).encode_and_split(serde_buffer);
        // the upper bound is exclusive for range deletions, therefore we need to increase it
        let to_bytes = RecordKey::new(id, new_trim_point.next()).encode_and_split(serde_buffer);

        trace!(
            loglet_id = id,
            "Trim log range: [{}, {})",
            old_trim_point.next(),
            new_trim_point.next()
        );
        // We probably need to measure whether range delete is better than single deletes for
        // multiple trim operations
        write_batch.delete_range_cf(data_cf, from_bytes, to_bytes);
    }

    async fn commit(&mut self, opts: &LocalLogletOptions, write_batch: WriteBatch) {
        let mut write_opts = rocksdb::WriteOptions::new();
        write_opts.disable_wal(opts.rocksdb.rocksdb_disable_wal());
        write_opts.set_sync(!opts.rocksdb_disable_wal_fsync());

        trace!(
            "Committing local loglet current write batch: {} items",
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
                "local-loglet-write-batch",
                Priority::High,
                io_mode,
                write_opts,
                write_batch,
            )
            .await;

        match result {
            Ok(_batch) => {
                self.send_acks(Ok(()));
            }
            Err((e, _batch)) => {
                error!("Failed to commit local loglet write batch: {}", e);
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
    sender: mpsc::Sender<LogStoreWriteCommand>,
}

impl RocksDbLogWriterHandle {
    pub async fn enqueue_seal(&self, loglet_id: u64) -> Result<AckRecv, ShutdownError> {
        let (ack, receiver) = oneshot::channel();
        let log_state_updates = Some(LogStateUpdates::default().seal());
        self.send_command(LogStoreWriteCommand {
            loglet_id,
            data_update: None,
            log_state_updates,
            ack: Some(ack),
        })
        .await?;
        Ok(receiver)
    }

    pub async fn enqueue_put_records(
        &self,
        loglet_id: u64,
        start_offset: LogletOffset,
        payloads: Arc<[Record]>,
    ) -> Result<AckRecv, ShutdownError> {
        let (ack, receiver) = oneshot::channel();
        // Do not allow more than 65k records in a single batch!
        assert!(payloads.len() <= u16::MAX as usize);
        let last_offset_in_batch = (start_offset + payloads.len() as u32).prev();
        let data_update = DataUpdate::PutRecords {
            first_offset: start_offset,
            payloads,
        };

        let log_state_updates =
            Some(LogStateUpdates::default().update_release_pointer(last_offset_in_batch));
        self.send_command(LogStoreWriteCommand {
            loglet_id,
            data_update: Some(data_update),
            log_state_updates,
            ack: Some(ack),
        })
        .await?;
        Ok(receiver)
    }

    pub async fn enqueue_trim(
        &self,
        loglet_id: u64,
        old_trim_point: LogletOffset,
        new_trim_point: LogletOffset,
    ) -> Result<(), ShutdownError> {
        let data_update = DataUpdate::TrimLog {
            old_trim_point,
            new_trim_point,
        };
        let log_state_updates = Some(LogStateUpdates::default().update_trim_point(new_trim_point));

        self.send_command(LogStoreWriteCommand {
            loglet_id,
            data_update: Some(data_update),
            log_state_updates,
            ack: None,
        })
        .await
    }

    async fn send_command(&self, command: LogStoreWriteCommand) -> Result<(), ShutdownError> {
        if let Err(e) = self.sender.send(command).await {
            warn!(
                "Local loglet writer task is gone, not accepting the record: {}",
                e
            );
            return Err(ShutdownError);
        }
        Ok(())
    }
}
