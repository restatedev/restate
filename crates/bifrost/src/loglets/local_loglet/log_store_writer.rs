// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use bytes::{Bytes, BytesMut};
use futures::StreamExt as FutureStreamExt;
use metrics::histogram;
use restate_rocksdb::{IoMode, Priority, RocksDb};
use rocksdb::{BoundColumnFamily, WriteBatch};
use smallvec::SmallVec;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{debug, error, trace, warn};

use restate_core::{cancellation_watcher, task_center, ShutdownError, TaskKind};
use restate_types::config::LocalLogletOptions;
use restate_types::live::BoxedLiveLoad;
use restate_types::logs::SequenceNumber;

use crate::loglet::LogletOffset;
use crate::{Error, SMALL_BATCH_THRESHOLD_COUNT};

use super::keys::{MetadataKey, MetadataKind, RecordKey};
use super::log_state::LogStateUpdates;
use super::log_store::{DATA_CF, METADATA_CF};
use super::metric_definitions::{
    BIFROST_LOCAL_WRITE_BATCH_COUNT, BIFROST_LOCAL_WRITE_BATCH_SIZE_BYTES,
};

type Ack = oneshot::Sender<Result<(), Error>>;
type AckRecv = oneshot::Receiver<Result<(), Error>>;

pub struct LogStoreWriteCommand {
    log_id: u64,
    data_updates: SmallVec<[DataUpdate; SMALL_BATCH_THRESHOLD_COUNT]>,
    log_state_updates: Option<LogStateUpdates>,
    ack: Option<Ack>,
}

enum DataUpdate {
    PutRecord {
        offset: LogletOffset,
        data: Bytes,
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
            buffer: BytesMut::default(),
        }
    }

    /// Must be called from task_center context
    pub fn start(
        mut self,
        mut updateable: BoxedLiveLoad<LocalLogletOptions>,
    ) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        // big enough to allows a second full batch to queue up while the existing one is being processed
        let batch_size = std::cmp::max(1, updateable.live_load().writer_batch_commit_count);
        // leave twice as much space in the the channel to ensure we can enqueue up-to a full batch in
        // the backlog while we process this one.
        let (sender, receiver) = mpsc::channel(batch_size * 2);

        task_center().spawn_child(
            TaskKind::LogletProvider,
            "local-loglet-writer",
            None,
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
        buffer.clear();

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
                for data_command in command.data_updates {
                    match data_command {
                        DataUpdate::PutRecord { offset, data } => Self::put_record(
                            &data_cf,
                            &mut write_batch,
                            command.log_id,
                            offset,
                            data,
                        ),
                        DataUpdate::TrimLog {
                            old_trim_point,
                            new_trim_point,
                        } => Self::trim_log(
                            &data_cf,
                            &mut write_batch,
                            command.log_id,
                            old_trim_point,
                            new_trim_point,
                        ),
                    }
                }

                // todo: future optimization. pre-merge all updates within a batch before writing
                // the merge to rocksdb.
                if let Some(logstate_updates) = command.log_state_updates {
                    Self::update_log_state(
                        &metadata_cf,
                        &mut write_batch,
                        command.log_id,
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
        log_id: u64,
        updates: LogStateUpdates,
        buffer: &mut BytesMut,
    ) {
        updates.encode(buffer).expect("encode");
        write_batch.merge_cf(
            metadata_cf,
            MetadataKey::new(log_id, MetadataKind::LogState).to_bytes(),
            buffer,
        );
    }

    fn put_record(
        data_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        id: u64,
        offset: LogletOffset,
        data: Bytes,
    ) {
        let key = RecordKey::new(id, offset);
        write_batch.put_cf(data_cf, &key.to_bytes(), data);
    }

    fn trim_log(
        data_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        id: u64,
        old_trim_point: LogletOffset,
        new_trim_point: LogletOffset,
    ) {
        // the old trim point has already been removed on the previous trim operation
        let from = RecordKey::new(id, old_trim_point.next());
        // the upper bound is exclusive for range deletions, therefore we need to increase it
        let to = RecordKey::new(id, new_trim_point.next());

        trace!("Trim log range: [{from:?}, {to:?})");
        // We probably need to measure whether range delete is better than single deletes for
        // multiple trim operations
        write_batch.delete_range_cf(data_cf, &from.to_bytes(), &to.to_bytes());
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

        if let Err(e) = result {
            error!("Failed to commit local loglet write batch: {}", e);
            self.send_acks(Err(Error::LogStoreError(e.into())));
            return;
        }

        self.send_acks(Ok(()));
    }

    fn send_acks(&mut self, result: Result<(), Error>) {
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
    pub async fn enqueue_put_record(
        &self,
        log_id: u64,
        offset: LogletOffset,
        data: Bytes,
    ) -> Result<AckRecv, ShutdownError> {
        self.enqueue_put_records(log_id, offset, &[data]).await
    }

    pub async fn enqueue_put_records(
        &self,
        log_id: u64,
        mut start_offset: LogletOffset,
        records: &[Bytes],
    ) -> Result<AckRecv, ShutdownError> {
        let (ack, receiver) = oneshot::channel();
        let mut data_updates = SmallVec::with_capacity(records.len());
        for record in records {
            data_updates.push(DataUpdate::PutRecord {
                offset: start_offset,
                data: record.clone(),
            });
            start_offset = start_offset.next();
        }

        let data_updates = data_updates;
        let log_state_updates =
            Some(LogStateUpdates::default().update_release_pointer(start_offset.prev()));
        self.send_command(LogStoreWriteCommand {
            log_id,
            data_updates,
            log_state_updates,
            ack: Some(ack),
        })
        .await?;
        Ok(receiver)
    }

    pub async fn enqueue_trim(
        &self,
        log_id: u64,
        old_trim_point: LogletOffset,
        new_trim_point: LogletOffset,
    ) -> Result<(), ShutdownError> {
        let mut data_updates = SmallVec::with_capacity(1);
        data_updates.push(DataUpdate::TrimLog {
            old_trim_point,
            new_trim_point,
        });
        let log_state_updates = Some(LogStateUpdates::default().update_trim_point(new_trim_point));

        self.send_command(LogStoreWriteCommand {
            log_id,
            data_updates,
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
