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

use bytes::Bytes;
use rocksdb::{WriteBatch, DB};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{debug, trace, warn};

use restate_core::{cancellation_watcher, task_center, ShutdownError, TaskKind};

use crate::loglet::LogletOffset;
use crate::Error;

use super::keys::{MetadataKey, MetadataKind, RecordKey};
use super::log_state::LogStateUpdates;
use super::log_store::{DATA_CF, METADATA_CF};

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub channel_size: usize,
    pub commit_time_interval: Duration,
    pub batch_size_threshold: usize,
    pub flush_wal_on_commit: bool,
    pub disable_wal: bool,
}

type Ack = oneshot::Sender<Result<(), Error>>;
type AckRecv = oneshot::Receiver<Result<(), Error>>;

pub struct LogStoreWriteCommand {
    log_id: u64,
    data_update: Option<DataUpdate>,
    log_state_updates: Option<LogStateUpdates>,
    ack: Option<Ack>,
}

enum DataUpdate {
    PutRecord { offset: LogletOffset, data: Bytes },
}

pub(crate) struct LogStoreWriter {
    opts: WriterOptions,
    rocksdb_write_options: rocksdb::WriteOptions,
    current_batch: WriteBatch,
    current_batch_acks: Vec<Ack>,
    db: Arc<DB>,
    commit_timer: tokio::time::Interval,
}

impl LogStoreWriter {
    pub(crate) fn new(db: Arc<DB>, opts: WriterOptions) -> Self {
        let mut rocksdb_write_options = rocksdb::WriteOptions::new();
        rocksdb_write_options.disable_wal(opts.disable_wal);
        let mut commit_timer = tokio::time::interval(opts.commit_time_interval);
        commit_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            opts,
            rocksdb_write_options,
            current_batch: WriteBatch::default(),
            current_batch_acks: Default::default(),
            db,
            commit_timer,
        }
    }

    /// Must be called from task_center context
    pub fn start(mut self) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        let (sender, mut receiver) = mpsc::channel(self.opts.channel_size);
        task_center().spawn_child(
            TaskKind::LogletProvider,
            "local-loglet-writer",
            None,
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancellation_watcher() => {
                            break;
                        }
                        _ = self.commit_timer.tick() => {
                            if !self.current_batch.is_empty() {
                                trace!("Triggering local loglet write batch commit due to max latency");
                                self.commit();
                            }
                        }
                        cmd = receiver.recv() => {
                            if let Some(cmd) = cmd {
                                self.handle_command(cmd);
                            } else {
                                break;
                            }
                        }
                    }
                }
                self.commit();
                debug!("Local loglet writer task finished");
                Ok(())
            },
        )?;
        Ok(RocksDbLogWriterHandle { sender })
    }

    fn handle_command(&mut self, command: LogStoreWriteCommand) {
        if let Some(data_command) = command.data_update {
            match data_command {
                DataUpdate::PutRecord { offset, data } => {
                    self.put_record(command.log_id, offset, data)
                }
            }
        }

        if let Some(logstate_updates) = command.log_state_updates {
            self.update_log_state(command.log_id, logstate_updates)
        }

        if let Some(ack) = command.ack {
            self.current_batch_acks.push(ack);
        }

        if self.current_batch.len() >= self.opts.batch_size_threshold {
            trace!("Local loglet write batch is full, committing");
            self.commit();
        }
    }

    fn update_log_state(&mut self, log_id: u64, updates: LogStateUpdates) {
        let metadata_cf = self.db.cf_handle(METADATA_CF).expect("metadata cf exists");
        self.current_batch.merge_cf(
            metadata_cf,
            MetadataKey::new(log_id, MetadataKind::LogState).to_bytes(),
            updates.to_bytes(),
        );
    }

    fn put_record(&mut self, id: u64, offset: LogletOffset, data: Bytes) {
        let key = RecordKey::new(id, offset);
        let data_cf = self.db.cf_handle(DATA_CF).expect("data cf exists");
        self.current_batch.put_cf(data_cf, &key.to_bytes(), data);
    }

    fn commit(&mut self) {
        let current_batch = std::mem::take(&mut self.current_batch);
        trace!(
            "Committing local loglet current write batch: {} items",
            current_batch.len(),
        );

        if let Err(e) = self
            .db
            .write_opt(current_batch, &self.rocksdb_write_options)
        {
            warn!("Failed to commit local loglet write batch: {}", e);
            self.send_acks(Err(Error::LogStoreError(e.into())));
            return;
        }

        if self.opts.flush_wal_on_commit && !self.opts.disable_wal {
            if let Err(e) = self.db.flush_wal(true) {
                warn!("Failed to flush rocksdb WAL in local loglet : {}", e);
                self.send_acks(Err(Error::LogStoreError(e.into())));
                return;
            }
        }
        self.commit_timer
            .reset_after(self.opts.commit_time_interval);
        self.send_acks(Ok(()));
    }

    fn send_acks(&mut self, result: Result<(), Error>) {
        self.current_batch_acks.drain(..).for_each(|a| {
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
        release_immediately: bool,
    ) -> Result<AckRecv, ShutdownError> {
        let (ack, receiver) = oneshot::channel();
        let data_update = Some(DataUpdate::PutRecord { offset, data });
        let log_state_updates = if release_immediately {
            Some(LogStateUpdates::default().update_release_pointer(offset))
        } else {
            None
        };
        if let Err(e) = self
            .sender
            .send(LogStoreWriteCommand {
                log_id,
                data_update,
                log_state_updates,
                ack: Some(ack),
            })
            .await
        {
            warn!(
                "Local loglet writer task is gone, not accepting the record: {}",
                e
            );
            return Err(ShutdownError);
        }

        Ok(receiver)

        // receiver.await.unwrap_or_else(|_| {
        //     warn!("Unsure if the local loglet record was written, the ack channel was dropped");
        //     Err(Error::Shutdown(ShutdownError))
        // })
    }
}
