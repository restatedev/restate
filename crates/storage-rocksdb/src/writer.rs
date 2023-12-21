// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{try_write_batch, DB};
use futures::ready;
use futures_util::FutureExt;
use restate_storage_api::StorageError;
use restate_types::errors::ThreadJoinError;
use rocksdb::WriteBatch;
use std::future::Future;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;

pub struct WriteCommand {
    write_batch: WriteBatch,
    response_tx: Sender<Result<(), StorageError>>,
}

impl WriteCommand {
    pub fn new(write_batch: WriteBatch, response_tx: Sender<Result<(), StorageError>>) -> Self {
        Self {
            write_batch,
            response_tx,
        }
    }
}

type Error = anyhow::Error;

pub struct Writer {
    db: Arc<DB>,
    rx: UnboundedReceiver<WriteCommand>,

    // for creating `WriterHandle`s
    tx: UnboundedSender<WriteCommand>,
}

impl Writer {
    pub fn new(db: Arc<DB>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self { db, rx, tx }
    }

    pub fn create_writer_handle(&self) -> WriterHandle {
        WriterHandle {
            tx: self.tx.clone(),
        }
    }

    pub fn run(self) -> JoinHandle {
        let (tx, rx) = tokio::sync::oneshot::channel();

        std::thread::Builder::new()
            .name("rs:rocksdb".to_owned())
            .spawn(|| {
                // AssertUnwindSafe is safe because we don't access self after catch_unwind again
                let result = panic::catch_unwind(AssertUnwindSafe(|| self.run_inner()));

                // we don't care if the receiver is dropped
                let _ = tx.send(result);
            })
            .expect("RocksDB writer thread should be spawnable");

        JoinHandle::new(rx)
    }

    fn run_inner(self) -> Result<(), Error> {
        let db = self.db;
        let mut rx = self.rx;
        drop(self.tx);

        let mut replies: Vec<Sender<Result<(), StorageError>>> = Vec::new();

        'out: while let Some(WriteCommand {
            write_batch,
            response_tx,
        }) = rx.blocking_recv()
        {
            replies.push(response_tx);
            if !try_write_batch(&db, &mut replies, write_batch) {
                continue;
            }
            //
            // optimistically try taking more batches
            //
            while let Ok(WriteCommand {
                write_batch,
                response_tx,
            }) = rx.try_recv()
            {
                replies.push(response_tx);
                if !try_write_batch(&db, &mut replies, write_batch) {
                    continue 'out;
                }
            }
            //
            // okay now that we wrote everything, let us commit
            //
            db.flush_wal(true)?;
            //
            // notify everyone of the success
            //
            replies.drain(..).for_each(|f| {
                let _ = f.send(Ok(()));
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WriterHandle {
    tx: UnboundedSender<WriteCommand>,
}

impl WriterHandle {
    pub async fn write(&self, write_batch: WriteBatch) -> Result<(), StorageError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WriteCommand::new(write_batch, response_tx);
        self.tx
            .send(command)
            .map_err(|_| StorageError::OperationalError)?;
        response_rx
            .await
            .map_err(|_| StorageError::OperationalError)?
    }
}

pub struct JoinHandle {
    rx: tokio::sync::oneshot::Receiver<std::thread::Result<Result<(), Error>>>,
}

impl JoinHandle {
    fn new(rx: tokio::sync::oneshot::Receiver<std::thread::Result<Result<(), Error>>>) -> Self {
        Self { rx }
    }
}

impl Future for JoinHandle {
    type Output = Result<Result<(), Error>, ThreadJoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(self.rx.poll_unpin(cx));

        Poll::Ready(
            result
                .map_err(|_| ThreadJoinError::UnexpectedTermination)
                .and_then(|result| result.map_err(|panic| ThreadJoinError::Panic(panic))),
        )
    }
}
