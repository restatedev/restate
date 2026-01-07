// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use bytes::BytesMut;
use futures::FutureExt;
use pin_project::pin_project;
use restate_types::logs::Record;
use tokio::sync::{mpsc, oneshot};
use tracing::{trace, warn};

use restate_core::{ShutdownError, TaskCenter, TaskHandle, cancellation_watcher};
use restate_types::storage::StorageEncode;

use crate::error::EnqueueError;
use crate::{Appender, InputRecord, Result};

/// Performs appends in the background concurrently while maintaining the order of records
/// produced from the same producer. It runs as a background task and batches records whenever
/// possible to reduce round-trips to the loglet.
#[pin_project]
pub struct BackgroundAppender<T> {
    appender: Appender,
    /// The number of records that can be buffered in the append queue
    queue_capacity: usize,
    /// The number of records that can get batched together before appending to the log
    max_batch_size: usize,
    /// Reusable vector for buffering recv() operations
    recv_buffer: Vec<AppendOperation>,
    /// Reusable vector for callbacks of enqueue_with_notification calls
    notif_buffer: Vec<oneshot::Sender<()>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> BackgroundAppender<T>
where
    T: StorageEncode,
{
    pub fn new(appender: Appender, queue_capacity: usize, max_batch_size: usize) -> Self {
        Self {
            appender,
            queue_capacity,
            max_batch_size,
            recv_buffer: Vec::with_capacity(max_batch_size),
            notif_buffer: Vec::with_capacity(max_batch_size),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Start the background appender as a TaskCenter background task. Note that the task will not
    /// automatically react to TaskCenter's shutdown signal, it gives control over the shutdown
    /// behaviour to the owner of [`AppenderHandle`] to drain or drop when appropriate.
    pub fn start(self, name: &'static str) -> Result<AppenderHandle<T>, ShutdownError> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.queue_capacity);
        let record_size_limit = self.appender.record_size_limit();

        let handle = TaskCenter::spawn_unmanaged_child(
            restate_core::TaskKind::BifrostAppender,
            name,
            self.run(rx),
        )?;

        Ok(AppenderHandle {
            inner_handle: Some(handle),
            sender: Some(LogSender {
                arena: BytesMut::default(),
                tx,
                record_size_limit,
                _phantom: std::marker::PhantomData,
            }),
        })
    }

    async fn run(self, mut rx: mpsc::Receiver<AppendOperation>) -> Result<()> {
        let Self {
            mut appender,
            max_batch_size,
            mut notif_buffer,
            mut recv_buffer,
            ..
        } = self;

        // fused to avoid a busy loop while draining.
        let mut drain_fut = std::pin::pin!(cancellation_watcher().fuse());
        loop {
            tokio::select! {
                _ = &mut drain_fut => {
                    trace!("Draining the background appender");
                    // stop accepting messages and drain the queue
                    rx.close();
                }
                received = rx.recv_many(&mut recv_buffer, max_batch_size) => {
                    if received == 0 {
                        // channel is closed, appender is drained
                        break;
                    }

                    // The background appender stops if a batch write failed. All enqueued messages
                    // will be dropped and senders will receive [`EnqueueError::Closed`]
                    //
                    // All buffers get reset within `process_appends()`
                    Self::process_appends(
                        &mut appender,
                        &mut recv_buffer,
                        &mut notif_buffer,
                    ).await?;
                }
            }
        }

        Ok(())
    }

    async fn process_appends(
        appender: &mut Appender,
        buffered_records: &mut Vec<AppendOperation>,
        notif_buffer: &mut Vec<oneshot::Sender<()>>,
    ) -> Result<()> {
        let mut batch = Vec::with_capacity(buffered_records.len());
        for record in buffered_records.drain(..) {
            match record {
                AppendOperation::Enqueue(record) => {
                    batch.push(record);
                }
                AppendOperation::EnqueueWithNotification(record, tx) => {
                    batch.push(record);
                    notif_buffer.push(tx);
                }
                AppendOperation::Canary(tx) => {
                    notif_buffer.push(tx);
                }
                AppendOperation::MarkAsPreferred => {
                    appender.mark_as_preferred();
                }
                AppendOperation::ForgetPreference => {
                    appender.forget_preference();
                }
            }
        }

        // Failure to append will stop the whole task
        if !batch.is_empty() {
            appender.append_batch_erased(batch.into()).await?;
        }

        // Notify those who asked for a commit notification
        notif_buffer.drain(..).for_each(|tx| {
            let _ = tx.send(());
        });
        // Clear buffers
        notif_buffer.clear();
        Ok(())
    }
}

/// Handle of the background appender.
///
/// Dropping this handle will async-request a graceful drain of the background task with no guarantee
/// on whether pending appends have completed or not. The safest way to drain this
/// handle before dropping is to call [`Self::drain()`] to wait for all enqueued appends
/// to complete and to reject new enqueues.
pub struct AppenderHandle<T> {
    // This is always Some(). This is only set to None on detach().
    sender: Option<LogSender<T>>,
    inner_handle: Option<TaskHandle<Result<()>>>,
}

impl<T> Drop for AppenderHandle<T> {
    fn drop(&mut self) {
        // trigger drain on drop but don't block.
        if let Some(handle) = self.inner_handle.as_ref() {
            handle.cancel()
        }
    }
}

impl<T> AppenderHandle<T> {
    /// Detaches the handle from the background task. When detached, the background task will
    /// automatically be drained and stopped after all LogSender instances are dropped.
    pub fn detach(mut self) -> LogSender<T> {
        let sender = std::mem::take(&mut self.sender);
        // do not run the destructor because we don't want to drain. If the last sender (perhaps
        // the one we are just returning below) is dropped, the background task will stop.
        std::mem::forget(self);
        sender.unwrap()
    }

    pub async fn drain(mut self) -> Result<(), ShutdownError> {
        let handle = std::mem::take(&mut self.inner_handle);
        // We are confident that handle is set. This is option just to support the Drop trait
        // implementation requirements.
        let handle = handle.unwrap();

        // trigger the drain
        handle.cancel();
        // wait for the receiver to drop (appender terminates)
        self.sender.as_ref().unwrap().tx.closed().await;

        // What to do if task panics!
        if let Err(err) = handle.await {
            warn!(
                ?err,
                "Appender task might have been cancelled or panicked while draining",
            );
        }
        Ok(())
    }

    /// If you need an owned LogSender, clone this.
    pub fn sender(&mut self) -> &mut LogSender<T> {
        self.sender.as_mut().unwrap()
    }

    /// Waits for the underlying appender task to finish.
    ///
    /// This function will return immediately if the underlying appender task has already finished.
    ///
    /// The appender task runs as an unmanaged task for which the [`TaskCenter`] will not handle
    /// errors. If the user of the [`BackgroundAppender`] requires the proper functioning of the
    /// appender task but does not want to track it on a per-record basis via
    /// [`LogSender::enqueue_with_notification`], then this method can be used to react to task
    /// failures or unexpected terminations.
    pub async fn join(&mut self) -> Result<()> {
        self.inner_handle.as_mut().expect("must be present").await?
    }
}

pub struct LogSender<T> {
    arena: BytesMut,
    tx: tokio::sync::mpsc::Sender<AppendOperation>,
    record_size_limit: NonZeroUsize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Clone for LogSender<T> {
    fn clone(&self) -> Self {
        Self {
            arena: BytesMut::default(),
            tx: self.tx.clone(),
            record_size_limit: self.record_size_limit,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: StorageEncode> LogSender<T> {
    fn check_record_size<E>(&self, record: &Record) -> Result<(), EnqueueError<E>> {
        let record_size = record.estimated_encode_size();
        if record_size > self.record_size_limit.get() {
            return Err(EnqueueError::RecordTooLarge {
                record_size,
                limit: self.record_size_limit,
            });
        }
        Ok(())
    }

    /// Attempt to enqueue a record to the appender. Returns immediately if the
    /// appender is pushing back or if the appender is draining or drained.
    pub fn try_enqueue<A>(&mut self, record: A) -> Result<(), EnqueueError<A>>
    where
        A: Into<InputRecord<T>>,
    {
        let permit = match self.tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => return Err(EnqueueError::Full(record)),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(EnqueueError::Closed(record)),
        };

        let record = record.into().into_record().ensure_encoded(&mut self.arena);
        self.check_record_size(&record)?;
        permit.send(AppendOperation::Enqueue(record));
        Ok(())
    }

    /// Enqueues an append and returns a commit token
    pub fn try_enqueue_with_notification<A>(
        &mut self,
        record: A,
    ) -> Result<CommitToken, EnqueueError<A>>
    where
        A: Into<InputRecord<T>>,
    {
        let permit = match self.tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => return Err(EnqueueError::Full(record)),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(EnqueueError::Closed(record)),
        };

        let (tx, rx) = oneshot::channel();
        let record = record.into().into_record().ensure_encoded(&mut self.arena);
        self.check_record_size(&record)?;
        permit.send(AppendOperation::EnqueueWithNotification(record, tx));
        Ok(CommitToken { rx })
    }

    /// Waits for capacity on the channel and returns an error if the appender is
    /// draining or drained.
    pub async fn enqueue<A>(&mut self, record: A) -> Result<(), EnqueueError<A>>
    where
        A: Into<InputRecord<T>>,
    {
        let Ok(permit) = self.tx.reserve().await else {
            return Err(EnqueueError::Closed(record));
        };
        let record = record.into().into_record().ensure_encoded(&mut self.arena);
        self.check_record_size(&record)?;
        permit.send(AppendOperation::Enqueue(record));

        Ok(())
    }

    /// Attempt to enqueue a record to the appender. Returns immediately if the
    /// appender is pushing back or if the appender is draining or drained.
    ///
    /// Attempts to enqueue all records in the iterator. This will immediately return if there is
    /// no capacity in the channel to enqueue _all_ records.
    pub fn try_enqueue_many<I, A>(&mut self, records: I) -> Result<(), EnqueueError<I>>
    where
        I: Iterator<Item = A> + ExactSizeIterator,
        A: Into<InputRecord<T>>,
    {
        let permits = match self.tx.try_reserve_many(records.len()) {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => return Err(EnqueueError::Full(records)),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(EnqueueError::Closed(records)),
        };

        for (permit, record) in std::iter::zip(permits, records) {
            let record = record.into().into_record().ensure_encoded(&mut self.arena);
            self.check_record_size(&record)?;
            permit.send(AppendOperation::Enqueue(record));
        }
        Ok(())
    }

    /// Attempts to enqueue all records in the iterator. This function waits until there is enough
    /// capacity in the channel to enqueue _all_ records to avoid partial enqueues.
    ///
    /// The method is cancel safe in the sense that if enqueue_many is used in a `tokio::select!`,
    /// no records are enqueued if another branch completed.
    pub async fn enqueue_many<I, A>(&mut self, records: I) -> Result<(), EnqueueError<I>>
    where
        I: Iterator<Item = A> + ExactSizeIterator,
        A: Into<InputRecord<T>>,
    {
        let Ok(permits) = self.tx.reserve_many(records.len()).await else {
            return Err(EnqueueError::Closed(records));
        };

        for (permit, record) in std::iter::zip(permits, records) {
            let record = record.into().into_record().ensure_encoded(&mut self.arena);
            self.check_record_size(&record)?;
            permit.send(AppendOperation::Enqueue(record));
        }

        Ok(())
    }

    /// Enqueues a record and returns a [`CommitToken`] future that's resolved when the record is
    /// committed.
    pub async fn enqueue_with_notification<A>(
        &mut self,
        record: A,
    ) -> Result<CommitToken, EnqueueError<A>>
    where
        A: Into<InputRecord<T>>,
    {
        let Ok(permit) = self.tx.reserve().await else {
            return Err(EnqueueError::Closed(record));
        };

        let (tx, rx) = oneshot::channel();
        let record = record.into().into_record().ensure_encoded(&mut self.arena);
        self.check_record_size(&record)?;
        permit.send(AppendOperation::EnqueueWithNotification(record, tx));

        Ok(CommitToken { rx })
    }

    /// Returns a [`CommitToken`] that is resolved once all previously enqueued records are committed.
    pub async fn notify_committed(&self) -> Result<CommitToken, EnqueueError<()>> {
        let Ok(permit) = self.tx.reserve().await else {
            // channel is closed, this should happen the appender is draining or has been darained
            // already
            return Err(EnqueueError::Closed(()));
        };

        let (tx, rx) = oneshot::channel();
        let canary = AppendOperation::Canary(tx);
        permit.send(canary);

        Ok(CommitToken { rx })
    }

    /// Marks this node as a preferred writer for the underlying log
    pub async fn mark_as_preferred(&self) -> Result<(), EnqueueError<()>> {
        if self
            .tx
            .send(AppendOperation::MarkAsPreferred)
            .await
            .is_err()
        {
            return Err(EnqueueError::Closed(()));
        };

        Ok(())
    }

    /// Removes the preference about this node being the preferred writer for the log
    pub async fn forget_preference(&self) -> Result<(), EnqueueError<()>> {
        if self
            .tx
            .send(AppendOperation::ForgetPreference)
            .await
            .is_err()
        {
            return Err(EnqueueError::Closed(()));
        };

        Ok(())
    }
}

/// A future that resolves when a record is committed by the background appender.
pub struct CommitToken {
    rx: oneshot::Receiver<()>,
}

impl std::future::Future for CommitToken {
    type Output = Result<(), oneshot::error::RecvError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.rx.poll_unpin(cx)
    }
}

enum AppendOperation {
    Enqueue(Record),
    EnqueueWithNotification(Record, oneshot::Sender<()>),
    // A message denoting a request to be notified when it's processed by the appender.
    // It's used to check if previously enqueued appends have been committed or not
    Canary(oneshot::Sender<()>),
    /// Let's bifrost know that this node is the preferred writer of this log
    MarkAsPreferred,
    /// Let's bifrost know that this node might not be the preferred writer of this log
    ForgetPreference,
}
