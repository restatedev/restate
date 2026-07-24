// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_memory::{MemoryLease, MemoryPool, NonZeroByteCount};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tracing::{trace, warn};

use restate_core::{ShutdownError, TaskCenter, TaskHandle, cancellation_token};
use restate_types::logs::Record;
use restate_types::storage::StorageEncode;

use crate::bifrost::PreferenceControl;
use crate::error::EnqueueError;
use crate::{Appender, InputRecord, Result};

/// Performs appends in the background concurrently while maintaining the order of records
/// produced from the same producer. It runs as a background task and batches records whenever
/// possible to reduce round-trips to the loglet.
#[pin_project]
pub struct BackgroundAppender<T> {
    appender: Appender<T>,
    /// Memory pool that buffered records are charged against (see [`LogSender::enqueue`]).
    /// The appender enforces the memory pool budget at enqueue time.
    memory_pool: MemoryPool,
    /// The number of records that can get batched together before appending to the log
    max_batch_size: usize,
    /// Reusable vector for buffering recv() operations
    current_batch: Batch,
    /// Reusable vector for callbacks of enqueue_with_notification calls
    notif_buffer: Vec<oneshot::Sender<()>>,
}

impl<T: StorageEncode> BackgroundAppender<T> {
    /// If `memory_limit` is provided, each buffered record reserves its estimated encoded size
    /// from an internally owned pool (going into overdraft if needed) and releases it after the
    /// record is durably appended.
    pub fn new(
        appender: Appender<T>,
        memory_limit: Option<NonZeroByteCount>,
        max_batch_size: usize,
    ) -> Self {
        Self {
            appender,
            memory_pool: memory_limit
                .map(MemoryPool::with_capacity)
                .unwrap_or(MemoryPool::unlimited()),
            max_batch_size,
            current_batch: Batch::with_capacity(max_batch_size),
            notif_buffer: Vec::with_capacity(max_batch_size),
        }
    }

    /// Start the background appender as a TaskCenter background task. Note that the task will not
    /// automatically react to TaskCenter's shutdown signal, it gives control over the shutdown
    /// behaviour to the owner of [`AppenderHandle`] to drain or drop when appropriate.
    pub fn start(self, name: &'static str) -> Result<AppenderHandle<T>, ShutdownError> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mem_pool = self.memory_pool.clone();
        let record_size_limit = self.appender.record_size_limit();
        let preference = self.appender.preference.clone();

        let handle = TaskCenter::spawn_unmanaged_child(
            restate_core::TaskKind::BifrostAppender,
            name,
            self.run(rx),
        )?;

        Ok(AppenderHandle {
            inner_handle: Some(handle),
            sender: Some(LogSender {
                arena: BytesMut::default(),
                mem_pool,
                tx,
                record_size_limit,
                preference,
                _phantom: std::marker::PhantomData,
            }),
        })
    }

    async fn run(self, mut rx: mpsc::UnboundedReceiver<AppendOperation>) -> Result<()> {
        let Self {
            mut appender,
            max_batch_size,
            mut notif_buffer,
            mut current_batch,
            ..
        } = self;

        let cancel_token = cancellation_token();
        // to avoid a busy loop while draining.
        let mut draining = false;

        let batch_limit_bytes = appender.record_size_limit().get();

        'main: loop {
            // Wait for the next operation or drain signal
            let op = tokio::select! {
                biased;
                _ = cancel_token.cancelled(), if !draining => {
                    trace!("Draining the background appender");
                    // stop accepting messages and drain the queue
                    rx.close();
                    draining = true;
                    continue;
                }
                Some(op) = rx.recv() => op,
                else => { break 'main; }
            };

            // Check if this operation fits in the current batch (by size and count)
            if current_batch.can_fit(op.cost_in_bytes(), batch_limit_bytes, max_batch_size) {
                current_batch.push(op);
            } else {
                // Current batch is full, flush it first
                Self::process_appends(&mut appender, &mut current_batch, &mut notif_buffer).await?;
                // Then add the operation to the new (empty) batch
                current_batch.push(op);
            }

            // Opportunistically drain the queue to fill the batch
            'opportunistic: loop {
                match rx.try_recv() {
                    Ok(op)
                        if current_batch.can_fit(
                            op.cost_in_bytes(),
                            batch_limit_bytes,
                            max_batch_size,
                        ) =>
                    {
                        current_batch.push(op);
                    }
                    Ok(op) => {
                        // Batch is full, flush it
                        Self::process_appends(&mut appender, &mut current_batch, &mut notif_buffer)
                            .await?;
                        // Add op to the next batch
                        current_batch.push(op);
                    }
                    Err(TryRecvError::Empty) => {
                        // No more messages available, flush current batch and wait for more
                        Self::process_appends(&mut appender, &mut current_batch, &mut notif_buffer)
                            .await?;
                        break 'opportunistic;
                    }
                    Err(TryRecvError::Disconnected) => {
                        // Channel closed, flush remaining and exit outer loop
                        break 'main;
                    }
                }
            }
        }

        // Make sure to flush any remaining items before exiting.
        Self::process_appends(&mut appender, &mut current_batch, &mut notif_buffer).await?;

        Ok(())
    }

    async fn process_appends(
        appender: &mut Appender<T>,
        buffered_records: &mut Batch,
        notif_buffer: &mut Vec<oneshot::Sender<()>>,
    ) -> Result<()> {
        let mut batch = Vec::with_capacity(buffered_records.inner.len());
        // Holds the batch's memory leases until the append completes.
        let mut leases = Vec::with_capacity(buffered_records.inner.len());
        for record in buffered_records.inner.drain(..) {
            match record {
                AppendOperation::Enqueue(record, lease) => {
                    batch.push(record);
                    leases.push(lease);
                }
                AppendOperation::EnqueueWithNotification(record, tx, lease) => {
                    batch.push(record);
                    leases.push(lease);
                    notif_buffer.push(tx);
                }
                AppendOperation::Canary(tx) => {
                    notif_buffer.push(tx);
                }
            }
        }

        // Failure to append will stop the whole task
        if !batch.is_empty() {
            appender.append_batch_erased(batch.into()).await?;
        }

        // Records are durably appended, we can now drop the leases.
        drop(leases);

        // Notify those who asked for a commit notification
        notif_buffer.drain(..).for_each(|tx| {
            let _ = tx.send(());
        });
        // Clear buffers
        notif_buffer.clear();
        buffered_records.reset();

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

    /// Returns the sender owned by this handle.
    pub fn sender(&mut self) -> &mut LogSender<T> {
        self.sender.as_mut().unwrap()
    }

    /// Returns a shared reference to the sender owned by this handle.
    pub fn sender_ref(&self) -> &LogSender<T> {
        self.sender.as_ref().unwrap()
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

pub struct EnqueueWithNotificationResult {
    pub commit_token: CommitToken,
    pub bytes_written: usize,
}

pub struct LogSender<T> {
    arena: BytesMut,
    mem_pool: MemoryPool,
    tx: tokio::sync::mpsc::UnboundedSender<AppendOperation>,
    record_size_limit: NonZeroUsize,
    /// Controls the recovery preference of the underlying appender.
    ///
    /// This is shared between the handle and the appender.
    preference: PreferenceControl,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: StorageEncode> LogSender<T> {
    /// Returns whether the next record or batch can be admitted.
    pub fn has_capacity(&self) -> bool {
        self.mem_pool.available() > 0
    }

    /// Waits until the next record or batch can be admitted.
    pub fn wait_for_capacity(&self) -> impl std::future::Future<Output = ()> + 'static {
        let mem_pool = self.mem_pool.clone();
        async move { mem_pool.wait_until_available().await }
    }

    fn admit<E>(&self, payload: E) -> Result<E, EnqueueError<E>> {
        if self.tx.is_closed() {
            Err(EnqueueError::Closed(payload))
        } else if self.mem_pool.available() == 0 {
            Err(EnqueueError::Full(payload))
        } else {
            Ok(payload)
        }
    }

    fn check_record_size<E>(&self, record: &Record) -> Result<usize, EnqueueError<E>> {
        let record_size = record.estimated_encode_size();
        if record_size > self.record_size_limit.get() {
            return Err(EnqueueError::RecordTooLarge {
                record_size,
                limit: self.record_size_limit,
            });
        }
        Ok(record_size)
    }

    /// Enqueues a record while charging its estimated size to the appender's memory
    /// pool. Returns [`EnqueueError::Full`] without encoding the record if the pool is exhausted.
    /// Otherwise, the record is admitted and may put the pool into deficit once its encoded size
    /// is known. Also returns an error if the record exceeds the size limit or if the appender is
    /// draining or drained.
    pub fn enqueue<A>(&mut self, record: A) -> Result<usize, EnqueueError<Record>>
    where
        A: Into<InputRecord<T>>,
    {
        let record = self.admit(record.into().into_record())?;
        let record = record.ensure_encoded(&mut self.arena);
        let record_size = self.check_record_size(&record)?;
        let lease = self.mem_pool.force_reserve(record_size);
        self.tx
            .send(AppendOperation::Enqueue(record, lease))
            .map_err(|r| {
                EnqueueError::Closed(record_from_append_operation(r.0).expect("has record"))
            })?;

        Ok(record_size)
    }

    /// Enqueues a record bypassing all memory accounting (i.e. will never fail with `Full`).
    pub fn enqueue_unaccounted<A>(&mut self, record: A) -> Result<usize, EnqueueError<Record>>
    where
        A: Into<InputRecord<T>>,
    {
        let record = record.into().into_record().ensure_encoded(&mut self.arena);
        let record_size = self.check_record_size(&record)?;
        self.tx
            .send(AppendOperation::Enqueue(record, MemoryLease::unlinked()))
            .map_err(|r| {
                EnqueueError::Closed(record_from_append_operation(r.0).expect("has record"))
            })?;

        Ok(record_size)
    }

    /// Enqueues a record while charging its estimated size to the appender's memory
    /// pool. Returns [`EnqueueError::Full`] without encoding the record if the pool is exhausted.
    /// Otherwise, the record is admitted and may put the pool into deficit once its encoded size
    /// is known. Returns an error if the appender is draining or drained.
    ///
    /// Unlike [`enqueue`](Self::enqueue), this does not check the record size and
    /// accepts a record of any size.
    ///
    /// Callers have to ensure that record is not larger than the network message size limit.
    pub fn enqueue_unchecked<A>(&mut self, record: A) -> Result<usize, EnqueueError<Record>>
    where
        A: Into<InputRecord<T>>,
    {
        let record = self.admit(record.into().into_record())?;
        let record = record.ensure_encoded(&mut self.arena);
        let record_size = record.estimated_encode_size();
        let lease = self.mem_pool.force_reserve(record_size);
        self.tx
            .send(AppendOperation::Enqueue(record, lease))
            .map_err(|r| {
                EnqueueError::Closed(record_from_append_operation(r.0).expect("has record"))
            })?;

        Ok(record_size)
    }

    /// Enqueues all records in the iterator without blocking, charging their estimated sizes to
    /// the appender's memory pool.
    ///
    /// Fails with [`EnqueueError::Full`] if the memory pool is exhausted. Otherwise, the records
    /// are admitted as one overdraft unit that may put the pool in deficit by as much as the sum of
    /// of the record sizes.
    ///
    /// Note that records are enqueued one at a time: if a record fails the size check or the
    /// appender is draining, records enqueued before the failure remain in the queue. The function
    /// returns the estimated encoded size of all records enqueued.
    pub fn enqueue_many<I, A>(&mut self, records: I) -> Result<usize, EnqueueError<()>>
    where
        I: Iterator<Item = A> + ExactSizeIterator,
        A: Into<InputRecord<T>>,
    {
        if records.len() == 0 {
            return Ok(0);
        }
        self.admit(())?;

        let mut written_bytes = 0;
        for record in records {
            let record = record.into().into_record().ensure_encoded(&mut self.arena);
            let record_size = self.check_record_size(&record)?;
            let lease = self.mem_pool.force_reserve(record_size);
            self.tx
                .send(AppendOperation::Enqueue(record, lease))
                .map_err(|_| EnqueueError::Closed(()))?;
            written_bytes += record_size;
        }

        Ok(written_bytes)
    }

    /// Enqueues all records in the iterator without blocking or checking their encoded sizes.
    /// The entire iterator is admitted as one overdraft unit.
    ///
    /// Fails with [`EnqueueError::Full`] if the memory pool is exhausted. Otherwise, the records
    /// are admitted as one overdraft unit that may put the pool in deficit by as much as the sum of
    /// of the record sizes.
    ///
    /// Callers have to ensure that every record is not larger than the network message size limit.
    pub fn enqueue_many_unchecked<I, A>(&mut self, records: I) -> Result<usize, EnqueueError<()>>
    where
        I: Iterator<Item = A> + ExactSizeIterator,
        A: Into<InputRecord<T>>,
    {
        if records.len() == 0 {
            return Ok(0);
        }
        self.admit(())?;

        let mut written_bytes = 0;
        for record in records {
            let record = record.into().into_record().ensure_encoded(&mut self.arena);
            let record_size = record.estimated_encode_size();
            let lease = self.mem_pool.force_reserve(record_size);
            self.tx
                .send(AppendOperation::Enqueue(record, lease))
                .map_err(|_| EnqueueError::Closed(()))?;
            written_bytes += record_size;
        }

        Ok(written_bytes)
    }

    /// Enqueues a record and returns its estimated encoded size together with a [`CommitToken`]
    /// future that's resolved when the record is committed.
    /// Returns [`EnqueueError::Full`] without encoding the record if the pool is exhausted.
    /// Otherwise, the record is admitted and may put the pool into deficit once its encoded size
    /// is known.
    pub fn enqueue_with_notification<A>(
        &mut self,
        record: A,
    ) -> Result<EnqueueWithNotificationResult, EnqueueError<Record>>
    where
        A: Into<InputRecord<T>>,
    {
        let record = self.admit(record.into().into_record())?;
        let (tx, rx) = oneshot::channel();
        let record = record.ensure_encoded(&mut self.arena);
        let record_size = self.check_record_size(&record)?;
        let lease = self.mem_pool.force_reserve(record_size);
        self.tx
            .send(AppendOperation::EnqueueWithNotification(record, tx, lease))
            .map_err(|a| {
                EnqueueError::Closed(record_from_append_operation(a.0).expect("has record"))
            })?;

        Ok(EnqueueWithNotificationResult {
            commit_token: CommitToken { rx },
            bytes_written: record_size,
        })
    }

    /// Returns a [`CommitToken`] that is resolved once all previously enqueued records are committed.
    pub fn notify_committed(&self) -> Result<CommitToken, EnqueueError<()>> {
        let (tx, rx) = oneshot::channel();
        let canary = AppendOperation::Canary(tx);
        self.tx.send(canary).map_err(|_| EnqueueError::Closed(()))?;

        Ok(CommitToken { rx })
    }

    /// Marks this node as a preferred writer for the underlying log
    pub fn mark_as_preferred(&self) {
        self.preference.mark_as_preferred();
    }

    /// Removes the preference about this node being the preferred writer for the log
    pub fn forget_preference(&self) {
        self.preference.forget_preference();
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
    Enqueue(Record, MemoryLease),
    EnqueueWithNotification(Record, oneshot::Sender<()>, MemoryLease),
    // A message denoting a request to be notified when it's processed by the appender.
    // It's used to check if previously enqueued appends have been committed or not
    Canary(oneshot::Sender<()>),
}

impl AppendOperation {
    fn cost_in_bytes(&self) -> usize {
        match self {
            AppendOperation::Enqueue(record, _) => record.estimated_encode_size(),
            AppendOperation::EnqueueWithNotification(record, _, _) => {
                record.estimated_encode_size()
            }
            AppendOperation::Canary(_) => 0,
        }
    }
}

struct Batch {
    inner: Vec<AppendOperation>,
    bytes_accumulated: usize,
}

impl Batch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
            bytes_accumulated: 0,
        }
    }

    /// Returns true if an operation of the given size can fit in the batch
    /// without exceeding the byte limit or max count.
    fn can_fit(&self, op_size: usize, byte_limit: usize, max_count: usize) -> bool {
        self.bytes_accumulated + op_size <= byte_limit && self.inner.len() < max_count
    }

    fn push(&mut self, op: AppendOperation) {
        self.bytes_accumulated += op.cost_in_bytes();
        self.inner.push(op);
    }

    fn reset(&mut self) {
        self.bytes_accumulated = 0;
        self.inner.clear();
    }
}

fn record_from_append_operation(op: AppendOperation) -> Option<Record> {
    match op {
        AppendOperation::Enqueue(record, _) => Some(record),
        AppendOperation::EnqueueWithNotification(record, _, _) => Some(record),
        AppendOperation::Canary(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use static_assertions::assert_not_impl_any;

    use restate_types::logs::{Keys, Record};
    use restate_types::storage::PolyBytes;
    use restate_types::time::NanosSinceEpoch;

    use super::*;

    assert_not_impl_any!(LogSender<String>: Clone);

    fn make_record_with_size(size: usize) -> Record {
        // Create a record with approximately the given size
        // The actual size will be dominated by the PolyBytes::Bytes variant
        let payload = vec![0u8; size];
        Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Bytes(payload.into()),
        )
    }

    #[test]
    fn append_operation_cost_in_bytes() {
        // Test that Enqueue operations report their record size
        let record = make_record_with_size(100);
        let expected_size = record.estimated_encode_size();
        let op = AppendOperation::Enqueue(record, MemoryLease::unlinked());
        assert_eq!(op.cost_in_bytes(), expected_size);

        // Test that EnqueueWithNotification also reports record size
        let record = make_record_with_size(200);
        let expected_size = record.estimated_encode_size();
        let (tx, _rx) = oneshot::channel();
        let op = AppendOperation::EnqueueWithNotification(record, tx, MemoryLease::unlinked());
        assert_eq!(op.cost_in_bytes(), expected_size);

        // Test that control operations have zero cost
        let (tx, _rx) = oneshot::channel();
        assert_eq!(AppendOperation::Canary(tx).cost_in_bytes(), 0);
    }

    #[test]
    fn batch_can_fit_by_bytes() {
        let mut batch = Batch::with_capacity(100);
        let byte_limit = 1000;
        let max_count = 100; // High count limit, so bytes is the constraint

        // Empty batch can fit a record smaller than limit
        assert!(batch.can_fit(500, byte_limit, max_count));
        assert!(batch.can_fit(1000, byte_limit, max_count));
        assert!(!batch.can_fit(1001, byte_limit, max_count));

        // Add a 400-byte record
        let record = make_record_with_size(400);
        let record_size = record.estimated_encode_size();
        batch.push(AppendOperation::Enqueue(record, MemoryLease::unlinked()));
        assert_eq!(batch.bytes_accumulated, record_size);

        // Can fit another record if total stays under limit
        let remaining = byte_limit - batch.bytes_accumulated;
        assert!(batch.can_fit(remaining, byte_limit, max_count));
        assert!(!batch.can_fit(remaining + 1, byte_limit, max_count));
    }

    #[test]
    fn batch_can_fit_by_count() {
        let mut batch = Batch::with_capacity(100);
        let byte_limit = 1_000_000; // High byte limit, so count is the constraint
        let max_count = 3;

        // Add records until count limit
        for i in 0..3 {
            assert!(
                batch.can_fit(100, byte_limit, max_count),
                "Should fit record {i}"
            );
            let record = make_record_with_size(100);
            batch.push(AppendOperation::Enqueue(record, MemoryLease::unlinked()));
        }

        // Now at max count, cannot fit more regardless of size
        assert!(!batch.can_fit(1, byte_limit, max_count));
        assert!(!batch.can_fit(100, byte_limit, max_count));
        assert_eq!(batch.inner.len(), 3);
    }

    #[test]
    fn batch_bytes_accumulated_tracking() {
        let mut batch = Batch::with_capacity(10);
        let mut expected_total = 0;

        // Add records and verify accumulated bytes
        for _ in 0..5 {
            let record = make_record_with_size(100);
            let size = record.estimated_encode_size();
            expected_total += size;
            batch.push(AppendOperation::Enqueue(record, MemoryLease::unlinked()));
            assert_eq!(batch.bytes_accumulated, expected_total);
        }

        // Control operations don't add to byte count
        let (tx, _rx) = oneshot::channel();
        batch.push(AppendOperation::Canary(tx));
        assert_eq!(batch.bytes_accumulated, expected_total);
    }
}
