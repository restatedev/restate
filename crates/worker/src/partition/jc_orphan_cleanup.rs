// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::time::Instant;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use restate_core::{TaskCenter, TaskHandle, TaskKind, cancellation_token};
use restate_partition_store::{
    PartitionStore, ScanCursor, apply_orphaned_completion_id_index_cleanup,
    mark_orphaned_completion_id_index_cleanup_done, scan_orphaned_completion_id_index_entries,
};
use restate_types::identifiers::{InvocationId, PartitionId};

/// Number of invocations inspected by a single blocking scan.
const SCAN_INVOCATIONS_PER_CHUNK: usize = 512;
/// Number of invocations cleaned up per partition processor tick.
const PROCESSOR_INVOCATIONS_PER_BATCH: usize = 32;

enum CleanupEvent {
    /// Invocations whose journal is gone but which still have completion-id index entries.
    Candidates(Vec<InvocationId>),
    /// The scanner reached the end of the index, the cleanup can be marked as done.
    ScanCompleted { scanned_invocations: usize },
}

/// Handle to a running cleanup of orphaned journal completion-id index entries.
///
/// The index is scanned in a background task which reports orphan candidates in small batches.
/// Deleting them is driven by the partition processor via [`Handle::on_tick`] so that the cleanup
/// cannot monopolize the store.
pub(super) struct Handle {
    partition_id: PartitionId,
    started: Instant,
    scanner: TaskHandle<()>,
    rx: mpsc::Receiver<CleanupEvent>,
    deleted_invocations: usize,
    recreated_invocations: usize,
    active: bool,
}

impl Handle {
    /// Returns `false` once the cleanup reached a terminal state and [`Handle::on_tick`] no longer
    /// needs to be called.
    pub(super) fn is_active(&self) -> bool {
        self.active
    }

    /// Applies at most one batch of pending deletions.
    pub(super) async fn on_tick(&mut self, storage: &mut PartitionStore) {
        let event = match self.rx.try_recv() {
            Ok(event) => event,
            Err(mpsc::error::TryRecvError::Empty) => return,
            // the scanner stopped before reaching the end of the index; the cleanup is retried on
            // the next startup
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.active = false;
                return;
            }
        };

        match event {
            CleanupEvent::Candidates(candidates) => {
                match apply_orphaned_completion_id_index_cleanup(storage, &candidates).await {
                    Ok(applied) => {
                        self.deleted_invocations += applied.deleted_invocations;
                        self.recreated_invocations += applied.recreated_invocations;
                    }
                    Err(err) => self.fail(
                        &err,
                        "Failed to apply orphaned journal completion-id index cleanup",
                    ),
                }
            }
            CleanupEvent::ScanCompleted {
                scanned_invocations,
            } => match mark_orphaned_completion_id_index_cleanup_done(storage).await {
                Ok(()) => {
                    self.active = false;
                    info!(
                        partition_id = %self.partition_id,
                        scanned_invocations,
                        deleted_invocations = self.deleted_invocations,
                        recreated_invocations = self.recreated_invocations,
                        elapsed = ?self.started.elapsed(),
                        "Completed cleanup of orphaned journal completion-id index entries"
                    );
                }
                Err(err) => self.fail(
                    &err,
                    "Failed to mark orphaned journal completion-id index cleanup as complete",
                ),
            },
        }
    }

    /// Stops the background scan. The cleanup is retried on the next startup unless it completed.
    pub(super) async fn stop(mut self) {
        self.active = false;
        self.scanner.cancel();
        let _ = self.scanner.await;
    }

    fn fail(&mut self, error: &impl Display, message: &'static str) {
        self.active = false;
        // let the scanner stop producing candidates that nobody applies anymore
        self.rx.close();
        warn!(
            partition_id = %self.partition_id,
            deleted_invocations = self.deleted_invocations,
            recreated_invocations = self.recreated_invocations,
            elapsed = ?self.started.elapsed(),
            %error,
            "{message}"
        );
    }
}

/// Starts the one-time cleanup of orphaned journal completion-id index entries, unless this
/// partition has already been cleaned up.
pub(super) async fn start(storage: &mut PartitionStore) -> Option<Handle> {
    let partition_id = storage.partition_id();

    match storage.needs_jc_orphan_cleanup().await {
        Ok(true) => {}
        Ok(false) => return None,
        Err(err) => {
            warn!(
                %partition_id,
                %err,
                "Failed to determine whether orphaned journal completion-id index entries need to \
                 be cleaned up"
            );
            return None;
        }
    }

    // keeps the scanner at most one batch ahead of the partition processor
    let (tx, rx) = mpsc::channel(1);
    let scanner_storage = storage.clone();
    let scanner =
        TaskCenter::spawn_unmanaged_child(TaskKind::Cleaner, "jc-orphan-cleanup", async move {
            let cancel = cancellation_token();
            if let Some(Err(err)) = cancel
                .run_until_cancelled(run_scanner(scanner_storage, tx, &cancel))
                .await
            {
                warn!(
                    %partition_id,
                    %err,
                    "Failed to scan for orphaned journal completion-id index entries"
                );
            }
        })
        .ok()?;

    info!(
        %partition_id,
        "Starting cleanup of orphaned journal completion-id index entries"
    );

    Some(Handle {
        partition_id,
        started: Instant::now(),
        scanner,
        rx,
        deleted_invocations: 0,
        recreated_invocations: 0,
        active: true,
    })
}

async fn run_scanner(
    mut storage: PartitionStore,
    tx: mpsc::Sender<CleanupEvent>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let mut resume_after = None;
    let mut scanned_invocations = 0;

    loop {
        let scan_cancel = cancel.clone();
        // The scan is blocking and the closure must be `'static`, hence the store is moved into
        // the blocking task and handed back.
        let (returned_storage, chunk) = tokio::task::spawn_blocking(move || {
            let chunk = scan_orphaned_completion_id_index_entries(
                &mut storage,
                resume_after,
                SCAN_INVOCATIONS_PER_CHUNK,
                || scan_cancel.is_cancelled(),
            );
            (storage, chunk)
        })
        .await
        .context("orphaned jc cleanup scan task failed")?;
        storage = returned_storage;
        let chunk = chunk?;
        scanned_invocations += chunk.scanned_invocations;

        for candidates in chunk.candidates.chunks(PROCESSOR_INVOCATIONS_PER_BATCH) {
            if tx
                .send(CleanupEvent::Candidates(candidates.to_vec()))
                .await
                .is_err()
            {
                // the partition processor stopped applying candidates
                return Ok(());
            }
        }

        match chunk.next {
            ScanCursor::ResumeAfter(invocation_id) => resume_after = Some(invocation_id),
            ScanCursor::Cancelled => return Ok(()),
            ScanCursor::Complete => {
                let _ = tx
                    .send(CleanupEvent::ScanCompleted {
                        scanned_invocations,
                    })
                    .await;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use restate_core::TaskCenter;
    use restate_rocksdb::RocksDbManager;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;

    use super::*;

    #[test_log::test(restate_core::test(flavor = "multi_thread"))]
    async fn empty_cleanup_marks_partition_jc_cleanup_complete() {
        let rocksdb_manager = RocksDbManager::init();
        TaskCenter::current().set_on_shutdown(Box::pin(async {
            rocksdb_manager.shutdown().await;
        }));

        let manager = restate_partition_store::PartitionStoreManager::create(true)
            .await
            .expect("manager creation succeeds");
        let mut storage = manager
            .open(
                &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
                None,
            )
            .await
            .expect("partition store opens");
        assert!(storage.needs_jc_orphan_cleanup().await.unwrap());

        let mut cleanup = start(&mut storage).await.expect("cleanup is needed");
        while cleanup.is_active() {
            cleanup.on_tick(&mut storage).await;
            tokio::task::yield_now().await;
        }
        cleanup.stop().await;

        assert!(!storage.needs_jc_orphan_cleanup().await.unwrap());
        // a second start is a no-op now that the partition is marked as cleaned up
        assert!(start(&mut storage).await.is_none());
    }
}
