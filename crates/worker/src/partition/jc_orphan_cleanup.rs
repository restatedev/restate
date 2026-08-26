// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::time::Instant;

use anyhow::Context;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use restate_partition_store::{
    OrphanCleanupResult, PartitionStore, cleanup_orphaned_completion_id_index_entries,
};
use restate_storage_api::StorageError;

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("orphaned jc cleanup was cancelled")]
    Cancelled,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

pub(super) async fn run(
    storage: &mut PartitionStore,
    cancel: CancellationToken,
) -> Result<(), Error> {
    run_with(storage, cancel, |mut storage, cancel| async move {
        tokio::task::spawn_blocking(move || {
            cleanup_orphaned_completion_id_index_entries(&mut storage, || cancel.is_cancelled())
        })
        .await
        .context("orphaned jc cleanup blocking task failed")
        .map_err(StorageError::Generic)?
    })
    .await
}

async fn run_with<F, Fut>(
    storage: &mut PartitionStore,
    cancel: CancellationToken,
    cleanup: F,
) -> Result<(), Error>
where
    F: FnOnce(PartitionStore, CancellationToken) -> Fut,
    Fut: Future<Output = Result<OrphanCleanupResult, StorageError>>,
{
    if !storage.needs_jc_orphan_cleanup().await? {
        return Ok(());
    }

    let partition_id = storage.partition_id();
    info!(
        %partition_id,
        "Starting orphaned journal completion-id index cleanup"
    );
    let start = Instant::now();
    let outcome = match cleanup(storage.clone(), cancel).await {
        Ok(outcome) => outcome,
        Err(err) => {
            warn!(
                %partition_id,
                elapsed = ?start.elapsed(),
                "Failed to clean up orphaned journal completion-id index entries: {err}"
            );
            return Err(err.into());
        }
    };

    if outcome.cancelled {
        info!(
            %partition_id,
            scanned_entries = outcome.scanned_entries,
            scanned_invocations = outcome.scanned_invocations,
            deleted_entries = outcome.deleted_entries,
            affected_invocations = outcome.affected_invocations,
            elapsed = ?start.elapsed(),
            "Orphaned journal completion-id index cleanup cancelled; \
             will retry on next opted-in startup"
        );
        return Err(Error::Cancelled);
    }

    if let Err(err) = storage.mark_jc_orphan_cleanup_done().await {
        warn!(
            %partition_id,
            scanned_entries = outcome.scanned_entries,
            scanned_invocations = outcome.scanned_invocations,
            deleted_entries = outcome.deleted_entries,
            affected_invocations = outcome.affected_invocations,
            elapsed = ?start.elapsed(),
            "Failed to mark orphaned journal completion-id index cleanup as complete: {err}"
        );
        return Err(err.into());
    }

    info!(
        %partition_id,
        scanned_entries = outcome.scanned_entries,
        scanned_invocations = outcome.scanned_invocations,
        deleted_entries = outcome.deleted_entries,
        affected_invocations = outcome.affected_invocations,
        elapsed = ?start.elapsed(),
        "Completed orphaned journal completion-id index cleanup"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use restate_core::TaskCenter;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::StorageError;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;

    use super::*;

    fn cleanup_result(cancelled: bool) -> OrphanCleanupResult {
        OrphanCleanupResult {
            scanned_entries: 4,
            scanned_invocations: 2,
            deleted_entries: 2,
            affected_invocations: 1,
            cancelled,
        }
    }

    #[restate_core::test]
    async fn cleanup_startup_policy_and_marker_lifecycle() {
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

        assert_matches!(
            run_with(&mut storage, CancellationToken::new(), |_, _| async {
                Ok(cleanup_result(true))
            })
            .await,
            Err(Error::Cancelled)
        );
        assert!(storage.needs_jc_orphan_cleanup().await.unwrap());

        let failed = run_with(&mut storage, CancellationToken::new(), |_, _| async {
            Err(StorageError::OperationalError)
        })
        .await;
        assert_matches!(failed, Err(Error::Storage(StorageError::OperationalError)));
        assert!(storage.needs_jc_orphan_cleanup().await.unwrap());

        run_with(&mut storage, CancellationToken::new(), |_, _| async {
            Ok(cleanup_result(false))
        })
        .await
        .expect("successful cleanup allows startup");
        assert!(!storage.needs_jc_orphan_cleanup().await.unwrap());

        run_with(&mut storage, CancellationToken::new(), |_, _| async {
            panic!("completed cleanup must not run again")
        })
        .await
        .expect("completed cleanup allows startup");
    }
}
