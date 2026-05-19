// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{PartitionStore, Result};
use restate_storage_api::StorageError;
pub use restate_storage_api::fsm_table::{LATEST_STORAGE_FORMAT, StorageFormatVersion};

/// Extension trait on `StorageFormatVersion` providing the migration runners. Rust
/// requires a trait to add methods to a type defined in another crate; this trait
/// lives in `restate-partition-store` because the runners need `&mut PartitionStore`.
// todo(tillrohrmann) validate usefulness and signatures
pub trait StorageFormatVersionExt: Sized {
    /// Eager entry point: run forward through all migrations up to `target`, regardless
    /// of local/coordinated kind. Called from the `MigrationBarrierCommand` apply path,
    /// where the barrier itself provides the cross-replica coordination.
    fn run_migrations_up_to(
        self,
        target: StorageFormatVersion,
        storage: &mut PartitionStore,
    ) -> impl std::future::Future<Output = Result<Self>> + Send;
}

impl StorageFormatVersionExt for StorageFormatVersion {
    async fn run_migrations_up_to(
        mut self,
        target: StorageFormatVersion,
        storage: &mut PartitionStore,
    ) -> Result<StorageFormatVersion> {
        while self != target {
            do_migration(self, storage).await?;
            self = self.next();
        }
        Ok(self)
    }
}

// Add migrations here!
async fn do_migration(version: StorageFormatVersion, _storage: &mut PartitionStore) -> Result<()> {
    match version {
        StorageFormatVersion::None => {
            // Version 1.6+ does not support upgrading from pre-1.5
            // The InvocationStatusV1 migration was removed in 1.6
            return Err(StorageError::Generic(anyhow::anyhow!(
                "Cannot upgrade from version <1.5 directly to 1.6 or later. \
                 Please upgrade to version 1.5 first, which will migrate your data, \
                 and then upgrade to 1.6+"
            )));
        }
        StorageFormatVersion::V1_5 => {
            // todo(tillrohrmann) follow-up: migrate inbox / invocation_status /
            //  state / promise / timer tables to vqueue layout. This is a coordinated
            //  migration — only ever invoked via MigrationBarrierCommand apply.
        }
        StorageFormatVersion::Vqueues => {
            // Latest known version — no migration past this yet.
        }
    }
    Ok(())
}
