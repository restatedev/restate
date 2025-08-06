// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use strum::EnumCount;

use crate::invocation_status_table::run_invocation_status_v1_migration;
use crate::{PartitionStoreTransaction, Result};

#[derive(Eq, PartialEq, strum::FromRepr, strum::EnumCount)]
#[repr(u16)]
pub(crate) enum LastExecutedMigration {
    None = 0,
    /// Invocation status V1 -> V2
    V1_5 = 1,
}

pub(crate) const LATEST_MIGRATION: LastExecutedMigration =
    LastExecutedMigration::from_repr((LastExecutedMigration::COUNT as u16) - 1).unwrap();

impl From<u16> for LastExecutedMigration {
    fn from(value: u16) -> Self {
        LastExecutedMigration::from_repr(value).unwrap_or(LastExecutedMigration::V1_5)
    }
}

impl LastExecutedMigration {
    fn next(self) -> Self {
        ((self as u16) + 1).into()
    }

    pub(crate) async fn run_all_migrations(
        mut self,
        storage: &mut PartitionStoreTransaction<'_>,
    ) -> Result<Self> {
        while self != LATEST_MIGRATION {
            self.do_migration(storage).await?;
            self = self.next();
        }
        Ok(self)
    }

    // Add migrations here!
    async fn do_migration(&self, storage: &mut PartitionStoreTransaction<'_>) -> Result<()> {
        match self {
            LastExecutedMigration::None => {
                run_invocation_status_v1_migration(storage).await?;
            }
            LastExecutedMigration::V1_5 => {}
        }
        Ok(())
    }
}
