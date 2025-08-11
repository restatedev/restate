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
use crate::{PartitionStore, Result};

// NOTE: The representation numbers here must be strictly monotonically increasing.
#[derive(Debug, Eq, PartialEq, strum::FromRepr, strum::EnumCount)]
#[repr(u16)]
pub(crate) enum SchemaVersion {
    /// Before 1.5
    None = 0,
    /// Migrations:
    /// * Invocation status V1 -> V2
    V1_5 = 1,
}

pub(crate) const LATEST_VERSION: SchemaVersion =
    SchemaVersion::from_repr((SchemaVersion::COUNT as u16) - 1).unwrap();

impl From<u16> for SchemaVersion {
    fn from(value: u16) -> Self {
        SchemaVersion::from_repr(value).unwrap_or(SchemaVersion::V1_5)
    }
}

impl SchemaVersion {
    fn next(self) -> Self {
        ((self as u16) + 1).into()
    }

    pub(crate) async fn run_all_migrations(mut self, storage: &mut PartitionStore) -> Result<Self> {
        while self != LATEST_VERSION {
            self.do_migration(storage).await?;
            self = self.next();
        }
        Ok(self)
    }

    // Add migrations here!
    async fn do_migration(&self, storage: &mut PartitionStore) -> Result<()> {
        match self {
            SchemaVersion::None => {
                run_invocation_status_v1_migration(storage).await?;
            }
            SchemaVersion::V1_5 => {}
        }
        Ok(())
    }
}
