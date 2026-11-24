// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::context::SelectPartitions;
use crate::source_registry::SourceRegistry;
use crate::tables::deployment::schema::SysDeploymentTable;
use crate::tables::invocation_state::SysInvocationStateTable;
use crate::tables::invocation_status::SysInvocationStatusTable;
use crate::tables::journal::SysJournalTable;
use crate::tables::journal_events::SysJournalEventsTable;
use crate::tables::locks::SysLocksTable;
use crate::tables::promise::SysPromiseTable;
use crate::tables::rules::schema::SysRulesTable;
use crate::tables::scheduler_status::schema::SysSchedulerTable;
use crate::tables::service::schema::SysServiceTable;
use crate::tables::state::StateTable;
use crate::tables::stats::deployment_stats::schema::SysDeploymentStatsTable;
use crate::tables::stats::service_stats::schema::SysServiceStatsTable;
use crate::tables::stats::virtual_object_stats::schema::SysVirtualObjectStatsTable;
use crate::tables::user_limits::SysUserLimitsTable;
use crate::tables::vqueue_entry_status::schema::SysVqueueEntryStatusTable;
use crate::tables::vqueue_meta::SysVqueueMetaTable;
use crate::{BuildError, QuerySource};

use super::{RegisterTable, TableInventoryBuilder};

/// V2's representative partition tables, optionally extended with cluster metadata.
pub struct UserTables<P, M = ()> {
    partition_selector: P,
    sources: SourceRegistry,
    metadata: M,
}

impl<P> UserTables<P> {
    pub fn new(partition_selector: P, sources: SourceRegistry) -> Self {
        Self {
            partition_selector,
            sources,
            metadata: (),
        }
    }

    /// Adds the metadata tables, read through the same sources.
    pub fn with_metadata(self) -> UserTables<P, MetadataTables> {
        UserTables {
            metadata: MetadataTables::new(self.sources.clone()),
            partition_selector: self.partition_selector,
            sources: self.sources,
        }
    }
}

/// Cluster metadata tables. Offline partition snapshots do not supply this group.
///
/// Each query routes to one node serving the table's complete metadata, preferring
/// this node. Nodes serve metadata once
/// [`register_metadata_scanners`](crate::local_scanners::register_metadata_scanners)
/// has registered it with their sources.
pub struct MetadataTables {
    sources: SourceRegistry,
}

impl MetadataTables {
    pub fn new(sources: SourceRegistry) -> Self {
        Self { sources }
    }
}

impl RegisterTable for MetadataTables {
    async fn register(&self, inventory: &mut TableInventoryBuilder<'_>) -> Result<(), BuildError> {
        inventory.add::<SysServiceTable>(
            "public",
            &SysServiceTable::identity(),
            SysServiceTable::create_provider(self.sources.clone()),
        )?;
        inventory.add::<SysDeploymentTable>(
            "public",
            &SysDeploymentTable::identity(),
            SysDeploymentTable::create_provider(self.sources.clone()),
        )?;
        inventory.add::<SysRulesTable>(
            "public",
            &SysRulesTable::identity(),
            SysRulesTable::create_provider(self.sources.clone()),
        )
    }
}

impl<P, M> RegisterTable for UserTables<P, M>
where
    P: SelectPartitions + Clone,
    M: RegisterTable,
{
    async fn register(&self, inventory: &mut TableInventoryBuilder<'_>) -> Result<(), BuildError> {
        self.metadata.register(inventory).await?;
        inventory.add::<SysUserLimitsTable>(
            "public",
            &SysUserLimitsTable::identity(),
            SysUserLimitsTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysInvocationStatusTable>(
            "public",
            &SysInvocationStatusTable::identity(),
            SysInvocationStatusTable::create_provider(
                self.partition_selector.clone(),
                &self.sources,
            ),
        )?;
        inventory.add::<SysInvocationStateTable>(
            "public",
            &SysInvocationStateTable::identity(),
            SysInvocationStateTable::create_provider(
                self.partition_selector.clone(),
                &self.sources,
            ),
        )?;
        inventory.add::<StateTable>(
            "public",
            "state",
            StateTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysJournalTable>(
            "public",
            &SysJournalTable::identity(),
            SysJournalTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysJournalEventsTable>(
            "public",
            &SysJournalEventsTable::identity(),
            SysJournalEventsTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysLocksTable>(
            "public",
            &SysLocksTable::identity(),
            SysLocksTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysPromiseTable>(
            "public",
            &SysPromiseTable::identity(),
            SysPromiseTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysVqueueMetaTable>(
            "public",
            &SysVqueueMetaTable::identity(),
            SysVqueueMetaTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysSchedulerTable>(
            "public",
            &SysSchedulerTable::identity(),
            SysSchedulerTable::create_provider(self.partition_selector.clone(), &self.sources),
        )?;
        inventory.add::<SysVqueueEntryStatusTable>(
            "public",
            &SysVqueueEntryStatusTable::identity(),
            SysVqueueEntryStatusTable::create_provider(
                self.partition_selector.clone(),
                &self.sources,
            ),
        )?;
        inventory.add::<SysVirtualObjectStatsTable>(
            "public",
            &SysVirtualObjectStatsTable::identity(),
            SysVirtualObjectStatsTable::create_provider(
                self.partition_selector.clone(),
                &self.sources,
            )?,
        )?;
        inventory.add::<SysServiceStatsTable>(
            "public",
            &SysServiceStatsTable::identity(),
            SysServiceStatsTable::create_provider(self.partition_selector.clone(), &self.sources)?,
        )?;
        inventory.add::<SysDeploymentStatsTable>(
            "public",
            &SysDeploymentStatsTable::identity(),
            SysDeploymentStatsTable::create_provider(
                self.partition_selector.clone(),
                &self.sources,
            )?,
        )?;
        inventory
            .add_view("sys_invocation", "public", include_str!("invocation.sql"))
            .await?;
        // The V1 `sys_vqueues` projection over current entry status, for existing
        // clients (the Web UI).
        inventory
            .add_view("sys_vqueues", "public", include_str!("vqueues.sql"))
            .await
    }
}
