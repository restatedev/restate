// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::execution::context::SessionContext;

use crate::BuildError;
use crate::context::SelectPartitions;
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::RegisterTable;

/// A query context registerer that exposes only the partition-store-backed tables.
///
/// Unlike [`super::UserTables`], it needs neither a schema registry nor a metadata-store client, so it can
/// run against partition data with no live cluster behind it — e.g. a snapshot restored by an
/// offline debugging tool. The leader-owned tables (`sys_scheduler`, `sys_user_limits`) are
/// intentionally omitted: that state is ephemeral and never present in a snapshot. The invoker
/// columns of `sys_invocation_state` are likewise leader-owned and resolve to nulls here, which is
/// the truthful answer for a snapshot; the table is still registered because the `sys_invocation`
/// view joins against it.
pub struct PartitionTables<P> {
    partition_selector: P,
    remote_scanner_manager: RemoteScannerManager,
}

impl<P> PartitionTables<P> {
    pub fn new(partition_selector: P, remote_scanner_manager: RemoteScannerManager) -> Self {
        Self {
            partition_selector,
            remote_scanner_manager,
        }
    }
}

impl<P> RegisterTable for PartitionTables<P>
where
    P: SelectPartitions + Clone,
{
    async fn register(&self, ctx: &SessionContext) -> Result<(), BuildError> {
        crate::invocation_state::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::invocation_status::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::locks::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::state::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::journal::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::journal_events::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::inbox::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::promise::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::vqueue_meta::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::vqueues::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;

        ctx.sql(super::user_tables::SYS_INVOCATION_VIEW).await?;

        Ok(())
    }
}
