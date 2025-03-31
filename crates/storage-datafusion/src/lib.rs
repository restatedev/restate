// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use derive_more::Debug;
use restate_datafusion::{
    BuildError,
    context::{QueryContext, RegisterTable, SelectPartitions},
    remote_query_scanner_manager::RemoteScannerManager,
};
use restate_invoker_api::StatusHandle;
use restate_partition_store::PartitionStoreManager;
use restate_types::{
    config::QueryEngineOptions,
    live::Live,
    schema::{deployment::DeploymentResolver, service::ServiceMetadataResolver},
};

mod deployment;
mod empty_invoker_status_handle;
mod idempotency;
mod inbox;
mod invocation_state;
mod invocation_status;
mod journal;
mod keyed_service_status;
mod promise;
mod service;
mod state;
pub use empty_invoker_status_handle::EmptyInvokerStatusHandle;

#[cfg(feature = "table_docs")]
pub mod table_docs;

#[cfg(test)]
mod tests;

const SYS_INVOCATION_VIEW: &str = "CREATE VIEW sys_invocation as SELECT
            ss.id,
            ss.target,
            ss.target_service_name,
            ss.target_service_key,
            ss.target_handler_name,
            ss.target_service_ty,
            ss.idempotency_key,
            ss.invoked_by,
            ss.invoked_by_service_name,
            ss.invoked_by_id,
            ss.invoked_by_target,
            ss.pinned_deployment_id,
            ss.pinned_service_protocol_version,
            ss.trace_id,
            ss.journal_size,
            ss.created_at,
            ss.modified_at,
            ss.inboxed_at,
            ss.scheduled_at,
            ss.running_at,
            ss.completed_at,

            sis.retry_count,
            sis.last_start_at,
            sis.next_retry_at,
            sis.last_attempt_deployment_id,
            sis.last_attempt_server,
            sis.last_failure,
            sis.last_failure_error_code,
            sis.last_failure_related_entry_index,
            sis.last_failure_related_entry_name,
            sis.last_failure_related_entry_type,

            arrow_cast(CASE
                WHEN ss.status = 'inboxed' THEN 'pending'
                WHEN ss.status = 'scheduled' THEN 'scheduled'
                WHEN ss.status = 'completed' THEN 'completed'
                WHEN ss.status = 'suspended' THEN 'suspended'
                WHEN sis.in_flight THEN 'running'
                WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                ELSE 'ready'
            END, 'LargeUtf8') AS status,
            ss.completion_result,
            ss.completion_failure
        FROM sys_invocation_status ss
        LEFT JOIN sys_invocation_state sis ON ss.id = sis.id";

/// A query context registerer for user tables
pub struct UserTables<P, S, D> {
    partition_selector: P,
    local_partition_store_manager: Option<PartitionStoreManager>,
    status: Option<S>,
    schemas: Live<D>,
    remote_scanner_manager: RemoteScannerManager,
}

impl<P, S, D> UserTables<P, S, D> {
    pub fn new(
        partition_selector: P,
        local_partition_store_manager: Option<PartitionStoreManager>,
        status: Option<S>,
        schemas: Live<D>,
        remote_scanner_manager: RemoteScannerManager,
    ) -> Self {
        Self {
            partition_selector,
            local_partition_store_manager,
            status,
            schemas,
            remote_scanner_manager,
        }
    }
}

impl<P, S, D> RegisterTable for UserTables<P, S, D>
where
    P: SelectPartitions + Clone,
    S: StatusHandle + Send + Sync + Clone + Debug + 'static,
    D: DeploymentResolver + ServiceMetadataResolver + Send + Sync + Clone + 'static,
{
    async fn register(&self, ctx: &QueryContext) -> Result<(), BuildError> {
        // ----- non partitioned tables -----
        crate::deployment::register_self(ctx, self.schemas.clone())?;
        crate::service::register_self(ctx, self.schemas.clone())?;
        // ----- partition-key-based -----
        crate::invocation_state::register_self(
            ctx,
            self.partition_selector.clone(),
            self.status.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::invocation_status::register_self(
            ctx,
            self.partition_selector.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::keyed_service_status::register_self(
            ctx,
            self.partition_selector.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::state::register_self(
            ctx,
            self.partition_selector.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::journal::register_self(
            ctx,
            self.partition_selector.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::inbox::register_self(
            ctx,
            self.partition_selector.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::idempotency::register_self(
            ctx,
            self.partition_selector.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::promise::register_self(
            ctx,
            self.partition_selector.clone(),
            self.local_partition_store_manager.clone(),
            &self.remote_scanner_manager,
        )?;

        self.create_view(ctx, SYS_INVOCATION_VIEW).await?;

        Ok(())
    }
}

/// A shortcut to create a query context with built in
/// UserTables
#[allow(clippy::too_many_arguments)]
pub async fn user_query_context(
    options: &QueryEngineOptions,
    partition_selector: impl SelectPartitions + Clone,
    local_partition_store_manager: Option<PartitionStoreManager>,
    status: Option<impl StatusHandle + Send + Sync + Debug + Clone + 'static>,
    schemas: Live<
        impl DeploymentResolver + ServiceMetadataResolver + Send + Sync + Clone + 'static,
    >,
    remote_scanner_manager: RemoteScannerManager,
) -> Result<QueryContext, BuildError> {
    let tables = UserTables::new(
        partition_selector,
        local_partition_store_manager,
        status,
        schemas,
        remote_scanner_manager,
    );

    QueryContext::create(options, tables).await
}
