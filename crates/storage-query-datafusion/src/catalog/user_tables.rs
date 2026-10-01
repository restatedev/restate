// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use restate_limiter::rule_book::RuleBookObserver;
use restate_metadata_store::MetadataStoreClient;
use restate_types::live::Live;
use restate_types::schema::deployment::DeploymentResolver;
use restate_types::schema::service::ServiceMetadataResolver;

use crate::BuildError;
use crate::context::SelectPartitions;
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::RegisterTable;

pub(super) const SYS_INVOCATION_VIEW: &str = "CREATE VIEW sys_invocation as SELECT
            ss.id,
            ss.vqueue_id,
            ss.target,
            ss.target_service_name,
            ss.target_service_key,
            ss.target_handler_name,
            ss.target_service_ty,
            ss.scope,
            ss.limit_key,
            ss.idempotency_key,
            ss.invoked_by,
            ss.invoked_by_service_name,
            ss.invoked_by_id,
            ss.invoked_by_subscription_id,
            ss.invoked_by_target,
            ss.restarted_from,
            ss.pinned_deployment_id,
            ss.pinned_service_protocol_version,
            ss.trace_id,
            ss.journal_size,
            ss.journal_commands_size,
            ss.created_at,
            ss.created_using_restate_version,
            ss.modified_at,
            ss.inboxed_at,
            ss.scheduled_at,
            ss.scheduled_start_at,
            ss.running_at,
            ss.completed_at,
            ss.completion_retention,
            ss.journal_retention,
            ss.suspended_waiting_for_completions,
            ss.suspended_waiting_for_signals,
            ss.suspended_waiting_future_json,

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
            sis.last_failure_related_command_index,
            sis.last_failure_related_command_name,
            sis.last_failure_related_command_type,
            sis.last_awaiting_on_future_json,

            arrow_cast(CASE
                WHEN ss.status = 'inboxed' THEN 'pending'
                WHEN ss.status = 'scheduled' THEN 'scheduled'
                WHEN ss.status = 'completed' THEN 'completed'
                WHEN ss.status = 'suspended' THEN 'suspended'
                WHEN ss.status = 'paused' THEN 'paused'
                WHEN sis.in_flight THEN 'running'
                WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                ELSE 'ready'
            END, 'LargeUtf8') AS status,
            ss.completion_result,
            ss.completion_failure
        FROM sys_invocation_state sis
        RIGHT JOIN sys_invocation_status ss ON ss.id = sis.id";

/// User-facing partition tables and views, optionally extended with metadata-backed tables.
///
/// Live tables are always registered. Their backend determines whether live rows are available;
/// offline backends return empty live data for restored partitions. Without [`MetadataTables`],
/// `sys_service`, `sys_deployment`, and `sys_rules` are absent from the catalog.
pub struct UserTables<P, M = ()> {
    partition_selector: P,
    remote_scanner_manager: RemoteScannerManager,
    metadata: M,
}

impl<P> UserTables<P> {
    pub fn new(partition_selector: P, remote_scanner_manager: RemoteScannerManager) -> Self {
        Self {
            partition_selector,
            remote_scanner_manager,
            metadata: (),
        }
    }

    pub fn with_metadata<S>(self, metadata: MetadataTables<S>) -> UserTables<P, MetadataTables<S>> {
        UserTables {
            partition_selector: self.partition_selector,
            remote_scanner_manager: self.remote_scanner_manager,
            metadata,
        }
    }
}

/// Dependencies for the service, deployment, and rule tables. Supply this group only when
/// cluster metadata is available; offline partition snapshots do not contain that metadata.
pub struct MetadataTables<S> {
    rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    schemas: Live<S>,
    metadata_store_client: MetadataStoreClient,
}

impl<S> MetadataTables<S> {
    pub fn new(
        schemas: Live<S>,
        metadata_store_client: MetadataStoreClient,
        rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    ) -> Self {
        Self {
            rule_book_observer,
            schemas,
            metadata_store_client,
        }
    }
}

impl<S> RegisterTable for MetadataTables<S>
where
    S: DeploymentResolver + ServiceMetadataResolver + Send + Sync + Debug + Clone + 'static,
{
    async fn register(&self, ctx: &SessionContext) -> Result<(), BuildError> {
        crate::deployment::register_self(ctx, self.schemas.clone())?;
        crate::service::register_self(ctx, self.schemas.clone())?;
        crate::rules::register_self(
            ctx,
            self.metadata_store_client.clone(),
            self.rule_book_observer.clone(),
        )?;
        Ok(())
    }
}

impl<P, M> RegisterTable for UserTables<P, M>
where
    P: SelectPartitions + Clone,
    M: RegisterTable,
{
    async fn register(&self, ctx: &SessionContext) -> Result<(), BuildError> {
        self.metadata.register(ctx).await?;
        crate::invocation_state::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::scheduler_status::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::user_limits::register_self(
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
        // VQueues Tables
        crate::vqueue_meta::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::vqueue_entry_status::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;
        crate::vqueues::register_self(
            ctx,
            self.partition_selector.clone(),
            &self.remote_scanner_manager,
        )?;

        ctx.sql(SYS_INVOCATION_VIEW).await?;

        Ok(())
    }
}
