// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cache::{DeploymentStatusCache, DeploymentStatusEntry, DeploymentStatusSnapshot};
use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_limiter::rule_book::RuleBookObserver;
use restate_metadata_store::MetadataStoreClient;
use restate_service_protocol_v4::serdes::SerdesClient;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::identifiers::DeploymentId;
use restate_types::schema::registry::{MetadataService, SchemaRegistry};
use restate_wal_protocol::Envelope;
use std::sync::Arc;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport> {
    pub schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
    pub serdes_client: SerdesClient,
    pub invocation_client: Invocations,
    pub ingestion_client: IngestionClient<Transport, Envelope>,
    /// Used by handlers that mutate cluster-global metadata-store keys
    /// directly (e.g. the rule book) via `read_modify_write`.
    pub metadata_store_client: MetadataStoreClient,
    // Some value if the query endpoint is activated
    pub query_context: QueryContext,
    pub rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    deployment_status_cache: DeploymentStatusCache,
}

impl<Metadata, Discovery, Telemetry, Invocations, Transport>
    AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>
where
    Transport: TransportConnect,
{
    pub fn new(
        schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
        serdes_client: SerdesClient,
        invocation_client: Invocations,
        ingestion_client: IngestionClient<Transport, Envelope>,
        metadata_store_client: MetadataStoreClient,
        query_context: QueryContext,
        rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    ) -> Self {
        Self {
            schema_registry,
            serdes_client,
            invocation_client,
            ingestion_client,
            metadata_store_client,
            query_context,
            rule_book_observer,
            deployment_status_cache: DeploymentStatusCache::new(),
        }
    }
}

impl<Metadata, Discovery, Telemetry, Invocations, Transport>
    AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>
where
    Metadata: MetadataService,
{
    pub async fn deployment_statuses(
        &self,
        force_refresh: bool,
    ) -> anyhow::Result<DeploymentStatusSnapshot> {
        self.deployment_status_cache
            .get_all(&self.schema_registry, &self.query_context, force_refresh)
            .await
    }

    pub async fn deployment_status(
        &self,
        deployment_id: DeploymentId,
        force_refresh: bool,
    ) -> anyhow::Result<DeploymentStatusEntry> {
        self.deployment_status_cache
            .get(
                &self.schema_registry,
                &self.query_context,
                deployment_id,
                force_refresh,
            )
            .await
    }
}
