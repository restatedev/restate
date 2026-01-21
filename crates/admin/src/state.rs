// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::schema::registry::SchemaRegistry;
use restate_wal_protocol::Envelope;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport> {
    pub schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
    pub invocation_client: Invocations,
    pub ingestion_client: IngestionClient<Transport, Envelope>,
    // Some value if the query endpoint is activated
    pub query_context: Option<QueryContext>,
}

impl<Metadata, Discovery, Telemetry, Invocations, Transport>
    AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>
where
    Transport: TransportConnect,
{
    pub fn new(
        schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
        invocation_client: Invocations,
        ingestion_client: IngestionClient<Transport, Envelope>,
        query_context: Option<QueryContext>,
    ) -> Self {
        Self {
            schema_registry,
            invocation_client,
            ingestion_client,
            query_context,
        }
    }
}
