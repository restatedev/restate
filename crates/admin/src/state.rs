// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_limiter::RuleBook;
use restate_metadata_store::MetadataStoreClient;
use restate_service_protocol_v4::serdes::SerdesClient;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::schema::registry::SchemaRegistry;
use restate_wal_protocol::Envelope;

/// Fire-and-forget callback that pushes a freshly written rule book
/// into a co-located worker's `RuleBookCache`. `None` on admin-only
/// nodes; the worker then learns about updates via its metadata-store
/// poll loop instead.
///
/// Takes the book by value: the cache only allocates an `Arc` for it
/// on the newer-version branch, so admin handlers don't have to
/// pre-wrap their result.
pub type RuleBookObserver = Arc<dyn Fn(RuleBook) + Send + Sync>;

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
    pub query_context: Option<QueryContext>,
    pub rule_book_observer: Option<RuleBookObserver>,
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
        query_context: Option<QueryContext>,
        rule_book_observer: Option<RuleBookObserver>,
    ) -> Self {
        Self {
            schema_registry,
            serdes_client,
            invocation_client,
            ingestion_client,
            metadata_store_client,
            query_context,
            rule_book_observer,
        }
    }
}
