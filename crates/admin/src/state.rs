// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::TransportConnect;
use restate_ingress_client::IngressClient;
use restate_types::schema::registry::SchemaRegistry;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport> {
    pub schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
    pub invocation_client: Invocations,
    pub ingress: IngressClient<Transport>,
}

impl<Metadata, Discovery, Telemetry, Invocations, Transport>
    AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>
where
    Transport: TransportConnect,
{
    pub fn new(
        schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
        invocation_client: Invocations,
        ingress: IngressClient<Transport>,
    ) -> Self {
        Self {
            schema_registry,
            invocation_client,
            ingress,
        }
    }
}
