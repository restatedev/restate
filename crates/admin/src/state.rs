// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_bifrost::Bifrost;
use restate_types::schema::registry::SchemaRegistry;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<Metadata, Discovery, Telemetry, Invocations> {
    pub schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
    pub invocation_client: Invocations,
    pub bifrost: Bifrost,
}

impl<Metadata, Discovery, Telemetry, Invocations>
    AdminServiceState<Metadata, Discovery, Telemetry, Invocations>
{
    pub fn new(
        schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
        invocation_client: Invocations,
        bifrost: Bifrost,
    ) -> Self {
        Self {
            schema_registry,
            invocation_client,
            bifrost,
        }
    }
}
