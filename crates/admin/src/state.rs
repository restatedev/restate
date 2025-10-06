// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::schema_registry::SchemaRegistry;
use restate_bifrost::Bifrost;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<IC> {
    pub schema_registry: SchemaRegistry,
    pub invocation_client: IC,
    pub bifrost: Bifrost,
}

impl<IC> AdminServiceState<IC> {
    pub fn new(schema_registry: SchemaRegistry, invocation_client: IC, bifrost: Bifrost) -> Self {
        Self {
            schema_registry,
            invocation_client,
            bifrost,
        }
    }
}
