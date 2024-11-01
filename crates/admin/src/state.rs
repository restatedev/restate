// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//

use crate::schema_registry::SchemaRegistry;
use restate_bifrost::Bifrost;
use restate_storage_query_datafusion::context::QueryContext;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<V> {
    pub schema_registry: SchemaRegistry<V>,
    pub bifrost: Bifrost,
}

#[derive(Clone)]
pub struct QueryServiceState {
    pub query_context: QueryContext,
}

impl<V> AdminServiceState<V> {
    pub fn new(schema_registry: SchemaRegistry<V>, bifrost: Bifrost) -> Self {
        Self {
            schema_registry,
            bifrost,
        }
    }
}
