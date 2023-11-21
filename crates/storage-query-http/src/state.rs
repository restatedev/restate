// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_query_datafusion::context::QueryContext;

/// Handlers share this state
#[derive(Clone)]
pub struct EndpointState {
    query_context: QueryContext,
}

impl EndpointState {
    pub fn new(query_context: QueryContext) -> Self {
        Self { query_context }
    }

    pub fn query_context(&self) -> &QueryContext {
        &self.query_context
    }
}
