// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod convert;
mod error;
mod query;

use axum::{routing::post, Router};
use std::sync::Arc;

use restate_storage_query_datafusion::context::QueryContext;

#[derive(Clone)]
pub struct QueryServiceState {
    pub query_context: QueryContext,
}

pub fn router(query_context: QueryContext) -> Router {
    let query_state = Arc::new(QueryServiceState { query_context });

    // Setup the router
    axum::Router::new()
        .route("/query", post(query::query))
        .with_state(query_state)
}
