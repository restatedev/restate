// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
mod query;

use axum::{routing::post, Router};
use std::sync::Arc;

use crate::state::QueryServiceState;

pub fn create_router(state: Arc<QueryServiceState>) -> Router<()> {
    // Setup the router
    axum::Router::new()
        .route("/query", post(query::query))
        .with_state(state)
}
