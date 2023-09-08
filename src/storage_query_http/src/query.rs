// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use axum::body::StreamBody;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::{http, Json};
use datafusion::error::DataFusionError;
use futures::StreamExt;
use okapi_operation::*;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::map::Map as JsonMap;
use serde_json::Value;
use serde_with::serde_as;

use crate::error::StorageApiError;
use crate::state::EndpointState;

#[serde_as]
#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryRequest {
    /// # Query
    ///
    /// SQL query to run against the storage
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[schemars(with = "String")]
    pub query: String,
}

/// Query storage
#[openapi(
    summary = "Query storage",
    description = "Query the storage API",
    operation_id = "query",
    tags = "storage",
    responses(
        ignore_return_type = true,
        response(
            status = "200",
            description = "OK",
            content = "Json<Vec<JsonMap<String, Value>>>",
        ),
        from_type = "StorageApiError",
    )
)]
pub async fn query(
    State(state): State<Arc<EndpointState>>,
    #[request_body(required = true)] Json(payload): Json<QueryRequest>,
) -> Result<impl IntoResponse, StorageApiError> {
    let stream = state
        .query_context()
        .execute(payload.query.as_str())
        .await
        .map_err(StorageApiError::DataFusionError)?;
    let body = StreamBody::new(stream.map(|batch| -> Result<Vec<u8>, DataFusionError> {
        let mut buf = Vec::new();
        let mut writer = datafusion::arrow::json::writer::ArrayWriter::new(&mut buf);
        writer.write(&batch?)?;
        Ok(buf)
    }));
    Ok(([(http::header::CONTENT_TYPE, "application/json")], body))
}
