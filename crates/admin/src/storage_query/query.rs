// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;

use crate::query_utils::{JsonWriter, WriteRecordBatchStream};

use super::QueryServiceState;
use super::error::StorageQueryError;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Json, http};
use datafusion::arrow::ipc::writer::StreamWriter;
use futures::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use okapi_operation::*;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_with::serde_as;

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
    responses(ignore_return_type = true, from_type = "StorageQueryError")
)]
pub async fn query(
    State(state): State<Arc<QueryServiceState>>,
    headers: HeaderMap,
    #[request_body(required = true)] Json(payload): Json<QueryRequest>,
) -> Result<impl IntoResponse, StorageQueryError> {
    let record_batch_stream = state.query_context.execute(&payload.query).await?;

    let (result_stream, content_type) = match headers.get(http::header::ACCEPT) {
        Some(v) if v == HeaderValue::from_static("application/json") => (
            WriteRecordBatchStream::<JsonWriter>::new(record_batch_stream, payload.query)?
                .map_ok(Frame::data)
                .left_stream(),
            "application/json",
        ),
        _ => (
            WriteRecordBatchStream::<StreamWriter<Vec<u8>>>::new(
                record_batch_stream,
                payload.query,
            )?
            .map_ok(Frame::data)
            .right_stream(),
            "application/vnd.apache.arrow.stream",
        ),
    };

    let mut result_stream = result_stream.peekable();

    // return an error (instead of just closing the stream) if there is a error getting the first record batch (eg, out of memory)
    if let Some(Err(_)) = futures::stream::Peekable::peek(Pin::new(&mut result_stream)).await {
        let err = result_stream.next().await.unwrap().unwrap_err();
        return Err(StorageQueryError::DataFusion(err));
    }

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, content_type)
        .body(StreamBody::new(result_stream))
        .expect("content-type header is correct"))
}
