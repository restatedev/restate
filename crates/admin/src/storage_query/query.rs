// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::query_utils::{JsonWriter, WriteRecordBatchStream};

use super::QueryServiceState;
use super::convert::{ConvertRecordBatchStream, V1_CONVERTER};
use super::error::StorageQueryError;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json, http};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use okapi_operation::*;
use restate_admin_rest_model::version::AdminApiVersion;
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
    Extension(version): Extension<AdminApiVersion>,
    headers: HeaderMap,
    #[request_body(required = true)] Json(payload): Json<QueryRequest>,
) -> Result<impl IntoResponse, StorageQueryError> {
    let record_batch_stream = state.query_context.execute(&payload.query).await?;

    let record_batch_stream: SendableRecordBatchStream = match version {
        AdminApiVersion::V1 => Box::pin(ConvertRecordBatchStream::new(
            V1_CONVERTER,
            record_batch_stream,
        )),
        // treat 'unknown' as latest, users can specify 1 if they want to maintain old behaviour
        AdminApiVersion::Unknown | AdminApiVersion::V2 => record_batch_stream,
    };

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

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, content_type)
        .body(StreamBody::new(result_stream))
        .expect("content-type header is correct"))
}
