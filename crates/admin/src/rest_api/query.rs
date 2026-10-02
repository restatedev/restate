// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, http};
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::common::DataFusionError;
use futures::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use parking_lot::Mutex;
use serde::Serialize;

use restate_admin_rest_model::query::QueryRequest;
use restate_core::network::TransportConnect;
use restate_storage_query_api::errors::{QueryExecutionError, SessionError};
use restate_storage_query_api::{
    AdminUser, QueryOptions, QuerySession, RecordBatchWriter, SessionOptions,
    WriteRecordBatchStream,
};
use restate_types::invocation::client::InvocationClient;
use restate_types::schema::registry::{DiscoveryClient, MetadataService, TelemetryClient};

use crate::query_context::collect_query_headers;
use crate::state::AdminServiceState;

const RETRY_AFTER_HEADER: &str = "Retry-After";
const QUERY_SESSION_HEADER: &str = "x-restate-query-session-id";

/// Error response for query endpoint.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct QueryErrorBody {
    message: String,
}

/// Errors that can occur when executing a query.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryError {
    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error("Query service not available")]
    Unavailable,
    #[error("Rate limited")]
    RateLimited(#[from] gardal::RateLimited),
    #[error("Session initialization error: {0}")]
    SessionInitialization(datafusion::error::DataFusionError),
}

impl From<QueryExecutionError> for QueryError {
    fn from(err: QueryExecutionError) -> Self {
        match err {
            QueryExecutionError::DataFusion(e) => Self::Datafusion(e),
        }
    }
}

impl From<SessionError> for QueryError {
    fn from(err: SessionError) -> Self {
        match err {
            SessionError::EngineDisabled => Self::Unavailable,
            SessionError::RateLimited(e) => Self::RateLimited(e),
            SessionError::DataFusion(e) => Self::SessionInitialization(e),
        }
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        let mut headers = http::HeaderMap::new();
        let status_code = match &self {
            QueryError::Datafusion(datafusion::error::DataFusionError::Plan(_))
            | QueryError::Datafusion(datafusion::error::DataFusionError::SchemaError(_, _))
            | QueryError::Datafusion(datafusion::error::DataFusionError::SQL(_, _)) => {
                StatusCode::BAD_REQUEST
            }
            QueryError::Datafusion(_) | QueryError::SessionInitialization(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            QueryError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            QueryError::RateLimited(e) => {
                headers.insert(
                    RETRY_AFTER_HEADER,
                    HeaderValue::from(std::cmp::max(e.earliest_retry_after().as_secs(), 1)),
                );
                StatusCode::TOO_MANY_REQUESTS
            }
        };

        (
            status_code,
            headers,
            Json(QueryErrorBody {
                message: self.to_string(),
            }),
        )
            .into_response()
    }
}

/// Query the system and service state by using SQL.
#[utoipa::path(
    post,
    path = "/query",
    operation_id = "query",
    tag = "introspection",
    responses(
        (status = 200, description = "Query results",
            content (
                ("application/vnd.apache.arrow.stream"),
                ("application/json", example = json!({"rows": []}))
            ),
            headers(("X-Restate-Query-Session-Id" = String, description = "Server-generated query session ID"))),
        (status = 400, description = "Error during planning: table 'mytable' not found", body = QueryErrorBody),
        (status = 500, description = "Internal query error", body = QueryErrorBody),
        (status = 503, description = "Query service not available", body = QueryErrorBody),
    )
)]
pub(crate) async fn query<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    headers: HeaderMap,
    Json(payload): Json<QueryRequest>,
) -> Result<Response, QueryError>
where
    Metadata: MetadataService + Send + Sync + Clone + 'static,
    Discovery: DiscoveryClient + Send + Sync + Clone + 'static,
    Telemetry: TelemetryClient + Send + Sync + Clone + 'static,
    Invocations: InvocationClient + Send + Sync + Clone + 'static,
    Transport: TransportConnect,
{
    let session = state.query_engine.create_session(SessionOptions {
        headers: collect_query_headers(&headers),
        ..Default::default()
    })?;

    query_in_session(session.as_ref(), &headers, payload).await
}

async fn query_in_session(
    session: &dyn QuerySession<AdminUser>,
    headers: &HeaderMap,
    payload: QueryRequest,
) -> Result<Response, QueryError> {
    let session_id = HeaderValue::from_str(session.session_id())
        .map_err(|err| DataFusionError::Internal(format!("Invalid query session ID: {err}")))?;
    // Preserve correlation on planning and first-batch errors as well as success.
    let mut response = execute_query(session, headers, payload)
        .await
        .into_response();
    response
        .headers_mut()
        .insert(QUERY_SESSION_HEADER, session_id);
    Ok(response)
}

async fn execute_query(
    session: &dyn QuerySession<AdminUser>,
    headers: &HeaderMap,
    payload: QueryRequest,
) -> Result<Response, QueryError> {
    let query_result = session.execute(&payload.query, QueryOptions {}).await?;

    let (result_stream, content_type) = match headers.get(http::header::ACCEPT) {
        Some(v) if v == HeaderValue::from_static("application/json") => (
            WriteRecordBatchStream::<JsonWriter>::new(query_result.stream, query_result.metadata)?
                .map_ok(Frame::data)
                .left_stream(),
            "application/json",
        ),
        _ => (
            WriteRecordBatchStream::<StreamWriter<Vec<u8>>>::new(
                query_result.stream,
                query_result.metadata,
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
        return Err(err.into());
    }

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, content_type)
        .body(StreamBody::new(result_stream))
        .expect("content-type header is correct")
        .into_response())
}

#[derive(Clone)]
// unfortunately the json writer doesnt give a way to get a mutable reference to the underlying writer, so we need another pointer in to its buffer
// we use a lock here to help make the writer send/sync, despite it being totally uncontended :(
struct LockWriter(Arc<Mutex<Vec<u8>>>);

impl LockWriter {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn take(&self) -> Vec<u8> {
        let mut vec = self.0.lock();
        let new_vec = Vec::with_capacity(vec.capacity());
        std::mem::replace(&mut vec, new_vec)
    }
}

impl Write for LockWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().flush()
    }
}

pub(crate) struct JsonWriter {
    json_writer: datafusion::arrow::json::Writer<LockWriter, JsonArray>,
    lock_writer: LockWriter,
    finished: bool,
}

impl RecordBatchWriter for JsonWriter {
    fn new(_schema: &Schema) -> Result<Self, DataFusionError> {
        let mut lock_writer = LockWriter::new();
        // we write out under 'rows' key so that we may add extra keys later (eg 'schema')
        lock_writer.write_all(br#"{"rows":"#)?;
        Ok(Self {
            json_writer: datafusion::arrow::json::Writer::new(lock_writer.clone()),
            lock_writer,
            finished: false,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<Bytes, DataFusionError> {
        self.json_writer.write(batch)?;
        Ok(Bytes::from(self.lock_writer.take()))
    }

    fn finish(&mut self) -> Result<Bytes, DataFusionError> {
        if !self.finished {
            self.finished = true;

            self.json_writer.finish()?;
            self.lock_writer.write_all(b"}")?;
        }
        Ok(Bytes::from(self.lock_writer.take()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use http_body_util::BodyExt;

    use restate_storage_query_datafusion::DataFusionEnv;
    use restate_storage_query_datafusion::context::DataFusionQueryEngine;

    use super::*;

    #[tokio::test]
    async fn query_context_and_session_header_preserve_streaming_responses() {
        let mut headers = HeaderMap::new();
        assert!(collect_query_headers(&headers).is_empty());
        for (name, value) in [
            ("X-Restate-Query-Client", "ui"),
            ("X-Restate-Query-Origin", "built-in"),
            ("X-RestateCloud-User-id", "user-1"),
            ("X-RestateCloud-Environment-id", "env-1"),
            ("X-RestateCloud-Caller-Principal", "principal-1"),
            ("Authorization", "Bearer private-token"),
            ("Cookie", "private-cookie"),
            ("X-Unlisted-Header", "ignored"),
        ] {
            headers.insert(
                name.parse::<http::header::HeaderName>().unwrap(),
                HeaderValue::from_static(value),
            );
        }
        headers.append(
            "x-restate-query-client",
            HeaderValue::from_static("second-client"),
        );
        let collected = collect_query_headers(&headers);
        assert_eq!(collected.len(), 6);
        assert_eq!(collected["x-restate-query-client"], "ui");
        assert_eq!(
            collected.get_all("x-restate-query-client").iter().count(),
            2
        );
        assert_eq!(collected["x-restate-query-origin"], "built-in");
        assert_eq!(collected["x-restatecloud-user-id"], "user-1");
        assert_eq!(collected["x-restatecloud-environment-id"], "env-1");
        assert_eq!(collected["x-restatecloud-caller-principal"], "principal-1");
        assert!(!collected.contains_key("authorization"));
        assert!(!collected.contains_key("cookie"));
        assert!(!collected.contains_key("x-unlisted-header"));
        let grpc_metadata = tonic::metadata::MetadataMap::from_headers(headers.clone());
        assert_eq!(collect_query_headers(grpc_metadata.as_ref()), collected);

        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new()).unwrap();
        let engine = DataFusionQueryEngine::<AdminUser>::from_inventory(env, None, vec![]);
        let session = engine
            .create_session(SessionOptions {
                headers: collected.clone(),
                ..Default::default()
            })
            .unwrap();
        let result = session.execute("SELECT 42", QueryOptions {}).await.unwrap();
        assert_eq!(result.metadata.headers, collected);
        drop(result);
        for accept in ["application/json", "application/vnd.apache.arrow.stream"] {
            headers.insert(http::header::ACCEPT, HeaderValue::from_static(accept));
            let response = query_in_session(
                session.as_ref(),
                &headers,
                QueryRequest {
                    query: "SELECT 42 AS answer".into(),
                },
            )
            .await
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[QUERY_SESSION_HEADER],
                session.session_id()
            );
            assert_eq!(response.headers()[http::header::CONTENT_TYPE], accept);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            if accept == "application/json" {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                    serde_json::json!({"rows": [{"answer": 42}]})
                );
            } else {
                let batches = datafusion::arrow::ipc::reader::StreamReader::try_new(
                    std::io::Cursor::new(bytes),
                    None,
                )
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
            }
        }
        let response = query_in_session(
            session.as_ref(),
            &headers,
            QueryRequest {
                query: "SELECT (".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers()[QUERY_SESSION_HEADER],
            session.session_id()
        );

        headers.insert(
            "x-restate-query-client",
            HeaderValue::from_bytes(b"\xff").unwrap(),
        );
        assert_eq!(
            collect_query_headers(&headers)["x-restate-query-client"].as_bytes(),
            b"\xff"
        );
    }
}
