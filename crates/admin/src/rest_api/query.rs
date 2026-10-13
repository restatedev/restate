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
    AdminUser, QueryEngine, QueryOptions, QuerySession, RecordBatchWriter, SessionOptions,
    WriteRecordBatchStream,
};
use restate_types::invocation::client::InvocationClient;
use restate_types::schema::registry::{DiscoveryClient, MetadataService, TelemetryClient};

use crate::query_context::collect_query_headers;
use crate::state::AdminServiceState;

const RETRY_AFTER_HEADER: &str = "Retry-After";
const QUERY_SESSION_HEADER: &str = "x-restate-query-session-id";
// Internal staging selector, consumed before diagnostic-context header collection.
const QUERY_ENGINE_HEADER: &str = "x-restate-query-engine";

#[derive(Clone, Copy)]
enum EngineSelection {
    Legacy,
    Distributed,
}

impl EngineSelection {
    fn from_headers(headers: &HeaderMap, default: Self) -> Result<Self, QueryError> {
        let mut values = headers.get_all(QUERY_ENGINE_HEADER).iter();
        let selection = match values.next().map(HeaderValue::as_bytes) {
            None => default,
            Some(b"v1") => Self::Legacy,
            Some(b"v2") => Self::Distributed,
            _ => return Err(QueryError::InvalidEngineSelection),
        };
        if values.next().is_some() {
            return Err(QueryError::InvalidEngineSelection);
        }
        Ok(selection)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "v1",
            Self::Distributed => "v2",
        }
    }
}

/// Error response for query endpoint.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct QueryErrorBody {
    message: String,
}

/// Errors that can occur when executing a query.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryError {
    #[error("X-Restate-Query-Engine must contain a single value: v1 or v2")]
    InvalidEngineSelection,
    #[error("Query engine v2 is not enabled on this node")]
    DistributedUnavailable,
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
            QueryError::InvalidEngineSelection => StatusCode::BAD_REQUEST,
            QueryError::Unavailable | QueryError::DistributedUnavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
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
            headers(
                ("X-Restate-Query-Session-Id" = String, description = "Server-generated query session ID"),
                ("X-Restate-Query-Engine" = String, description = "Selected query engine: v1 or v2 (distributed)"),
                ("Server-Timing" = String, description = "Query planning duration in milliseconds")
            )),
        (status = 400, description = "Invalid query or engine selection", body = QueryErrorBody),
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
    let default = if state.query_engine_v2_default {
        EngineSelection::Distributed
    } else {
        EngineSelection::Legacy
    };
    query_with_engines(
        state.query_engine.as_ref(),
        state.distributed_query_engine.as_deref(),
        default,
        &headers,
        payload,
    )
    .await
}

async fn query_with_engines(
    legacy: &dyn QueryEngine<AdminUser>,
    distributed: Option<&dyn QueryEngine<AdminUser>>,
    default: EngineSelection,
    headers: &HeaderMap,
    payload: QueryRequest,
) -> Result<Response, QueryError> {
    let selection = EngineSelection::from_headers(headers, default)?;
    let mut response = async {
        let engine = match selection {
            EngineSelection::Legacy => legacy,
            EngineSelection::Distributed => {
                distributed.ok_or(QueryError::DistributedUnavailable)?
            }
        };
        let session = engine.create_session(SessionOptions {
            headers: collect_query_headers(headers),
            ..Default::default()
        })?;
        tracing::info!(target: "query_engine", session = %session.session_id(),
            engine = selection.as_str(), "Selected query engine");
        query_in_session(session.as_ref(), headers, payload).await
    }
    .await
    .into_response();
    response.headers_mut().insert(
        QUERY_ENGINE_HEADER,
        HeaderValue::from_static(selection.as_str()),
    );
    Ok(response)
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
    let server_timing = format!(
        "planning;dur={:.3}",
        query_result.metadata.planning_duration.as_secs_f64() * 1000.0
    );

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
        .header("server-timing", server_timing)
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http_body_util::BodyExt;

    use restate_storage_query_datafusion::DataFusionEnv;
    use restate_storage_query_datafusion::context::DataFusionQueryEngine;

    use super::*;

    struct CountingEngine {
        inner: Arc<dyn QueryEngine<AdminUser>>,
        sessions: AtomicUsize,
    }

    impl QueryEngine<AdminUser> for CountingEngine {
        fn create_session(
            &self,
            opts: SessionOptions,
        ) -> Result<Arc<dyn QuerySession<AdminUser>>, SessionError> {
            assert!(!opts.headers.contains_key(QUERY_ENGINE_HEADER));
            self.sessions.fetch_add(1, Ordering::Relaxed);
            self.inner.create_session(opts)
        }
    }

    #[tokio::test]
    async fn engine_selection_is_explicit_and_preserves_responses() {
        let engine = || {
            let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new())
                .unwrap()
                .with_mock_clock(restate_types::clock::MockClock::new())
                .unwrap();
            CountingEngine {
                inner: DataFusionQueryEngine::<AdminUser>::from_inventory(env, None, vec![]),
                sessions: AtomicUsize::new(0),
            }
        };
        let legacy = engine();
        let distributed = engine();
        let payload = || QueryRequest {
            query: "SELECT 42 AS answer".into(),
        };
        let mut headers = HeaderMap::new();
        for default in [EngineSelection::Legacy, EngineSelection::Distributed] {
            headers.remove(QUERY_ENGINE_HEADER);
            for selection in [None, Some("v1"), Some("v2")] {
                for accept in ["application/json", "application/vnd.apache.arrow.stream"] {
                    headers.insert(http::header::ACCEPT, HeaderValue::from_static(accept));
                    if let Some(selection) = selection {
                        headers.insert(QUERY_ENGINE_HEADER, HeaderValue::from_static(selection));
                    }
                    let response = query_with_engines(
                        &legacy,
                        Some(&distributed),
                        default,
                        &headers,
                        payload(),
                    )
                    .await
                    .unwrap();
                    assert_eq!(response.status(), StatusCode::OK);
                    assert_eq!(
                        response.headers()[QUERY_ENGINE_HEADER],
                        selection.unwrap_or(default.as_str())
                    );
                    assert!(response.headers().contains_key(QUERY_SESSION_HEADER));
                    assert!(
                        response.headers()["server-timing"]
                            .to_str()
                            .unwrap()
                            .starts_with("planning;dur=")
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
                        assert_eq!(
                            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                            1
                        );
                    }
                }
            }
        }
        assert_eq!(legacy.sessions.load(Ordering::Relaxed), 6);
        assert_eq!(distributed.sessions.load(Ordering::Relaxed), 6);

        let response =
            query_with_engines(&legacy, None, EngineSelection::Legacy, &headers, payload())
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v2");
        assert!(!response.headers().contains_key(QUERY_SESSION_HEADER));
        let response = query_with_engines(
            &legacy,
            Some(&distributed),
            EngineSelection::Legacy,
            &headers,
            QueryRequest {
                query: "SELECT (".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v2");
        assert!(response.headers().contains_key(QUERY_SESSION_HEADER));

        headers.append(QUERY_ENGINE_HEADER, HeaderValue::from_static("v1"));
        assert_eq!(
            query_with_engines(
                &legacy,
                Some(&distributed),
                EngineSelection::Distributed,
                &headers,
                payload()
            )
            .await
            .into_response()
            .status(),
            StatusCode::BAD_REQUEST
        );
        for invalid in [
            HeaderValue::from_static("unknown"),
            HeaderValue::from_bytes(b"\xff").unwrap(),
        ] {
            headers.insert(QUERY_ENGINE_HEADER, invalid);
            assert_eq!(
                query_with_engines(
                    &legacy,
                    Some(&distributed),
                    EngineSelection::Distributed,
                    &headers,
                    payload()
                )
                .await
                .into_response()
                .status(),
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            legacy.sessions.load(Ordering::Relaxed),
            6,
            "selection failures must not fall back"
        );
        assert_eq!(distributed.sessions.load(Ordering::Relaxed), 7);

        headers.remove(QUERY_ENGINE_HEADER);
        let response = query_with_engines(
            &legacy,
            None,
            EngineSelection::Distributed,
            &headers,
            payload(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v2");
        assert_eq!(legacy.sessions.load(Ordering::Relaxed), 6);

        headers.insert(QUERY_ENGINE_HEADER, HeaderValue::from_static("v1"));
        let response = query_with_engines(
            &legacy,
            None,
            EngineSelection::Distributed,
            &headers,
            payload(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v1");
        response.into_body().collect().await.unwrap();
        assert_eq!(legacy.sessions.load(Ordering::Relaxed), 7);
    }

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

        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new())
            .unwrap()
            .with_mock_clock(restate_types::clock::MockClock::new())
            .unwrap();
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
