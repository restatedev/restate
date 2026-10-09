// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A wrapper client for the datafusion HTTP service.

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow::{
    array::AsArray,
    datatypes::{Int64Type, SchemaRef},
};
use bytes::Buf;
use clap::ValueEnum;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use restate_types::SemanticRestateVersion;

use crate::cli_env::CliEnv;
use crate::clients::AdminClient;

use super::errors::{ApiError, ApiErrorBody, ClientError};

/// Header selecting the query engine on the admin `/query` endpoint. The response carries the
/// same header with the engine that served the query.
const QUERY_ENGINE_HEADER: &str = "x-restate-query-engine";

/// Query engine that runs a SQL query on the server.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryEngine {
    /// The default query engine
    V1,
    /// The experimental distributed query engine. Requires the `query-engine-v2`
    /// experimental feature on the server
    V2,
}

impl QueryEngine {
    fn as_header_value(&self) -> &'static str {
        match self {
            QueryEngine::V1 => "v1",
            QueryEngine::V2 => "v2",
        }
    }
}

/// A handy client for the datafusion HTTP service.
#[derive(Clone)]
pub struct DataFusionHttpClient {
    pub(crate) inner: AdminClient,
    /// Engine requested through the query-engine header, `None` leaves the choice to the server.
    query_engine: Option<QueryEngine>,
}

impl From<AdminClient> for DataFusionHttpClient {
    fn from(value: AdminClient) -> Self {
        DataFusionHttpClient {
            inner: value,
            query_engine: None,
        }
    }
}

impl DataFusionHttpClient {
    pub async fn new(env: &CliEnv) -> anyhow::Result<Self> {
        let inner = AdminClient::new(env).await?;

        Ok(Self::from(inner))
    }

    /// Run the queries on the given engine instead of the server's default one.
    pub fn with_query_engine(mut self, query_engine: Option<QueryEngine>) -> Self {
        self.query_engine = query_engine;
        self
    }

    /// Prepare a request builder for a DataFusion request.
    fn prepare(&self) -> Result<reqwest::RequestBuilder, ClientError> {
        let mut builder = self
            .inner
            .prepare(reqwest::Method::POST, self.inner.versioned_url(["query"]));
        if let Some(engine) = self.query_engine {
            builder = builder.header(QUERY_ENGINE_HEADER, engine.as_header_value());
        }
        Ok(builder)
    }

    pub async fn run_json_query<T: serde::de::DeserializeOwned>(
        &self,
        query: String,
    ) -> Result<Vec<T>, ClientError> {
        debug!("Sending request sql query with json output '{}'", query);
        let resp = self
            .prepare()?
            .header(http::header::ACCEPT, "application/json")
            .json(&SqlQueryRequest { query })
            .send()
            .await?;

        let http_status_code = resp.status();
        let url = resp.url().clone();
        if !resp.status().is_success() {
            let body = resp.text().await?;
            info!("Response from {} ({})", url, http_status_code);
            info!("  {}", body);
            // Wrap the error into ApiError
            return Err(ClientError::Api(ApiError {
                http_status_code,
                url: url.into(),
                body: ApiErrorBody::parse(body),
            }));
        }

        match resp.headers().get(http::header::CONTENT_TYPE) {
            Some(header) if header.eq("application/json") => {}
            _ => {
                return Err(ClientError::JSONSupport(
                    self.inner.base_url.clone(),
                    self.inner.restate_server_version.to_string(),
                ));
            }
        }

        // We read the entire payload first in-memory to simplify the logic, however,
        // if this ever becomes a problem, we can use bytes_stream() (requires
        // reqwest's stream feature) and stitch that with the stream reader.
        let payload = resp.bytes().await?.reader();

        Ok(serde_json::from_reader::<_, JsonResponse<T>>(payload)?.rows)
    }

    pub async fn run_arrow_query(&self, query: String) -> Result<SqlResponse, ClientError> {
        debug!("Sending request sql query with arrow output '{}'", query);
        let resp = self
            .prepare()?
            .json(&SqlQueryRequest { query })
            .send()
            .await?;

        let http_status_code = resp.status();
        let url = resp.url().clone();
        if !resp.status().is_success() {
            let body = resp.text().await?;
            info!("Response from {} ({})", url, http_status_code);
            info!("  {}", body);
            // Wrap the error into ApiError
            return Err(ClientError::Api(ApiError {
                http_status_code,
                url: url.into(),
                body: ApiErrorBody::parse(body),
            }));
        }

        let engine = resp
            .headers()
            .get(QUERY_ENGINE_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);

        // We read the entire payload first in-memory to simplify the logic, however,
        // if this ever becomes a problem, we can use bytes_stream() (requires
        // reqwest's stream feature) and stitch that with the stream reader.
        let payload = resp.bytes().await?.reader();
        let reader = StreamReader::try_new(payload, None)?;
        let schema = reader.schema();

        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        Ok(SqlResponse {
            schema,
            batches,
            engine,
        })
    }

    pub async fn run_count_agg_query(&self, query: String) -> Result<i64, ClientError> {
        let resp = self.run_arrow_query(query).await?;

        Ok(resp
            .batches
            .first()
            .and_then(|batch| batch.column(0).as_primitive::<Int64Type>().values().first())
            .cloned()
            .unwrap_or(0))
    }

    pub async fn check_columns_exists(
        &self,
        table: &str,
        columns: &[&str],
    ) -> Result<bool, ClientError> {
        let expected_count = columns.len();

        let actual_count = self
            .run_count_agg_query(format!(
                "SELECT COUNT(*) FROM information_schema.columns
            WHERE
                table_name = '{table}'
                AND column_name IN ({})",
                columns.iter().map(|s| format!("'{s}'")).join(", ")
            ))
            .await?;

        Ok(actual_count as usize == expected_count)
    }

    pub fn server_version(&self) -> &SemanticRestateVersion {
        &self.inner.restate_server_version
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct SqlQueryRequest {
    pub query: String,
}

pub struct SqlResponse {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    /// Engine that served the query, as reported by the server (older servers omit it).
    pub engine: Option<String>,
}

#[derive(Deserialize)]
pub struct JsonResponse<T> {
    pub rows: Vec<T>,
}

// Ensure that client is Send + Sync. Compiler will fail if it's not.
const _: () = {
    const fn assert_send<T: Send + Sync>() {}
    assert_send::<DataFusionHttpClient>();
};
