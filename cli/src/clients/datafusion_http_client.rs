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

use super::errors::ApiError;

use crate::cli_env::CliEnv;
use crate::clients::AdminClient;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow::{
    array::AsArray,
    datatypes::{Int64Type, SchemaRef},
};
use bytes::Buf;
use itertools::Itertools;
use restate_types::SemanticRestateVersion;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info};
use url::Url;

#[derive(Error, Debug)]
#[error(transparent)]
pub enum Error {
    Api(#[from] Box<ApiError>),
    #[error(
        "The Restate server '{0}' lacks JSON /query support. Please update the CLI to match the Restate server version '{1}'."
    )]
    JSONSupport(Url, String),
    #[error("(Protocol error) {0}")]
    Serialization(#[from] serde_json::Error),
    Network(#[from] reqwest::Error),
    Arrow(#[from] ArrowError),
    UrlParse(#[from] url::ParseError),
}

/// A handy client for the datafusion HTTP service.
#[derive(Clone)]
pub struct DataFusionHttpClient {
    pub(crate) inner: AdminClient,
}

impl From<AdminClient> for DataFusionHttpClient {
    fn from(value: AdminClient) -> Self {
        DataFusionHttpClient { inner: value }
    }
}

impl DataFusionHttpClient {
    pub async fn new(env: &CliEnv) -> anyhow::Result<Self> {
        let inner = AdminClient::new(env).await?;

        Ok(Self { inner })
    }

    /// Prepare a request builder for a DataFusion request.
    fn prepare(&self) -> Result<reqwest::RequestBuilder, Error> {
        Ok(self
            .inner
            .prepare(reqwest::Method::POST, self.inner.versioned_url(["query"])))
    }

    pub async fn run_json_query<T: serde::de::DeserializeOwned>(
        &self,
        query: String,
    ) -> Result<Vec<T>, Error> {
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
            return Err(Error::Api(Box::new(ApiError {
                http_status_code,
                url,
                body: serde_json::from_str(&body)?,
            })));
        }

        match resp.headers().get(http::header::CONTENT_TYPE) {
            Some(header) if header.eq("application/json") => {}
            _ => {
                return Err(Error::JSONSupport(
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

    pub async fn run_arrow_query(&self, query: String) -> Result<SqlResponse, Error> {
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
            return Err(Error::Api(Box::new(ApiError {
                http_status_code,
                url,
                body: serde_json::from_str(&body)?,
            })));
        }

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

        Ok(SqlResponse { schema, batches })
    }

    pub async fn run_count_agg_query(&self, query: String) -> Result<i64, Error> {
        let resp = self.run_arrow_query(query).await?;

        Ok(resp
            .batches
            .first()
            .and_then(|batch| batch.column(0).as_primitive::<Int64Type>().values().first())
            .cloned()
            .unwrap_or(0))
    }

    pub async fn check_columns_exists(&self, table: &str, columns: &[&str]) -> Result<bool, Error> {
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
