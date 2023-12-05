// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use crate::build_info;
use crate::cli_env::CliEnv;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use bytes::Buf;
use serde::Serialize;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Error, Debug)]
#[error(transparent)]
pub enum Error {
    Api(#[from] ApiError),
    #[error("(Protocol error) {0}")]
    Serialization(#[from] serde_json::Error),
    Network(#[from] reqwest::Error),
    ArrowError(#[from] ArrowError),
    UrlParseError(#[from] url::ParseError),
}

/// A handy client for the datafusion HTTP service.
#[derive(Clone)]
pub struct DataFusionHttpClient {
    pub(crate) inner: reqwest::Client,
    pub(crate) base_url: reqwest::Url,
}

impl DataFusionHttpClient {
    pub fn new(env: &CliEnv) -> reqwest::Result<Self> {
        let raw_client = reqwest::Client::builder()
            .user_agent(format!(
                "{}/{} {}-{}",
                env!("CARGO_PKG_NAME"),
                build_info::RESTATE_CLI_VERSION,
                std::env::consts::OS,
                std::env::consts::ARCH,
            ))
            .build()?;

        Ok(Self {
            inner: raw_client,
            base_url: env.datafusion_http_base_url.clone(),
        })
    }

    pub async fn run_query(&self, query: String) -> Result<SqlResponse, Error> {
        let url = self.base_url.join("/api/query")?;

        debug!("Sending request sql query '{}'", query);
        // TODO: Add authentication
        let resp = self
            .inner
            .request(reqwest::Method::POST, url)
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
            return Err(Error::Api(ApiError {
                http_status_code,
                url,
                body: serde_json::from_str(&body)?,
            }));
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
}

#[derive(Serialize, Debug, Clone)]
pub struct SqlQueryRequest {
    pub query: String,
}

pub struct SqlResponse {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

// Ensure that client is Send + Sync. Compiler will fail if it's not.
const _: () = {
    const fn assert_send<T: Send + Sync>() {}
    assert_send::<DataFusionHttpClient>();
};
