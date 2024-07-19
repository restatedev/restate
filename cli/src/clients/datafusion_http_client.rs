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

use crate::cli_env::CliEnv;
use crate::clients::AdminClient;
use arrow::array::{AsArray, StructArray};
use arrow::datatypes::{ArrowPrimitiveType, Int64Type, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow_convert::deserialize::{arrow_array_deserialize_iterator, ArrowDeserialize};
use arrow_convert::field::ArrowField;
use bytes::Buf;
use itertools::Itertools;
use serde::Serialize;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Error, Debug)]
#[error(transparent)]
pub enum Error {
    Api(#[from] Box<ApiError>),
    #[error("(Protocol error) {0}")]
    Serialization(#[from] serde_json::Error),
    Network(#[from] reqwest::Error),
    Arrow(#[from] ArrowError),
    #[error("Mapping from query '{0}': {1}")]
    Mapping(String, #[source] ArrowError),
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
            .prepare(reqwest::Method::POST, self.inner.base_url.join("/query")?))
    }

    pub async fn run_query(&self, query: String) -> Result<SqlResponse, Error> {
        debug!("Sending request sql query '{}'", query);
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

    pub async fn run_query_and_map_results<T: ArrowDeserialize + ArrowField<Type = T> + 'static>(
        &self,
        query: String,
    ) -> Result<impl Iterator<Item = T>, Error> {
        let sql_response = self.run_query(query.clone()).await?;
        let mut results = Vec::new();
        for batch in sql_response.batches {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            results.reserve(n);

            // Map results using arrow_convert
            for row in arrow_array_deserialize_iterator::<T>(&StructArray::from(batch))
                .map_err(|e| Error::Mapping(query.clone(), e))?
            {
                results.push(row);
            }
        }
        Ok(results.into_iter())
    }

    pub async fn run_count_agg_query(&self, query: String) -> Result<i64, Error> {
        let resp = self.run_query(query).await?;

        Ok(get_column_as::<Int64Type>(&resp.batches, 0)
            .first()
            .map(|v| **v)
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
}

fn get_column_as<T>(
    batches: &[RecordBatch],
    column_index: usize,
) -> Vec<&<T as ArrowPrimitiveType>::Native>
where
    T: ArrowPrimitiveType,
{
    let mut output = vec![];
    for batch in batches {
        let col = batch.column(column_index);
        assert_eq!(col.data_type(), &T::DATA_TYPE);

        let l = col.as_primitive::<T>();
        output.extend(l.values());
    }
    output
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
