// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_meta::MetaReader;
use restate_node_services::schema::schema_server::Schema;
use restate_node_services::schema::{FetchSchemasRequest, FetchSchemasResponse};
use tonic::{Request, Response, Status};

pub struct SchemaHandler<S> {
    schema_reader: S,
}

impl<S> SchemaHandler<S> {
    pub fn new(meta_reader: S) -> Self {
        Self {
            schema_reader: meta_reader,
        }
    }
}

#[async_trait::async_trait]
impl<S> Schema for SchemaHandler<S>
where
    S: MetaReader + Send + Sync + 'static,
{
    async fn fetch_schemas(
        &self,
        _request: Request<FetchSchemasRequest>,
    ) -> Result<Response<FetchSchemasResponse>, Status> {
        let schema_updates = self.schema_reader.read().await.map_err(|err| {
            Status::internal(format!("Could not read schema information: '{}'", err))
        })?;

        let serialized_updates =
            bincode::serde::encode_to_vec(schema_updates, bincode::config::standard()).map_err(
                |err| {
                    Status::internal(format!("Could not serialize schema information: '{}'", err))
                },
            )?;

        Ok(Response::new(FetchSchemasResponse {
            schemas_bin: serialized_updates.into(),
        }))
    }
}
