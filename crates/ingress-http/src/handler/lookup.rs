// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full};
use serde::Serialize;

use restate_types::identifiers::InvocationId;

use super::{APPLICATION_JSON, Handler, HandlerError, InvocationTargetRequest};

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
#[serde(rename_all = "camelCase")]
struct LookupResponse {
    invocation_id: InvocationId,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher> {
    pub(crate) async fn handle_lookup<B: http_body::Body>(
        self,
        req: Request<B>,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        if req.method() != Method::POST {
            return Err(HandlerError::MethodNotAllowed);
        }

        let body_bytes = req
            .into_body()
            .collect()
            .await
            .map_err(|e| HandlerError::Body(e.into()))?
            .to_bytes();

        let target_request: InvocationTargetRequest = serde_json::from_slice(&body_bytes)
            .map_err(|e| HandlerError::Body(anyhow::anyhow!("invalid lookup body: {e}")))?;

        let invocation_query = target_request.into_invocation_query()?;
        let invocation_id = invocation_query.to_invocation_id();

        let body = serde_json::to_vec(&LookupResponse { invocation_id })
            .expect("LookupResponse serialization cannot fail");

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .body(Full::new(body.into()))
            .unwrap())
    }
}
