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
use bytestring::ByteString;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full};
use serde::{Deserialize, Serialize};

use restate_types::Scope;
use restate_types::identifiers::{IdempotencyId, InvocationId, ServiceId};
use restate_types::invocation::InvocationQuery;
use restate_util_string::{ReString, RestrictedValue};

use super::{APPLICATION_JSON, Handler, HandlerError};

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum InvocationTargetRequest {
    Workflow {
        name: ReString,
        key: ReString,
        #[serde(default)]
        scope: Option<ReString>,
    },
    Idempotency {
        service: ReString,
        #[serde(default)]
        service_key: Option<ReString>,
        handler: ReString,
        idempotency_key: ReString,
        #[serde(default)]
        scope: Option<ReString>,
    },
}

impl InvocationTargetRequest {
    fn into_invocation_query(self) -> Result<InvocationQuery, HandlerError> {
        let scope_value = match self {
            Self::Workflow { ref scope, .. } | Self::Idempotency { ref scope, .. } => scope.clone(),
        };

        // Unfortunately, we cannot first check the existence of the service/handler or workflow
        // because it might have been removed from the Schema after an invocation having completed :-(
        // For such a check to work, we need to keep information about previously registered services
        // and workflows. Hence, we can only validate that the scope value is valid and hope that
        // nobody is DOSing us with valid but meaningless scopes for the time being.
        let scope = match scope_value {
            None => None,
            Some(s) => Some(Scope::new(
                RestrictedValue::new(s)
                    .map_err(HandlerError::BadScopeValue)?
                    .as_str(),
            )),
        };

        Ok(match self {
            Self::Workflow { name, key, .. } => InvocationQuery::Workflow(ServiceId::new(
                scope,
                ByteString::from(name.as_str()),
                ByteString::from(key.as_str()),
            )),
            Self::Idempotency {
                service,
                service_key,
                handler,
                idempotency_key,
                ..
            } => InvocationQuery::IdempotencyId(IdempotencyId::new(
                ByteString::from(service.as_str()),
                service_key.map(|s| ByteString::from(s.as_str())),
                ByteString::from(handler.as_str()),
                ByteString::from(idempotency_key.as_str()),
                scope,
            )),
        })
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(Deserialize))]
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
