// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::handler::error::HandlerError;
use crate::handler::Handler;
use bytes::Bytes;
use http::{header, HeaderName, Response};
use http_body_util::Full;
use restate_schema_api::invocation_target::InvocationTargetMetadata;
use restate_types::identifiers::InvocationId;
use restate_types::ingress::IngressResponseResult;
use restate_types::invocation::InvocationTarget;
use tracing::{info, trace};

pub(crate) const IDEMPOTENCY_EXPIRES: HeaderName = HeaderName::from_static("idempotency-expires");
/// Contains the string representation of the invocation id
pub(crate) const X_RESTATE_ID: HeaderName = HeaderName::from_static("x-restate-id");

impl<Schemas, Dispatcher, StorageReader> Handler<Schemas, Dispatcher, StorageReader> {
    pub(crate) fn reply_with_invocation_response(
        response: IngressResponseResult,
        invocation_id: Option<InvocationId>,
        idempotency_expiry_time: Option<&str>,
        invocation_target_metadata_retriever: impl FnOnce(
            &InvocationTarget,
        ) -> Result<
            InvocationTargetMetadata,
            HandlerError,
        >,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        // Prepare response metadata
        let mut response_builder = hyper::Response::builder();

        // Add invocation id if any
        if let Some(id) = invocation_id {
            response_builder = response_builder.header(X_RESTATE_ID, id.to_string());
        }

        // Add idempotency expiry time if available
        if let Some(expiry_time) = idempotency_expiry_time {
            response_builder = response_builder.header(IDEMPOTENCY_EXPIRES, expiry_time);
        }

        match response {
            IngressResponseResult::Success(invocation_target, response_payload) => {
                trace!(rpc.response = ?response_payload, "Complete external HTTP request successfully");

                // Resolve invocation target metadata.
                // We need it for the output content type.
                let invocation_target_metadata =
                    invocation_target_metadata_retriever(&invocation_target)?;

                // Write out the content-type, if any
                // TODO fix https://github.com/restatedev/restate/issues/1496
                if let Some(ct) = invocation_target_metadata
                    .output_rules
                    .infer_content_type(response_payload.is_empty())
                {
                    response_builder = response_builder.header(
                        header::CONTENT_TYPE,
                        // TODO we need this to_str().unwrap() because these two HeaderValue come from two different http crates
                        //  We can remove it once https://github.com/restatedev/restate/issues/96 is done
                        ct.to_str().unwrap(),
                    )
                }

                Ok(response_builder.body(Full::new(response_payload)).unwrap())
            }
            IngressResponseResult::Failure(error) => {
                info!(rpc.response = ?error, "Complete external HTTP request with a failure");
                Ok(HandlerError::Invocation(error).fill_builder(response_builder))
            }
        }
    }
}
