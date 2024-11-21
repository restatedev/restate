// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use chrono::DateTime;
use http::{header, HeaderName, Response};
use http_body_util::Full;
use tracing::{info, trace};

use crate::handler::error::HandlerError;
use crate::handler::Handler;
use restate_types::invocation::InvocationTarget;
use restate_types::net::partition_processor::{IngressResponseResult, InvocationOutput};
use restate_types::schema::invocation_target::InvocationTargetMetadata;

pub(crate) const IDEMPOTENCY_EXPIRES: HeaderName = HeaderName::from_static("idempotency-expires");
/// Contains the string representation of the invocation id
pub(crate) const X_RESTATE_ID: HeaderName = HeaderName::from_static("x-restate-id");

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher> {
    pub(crate) fn reply_with_invocation_response(
        InvocationOutput {
            response,
            invocation_id,
            completion_expiry_time,
            ..
        }: InvocationOutput,
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
        if let Some(expiry_time) = completion_expiry_time {
            response_builder = response_builder.header(
                IDEMPOTENCY_EXPIRES,
                DateTime::from_timestamp_millis(expiry_time.as_u64() as i64)
                    .expect("conversion to chrono should work")
                    .to_rfc3339(),
            );
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
