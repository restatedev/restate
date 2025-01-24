// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::path_parsing::AwakeableRequestType;
use super::Handler;
use super::HandlerError;

use crate::RequestDispatcher;
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use http_body_util::Full;
use restate_types::errors::{codes, InvocationError};
use restate_types::identifiers::{AwakeableIdentifier, ExternalSignalIdentifier, WithInvocationId};
use restate_types::invocation::{InvocationResponse, ResponseResult};
use restate_types::journal_v2::{Signal, SignalResult};
use std::str::FromStr;
use tracing::{info, trace, warn};

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_awakeable<B: http_body::Body>(
        self,
        req: Request<B>,
        awakeable_request_type: AwakeableRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        // Check HTTP Method
        if req.method() != Method::POST {
            return Err(HandlerError::MethodNotAllowed);
        }

        // Collect body
        let collected_request_bytes = req
            .into_body()
            .collect()
            .await
            .map_err(|e| HandlerError::Body(e.into()))?
            .to_bytes();
        trace!(rpc.request = ?collected_request_bytes);

        let (awakeable_id, result) = match awakeable_request_type {
            AwakeableRequestType::Resolve { awakeable_id } => (
                awakeable_id,
                ResponseResult::from(Ok(collected_request_bytes)),
            ),
            AwakeableRequestType::Reject { awakeable_id } => (
                awakeable_id,
                ResponseResult::from(Err(InvocationError::new(
                    codes::UNKNOWN,
                    String::from_utf8_lossy(&collected_request_bytes).to_string(),
                ))),
            ),
        };

        // Try parse first as signal identifier
        let res = if let Ok(signal_id) = ExternalSignalIdentifier::from_str(&awakeable_id) {
            info!(
                restate.invocation.id = %signal_id.invocation_id(),
                restate.signal.id = %signal_id,
                "Processing awakeables request"
            );

            let (invocation_id, signal_id) = signal_id.into_inner();

            self.dispatcher
                .send_signal(
                    invocation_id,
                    Signal::new(
                        signal_id,
                        match result {
                            ResponseResult::Success(s) => SignalResult::Success(s),
                            ResponseResult::Failure(f) => SignalResult::Failure(f.into()),
                        },
                    ),
                )
                .await
        } else {
            let awakeable_identifier = AwakeableIdentifier::from_str(&awakeable_id)
                .map_err(|e| HandlerError::BadAwakeableId(awakeable_id, e))?;
            let (invocation_id, entry_index) = awakeable_identifier.into_inner();

            info!(
                restate.invocation.id = %invocation_id,
                restate.journal.index = entry_index,
                "Processing awakeables request"
            );
            self.dispatcher
                .send_invocation_response(InvocationResponse {
                    id: invocation_id,
                    entry_index,
                    result,
                })
                .await
        };

        if let Err(e) = res {
            warn!("Failed to dispatch awakeable completion: {}", e);
            return Err(HandlerError::Unavailable);
        }

        Ok(hyper::Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Full::default())
            .unwrap())
    }
}
