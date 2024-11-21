// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tokio::time::Instant;
use tracing::{instrument, trace};

use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{Incoming, NetworkError, Networking, TransportConnect};
use restate_core::ShutdownError;
use restate_types::config::Configuration;
use restate_types::net::log_server::{LogServerRequest, LogServerResponse};
use restate_types::retries::RetryPolicy;
use restate_types::PlainNodeId;

use crate::loglet::util::TailOffsetWatch;

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("task was aborted based on a precondition check")]
    Aborted,
    #[error("task failed after exhausting all retries, total time spent is {0:?}")]
    ExhaustedRetries(Duration),
    #[error("task stopped due to ongoing system shutdown")]
    Shutdown(#[from] ShutdownError),
}

#[allow(dead_code)]
pub enum Disposition<T> {
    Return(T),
    Retry,
    Abort,
}

#[allow(dead_code)]
pub fn passthrough<I>(msg: Incoming<I>) -> Disposition<I> {
    Disposition::Return(msg.into_body())
}

pub struct RunOnSingleNode<'a, T: LogServerRequest> {
    node_id: PlainNodeId,
    request: T,
    rpc_router: &'a RpcRouter<T>,
    known_global_tail: &'a TailOffsetWatch,
    retry_policy: RetryPolicy,
}

impl<'a, T: LogServerRequest> RunOnSingleNode<'a, T>
where
    T: LogServerRequest + Clone,
    T::ResponseMessage: LogServerResponse,
{
    pub fn new(
        node_id: PlainNodeId,
        request: T,
        rpc_router: &'a RpcRouter<T>,
        known_global_tail: &'a TailOffsetWatch,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            node_id,
            request,
            rpc_router,
            known_global_tail,
            retry_policy,
        }
    }

    /// Send a message to a single node and handles network-related failures with retries.
    #[instrument(skip_all, fields(loglet_id = %self.request.header().loglet_id, message = self.request.kind()))]
    pub async fn run<O, N: TransportConnect>(
        mut self,
        on_response: impl Fn(Incoming<T::ResponseMessage>) -> Disposition<O>,
        networking: &'a Networking<N>,
    ) -> Result<O, TaskError> {
        let start = Instant::now();
        let loglet_id = self.request.header().loglet_id;
        let request_timeout = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_rpc_timeout;

        let mut retry_iter = self.retry_policy.into_iter();

        let mut attempt = 0;
        loop {
            attempt += 1;
            self.request
                .refresh_header(self.known_global_tail.latest_offset());
            let next_pause = retry_iter.next();

            trace!(%loglet_id, "Sending {} message to node {}", self.request.kind(), self.node_id);
            // loop and retry until this task is aborted.
            let maybe_response = self
                .rpc_router
                .call_timeout(
                    networking,
                    self.node_id,
                    self.request.clone(),
                    request_timeout,
                )
                .await;

            match maybe_response {
                Ok(msg) => {
                    // update our view of global tail if we observed higher tail in response.
                    self.known_global_tail
                        .notify_offset_update(msg.body().header().known_global_tail);
                    match on_response(msg) {
                        Disposition::Return(v) => return Ok(v),
                        Disposition::Abort => return Err(TaskError::Aborted),
                        Disposition::Retry if next_pause.is_some() => {
                            trace!(
                                %attempt,
                                "Response received but a retry was requested. Will retry after {:?}",
                                next_pause.unwrap()
                            );
                        } // fall-through
                        Disposition::Retry => {
                            // exhausted retries budget. We represent this by timeout.
                            // give up.
                            trace!(%attempt, "Exhausted retries");
                            return Err(TaskError::ExhaustedRetries(start.elapsed()));
                        }
                    }
                }
                Err(NetworkError::Shutdown(err)) => {
                    // We are shutting down
                    return Err(TaskError::Shutdown(err));
                }
                // We'll retry in every other case of networking error
                Err(e) if next_pause.is_some() => {
                    trace!(
                        ?e,
                        %attempt,
                        "Request failed. Will retry after {:?}",
                        next_pause.unwrap()
                    );
                }
                Err(e) => {
                    trace!(
                        ?e,
                        %attempt,
                        "Request failed. Exhausted attempts after spending {:?}",
                        start.elapsed(),
                    );
                    // exhausted
                    // give up.
                    trace!(%attempt, "Exhausted retries");
                    return Err(TaskError::ExhaustedRetries(start.elapsed()));
                }
            }

            // Should we retry?
            if let Some(pause) = next_pause {
                tokio::time::sleep(pause).await;
            } else {
                // give up.
                return Err(TaskError::ExhaustedRetries(start.elapsed()));
            }
        }
    }
}
