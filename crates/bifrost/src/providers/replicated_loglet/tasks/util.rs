// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU8;
use std::time::Duration;

use adaptive_timeout::{AdaptiveTimeout, TimeoutConfig};
use tokio::time::Instant;
use tracing::{debug, instrument, trace};

use restate_core::ShutdownError;
use restate_core::network::{NetworkSender, Networking, RpcError, Swimlane, TransportConnect};
use restate_time_util::DurationExt;
use restate_types::PlainNodeId;
use restate_types::config::Configuration;
use restate_types::logs::TailOffsetWatch;
use restate_types::net::RpcRequest;
use restate_types::net::log_server::{LogServerMessage, LogServerResponse};

use crate::providers::replicated_loglet::LATENCY_TRACKER;

pub struct Attempts {
    /// None means infinite attempts
    inner: Option<NonZeroU8>,
}

impl Attempts {
    pub fn once() -> Self {
        Self {
            // SAFETY: we know that the value is non-zero
            inner: Some(unsafe { NonZeroU8::new_unchecked(1) }),
        }
    }

    pub fn exact(max_retries: NonZeroU8) -> Self {
        Self {
            inner: Some(max_retries),
        }
    }
}

struct Retries {
    /// None means infinite retries
    attempts: Option<NonZeroU8>,
    current: u32,
}

impl Retries {
    #[inline]
    pub const fn new(attempts: Attempts) -> Self {
        Self {
            attempts: attempts.inner,
            current: 0,
        }
    }

    #[inline]
    pub const fn current(&self) -> u32 {
        self.current
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        self.attempts
            .is_none_or(|max| self.current < max.get() as u32)
    }

    #[inline]
    pub const fn advance(&mut self) {
        self.current = self.current.saturating_add(1);
    }
}

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

pub struct RunOnSingleNode<'a, T: LogServerMessage> {
    node_id: PlainNodeId,
    request: T,
    known_global_tail: &'a TailOffsetWatch,
    attempts: Attempts,
}

impl<'a, T: LogServerMessage + RpcRequest> RunOnSingleNode<'a, T>
where
    T: LogServerMessage + Clone,
    T::Response: LogServerResponse,
{
    pub fn new(
        node_id: PlainNodeId,
        request: T,
        known_global_tail: &'a TailOffsetWatch,
        attempts: Attempts,
    ) -> Self {
        Self {
            node_id,
            request,
            known_global_tail,
            attempts,
        }
    }

    /// Send a message to a single node and handles network-related failures with retries.
    #[instrument(skip_all, fields(node_id = %self.node_id, loglet_id = %self.request.header().loglet_id, message = T::TYPE))]
    pub async fn run<O, N: TransportConnect>(
        mut self,
        on_response: impl Fn(PlainNodeId, T::Response) -> Disposition<O>,
        networking: &'a Networking<N>,
    ) -> Result<O, TaskError> {
        let start = Instant::now();
        let loglet_id = self.request.header().loglet_id;

        let rpc_backoff = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .rpc_timeout;
        let adaptive_timeout = AdaptiveTimeout::new(TimeoutConfig {
            backoff: rpc_backoff,
            quantile: 0.9999, // base timeout on P99.99
            safety_factor: 2.0,
        });

        let retry_config = AdaptiveTimeout::new(TimeoutConfig {
            backoff: rpc_backoff,
            quantile: 0.90,
            safety_factor: 1.0,
        });

        let mut retries = Retries::new(self.attempts);

        loop {
            retries.advance();
            self.request
                .refresh_header(self.known_global_tail.latest_offset());
            let timeout = adaptive_timeout.select_timeout_sync(
                &LATENCY_TRACKER,
                &[self.node_id],
                retries.current(),
                Instant::now(),
            );

            trace!(%loglet_id, "Sending {} message to node {}", T::TYPE, self.node_id);
            // loop and retry until this task is aborted.
            let attempt_start = Instant::now();
            let maybe_response = networking
                .call_rpc(
                    self.node_id,
                    Swimlane::default(),
                    self.request.clone(),
                    Some(loglet_id.into()),
                    Some(timeout),
                )
                .await;

            match maybe_response {
                Ok(msg) => {
                    // update our view of global tail if we observed higher tail in response.
                    self.known_global_tail
                        .notify_offset_update(msg.header().known_global_tail);
                    match on_response(self.node_id, msg) {
                        Disposition::Return(v) => {
                            LATENCY_TRACKER.record_latency_from(
                                &self.node_id,
                                attempt_start,
                                Instant::now(),
                            );
                            return Ok(v);
                        }
                        Disposition::Abort => return Err(TaskError::Aborted),
                        Disposition::Retry if retries.has_next() => {
                            trace!(
                                attempt = %retries.current(),
                                "Response received but a retry was requested. Will retry after {}",
                                timeout.friendly()
                            );
                        } // fall-through
                        Disposition::Retry => {
                            // exhausted retries budget. We represent this by timeout.
                            // give up.
                            trace!(attempt = %retries.current(), "Exhausted retries");
                            return Err(TaskError::ExhaustedRetries(start.elapsed()));
                        }
                    }
                }
                Err(err) => {
                    if let RpcError::Timeout(spent) = err {
                        // Record the time spent if we time out.
                        LATENCY_TRACKER.record_latency(&self.node_id, spent, Instant::now());
                    }
                    if retries.has_next() {
                        // We'll retry in every other case of networking error
                        trace!(
                            %err,
                            attempt = %retries.current(),
                            "Request failed. Will retry after {}",
                            timeout.friendly()
                        );
                    } else {
                        debug!(
                            %err,
                            attempt = %retries.current(),
                            "Request failed. Exhausted attempts after spending {}",
                            start.elapsed().friendly(),
                        );
                        return Err(TaskError::ExhaustedRetries(start.elapsed()));
                    }
                }
            }

            // Should we retry?
            if retries.has_next() {
                let delay = retry_config.select_timeout_sync(
                    &LATENCY_TRACKER,
                    &[self.node_id],
                    retries.current(),
                    Instant::now(),
                );
                tokio::time::sleep(delay).await;
            } else {
                // give up.
                return Err(TaskError::ExhaustedRetries(start.elapsed()));
            }
        }
    }
}
