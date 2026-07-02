// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tower middleware that strips `x-restate-audit-*` headers at ingress when
//! agent audit is disabled, preventing untrusted clients from injecting fake
//! audit context.
//!
//! When `agent_audit` is `true` the layer is a transparent pass-through.

use http::Request;
use std::task::{Context, Poll};
use tower::{Layer, Service};

use restate_types::invocation::audit::HEADER_PREFIX;

/// Layer that strips `x-restate-audit-*` request headers unless agent audit is enabled.
#[derive(Debug, Clone)]
pub struct AuditHeaderStripperLayer {
    agent_audit: bool,
}

impl AuditHeaderStripperLayer {
    pub fn new(agent_audit: bool) -> Self {
        Self { agent_audit }
    }
}

impl<S> Layer<S> for AuditHeaderStripperLayer {
    type Service = AuditHeaderStripper<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuditHeaderStripper {
            inner,
            agent_audit: self.agent_audit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuditHeaderStripper<S> {
    inner: S,
    agent_audit: bool,
}

impl<B, S> Service<Request<B>> for AuditHeaderStripper<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        if !self.agent_audit {
            let audit_keys: Vec<_> = req
                .headers()
                .keys()
                .filter(|name| name.as_str().starts_with(HEADER_PREFIX))
                .cloned()
                .collect();
            for key in audit_keys {
                req.headers_mut().remove(&key);
            }
        }
        self.inner.call(req)
    }
}
