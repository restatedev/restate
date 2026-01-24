// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{HeaderMap, Request};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::task::{Context, Poll};
use tower::{Layer, Service};

pub struct HttpTraceContextExtractorLayer;

impl<S> Layer<S> for HttpTraceContextExtractorLayer {
    type Service = HttpTraceContextExtractor<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HttpTraceContextExtractor { inner }
    }
}

#[derive(Debug, Clone)]
pub struct HttpTraceContextExtractor<S> {
    inner: S,
}

impl<B, S> Service<Request<B>> for HttpTraceContextExtractor<S>
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
        let tracing_context =
            TraceContextPropagator::new().extract(&HeaderExtractor(req.headers()));
        req.extensions_mut().insert(tracing_context);
        self.inner.call(req)
    }
}

// This is taken from opentelemetry-http,
//  we need this because that module is still on the old http crate version
// https://github.com/open-telemetry/opentelemetry-rust/blob/ef4701055cc39d3448d5e5392812ded00cdd4476/opentelemetry-http/src/lib.rs#L14 License APL 2.0
pub struct HeaderExtractor<'a>(pub &'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    /// Get a value for a key from the HeaderMap.  If the value is not valid ASCII, returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    /// Collect all the keys from the HeaderMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
    }
}
