// Copyright (c) 2024 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::error::Error;

use bytes::Bytes;
use http_body_util::{Either, Full};
use hyper::Response;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio_util::net::Listener;
use tracing::error;

use crate::handler::serve;

pub async fn run_listener<L>(
    mut listener: L,
    on_bind: impl FnOnce(),
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    L: Listener,
    L::Io: Unpin + Send + 'static,
{
    on_bind();

    loop {
        tokio::select! {
            incoming = listener.accept() => {
                let (tcp, _) = incoming?;
                let io = TokioIo::new(tcp);
                tokio::task::spawn(async move {
                    if let Err(err) = http2::Builder::new(TokioExecutor::new())
                        .timer(TokioTimer::new())
                        .serve_connection(io, service_fn(|req| async {
                            if req.uri().path() == "/discover" {
                                return Ok(Response::builder()
                                    .header("content-type", "application/vnd.restate.endpointmanifest.v1+json")
                                    .body(Either::Left(Full::new(Bytes::from(
                                        r#"{"protocolMode":"BIDI_STREAM","minProtocolVersion":5,"maxProtocolVersion":5,"services":[{"name":"Counter","ty":"VIRTUAL_OBJECT","handlers":[{"name":"add","input":{"required":false,"contentType":"application/json"},"output":{"setContentTypeIfEmpty":false,"contentType":"application/json"},"ty":"EXCLUSIVE"},{"name":"get","input":{"required":false,"contentType":"application/json"},"output":{"setContentTypeIfEmpty":false,"contentType":"application/json"},"ty":"EXCLUSIVE"}]}]}"#
                                    )))).unwrap());
                            }

                            let (head, body) = serve(req).await?.into_parts();
                            Result::<_, Infallible>::Ok(Response::from_parts(head, Either::Right(body)))
                        }))
                        .await
                    {
                        error!("Error serving connection: {:?}", err);
                    }
                });
            }
        }
    }
}
