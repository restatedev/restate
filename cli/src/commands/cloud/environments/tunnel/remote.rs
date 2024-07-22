// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use cling::prelude::*;
use tracing::info;
use url::Url;

use super::renderer::TunnelRenderer;

#[derive(Copy, Clone)]
pub(crate) enum RemotePort {
    Ingress,
    Admin,
}

impl ValueEnum for RemotePort {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Ingress, Self::Admin]
    }

    fn to_possible_value(&self) -> Option<builder::PossibleValue> {
        Some(builder::PossibleValue::new(match self {
            Self::Ingress => "8080",
            Self::Admin => "9070",
        }))
    }
}

impl From<RemotePort> for u16 {
    fn from(value: RemotePort) -> Self {
        match value {
            RemotePort::Ingress => 8080,
            RemotePort::Admin => 9070,
        }
    }
}

#[derive(Clone)]
struct HandlerState {
    client: reqwest_0_11::Client,
    base_url: Url,
    bearer_token: String,
    tunnel_renderer: Arc<TunnelRenderer>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("Failed to create local server")]
    Hyper(#[from] hyper_0_14::Error),
    #[error("Local server closed unexpectedly")]
    ServerClosed,
}

pub async fn run_remote(
    remote_port: RemotePort,
    client: reqwest_0_11::Client,
    base_url: &Url,
    bearer_token: &str,
    tunnel_renderer: Arc<TunnelRenderer>,
) -> Result<(), ServeError> {
    let router = axum_0_6::Router::new()
        .fallback(axum_0_6::routing::any(handler))
        .with_state(HandlerState {
            client: client.clone(),
            base_url: base_url.clone(),
            bearer_token: bearer_token.into(),
            tunnel_renderer: tunnel_renderer.clone(),
        });

    let server =
        axum_0_6::Server::try_bind(&SocketAddr::from(([127, 0, 0, 1], u16::from(remote_port))))?;

    server.serve(router.into_make_service()).await?;

    Err(ServeError::ServerClosed)
}

async fn handler(
    axum_0_6::extract::State(state): axum_0_6::extract::State<HandlerState>,
    req: axum_0_6::http::Request<axum_0_6::body::Body>,
) -> Result<axum_0_6::response::Response, Infallible> {
    let res: Result<_, anyhow::Error> = async {
        let (mut head, body) = req.into_parts();
        head.headers.insert(
            http_0_2::header::HOST,
            http_0_2::HeaderValue::from_str(state.base_url.authority())?,
        );
        let url = if let Some(path) = head.uri.path_and_query() {
            state.base_url.join(path.as_str())?
        } else {
            state.base_url
        };
        info!("Proxying request to {}", url);
        let request = state
            .client
            .request(head.method, url)
            .body(body)
            .headers(head.headers)
            .bearer_auth(&state.bearer_token)
            .build()?;
        let mut result = state.client.execute(request).await?;

        let mut response = axum_0_6::http::Response::builder().status(result.status());
        if let Some(headers) = response.headers_mut() {
            std::mem::swap(headers, result.headers_mut())
        };

        let body = axum_0_6::body::Body::wrap_stream(result.bytes_stream());
        Ok(response.body(axum_0_6::body::boxed(body))?)
    }
    .await;

    match res {
        Ok(resp) => {
            state.tunnel_renderer.clear_error();
            Ok(resp)
        }
        Err(err) => {
            state.tunnel_renderer.store_error(err);
            Ok(axum_0_6::response::Response::builder()
                .status(http_0_2::status::StatusCode::BAD_GATEWAY)
                .body(axum_0_6::body::boxed(axum_0_6::body::Body::empty()))
                .expect("failed to create http error response"))
        }
    }
}
