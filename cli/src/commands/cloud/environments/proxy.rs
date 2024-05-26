// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    build_info, c_error, c_success,
    cli_env::{CliEnv, EnvironmentType},
};
use anyhow::Result;
use clap::builder::PossibleValue;
use cling::prelude::*;
use itertools::Itertools;
use std::net::SocketAddr;
use tracing::info;
use url::Url;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_proxy")]
pub struct Proxy {
    /// The port to forward to the Cloud environment. Defaults to all ports
    port: Option<Port>,
}

#[derive(Copy, Clone)]
enum Port {
    Ingress,
    Admin,
}

impl ValueEnum for Port {
    fn value_variants<'a>() -> &'a [Self] {
        &[Port::Ingress, Port::Admin]
    }

    fn to_possible_value(&self) -> Option<builder::PossibleValue> {
        Some(PossibleValue::new(match self {
            Port::Ingress => "8080",
            Port::Admin => "9070",
        }))
    }
}

impl From<Port> for u16 {
    fn from(value: Port) -> Self {
        match value {
            Port::Ingress => 8080,
            Port::Admin => 9070,
        }
    }
}

#[derive(Clone)]
struct HandlerState {
    client: reqwest::Client,
    base_url: Url,
    bearer_token: String,
}

pub async fn run_proxy(State(env): State<CliEnv>, opts: &Proxy) -> Result<()> {
    match env.config.environment_type {
        EnvironmentType::Cloud => {},
        _ => return Err(anyhow::anyhow!("First switch to a Cloud environment using `restate config use-environment` or configure one with `restate cloud environment configure`"))
    }

    let bearer_token = if let Some(bearer_token) = &env.config.bearer_token {
        // the user may have specifically set an api token
        bearer_token.clone()
    } else if let Some(cloud_credentials) = &env.config.cloud.credentials {
        cloud_credentials.access_token()?.to_string()
    } else {
        return Err(anyhow::anyhow!(
            "Restate Cloud credentials have not been provided; first run `restate cloud login`"
        ));
    };

    let ports = match opts.port {
        None => vec![Port::Ingress, Port::Admin],
        Some(choice) => vec![choice],
    };

    let client = reqwest::Client::builder()
        .user_agent(format!(
            "{}/{} {}-{}",
            env!("CARGO_PKG_NAME"),
            build_info::RESTATE_CLI_VERSION,
            std::env::consts::OS,
            std::env::consts::ARCH,
        ))
        .connect_timeout(env.connect_timeout)
        .http2_prior_knowledge()
        .build()?;

    let servers: Vec<_> = ports
        .into_iter()
        .map(|port| {
            let base_url = match port {
                Port::Ingress => env.ingress_base_url()?,
                Port::Admin => env.admin_base_url()?,
            }
            .clone();

            let router = axum::Router::new()
                .fallback(axum::routing::any(handler))
                .with_state(HandlerState {
                    client: client.clone(),
                    base_url,
                    bearer_token: bearer_token.clone(),
                });

            let server =
                axum::Server::try_bind(&SocketAddr::from(([127, 0, 0, 1], u16::from(port))))?;

            c_success!("Serving on {}", server.local_addr());
            Result::<_, anyhow::Error>::Ok(server.serve(router.into_make_service()))
        })
        .try_collect()?;

    futures::future::try_join_all(servers).await?;
    Ok(())
}

struct HandlerError(anyhow::Error);

impl<T: Into<anyhow::Error>> From<T> for HandlerError {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl axum::response::IntoResponse for HandlerError {
    fn into_response(self) -> axum::response::Response {
        c_error!("Failed to handle request: {}", self.0);
        axum::response::Response::builder()
            .status(http::status::StatusCode::BAD_GATEWAY)
            .body(axum::body::boxed(axum::body::Body::empty()))
            .expect("failed to create http error response")
    }
}

async fn handler(
    axum::extract::State(state): axum::extract::State<HandlerState>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<axum::response::Response, HandlerError> {
    let (mut head, body) = req.into_parts();
    head.headers.insert(
        http::header::HOST,
        http::HeaderValue::from_str(state.base_url.authority())?,
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

    let mut response = axum::http::Response::builder().status(result.status());
    if let Some(headers) = response.headers_mut() {
        std::mem::swap(headers, result.headers_mut())
    };

    let body = axum::body::Body::wrap_stream(result.bytes_stream());
    Ok(response.body(axum::body::boxed(body))?)
}
