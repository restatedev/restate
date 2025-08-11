// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use cling::prelude::*;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use remote::RemotePort;
use tokio_util::sync::CancellationToken;

use restate_cli_util::CliContext;

use crate::cli_env::EnvironmentType;
use crate::clients::cloud::{CloudClient, CloudClientInterface};
use crate::{build_info, cli_env::CliEnv};

mod local;
mod remote;
mod renderer;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_tunnel")]
#[clap(visible_alias = "expose", visible_alias = "tun")]
pub struct Tunnel {
    /// The port of a local service to expose to the Environment
    #[clap(short = 'l', long)]
    local_port: Option<u16>,

    /// Remote port on the Environment to expose on localhost
    /// This argument can be repeated to specify multiple remote ports
    #[clap(short = 'r', long, action = clap::ArgAction::Append)]
    remote_port: Vec<remote::RemotePort>,

    /// If reattaching to a previous tunnel session, the tunnel server to connect to
    #[clap(long = "tunnel-url")]
    tunnel_url: Option<String>,

    /// A name for the tunnel; a random name will be generated if not provided
    #[clap(long = "tunnel-name")]
    tunnel_name: Option<String>,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Local(#[from] restate_cloud_tunnel_client::client::ServeError),
    #[error(transparent)]
    Remote(#[from] remote::ServeError),
    #[error("Ctrl-C pressed")]
    ControlC,
}

pub async fn run_tunnel(State(env): State<CliEnv>, opts: &Tunnel) -> Result<()> {
    let environment_info = match (
        &env.config.environment_type,
        &env.config.cloud.environment_info,
    ) {
        (EnvironmentType::Cloud, Some(environment_info)) => environment_info,
        _ => {
            return Err(anyhow::anyhow!(
                "First switch to a Cloud environment using `restate config use-environment` or configure one with `restate cloud environment configure`"
            ));
        }
    };

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

    let mut opts = opts.clone();
    if opts.local_port.is_none() && opts.remote_port.is_empty() {
        opts.local_port = Some(9080);
        opts.remote_port = vec![RemotePort::Ingress, RemotePort::Admin];
    }

    let client = CloudClient::new(&env)?;

    let environment_info = client
        .describe_environment(
            &environment_info.account_id,
            &environment_info.environment_id,
        )
        .await?
        .into_body()
        .await?;

    let user_agent = format!(
        "{}/{} {}-{}",
        env!("CARGO_PKG_NAME"),
        build_info::RESTATE_CLI_VERSION,
        std::env::consts::OS,
        std::env::consts::ARCH,
    );

    let connect_timeout = CliContext::get().connect_timeout();

    let h2_client = reqwest::Client::builder()
        .user_agent(user_agent.clone())
        .connect_timeout(connect_timeout)
        .http2_prior_knowledge()
        .build()?;

    let h1_client = reqwest::Client::builder()
        .user_agent(user_agent)
        .connect_timeout(connect_timeout)
        .http1_only()
        .build()?;

    let cancellation = CancellationToken::new();

    // prevent ctrl-c from clearing the screen, instead just cancel
    {
        let cancellation = cancellation.clone();
        let boxed: Box<dyn Fn() + Send> = Box::new(move || cancellation.cancel());
        *crate::EXIT_HANDLER.lock().unwrap() = Some(boxed);
    }

    let tunnel_renderer = Arc::new(renderer::TunnelRenderer::new(&opts.remote_port).unwrap());

    let local_fut = local::run_local(
        &env,
        h1_client.clone(),
        h2_client.clone(),
        &bearer_token,
        environment_info,
        &opts,
        tunnel_renderer.clone(),
    );

    let remote_futs = futures::stream::FuturesUnordered::new();
    for remote_port in &opts.remote_port {
        let base_url = match remote_port {
            remote::RemotePort::Ingress => env.ingress_base_url()?,
            remote::RemotePort::Admin => env.admin_base_url()?,
        };

        remote_futs.push(remote::run_remote(
            *remote_port,
            h2_client.clone(),
            base_url,
            &bearer_token,
            tunnel_renderer.clone(),
        ));
    }

    let mut rerender = tokio::time::interval(Duration::from_millis(100));

    let res = {
        let local_fut = local_fut.fuse();
        tokio::pin!(local_fut);
        let mut remote_futs = remote_futs;

        loop {
            tokio::select! {
                Err(local_err) = &mut local_fut => break Err(Error::Local(local_err)),
                Some(Err(remote_err)) = remote_futs.next() => break Err(Error::Remote(remote_err)),
                _ = cancellation.cancelled() => break Err(Error::ControlC),
                _ = rerender.tick() => {
                    tunnel_renderer.render();
                },
            }
        }
    };

    if let Some(mut renderer) = Arc::into_inner(tunnel_renderer) {
        if let Some(local) = renderer.local.take() {
            // dropping the renderer will exit the alt screen
            let (tunnel_url, tunnel_name, port) = local.into_tunnel_details();
            drop(renderer);
            let remote_ports = if opts.remote_port.is_empty() {
                String::new()
            } else {
                format!(
                    " --remote-port {}",
                    opts.remote_port
                        .iter()
                        .map(|port| u16::from(*port))
                        .join(" --remote-port ")
                )
            };
            eprintln!(
                "To retry with the same endpoint:\nrestate cloud env tunnel --local-port {port} --tunnel-url {tunnel_url} --tunnel-name {tunnel_name}{remote_ports}"
            );
        }
    };

    match res {
        Err(Error::ControlC) => {
            eprintln!("Exiting as Ctrl-C pressed");
            Ok(())
        }
        Ok(()) => Ok(()),
        Err(err) => Err(err.into()),
    }
}
