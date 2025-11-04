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
use rand::seq::IndexedRandom;
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
    /// Disable inbound requests from the Cloud Environment through the tunnel
    #[clap(long = "no-local")]
    no_inbound: bool,

    /// Remote port on the Environment to expose on localhost
    /// This argument can be repeated to specify multiple remote ports
    #[clap(short = 'r', long, action = clap::ArgAction::Append)]
    remote_port: Vec<remote::RemotePort>,

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
    if opts.remote_port.is_empty() {
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

    let alpn_client = reqwest::Client::builder()
        .user_agent(user_agent.clone())
        .connect_timeout(connect_timeout)
        .build()?;

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

    let tunnel_name = opts.tunnel_name.clone().unwrap_or(
        String::from_utf8_lossy(mnemonic::MN_WORDS.choose(&mut rand::rng()).unwrap()).to_string(),
    );

    let tunnel_renderer = Arc::new(
        renderer::TunnelRenderer::new(
            tunnel_name.clone(),
            environment_info.name.clone(),
            &opts.remote_port,
        )
        .unwrap(),
    );

    let local_fut = local::run_local(
        opts.no_inbound,
        alpn_client,
        h1_client.clone(),
        h2_client.clone(),
        &bearer_token,
        environment_info,
        tunnel_name.clone(),
        tunnel_renderer.clone(),
    );

    let remote_futs = futures::stream::FuturesUnordered::new();
    for remote_port in &opts.remote_port {
        let base_url = match remote_port {
            remote::RemotePort::Ingress => {
                let advertised_address = env.ingress_base_url()?.clone().into_address()?;
                if advertised_address.is_unix_domain_socket() {
                    return Err(anyhow::anyhow!(
                        "Cannot tunnel to an ingress server running on a unix domain socket"
                    ));
                }
                advertised_address.to_string()
            }
            remote::RemotePort::Admin => {
                let advertised_address = env.admin_base_url()?.clone().into_address()?;
                if advertised_address.is_unix_domain_socket() {
                    return Err(anyhow::anyhow!(
                        "Cannot tunnel to an admin server running on a unix domain socket"
                    ));
                }
                advertised_address.to_string()
            }
        }
        .parse()?;

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

    if let Some(renderer) = Arc::into_inner(tunnel_renderer)
        && renderer
            .local
            .target_connected
            .load(std::sync::atomic::Ordering::Relaxed)
            != 0
    {
        // dropping the renderer will exit the alt screen
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
            "To retry with the same tunnel name:\nrestate cloud env tunnel --tunnel-name {tunnel_name}{remote_ports}"
        );
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
