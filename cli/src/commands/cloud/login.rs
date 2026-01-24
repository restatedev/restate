// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use anyhow::{Context, Result, anyhow};
use axum::{extract, response::Html};
use cling::prelude::*;
use indicatif::ProgressBar;
use restate_cli_util::{CliContext, c_println, c_success, c_tip};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use toml_edit::{DocumentMut, table, value};
use url::Url;

use crate::{
    build_info,
    cli_env::CliEnv,
    clients::cloud::{CloudClient, CloudClientInterface},
};

use super::IdentityProvider;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_login")]
pub struct Login {}

pub async fn run_login(State(env): State<CliEnv>, _opts: &Login) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    let mut doc = config_data
        .parse::<DocumentMut>()
        .context("Failed to parse config file as TOML")?;

    let identity_provider = env.config.cloud.resolve_identity_provider().await?;
    let access_token = auth_flow(&env, &identity_provider).await?;

    write_access_token(&mut doc, &access_token)?;

    let mut env = env;
    env.config.cloud.credentials = Some(super::Credentials { access_token });

    let client = CloudClient::new(&env)?;
    let accounts = client.list_accounts().await?.into_body().await;

    // write out config
    env.write_config(&doc.to_string())
        .context("Failed to write to config file")?;
    c_success!(
        "Updated {} with Restate Cloud credentials",
        env.config_file.display()
    );

    match accounts {
        Ok(accounts) if accounts.accounts.is_empty() => {
            c_tip!(
                "It looks like you don't have an account yet.\nLog in to the UI at https://cloud.restate.dev to create one."
            )
        }
        // ignore error at this point; login was still a success
        _ => {}
    }

    Ok(())
}

async fn auth_flow(env: &CliEnv, identity_provider: &IdentityProvider) -> Result<String> {
    match identity_provider {
        IdentityProvider::Cognito {
            client_id,
            login_base_url,
        } => cognito_auth_flow(env, client_id, login_base_url).await,
        IdentityProvider::WorkOS { client_id } => workos_auth_flow(client_id).await,
    }
}

async fn cognito_auth_flow(env: &CliEnv, client_id: &str, login_base_url: &Url) -> Result<String> {
    let client = reqwest::Client::builder()
        .user_agent(format!(
            "{}/{} {}-{}",
            env!("CARGO_PKG_NAME"),
            build_info::RESTATE_CLI_VERSION,
            std::env::consts::OS,
            std::env::consts::ARCH,
        ))
        .https_only(true)
        .connect_timeout(CliContext::get().connect_timeout())
        .build()
        .context("Failed to build oauth token client")?;

    let redirect_ports = env.config.cloud.redirect_ports();
    let mut i = 0;
    let listener = loop {
        if i >= redirect_ports.len() {
            return Err(anyhow!(
                "Failed to bind oauth callback server to localhost. Tried ports: [{:?}]",
                redirect_ports
            ));
        }
        if let Ok(listener) =
            tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], redirect_ports[i])))
                .await
        {
            break listener;
        }
        i += 1
    };

    let port = listener
        .local_addr()
        .expect("failed to get local address of login http listener")
        .port();
    let redirect_uri = format!("http://localhost:{port}/callback");

    let (result_send, mut result_recv) = mpsc::channel(1);

    let state = uuid::Uuid::now_v7().simple().to_string();

    let mut login_uri = login_base_url.join("/login")?;
    login_uri
        .query_pairs_mut()
        .clear()
        .append_pair("response_type", "code")
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("state", &state)
        .append_pair("scope", "openid");

    let router = axum::Router::new()
        .route(
            "/callback",
            axum::routing::get(
                |extract::State(state): extract::State<RedirectState>,
                 extract::Query(params): extract::Query<RedirectParams>| async move {
                    let post_login = include_str!("./postlogin.html");
                    match handle_redirect(&state, params).await {
                        Ok(access_token) => {
                            state.result_send.send(Ok(access_token)).await.expect(
                                "Expected access_token to be sent successfully over channel",
                            );
                            Html::from(
                                post_login.replace(
                                    "MESSAGE",
                                    "Login Successful! You can close this window.",
                                ),
                            )
                        }
                        Err(err) => {
                            state
                                .result_send
                                .send(Err(err))
                                .await
                                .expect("Expected error to be sent successfully over channel");
                            Html::from(
                                post_login.replace("MESSAGE", "Login failed – please try again."),
                            )
                        }
                    }
                },
            ),
        )
        .with_state(RedirectState {
            client,
            login_base_url: login_base_url.clone(),
            client_id: client_id.to_string(),
            redirect_uri,
            result_send,
            state,
        });

    let server = axum::serve(listener, router.into_make_service());
    let server_fut = std::future::IntoFuture::into_future(server);
    tokio::pin!(server_fut);
    let result_fut = result_recv.recv();
    tokio::pin!(result_fut);

    c_println!("Opening browser to {login_uri}");

    if let Err(_err) = open::that(login_uri.to_string()) {
        c_println!("Failed to open browser automatically. Please open the above URL manually.")
    }

    let progress = ProgressBar::new_spinner();
    progress.set_style(indicatif::ProgressStyle::with_template("{spinner} {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));
    progress.set_message("Waiting for login redirect...");

    let result = tokio::select! {
        server_result = server_fut => {
            match server_result {
                Ok(()) => {
                    Err(anyhow!("Authentication callback server exited before we received an access token"))
                }
                Err(err) => {
                    Err(anyhow!("Authentication callback server exited unexpectedly: {err}"))
                }
            }
        }
        result = result_fut => {
            match result {
                Some(Ok(access_token)) => Ok(access_token),
                Some(Err(err)) => {
                    Err(anyhow!("Unable to obtain a valid access token: {err}"))
                }
                None => {
                    Err(anyhow!("Unable to obtain a valid access token (channel closed)"))
                }
            }
        }
    };
    progress.finish_and_clear();
    result
}

#[derive(Clone)]
struct RedirectState {
    client: reqwest::Client,
    login_base_url: Url,
    client_id: String,
    redirect_uri: String,
    state: String,
    result_send: mpsc::Sender<Result<String>>,
}

#[derive(Deserialize)]
struct RedirectParams {
    code: String,
    state: String,
}

#[derive(Serialize)]
struct TokenParams<'a> {
    grant_type: &'static str,
    client_id: &'a str,
    code: String,
    redirect_uri: &'a str,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

async fn handle_redirect(state: &RedirectState, params: RedirectParams) -> Result<String> {
    if state.state != params.state {
        return Err(anyhow!(
            "State mismatch; expected redirect with {} but instead received {}",
            state.state,
            params.state
        ));
    }

    let response: TokenResponse = state
        .client
        .post(state.login_base_url.join("/oauth2/token")?)
        .form(&TokenParams {
            grant_type: "authorization_code",
            client_id: &state.client_id,
            code: params.code,
            redirect_uri: &state.redirect_uri,
        })
        .send()
        .await
        .context("Failed to reach /oauth2/token endpoint")?
        .error_for_status()
        .context("Bad status code from /oauth2/token endpoint")?
        .json()
        .await
        .context("Failed to decode JSON response from /oauth2/token endpoint")?;

    Ok(response.access_token)
}

fn write_access_token(doc: &mut DocumentMut, access_token: &str) -> Result<()> {
    let cloud = doc["global"].or_insert(table())["cloud"].or_insert(table());
    cloud["access_token"] = value(access_token);

    Ok(())
}

async fn workos_auth_flow(client_id: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .user_agent(format!(
            "{}/{} {}-{}",
            env!("CARGO_PKG_NAME"),
            build_info::RESTATE_CLI_VERSION,
            std::env::consts::OS,
            std::env::consts::ARCH,
        ))
        .https_only(true)
        .connect_timeout(CliContext::get().connect_timeout())
        .build()
        .context("Failed to build HTTP client")?;

    let device_auth_response: DeviceAuthorizationResponse = client
        .post("https://api.workos.com/user_management/authorize/device")
        .form(&[("client_id", client_id)])
        .send()
        .await
        .context("Failed to request device authorization")?
        .error_for_status()
        .context("Bad status code from device authorization endpoint")?
        .json()
        .await
        .context("Failed to decode device authorization response")?;

    c_println!(
        "Please visit {} and enter code: {}",
        device_auth_response.verification_uri,
        device_auth_response.user_code
    );

    if let Err(_err) = open::that(device_auth_response.verification_uri_complete.clone()) {
        c_println!("Failed to open browser automatically. Please open the above URL manually.")
    }

    let progress = ProgressBar::new_spinner();
    progress.set_style(indicatif::ProgressStyle::with_template("{spinner} {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));
    progress.set_message("Waiting for authentication...");

    let mut interval = std::time::Duration::from_secs(device_auth_response.interval);
    let expires_at =
        std::time::Instant::now() + std::time::Duration::from_secs(device_auth_response.expires_in);

    loop {
        if std::time::Instant::now() > expires_at {
            progress.finish_and_clear();
            return Err(anyhow!("Device authorization expired. Please try again."));
        }

        tokio::time::sleep(interval).await;

        let token_result: Result<WorkOSAuthenticateResponse, _> = client
            .post("https://api.workos.com/user_management/authenticate")
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", &device_auth_response.device_code),
                ("client_id", client_id),
            ])
            .send()
            .await
            .context("Failed to poll for authentication")?
            .json()
            .await;

        match token_result {
            Ok(response) if response.access_token.is_some() => {
                progress.finish_and_clear();
                return Ok(response.access_token.unwrap());
            }
            Ok(response) if response.error.as_deref() == Some("authorization_pending") => {
                continue;
            }
            Ok(response) if response.error.as_deref() == Some("slow_down") => {
                interval += std::time::Duration::from_secs(1);
                continue;
            }
            Ok(response) if response.error.is_some() => {
                progress.finish_and_clear();
                return Err(anyhow!(
                    "Authentication failed: {}",
                    response.error.unwrap_or_else(|| "unknown error".into())
                ));
            }
            Ok(_) => {
                progress.finish_and_clear();
                return Err(anyhow!("Unexpected response from authentication endpoint"));
            }
            Err(err) => {
                progress.finish_and_clear();
                return Err(err.into());
            }
        }
    }
}

#[derive(Deserialize)]
struct DeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Deserialize)]
struct WorkOSAuthenticateResponse {
    access_token: Option<String>,
    error: Option<String>,
}
