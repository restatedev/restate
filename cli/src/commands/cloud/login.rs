// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use anyhow::{anyhow, Context, Result};
use axum::{extract, response::Html};
use cling::prelude::*;
use indicatif::ProgressBar;
use restate_cli_util::{c_println, c_success, c_tip, CliContext};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use toml_edit::{table, value, DocumentMut};
use url::Url;

use crate::{
    build_info,
    cli_env::CliEnv,
    clients::cloud::{CloudClient, CloudClientInterface},
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_login")]
pub struct Login {}

pub async fn run_login(State(env): State<CliEnv>, opts: &Login) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    let mut doc = config_data
        .parse::<DocumentMut>()
        .context("Failed to parse config file as TOML")?;

    let access_token = auth_flow(&env, opts).await?;

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
            c_tip!("It looks like you don't have an account yet.\nLog in to the UI at https://cloud.restate.dev to create one.")
        }
        // ignore error at this point; login was still a success
        _ => {}
    }

    Ok(())
}

async fn auth_flow(env: &CliEnv, _opts: &Login) -> Result<String> {
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

    let mut i = 0;
    let listener = loop {
        if i >= env.config.cloud.redirect_ports.len() {
            return Err(anyhow!(
                "Failed to bind oauth callback server to localhost. Tried ports: [{:?}]",
                env.config.cloud.redirect_ports
            ));
        }
        if let Ok(listener) = tokio::net::TcpListener::bind(SocketAddr::from((
            [127, 0, 0, 1],
            env.config.cloud.redirect_ports[i],
        )))
        .await
        {
            break listener;
        }
        i += 1
    };

    let port = listener.local_addr().unwrap().port();
    let redirect_uri = format!("http://localhost:{port}/callback");

    let (result_send, mut result_recv) = mpsc::channel(1);

    let state = uuid::Uuid::now_v7().simple().to_string();

    let mut login_uri = env.config.cloud.login_base_url.join("/login")?;
    login_uri
        .query_pairs_mut()
        .clear()
        .append_pair("response_type", "code")
        .append_pair("client_id", &env.config.cloud.client_id)
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
            login_base_url: env.config.cloud.login_base_url.clone(),
            client_id: env.config.cloud.client_id.clone(),
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
    open::that(login_uri.to_string())?;

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
