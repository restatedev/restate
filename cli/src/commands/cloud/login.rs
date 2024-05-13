use std::net::SocketAddr;

use crate::{build_info, c_success, c_tip, cli_env::CliEnv};
use anyhow::{anyhow, Context, Result};
use axum::extract::{self};
use base64::Engine;
use cling::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use toml_edit::{table, value, DocumentMut};
use url::Url;

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

    // write out config
    std::fs::write(env.config_file.as_path(), doc.to_string())
        .context("Failed to write to config file")?;
    c_success!(
        "Updated {} with Restate Cloud credentials",
        env.config_file.display()
    );

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct TokenClaims {
    exp: i64,
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
        .connect_timeout(env.connect_timeout)
        .build()
        .context("Failed to build oauth token client")?;

    let addr = SocketAddr::from(([127, 0, 0, 1], 27375));
    let server = axum::Server::try_bind(&addr)
        .context("Failed to bind oauth callback server to localhost")?;
    let port = server.local_addr().port();
    let redirect_uri = format!("http://localhost:{}", port);

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
            "/",
            axum::routing::get(
                |extract::State(state): extract::State<RedirectState>,
                 extract::Query(params): extract::Query<RedirectParams>| async move {
                    match handle_redirect(&state, params).await {
                        Ok(access_token) => {
                            state.result_send.send(Ok(access_token)).await.expect(
                                "Expected access_token to be sent successfully over channel",
                            );
                            "Login Successful! You can close this window."
                        }
                        Err(err) => {
                            state
                                .result_send
                                .send(Err(err))
                                .await
                                .expect("Expected error to be sent successfully over channel");
                            "Login failed – please try again."
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

    let server = server.serve(router.into_make_service());
    tokio::pin!(server);
    let result_fut = result_recv.recv();
    tokio::pin!(result_fut);

    c_tip!("If a browser does not open automatically, please continue by visiting: {login_uri}");
    open::that(login_uri.to_string())?;

    tokio::select! {
        server_result = server => {
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
    }
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
    let claims = match access_token
        .split('.')
        .nth(1)
        .map(|claims: &str| -> Result<TokenClaims> {
            Ok(serde_json::from_slice(
                &base64::prelude::BASE64_URL_SAFE_NO_PAD.decode(claims)?,
            )?)
        }) {
        Some(Ok(claims)) => claims,
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid access token; could not parse expiry"
            ))
        }
    };

    let cloud = doc["global"].or_insert(table())["cloud"].or_insert(table());
    cloud["access_token"] = value(access_token);
    cloud["access_token_expiry"] = value(claims.exp);

    Ok(())
}
