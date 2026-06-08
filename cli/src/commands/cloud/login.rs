// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, Result, anyhow};
use cling::prelude::*;
use indicatif::ProgressBar;
use restate_cli_util::{CliContext, c_println, c_success, c_tip};
use serde::Deserialize;
use toml_edit::{DocumentMut, table, value};

use crate::{
    build_info,
    cli_env::CliEnv,
    clients::cloud::{CloudClient, CloudClientInterface},
};

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

    let client_id = env.config.cloud.resolve_workos_client_id().await?;
    let access_token = workos_auth_flow(&client_id).await?;

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
