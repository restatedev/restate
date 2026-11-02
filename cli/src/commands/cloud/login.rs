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
use serde::Deserialize;
use toml_edit::{DocumentMut, Item, table, value};

use restate_cli_util::{CliContext, c_println};

use crate::{
    build_info,
    cli_env::CliEnv,
    clients::cloud::CloudClient,
    error::RestateCliError,
    ui::fmt::{Field, Formatter, IncludeFormatting, Outcome, OutputFormatter},
};

use super::profile::{Choice, ChosenEnvironment, CurrentEnvironment, choose_environment};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_login")]
#[command(after_help = after_help!(
    examples: [
        "restate cloud login",
        "restate cloud login --no-env    # only log in, pick the environment later with `restate cloud env configure`",
    ],
    learn_more: "https://docs.restate.dev/cloud",
))]
pub struct Login {
    /// Only log in: don't select a Cloud environment afterwards
    #[clap(long)]
    no_env: bool,
}

pub async fn run_login(State(env): State<CliEnv>, opts: &Login) -> Result<()> {
    // The device flow needs someone at a browser, so without a terminal (CI, piped
    // output, `--yes`) fail before touching the config or the network.
    if !CliContext::get().can_prompt() {
        return Err(RestateCliError::bad_input(
            "cannot log in to Restate Cloud in non-interactive mode: logging in needs a browser",
        )
        .with_unformatted_next_step("restate cloud login", "log in from a terminal")
        .into());
    }

    let mut doc = load_config_document(&env)?;

    let client_id = env.config.cloud.resolve_workos_client_id().await?;
    let access_token = workos_auth_flow(&client_id).await?;

    write_access_token(&mut doc, &access_token)?;
    env.write_config(&doc.to_string())
        .context("Failed to write to config file")?;

    let mut f = Formatter::new();
    f.outcome(
        "login",
        Field::with_display(
            "logged_in",
            format!(
                "Updated {} with Restate Cloud credentials",
                env.config_file.display()
            ),
        ),
        Outcome::Success,
    );

    if opts.no_env {
        return f.finish();
    }

    let mut env = env;
    env.config.cloud.credentials = Some(super::Credentials { access_token });
    // The login succeeded whatever happens next: a failure to select an environment is
    // reported, not an error.
    if let Err(err) = select_environment(&env, &mut f).await {
        f.outcome(
            "result",
            Field::with_display(
                "environment_not_selected",
                format!("Logged in, but couldn't select an environment: {err:#}"),
            ),
            Outcome::Failure,
        );
        f.next_step(
            "restate cloud env configure",
            "pick the environment to use",
            IncludeFormatting::No,
        );
    }
    f.finish()
}

/// Select the Cloud environment to use: silently when there is one account with one
/// environment, else by asking. A Cloud environment the CLI already uses is kept when it's
/// the only one, and offered as a choice otherwise.
async fn select_environment(env: &CliEnv, f: &mut Formatter) -> Result<()> {
    let client = CloudClient::new(env)?;
    let current = env
        .config
        .cloud
        .environment_info
        .as_ref()
        .map(|info| CurrentEnvironment {
            profile: env.environment.as_str().as_str(),
            environment_id: &info.environment_id,
        });
    let chosen = match choose_environment(&client, current.as_ref()).await? {
        Choice::Environment(chosen) => chosen,
        Choice::KeepCurrent => {
            f.outcome(
                "result",
                Field::with_display(
                    "environment_kept",
                    format!(
                        "Kept the current environment \"{}\"",
                        env.environment.as_str()
                    ),
                ),
                Outcome::Success,
            );
            f.next_step(
                "restate whoami",
                "see the environment the CLI talks to",
                IncludeFormatting::Yes,
            );
            return Ok(());
        }
        Choice::NoAccounts => {
            f.outcome(
                "result",
                Field::with_display(
                    "no_account",
                    "It looks like you don't have an account yet: log in to the UI at https://cloud.restate.dev to create one",
                ),
                Outcome::NothingToDo,
            );
            return Ok(());
        }
        Choice::NoEnvironments { account_name } => {
            f.outcome(
                "result",
                Field::with_display(
                    "no_environment",
                    format!(
                        "Account {account_name} has no environment yet: create one in the UI at https://cloud.restate.dev"
                    ),
                ),
                Outcome::NothingToDo,
            );
            f.next_step(
                "restate cloud env configure",
                "select the environment once created",
                IncludeFormatting::No,
            );
            return Ok(());
        }
    };

    let profile = chosen.environment.name.clone();
    let mut doc = load_config_document(env)?;
    // `default` and `global` are the config file's shared sections, not environments.
    let reserved = ["default", "global"]
        .iter()
        .any(|name| profile.eq_ignore_ascii_case(name));
    let taken = doc.get(&profile).is_some_and(|existing| {
        !is_cloud_environment(existing, &chosen.environment.environment_id)
    });
    if reserved || taken {
        f.outcome(
            "result",
            Field::with_display(
                "environment_name_taken",
                format!(
                    "The CLI config file can't have an environment named \"{profile}\": the name is reserved or already used by another environment"
                ),
            ),
            Outcome::NothingToDo,
        );
        f.next_step(
            "restate cloud env configure",
            "add it under another name",
            IncludeFormatting::No,
        );
        return Ok(());
    }
    write_cloud_profile(&mut doc, &profile, &chosen);
    env.write_config(&doc.to_string())
        .context("Failed to write to config file")?;
    select_profile(env, &profile)?;
    f.detail(
        "environment",
        &[
            ("name", Field::new(&profile)),
            (
                "environment_id",
                Field::new(&chosen.environment.environment_id),
            ),
            ("region", Field::new(&chosen.environment.region)),
            ("account", Field::new(&chosen.account_name)),
            ("account_id", Field::json_only(&chosen.account_id)),
        ],
    );
    f.outcome(
        "result",
        Field::with_display(
            "environment_selected",
            format!(
                "Using environment \"{profile}\" ({}, {}) of account {}",
                chosen.environment.environment_id, chosen.environment.region, chosen.account_name
            ),
        ),
        Outcome::Success,
    );
    f.next_step(
        "restate whoami",
        "see the environment the CLI now talks to",
        IncludeFormatting::Yes,
    );
    Ok(())
}

/// The config file as a TOML document, empty when there is none yet.
fn load_config_document(env: &CliEnv) -> Result<DocumentMut> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())
            .with_context(|| format!("Failed to read {}", env.config_file.display()))?
    } else {
        String::new()
    };
    config_data
        .parse::<DocumentMut>()
        .with_context(|| format!("Failed to parse {} as TOML", env.config_file.display()))
}

/// Whether a section of the config file is the Cloud environment `environment_id`.
fn is_cloud_environment(section: &Item, environment_id: &str) -> bool {
    section
        .get("cloud")
        .and_then(|cloud| cloud.get("environment_id"))
        .and_then(Item::as_str)
        == Some(environment_id)
}

/// Write the section of environment `name` for `chosen`, replacing an existing one.
fn write_cloud_profile(doc: &mut DocumentMut, name: &str, chosen: &ChosenEnvironment) {
    doc[name] = table();
    let profile = &mut doc[name];
    profile["environment_type"] = value("cloud");
    profile["ingress_base_url"] = value(&chosen.environment.ingress_base_url);
    profile["admin_base_url"] = value(&chosen.environment.admin_base_url);

    let cloud = profile["cloud"].or_insert(table());
    cloud["account_id"] = value(&chosen.account_id);
    cloud["environment_id"] = value(&chosen.environment.environment_id);
}

/// Make `name` the current environment, unless it already is.
fn select_profile(env: &CliEnv, name: &str) -> Result<()> {
    let current = if env.environment_file.is_file() {
        std::fs::read_to_string(&env.environment_file)
            .with_context(|| format!("Failed to read {}", env.environment_file.display()))?
    } else {
        String::new()
    };
    if current.trim() == name {
        return Ok(());
    }
    env.write_environment(name)
        .with_context(|| format!("Failed to write {}", env.environment_file.display()))
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
