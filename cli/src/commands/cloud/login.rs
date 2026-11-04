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
use serde_json::json;
use toml_edit::{DocumentMut, table, value};

use restate_cli_util::{CliContext, c_println};

use crate::{
    build_info,
    cli_env::{CliEnv, set_saved_profile},
    clients::cloud::{CloudClient, CloudClientInterface, types::GetUserIdentityResponse},
    commands::profile::sections::{CloudEnvironment, SyncReport, sync_cloud_profiles},
    console::choose,
    error::RestateCliError,
    ui::{
        fmt::{Field, Formatter, IfEmpty, IncludeFormatting, Outcome, OutputFormatter},
        with_progress,
    },
};

/// Log in to Restate Cloud and import its environments as profiles
///
/// The browser decides who you log in as. Back here, every account and environment that
/// user can reach becomes a profile named `<email>/<account>/<environment>` (existing ones
/// are refreshed under their current name, ones you can no longer reach are removed), and
/// one is selected: the current profile when it is that user's, the only one, or the one
/// you pick. Logging in as another user adds their profiles alongside.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_login")]
#[command(after_help = after_help!(
    examples: [
        "restate cloud login",
        "restate cloud login --no-select    # import the profiles, keep the current selection",
    ],
    learn_more: "https://docs.restate.dev/cloud",
))]
pub struct Login {
    /// Import the profiles without selecting one
    #[clap(long, alias = "no-env")]
    no_select: bool,
}

pub async fn run_login(State(env): State<CliEnv>, opts: &Login) -> Result<()> {
    // The device flow needs someone at a browser, so without a terminal (CI, piped
    // output, `--yes`) fail before touching the config or the network.
    if !CliContext::get().can_prompt() {
        return Err(RestateCliError::bad_input(
            "`restate cloud login` isn't supported in non-interactive mode: logging in needs a browser",
        )
        .with_unformatted_next_step("restate cloud login", "log in from a terminal")
        .into());
    }

    let client_id = env.config.cloud.resolve_workos_client_id().await?;
    let access_token = workos_auth_flow(&client_id).await?;

    let client = CloudClient::with_token(&env, access_token.clone())?;
    let identity = with_progress("Checking who you are...", async {
        client.get_user_identity().await?.into_body().await
    })
    .await
    .context("Failed to get the user's identity")?;
    let user_label = identity
        .email
        .clone()
        .unwrap_or_else(|| identity.user_id.clone());
    let environments = fetch_environments(&client).await?;

    let mut doc = env.load_config_document()?;
    write_user_credentials(&mut doc, &identity, &access_token);
    let report = sync_cloud_profiles(&mut doc, &identity.user_id, &user_label, &environments);

    let mut f = Formatter::new();
    f.outcome(
        "login",
        Field::with_display(
            json!({ "user_id": identity.user_id, "email": identity.email }),
            format!("Logged in to Restate Cloud as {user_label}"),
        ),
        Outcome::Success,
    );
    report_profiles(&mut f, &report, &environments);

    let selected = if opts.no_select {
        None
    } else {
        select_profile(&env, &mut doc, &report, &environments)?
    };
    env.write_config(&doc.to_string())
        .context("Failed to write the config file")?;

    match selected {
        Some(Selection { name, kept: true }) => f.outcome(
            "result",
            Field::with_display(
                json!({ "selected": name, "changed": false }),
                format!("Kept \"{name}\" as the current profile"),
            ),
            Outcome::Success,
        ),
        Some(Selection { name, kept: false }) => f.outcome(
            "result",
            Field::with_display(
                json!({ "selected": name, "changed": true }),
                format!("Current profile is now \"{name}\""),
            ),
            Outcome::Success,
        ),
        None if report.kept().next().is_none() => {
            f.outcome(
                "result",
                Field::with_display(
                    "no_environments",
                    format!(
                        "{user_label} can't reach any Restate Cloud environment yet; create one at https://cloud.restate.dev and log in again"
                    ),
                ),
                Outcome::NothingToDo,
            );
        }
        None => {}
    }
    f.next_step(
        "restate whoami",
        "see the server the CLI talks to",
        IncludeFormatting::Yes,
    );
    f.next_step(
        "restate profile use",
        "switch to another profile",
        IncludeFormatting::No,
    );
    f.finish()
}

/// Every environment of every account the user can reach.
async fn fetch_environments(client: &CloudClient) -> Result<Vec<CloudEnvironment>> {
    let accounts = with_progress("Listing accounts...", async {
        client.list_accounts().await?.into_body().await
    })
    .await
    .context("Failed to list the accounts")?
    .accounts;

    let mut environments = Vec::new();
    for account in accounts {
        let listed = with_progress("Listing environments...", async {
            client
                .list_environments(&account.account_id)
                .await?
                .into_body()
                .await
        })
        .await
        .with_context(|| {
            format!(
                "Failed to list the environments of account {}",
                account.name
            )
        })?
        .environments;
        for item in listed {
            let environment = with_progress("Describing environments...", async {
                client
                    .describe_environment(&account.account_id, &item.environment_id)
                    .await?
                    .into_body()
                    .await
            })
            .await
            .with_context(|| format!("Failed to describe environment {}", item.name))?;
            environments.push(CloudEnvironment {
                account_id: account.account_id.clone(),
                account_name: account.name.clone(),
                environment,
            });
        }
    }
    Ok(environments)
}

/// Store the user's token under `[global.cloud.users.<id>]`, dropping the single token of
/// older logins: profiles still relying on it were adopted by the sync when they were this
/// user's, and need a login as their user otherwise.
fn write_user_credentials(
    doc: &mut DocumentMut,
    identity: &GetUserIdentityResponse,
    access_token: &str,
) {
    let cloud = doc["global"].or_insert(table())["cloud"].or_insert(table());
    if let Some(cloud) = cloud.as_table_like_mut() {
        cloud.remove("access_token");
    }
    let user = cloud["users"].or_insert(table())[&identity.user_id].or_insert(table());
    match &identity.email {
        Some(email) => user["email"] = value(email),
        None => {
            if let Some(user) = user.as_table_like_mut() {
                user.remove("email");
            }
        }
    }
    user["access_token"] = value(access_token);
}

/// The profiles table: what the sync did to each.
fn report_profiles(f: &mut Formatter, report: &SyncReport, environments: &[CloudEnvironment]) {
    let row = |change: &str, name: &str, environment: Option<&CloudEnvironment>| {
        let (account, environment, region) = match environment {
            Some(e) => (
                e.account_name.as_str(),
                e.environment.name.as_str(),
                e.environment.region.as_str(),
            ),
            None => ("", "", ""),
        };
        [
            Field::new(change),
            Field::new(name),
            Field::new(account),
            Field::new(environment),
            Field::new(region),
        ]
    };
    // The sync yields one profile per environment, in order.
    let rows: Vec<[Field; 5]> = report
        .profiles
        .iter()
        .zip(environments)
        .map(|((name, change), environment)| row(change.as_str(), name, Some(environment)))
        .chain(report.removed.iter().map(|name| row("removed", name, None)))
        .collect();
    f.table(
        "profiles",
        &["change", "profile", "account", "environment", "region"],
        rows,
        IfEmpty::Say("No environments to import"),
    );
}

struct Selection {
    name: String,
    kept: bool,
}

/// Which of the user's profiles to make current: the one already selected when it is
/// theirs, else the only one, else the one they pick. `None` when they have none.
fn select_profile(
    env: &CliEnv,
    doc: &mut DocumentMut,
    report: &SyncReport,
    environments: &[CloudEnvironment],
) -> Result<Option<Selection>> {
    let names: Vec<&str> = report.kept().collect();
    let saved = env.saved_profile()?;
    if let Some(saved) = saved.as_deref().filter(|saved| names.contains(saved)) {
        return Ok(Some(Selection {
            name: saved.to_owned(),
            kept: true,
        }));
    }
    let name = match names.as_slice() {
        [] => return Ok(None),
        [only] => *only,
        _ => {
            // The user is known: `account/environment` identifies each profile.
            let labels: Vec<String> = environments
                .iter()
                .map(|e| format!("{}/{}", e.account_name, e.environment.name))
                .collect();
            names[choose("Select the environment to use:", &labels)?]
        }
    };
    set_saved_profile(doc, Some(name));
    Ok(Some(Selection {
        name: name.to_owned(),
        kept: false,
    }))
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
