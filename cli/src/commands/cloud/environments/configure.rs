// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{future::Future, str::FromStr};

use anyhow::{Context, Result};
use cling::prelude::*;
use indicatif::ProgressBar;
use itertools::Itertools;
use toml_edit::{DocumentMut, table, value};

use restate_cli_util::{c_error, c_success};

use crate::{
    cli_env::CliEnv,
    clients::cloud::{
        CloudClient, CloudClientInterface,
        types::{
            DescribeEnvironmentResponse, ListAccountsResponseAccountsItem,
            ListEnvironmentsResponseEnvironmentsItem,
        },
    },
    console::{choose, confirm_or_exit, input},
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_configure")]
pub struct Configure {
    /// The Cloud environment to configure the CLI for.
    /// Format: [ACCOUNT/]ENVIRONMENT where ACCOUNT and ENVIRONMENT may be either names or IDs.
    environment_specifier: Option<EnvironmentSpecifier>,
}

#[derive(Clone)]
struct EnvironmentSpecifier(Option<String>, String);

impl FromStr for EnvironmentSpecifier {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('/') {
            Some((account, environment)) => Ok(Self(Some(account.into()), environment.into())),
            None => Ok(Self(None, s.into())),
        }
    }
}

pub async fn run_configure(State(env): State<CliEnv>, opts: &Configure) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    let mut doc = config_data
        .parse::<DocumentMut>()
        .context("Failed to parse config file as TOML")?;

    let client = CloudClient::new(&env)?;

    let accounts = with_progress("Listing accounts...", async {
        client.list_accounts().await?.into_body().await
    })
    .await?
    .accounts;

    let (account, environment) =
        if let Some(EnvironmentSpecifier(account, environment)) = &opts.environment_specifier {
            (
                account.as_ref().map(String::as_str),
                Some(environment.as_str()),
            )
        } else {
            (None, None)
        };

    let account_i = if let Some(account) = account {
        match accounts
            .iter()
            .find_position(|acc| acc.account_id == account)
            .or(accounts.iter().find_position(|env| env.name == account))
        {
            Some((i, _)) => i,
            None => return Err(anyhow::anyhow!("Couldn't find account {account}",)),
        }
    } else {
        match accounts.len() {
            0 => {
                c_error!(
                    "No accounts set up; use the Restate Cloud UI to create your first account"
                );
                return Ok(());
            }
            1 => 0,
            _ => account_picker(&accounts)?,
        }
    };

    let environments = with_progress("Listing environments...", async {
        client
            .list_environments(&accounts[account_i].account_id)
            .await?
            .into_body()
            .await
    })
    .await?
    .environments;

    let environment_i = if let Some(environment) = environment {
        match environments
            .iter()
            .find_position(|env| env.environment_id == environment)
            .or(environments
                .iter()
                .find_position(|env| env.name == environment))
        {
            Some((i, _)) => i,
            None => {
                return Err(anyhow::anyhow!(
                    "Couldn't find environment {environment} in account {}",
                    accounts[account_i].name,
                ));
            }
        }
    } else {
        match environments.len() {
            0 => {
                c_error!(
                    "No environments set up; use the Restate Cloud UI to create your first environment"
                );
                return Ok(());
            }
            1 => 0,
            _ => environment_picker(&environments)?,
        }
    };

    let environment = with_progress("Describing environment...", async {
        client
            .describe_environment(
                &accounts[account_i].account_id,
                &environments[environment_i].environment_id,
            )
            .await?
            .into_body()
            .await
    })
    .await?;

    let profiles = list_profiles(&doc)?;
    let profile = profile_input(&profiles, &environments[environment_i].name)?;

    if profiles.contains(&profile) {
        confirm_or_exit(&format!("Overwrite existing profile {profile}?"))?
    }

    write_environment(
        &mut doc,
        &accounts[account_i].account_id,
        &environment,
        &profile,
    )?;

    let current_environment = if env.environment_file.is_file() {
        std::fs::read_to_string(env.environment_file.as_path())?
    } else {
        String::new()
    };
    if profile != current_environment.trim() {
        env.write_environment(&profile)?;
        c_success!("Updated {} to {}", env.environment_file.display(), profile);
    }

    // write out config
    env.write_config(&doc.to_string())?;
    c_success!(
        "Updated {} with environment details",
        env.config_file.display()
    );

    Ok(())
}

async fn with_progress<T>(msg: &'static str, f: impl Future<Output = T>) -> T {
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(msg);
    let result = f.await;
    progress.reset();
    result
}

fn account_picker(accounts: &[ListAccountsResponseAccountsItem]) -> Result<usize> {
    choose(
        "Select an Account:",
        accounts.iter().map(|acc| &acc.name).collect_vec().as_ref(),
    )
}

fn environment_picker(environments: &[ListEnvironmentsResponseEnvironmentsItem]) -> Result<usize> {
    choose(
        "Select an Environment:",
        environments
            .iter()
            .map(|env| &env.name)
            .collect_vec()
            .as_ref(),
    )
}

fn list_profiles(doc: &DocumentMut) -> Result<Vec<String>> {
    Ok(doc
        .iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case("global") && !k.eq_ignore_ascii_case("default"))
        .map(|(k, _)| k.to_string())
        .collect())
}

fn profile_input(profiles: &[String], environment_name: &str) -> Result<String> {
    input(
        &format!(
            "Choose a friendly name for the Environment for use with the CLI.\n  Current names: [{}]\n",
            profiles.join(", ")
        ),
        environment_name.into(),
    )
}

fn write_environment(
    doc: &mut DocumentMut,
    account_id: &str,
    environment: &DescribeEnvironmentResponse,
    profile: &str,
) -> Result<()> {
    let profile_block = doc[profile].or_insert(table());
    profile_block["environment_type"] = value("cloud");
    profile_block["ingress_base_url"] = value(&environment.ingress_base_url);
    profile_block["admin_base_url"] = value(&environment.admin_base_url);

    let cloud = profile_block["cloud"].or_insert(table());
    cloud["account_id"] = value(account_id);
    cloud["environment_id"] = value(&environment.environment_id);

    Ok(())
}
