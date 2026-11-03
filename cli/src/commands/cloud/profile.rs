// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Choosing a Restate Cloud environment (account and environment), as `restate env add` and
//! `restate cloud login` do.

use std::str::FromStr;

use anyhow::Result;
use itertools::Itertools;

use crate::clients::cloud::{
    CloudClient, CloudClientInterface,
    types::{DescribeEnvironmentResponse, ListEnvironmentsResponseEnvironmentsItem},
};
use crate::console::choose;
use crate::error::RestateCliError;
use crate::ui::with_progress;

/// `[ACCOUNT/]ENVIRONMENT`, each a name or an id.
#[derive(Clone, Debug)]
pub(crate) struct EnvironmentSpecifier(pub Option<String>, pub String);

impl FromStr for EnvironmentSpecifier {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('/') {
            Some((account, environment)) => Ok(Self(Some(account.into()), environment.into())),
            None => Ok(Self(None, s.into())),
        }
    }
}

/// A Cloud environment, with its account and the details from `DescribeEnvironment`.
pub(crate) struct ChosenEnvironment {
    pub account_id: String,
    pub account_name: String,
    pub environment: DescribeEnvironmentResponse,
}

/// The Cloud environment the CLI currently uses.
pub(crate) struct CurrentEnvironment<'a> {
    /// The name of its section in the config file.
    pub profile: &'a str,
    pub environment_id: &'a str,
}

/// The result of [`choose_environment`].
pub(crate) enum Choice {
    Environment(Box<ChosenEnvironment>),
    /// Keep the current environment: it's the only one, or the user chose to.
    KeepCurrent,
    /// The user has no account yet.
    NoAccounts,
    /// The account has no environment yet.
    NoEnvironments {
        account_name: String,
    },
}

/// Find the environment `specifier` names (no prompt; not found is an error), or pick one
/// given the environment the CLI currently uses (if any): silently when the user has one
/// account with one environment, else by asking (which fails when prompting isn't possible,
/// see [`choose`]). When asking, the current environment can be kept; it's kept without
/// asking when it's the only one.
pub(crate) async fn choose_environment(
    client: &CloudClient,
    specifier: Option<&EnvironmentSpecifier>,
    current: Option<&CurrentEnvironment<'_>>,
) -> Result<Choice> {
    let accounts = with_progress("Listing accounts...", async {
        client.list_accounts().await?.into_body().await
    })
    .await?
    .accounts;

    let (account, environment) = match specifier {
        Some(EnvironmentSpecifier(account, environment)) => {
            (account.as_deref(), Some(environment.as_str()))
        }
        None => (None, None),
    };

    let account_i = if let Some(account) = account {
        match accounts
            .iter()
            .find_position(|acc| acc.account_id == account)
            .or_else(|| accounts.iter().find_position(|acc| acc.name == account))
        {
            Some((i, _)) => i,
            None => {
                return Err(RestateCliError::not_found(format!(
                    "Couldn't find account {account}; the accounts are: [{}]",
                    accounts.iter().map(|acc| &acc.name).join(", ")
                ))
                .into());
            }
        }
    } else {
        match accounts.len() {
            0 => return Ok(Choice::NoAccounts),
            1 => 0,
            _ => match pick(
                "Select an Account:",
                accounts.iter().map(|acc| acc.name.clone()),
                current,
            )? {
                Some(i) => i,
                None => return Ok(Choice::KeepCurrent),
            },
        }
    };
    let account = &accounts[account_i];

    let environments = with_progress("Listing environments...", async {
        client
            .list_environments(&account.account_id)
            .await?
            .into_body()
            .await
    })
    .await?
    .environments;

    let is_current = |env: &ListEnvironmentsResponseEnvironmentsItem| {
        current.is_some_and(|current| current.environment_id == env.environment_id)
    };
    let environment_i = if let Some(environment) = environment {
        match environments
            .iter()
            .find_position(|env| env.environment_id == environment)
            .or_else(|| {
                environments
                    .iter()
                    .find_position(|env| env.name == environment)
            }) {
            Some((i, _)) => i,
            None => {
                return Err(RestateCliError::not_found(format!(
                    "Couldn't find environment {environment} in account {}; its environments are: [{}]",
                    account.name,
                    environments.iter().map(|env| &env.name).join(", ")
                ))
                .into());
            }
        }
    } else {
        match environments.as_slice() {
            [] => {
                return Ok(Choice::NoEnvironments {
                    account_name: account.name.clone(),
                });
            }
            [only] if is_current(only) => return Ok(Choice::KeepCurrent),
            [_] if current.is_none() => 0,
            _ => match pick(
                "Select an Environment:",
                environments.iter().map(|env| {
                    if is_current(env) {
                        format!("{} (current)", env.name)
                    } else {
                        env.name.clone()
                    }
                }),
                current,
            )? {
                Some(i) => i,
                None => return Ok(Choice::KeepCurrent),
            },
        }
    };

    let environment = with_progress("Describing environment...", async {
        client
            .describe_environment(
                &account.account_id,
                &environments[environment_i].environment_id,
            )
            .await?
            .into_body()
            .await
    })
    .await?;

    Ok(Choice::Environment(Box::new(ChosenEnvironment {
        account_id: account.account_id.clone(),
        account_name: account.name.clone(),
        environment,
    })))
}

/// Ask to pick one of `items`, or to keep `current` when there is one: `None` means keep.
fn pick(
    prompt: &str,
    items: impl Iterator<Item = String>,
    current: Option<&CurrentEnvironment<'_>>,
) -> Result<Option<usize>> {
    let mut items = items.collect_vec();
    let Some(current) = current else {
        return choose(prompt, &items).map(Some);
    };
    items.push(format!(
        "Keep using the current environment \"{}\"",
        current.profile
    ));
    let i = choose(prompt, &items)?;
    Ok((i + 1 < items.len()).then_some(i))
}

/// The error for a Cloud account or environment that doesn't exist yet, with the UI to
/// create it.
pub(crate) fn nothing_to_add(choice: &Choice) -> Option<RestateCliError> {
    match choice {
        Choice::NoAccounts => Some(RestateCliError::not_found(
            "It looks like you don't have a Restate Cloud account yet: log in to the UI at https://cloud.restate.dev to create one",
        )),
        Choice::NoEnvironments { account_name } => Some(RestateCliError::not_found(format!(
            "Account {account_name} has no environment yet: create one in the UI at https://cloud.restate.dev"
        ))),
        Choice::Environment(_) | Choice::KeepCurrent => None,
    }
}
