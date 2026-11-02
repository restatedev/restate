// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Choosing a Restate Cloud environment (account and environment), as `restate cloud login`
//! does.

use anyhow::Result;
use itertools::Itertools;

use crate::clients::cloud::{
    CloudClient, CloudClientInterface,
    types::{
        DescribeEnvironmentResponse, ListAccountsResponseAccountsItem,
        ListEnvironmentsResponseEnvironmentsItem,
    },
};
use crate::console::choose;
use crate::ui::with_progress;

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

/// Pick an environment, given the one the CLI currently uses (if any): silently when the
/// user has one account with one environment, else by asking (which fails when prompting
/// isn't possible, see [`choose`]). When asking, the current environment can be kept; it's
/// kept without asking when it's the only one.
pub(crate) async fn choose_environment(
    client: &CloudClient,
    current: Option<&CurrentEnvironment<'_>>,
) -> Result<Choice> {
    let accounts = with_progress("Listing accounts...", async {
        client.list_accounts().await?.into_body().await
    })
    .await?
    .accounts;

    let account_i = match accounts.len() {
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
    let environment_i = match environments.as_slice() {
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

pub(super) fn account_picker(accounts: &[ListAccountsResponseAccountsItem]) -> Result<usize> {
    choose(
        "Select an Account:",
        accounts.iter().map(|acc| &acc.name).collect_vec().as_ref(),
    )
}

pub(super) fn environment_picker(
    environments: &[ListEnvironmentsResponseEnvironmentsItem],
) -> Result<usize> {
    choose(
        "Select an Environment:",
        environments
            .iter()
            .map(|env| &env.name)
            .collect_vec()
            .as_ref(),
    )
}
