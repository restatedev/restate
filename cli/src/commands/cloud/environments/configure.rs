use crate::{
    c_success,
    cli_env::CliEnv,
    clients::{
        cloud::generated::{
            DescribeEnvironmentResponse, ListAccountsResponseAccountsItem,
            ListEnvironmentsResponseEnvironmentsItem,
        },
        cloud::{CloudClient, CloudClientInterface},
    },
    console::{choose, input},
};
use anyhow::{Context, Result};
use cling::prelude::*;
use itertools::Itertools;
use toml_edit::{table, value, DocumentMut};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_configure")]
pub struct Configure {}

pub async fn run_configure(State(env): State<CliEnv>, _opts: &Configure) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    let mut doc = config_data
        .parse::<DocumentMut>()
        .context("Failed to parse config file as TOML")?;

    let client = CloudClient::new(&env)?;

    let accounts = client.list_accounts().await?.into_body().await?.accounts;

    let account_i = match accounts.len() {
        0 => {
            return Err(anyhow::anyhow!(
                "No accounts set up; use the Restate Cloud UI to create your first account"
            ))
        }
        1 => 0,
        _ => account_picker(&accounts)?,
    };

    let environments = client
        .list_environments(&accounts[account_i].account_id)
        .await?
        .into_body()
        .await?
        .environments;

    let environment_i = match environments.len() {
        0 => {
            return Err(anyhow::anyhow!(
                "No environments set up; use the Restate Cloud UI to create your first environment"
            ))
        }
        1 => 0,
        _ => environment_picker(&environments)?,
    };

    let environment = client
        .describe_environment(
            &accounts[account_i].account_id,
            &environments[environment_i].environment_id,
        )
        .await?
        .into_body()
        .await?;

    let profiles = list_profiles(&doc)?;
    let profile = profile_input(&profiles, &environments[environment_i].environment_id)?; // TODO should be name here

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
        std::fs::write(env.environment_file.as_path(), &profile)?;
        c_success!("Updated {} to {}", env.environment_file.display(), profile);
    }

    // write out config
    std::fs::write(env.config_file.as_path(), doc.to_string())?;
    c_success!("Updated {} with credentials", env.config_file.display());

    Ok(())
}

fn account_picker(accounts: &[ListAccountsResponseAccountsItem]) -> Result<usize> {
    choose(
        "Select an Account:",
        accounts
            .iter()
            .map(|acc| format!("nameshouldgohere ({})", acc.account_id))
            .collect_vec()
            .as_ref(),
    )
}

fn environment_picker(environments: &[ListEnvironmentsResponseEnvironmentsItem]) -> Result<usize> {
    choose(
        "Select an Environment:",
        environments
            .iter()
            .map(|env| format!("nameshouldgohere ({})", env.environment_id))
            .collect_vec()
            .as_ref(),
    )
}

fn list_profiles(doc: &DocumentMut) -> Result<Vec<String>> {
    Ok(doc
        .iter()
        .filter(|(k, _)| k != &"global")
        .map(|(k, _)| k.to_string())
        .collect())
}

fn profile_input(profiles: &[String], environment_name: &str) -> Result<String> {
    input(
        &format!(
            "Choose a friendly name for the Environment for use with the CLI. Current names: [{}]",
            profiles.join(", ")
        ),
        environment_name.replace(' ', "-"),
    )
}

fn write_environment(
    doc: &mut DocumentMut,
    account_id: &str,
    environment: &DescribeEnvironmentResponse,
    profile: &str,
) -> Result<()> {
    let profile_block = doc[profile].or_insert(table());
    profile_block["ingress_base_url"] = value(&environment.ingress_base_url);
    profile_block["admin_base_url"] = value(&environment.admin_base_url);

    let cloud = profile_block["cloud"].or_insert(table());
    cloud["account_id"] = value(account_id);
    cloud["environment_id"] = value(&environment.environment_id);

    Ok(())
}
