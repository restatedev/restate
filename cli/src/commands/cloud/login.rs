use std::fmt::Display;

use crate::{
    c_success,
    cli_env::CliEnv,
    console::{choose, input},
    ui::watcher::Watch,
};
use anyhow::Result;
use base64::Engine;
use cling::prelude::*;
use serde::Deserialize;
use toml_edit::{table, value, DocumentMut};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_login")]
pub struct Login {
    /// Configure the CLI with a new Environment
    #[clap(long)]
    create_environment: bool,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_login(State(env): State<CliEnv>, opts: &Login) -> Result<()> {
    opts.watch.run(|| login(&env, opts)).await
}

async fn login(env: &CliEnv, login_opts: &Login) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    let mut doc = config_data.parse::<DocumentMut>()?;

    let access_token = auth_flow(env)?;

    write_access_token(&mut doc, &access_token)?;

    let (account_id, environment, profile) = if login_opts.create_environment
        || env.config.cloud.environment_info.is_none()
    {
        let accounts = list_accounts(env, &access_token).await?;

        let account_i = match accounts.len() {
            0 => {
                return Err(anyhow::anyhow!(
                    "No accounts set up; use the Restate Cloud UI to create your first account"
                ))
            }
            1 => 0,
            _ => account_picker(&accounts)?,
        };

        let mut environments = list_environments(env, &access_token, &accounts[account_i]).await?;

        let environment_i = match environments.len() {
            0 => {
                return Err(anyhow::anyhow!(
                "No environments set up; use the Restate Cloud UI to create your first environment"
            ))
            }
            1 => 0,
            _ => environment_picker(&environments)?,
        };

        let profiles = list_profiles(&doc)?;
        let profile = profile_input(&profiles, &environments[environment_i].name)?;

        (
            accounts[account_i].id.clone(),
            environments.swap_remove(environment_i),
            profile,
        )
    } else {
        let info = env.config.cloud.environment_info.as_ref().unwrap();
        let environment =
            describe_environment(env, &access_token, &info.account_id, &info.environment_id)
                .await?;

        (
            info.account_id.clone(),
            environment,
            env.environment.to_string(),
        )
    };

    let (key_id, api_key) =
        create_api_key(env, &access_token, &account_id, &environment.id).await?;
    c_success!("Created API Key {key_id} for the CLI");

    write_environment(&mut doc, &account_id, &environment, &api_key, &profile)?;

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

#[derive(Deserialize)]
struct TokenClaims {
    exp: i64,
}

fn auth_flow(_env: &CliEnv) -> Result<String> {
    Ok("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE3MTQxMjg3ODl9.ScqUsLzCQpB4meQpW3HaHoKQmIxO3jLndblXL9VXckc".into())
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

struct Account {
    id: String,
    name: String,
}

impl Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.name, self.id)
    }
}

async fn list_accounts(_env: &CliEnv, _access_token: &str) -> Result<Vec<Account>> {
    Ok(vec![
        Account {
            id: "acc_1".into(),
            name: "jack@restate.dev".into(),
        },
        Account {
            id: "acc_2".into(),
            name: "prod@restate.dev".into(),
        },
    ])
}

fn account_picker(accounts: &[Account]) -> Result<usize> {
    choose("Select an Account:", accounts)
}

struct Environment {
    id: String,
    // TODO; return these from server side
    name: String,
    ingress_base_url: String,
    admin_base_url: String,
}

impl Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.name, self.id)
    }
}

async fn list_environments(
    _env: &CliEnv,
    _access_token: &str,
    _account: &Account,
) -> Result<Vec<Environment>> {
    Ok(vec![
        Environment {
            id: "env_1".into(),
            name: "dev".into(),
            ingress_base_url: "https://service.dev.restate.cloud:8080".into(),
            admin_base_url: "https://service.dev.restate.cloud:9070".into(),
        },
        Environment {
            id: "env_2".into(),
            name: "prod".into(),
            ingress_base_url: "https://service.dev.restate.cloud:8080".into(),
            admin_base_url: "https://service.dev.restate.cloud:9070".into(),
        },
    ])
}

async fn describe_environment(
    _env: &CliEnv,
    _access_token: &str,
    _account_id: &str,
    _environment_id: &str,
) -> Result<Environment> {
    Ok(Environment {
        id: "env_1".into(),
        name: "dev".into(),
        ingress_base_url: "https://service.dev.restate.cloud:8080".into(),
        admin_base_url: "https://service.dev.restate.cloud:9070".into(),
    })
}

fn environment_picker(environments: &[Environment]) -> Result<usize> {
    choose("Select an Environment:", environments)
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

async fn create_api_key(
    _env: &CliEnv,
    _access_token: &str,
    _account_id: &str,
    _environment_id: &str,
) -> Result<(String, String)> {
    Ok(("key_1".into(), "key_1.secret".into()))
}

fn write_environment(
    doc: &mut DocumentMut,
    account_id: &str,
    environment: &Environment,
    api_key: &str,
    profile: &str,
) -> Result<()> {
    let profile_block = doc[profile].or_insert(table());
    profile_block["bearer_token"] = value(api_key);
    profile_block["ingress_base_url"] = value(&environment.ingress_base_url);
    profile_block["admin_base_url"] = value(&environment.admin_base_url);

    let cloud = profile_block["cloud"].or_insert(table());
    cloud["account_id"] = value(account_id);
    cloud["environment_id"] = value(&environment.id);

    Ok(())
}
