// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use cling::prelude::*;
use serde_json::{Value, json};

use restate_cli_util::{c_eprintln, c_println};

use crate::cli_env::{CliEnv, EnvironmentType};
use crate::clients::cloud::{CloudClient, CloudClientInterface, types::CreateApiKeyRequest};
use crate::error::RestateCliError;
use crate::ui::fmt::{Field, Formatter, IncludeFormatting, Outcome, OutputFormatter};

use super::deployment_name::hostname;

/// The keys the SDKs read, with the role each one needs and whether it carries the
/// environment's signing key (only the connect key does).
const KEYS: [(&str, &str, bool); 2] = [
    ("RESTATE_CONNECT_KEY", "rst:role::TunnelAccess", true),
    ("RESTATE_INGRESS_KEY", "rst:role::IngressAccess", false),
];
const DEFAULT_OUTPUT: &str = "restate.env";
/// The prefix of every Restate Cloud key the SDKs read.
const CLOUD_KEY_PREFIX: &str = "rstcloudv1_";

/// Mint the keys an SDK needs to reach the current Cloud environment, into a dotenv file
///
/// Creates two API keys for the environment of the current profile: RESTATE_CONNECT_KEY
/// (for a service that connects with the SDK's `connect()`) and RESTATE_INGRESS_KEY (for
/// invoking services from code), and writes them to restate.env in the current directory,
/// a file to load with your runtime (`node --env-file`, `uv run --env-file`, your IDE) or
/// to export in the shell. The file is only ever created: an existing one isn't touched,
/// and no key is minted when it can't be written. A comment above each key records its id,
/// to revoke it later. The keys are never printed, unless --stdout asks for the two
/// `KEY="value"` lines on stdout instead.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_connect_sdk")]
#[command(after_help = after_help!(
    examples: [
        "restate cloud connect-sdk",
        "restate cloud connect-sdk --output services/greeter/.env",
        "restate cloud connect-sdk --stdout >> .env",
    ],
    learn_more: "https://docs.restate.dev/cloud/getting-started",
))]
pub struct ConnectSdk {
    /// The dotenv file to create
    #[clap(long, value_name = "PATH", default_value = DEFAULT_OUTPUT, conflicts_with = "stdout")]
    output: PathBuf,

    /// Print the two `KEY="value"` lines on stdout instead of writing the file (everything
    /// else goes to stderr), to pipe them somewhere
    #[clap(long, conflicts_with = "json")]
    stdout: bool,
}

pub async fn run_connect_sdk(State(env): State<CliEnv>, opts: &ConnectSdk) -> Result<()> {
    let environment_info = match (
        &env.config.environment_type,
        &env.config.cloud.environment_info,
    ) {
        (EnvironmentType::Cloud, Some(environment_info)) => environment_info,
        _ => {
            return Err(RestateCliError::bad_input(format!(
                "The current profile \"{}\" is not a Restate Cloud environment",
                env.profile
            ))
            .with_next_step("restate profile use", "switch to a Cloud profile")
            .with_unformatted_next_step("restate cloud login", "import Cloud profiles")
            .into());
        }
    };
    let client = CloudClient::new(&env)?;

    // The coordinates the keys carry besides the API key.
    let environment = client
        .describe_environment(
            &environment_info.account_id,
            &environment_info.environment_id,
        )
        .await?
        .into_body()
        .await?;
    let region = region_domain(&environment.ingress_base_url).with_context(|| {
        format!(
            "Cannot tell the region of environment {} from its ingress URL {}",
            environment_info.environment_id, environment.ingress_base_url
        )
    })?;

    // Claim the file before minting anything: a key with no file to land in is lost.
    let mut file = if opts.stdout {
        None
    } else {
        Some(create_new(&opts.output)?)
    };

    let environment_name = environment_info
        .environment_name
        .as_deref()
        .unwrap_or(&environment_info.environment_id);
    let description = format!(
        "restate cloud connect-sdk on {}, {}",
        hostname(),
        chrono::Local::now().format("%Y-%m-%d")
    );
    // The file carries, as comments, what an agent needs to revoke the keys later; stdout
    // carries the bare `KEY="value"` lines, for piping.
    let mut lines = vec![format!(
        "# Restate Cloud keys for environment {environment_name} ({}), minted by `restate cloud connect-sdk`",
        environment_info.environment_id
    )];
    let mut values = Vec::with_capacity(KEYS.len());
    let mut minted = Vec::with_capacity(KEYS.len());
    let mut mint = async |variable: &str, role_id: &str, with_signing_key: bool| -> Result<()> {
        let key = client
            .create_api_key(
                &environment_info.account_id,
                CreateApiKeyRequest {
                    environment_id: environment_info.environment_id.clone(),
                    role_id: role_id.to_owned(),
                    description: Some(description.clone()),
                },
            )
            .await?
            .into_body()
            .await
            .with_context(|| format!("Failed to create the key for {variable}"))?;
        let cloud_key = cloud_key(
            &environment_info.environment_id,
            &key.api_key,
            &region,
            with_signing_key.then_some(environment.signing_public_key.as_str()),
        );
        let value = format!("{variable}=\"{cloud_key}\"");
        lines.push(format!("# key {} ({})", key.key_id, key.role_id));
        lines.push(value.clone());
        values.push(value);
        minted.push(json!({ "variable": variable, "key_id": key.key_id, "role_id": key.role_id }));
        Ok(())
    };
    // On a failure after minting started, leave no empty file behind (the command never
    // overwrites) and name the keys minted so far: they're lost to the caller and worth
    // revoking.
    let abandon = |err: anyhow::Error, file: Option<std::fs::File>, minted: &[Value]| {
        drop(file);
        if !opts.stdout {
            let _ = std::fs::remove_file(&opts.output);
        }
        let revoke = minted
            .iter()
            .filter_map(|key| key["key_id"].as_str())
            .collect::<Vec<_>>();
        if revoke.is_empty() {
            err
        } else {
            err.context(format!(
                "Nothing was written; revoke the key(s) already minted in the Cloud UI: {}",
                revoke.join(", ")
            ))
        }
    };
    for (variable, role_id, with_signing_key) in KEYS {
        if let Err(err) = mint(variable, role_id, with_signing_key).await {
            return Err(abandon(err, file.take(), &minted));
        }
    }
    let content = format!("{}\n", lines.join("\n"));

    if let Some(mut file) = file.take()
        && let Err(err) = file
            .write_all(content.as_bytes())
            .and_then(|()| file.flush())
            .with_context(|| format!("Failed to write {}", opts.output.display()))
    {
        return Err(abandon(err, Some(file), &minted));
    }

    if opts.stdout {
        // The content is the command's output; the rest is commentary, on stderr.
        c_println!("{}", values.join("\n"));
        c_eprintln!(
            "Minted {} and {} for environment {environment_name} ({})",
            KEYS[0].0,
            KEYS[1].0,
            minted
                .iter()
                .filter_map(|key| key["key_id"].as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        return Ok(());
    }

    let mut f = Formatter::new();
    f.detail(
        "keys",
        [
            ("file", Field::new(opts.output.display().to_string())),
            (
                "environment",
                Field::with_display(
                    json!({ "id": environment_info.environment_id, "name": environment_name }),
                    environment_name.to_owned(),
                ),
            ),
            (
                "variables",
                Field::with_display(
                    &minted,
                    KEYS.iter()
                        .map(|(variable, ..)| *variable)
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            ),
        ],
    );
    f.outcome(
        "result",
        Field::with_display(
            "written",
            format!(
                "Wrote {} with the keys of environment {environment_name}",
                opts.output.display()
            ),
        ),
        Outcome::Success,
    );
    if git_ignored(&opts.output) == Some(false) {
        f.warning(&format!(
            "{} is not ignored by git, and it holds keys: run `echo {} >> .gitignore`",
            opts.output.display(),
            opts.output.display()
        ));
    }
    f.next_step(
        &format!("set -a; source {}; set +a", opts.output.display()),
        "export the keys in this shell",
        IncludeFormatting::No,
    );
    f.finish()
}

/// The region domain of an environment (`eu.restate.cloud`): what follows `.env.` in the
/// host of its ingress URL (`https://<id>.env.<region>:8080`).
fn region_domain(ingress_base_url: &str) -> Option<String> {
    let url = url::Url::parse(ingress_base_url).ok()?;
    let (_, region) = url.host_str()?.split_once(".env.")?;
    (!region.is_empty()).then(|| region.to_owned())
}

/// A Restate Cloud key as the SDKs read it: `rstcloudv1_` followed by the base58 (Bitcoin
/// alphabet) of the query string `envid=…&apikey=…&region=…`, plus `&signingkey=…` for a
/// connect key.
fn cloud_key(
    environment_id: &str,
    api_key: &str,
    region: &str,
    signing_public_key: Option<&str>,
) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("envid", environment_id)
        .append_pair("apikey", api_key)
        .append_pair("region", region);
    if let Some(signing_public_key) = signing_public_key {
        query.append_pair("signingkey", signing_public_key);
    }
    format!(
        "{CLOUD_KEY_PREFIX}{}",
        bs58::encode(query.finish()).into_string()
    )
}

/// Create `path` for writing, failing if it exists (it's never read or replaced) or can't
/// be created.
fn create_new(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
    }
    match std::fs::File::create_new(path) {
        Ok(file) => Ok(file),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let rerun = crate::ui::fmt::rerun_command_without(&["--output"]);
            Err(RestateCliError::bad_input(format!(
                "{} already exists; it is never changed, so no key was minted",
                path.display()
            ))
            .with_next_step(format!("{rerun} --output <PATH>"), "write another file")
            .with_next_step(format!("{rerun} --stdout"), "print the keys to pipe them")
            .into())
        }
        Err(err) => Err(err).with_context(|| format!("Failed to create {}", path.display())),
    }
}

/// Whether git ignores `path`; `None` when there is no git, or no repository.
fn git_ignored(path: &Path) -> Option<bool> {
    let status = Command::new("git")
        .args(["check-ignore", "-q", "--"])
        .arg(path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()?;
    match status.code() {
        Some(0) => Some(true),
        Some(1) => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_new_claims_the_file_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("restate.env");
        assert!(create_new(&path).is_ok());
        let err = create_new(&path).unwrap_err().to_string();
        assert!(err.contains("already exists"), "{err}");
    }

    #[test]
    fn cloud_key_encodes_the_sdk_query_string() {
        let key = cloud_key(
            "env_201kxe0kdaynq7h1fsn2chhcbwb",
            "key_01abc.secret",
            "eu.restate.cloud",
            Some("publickeyv1_ChjENKeMvCtRnqG2mrBK1HmPKufgFUc98K8B3ononQvp"),
        );
        let query = bs58::decode(key.strip_prefix(CLOUD_KEY_PREFIX).unwrap())
            .into_vec()
            .unwrap();
        assert_eq!(
            String::from_utf8(query).unwrap(),
            "envid=env_201kxe0kdaynq7h1fsn2chhcbwb&apikey=key_01abc.secret&region=eu.restate.cloud&signingkey=publickeyv1_ChjENKeMvCtRnqG2mrBK1HmPKufgFUc98K8B3ononQvp"
        );
    }

    #[test]
    fn region_domain_is_what_follows_env_in_the_ingress_host() {
        assert_eq!(
            region_domain("https://201kxe0kdaynq7h1fsn2chhcbwb.env.acme.byoc.restate.cloud:8080/")
                .as_deref(),
            Some("acme.byoc.restate.cloud")
        );
    }
}
