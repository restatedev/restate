// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The sections of the CLI config file (one per profile), as `restate profile add`,
//! `restate profile use` and `restate cloud login` write them.

use anyhow::Result;
use toml_edit::{DocumentMut, table, value};

use restate_types::net::address::{AdminPort, AdvertisedAddress, HttpIngressPort};

use crate::cli_env::{CliEnv, LOCAL_PROFILE};
use crate::error::RestateCliError;

/// What a section of the config file says about its environment.
pub(crate) enum ProfileSpec<'a> {
    SelfHosted {
        admin_base_url: &'a AdvertisedAddress<AdminPort>,
        ingress_base_url: Option<&'a AdvertisedAddress<HttpIngressPort>>,
        bearer_token: Option<&'a str>,
    },
    #[cfg(feature = "cloud")]
    Cloud {
        account_id: &'a str,
        environment: &'a crate::clients::cloud::types::DescribeEnvironmentResponse,
    },
}

/// The names of the profiles the config file defines.
pub(crate) fn list_profiles(doc: &DocumentMut) -> Vec<String> {
    doc.iter()
        .filter(|(k, _)| !is_reserved(k))
        .map(|(k, _)| k.to_string())
        .collect()
}

/// `default` and `global` are the config file's shared sections, not profiles.
pub(crate) fn is_reserved(name: &str) -> bool {
    name.eq_ignore_ascii_case("global") || name.eq_ignore_ascii_case("default")
}

pub(crate) fn is_local(name: &str) -> bool {
    name == LOCAL_PROFILE.as_str().as_str()
}

/// Fails for a name that can't be a profile's: reserved, the built-in `local`, or one
/// that would read as a Cloud `ACCOUNT/ENVIRONMENT` specifier.
pub(crate) fn check_name(name: &str) -> Result<()> {
    let reason = if name.trim().is_empty() {
        "it is empty"
    } else if is_reserved(name) {
        "it is reserved for the config file's shared settings"
    } else if is_local(name) {
        "it is the built-in profile for a server on this machine"
    } else if name.contains('/') {
        "it would read as a Restate Cloud `ACCOUNT/ENVIRONMENT` specifier"
    } else {
        return Ok(());
    };
    Err(RestateCliError::bad_input(format!("\"{name}\" can't be a profile name: {reason}")).into())
}

/// Write the section of profile `name`, replacing an existing one.
pub(crate) fn write_section(doc: &mut DocumentMut, name: &str, spec: &ProfileSpec<'_>) {
    doc[name] = table();
    let profile = &mut doc[name];
    match spec {
        ProfileSpec::SelfHosted {
            admin_base_url,
            ingress_base_url,
            bearer_token,
        } => {
            profile["admin_base_url"] = value(admin_base_url.to_string());
            if let Some(ingress_base_url) = ingress_base_url {
                profile["ingress_base_url"] = value(ingress_base_url.to_string());
            }
            if let Some(bearer_token) = bearer_token {
                profile["bearer_token"] = value(*bearer_token);
            }
        }
        #[cfg(feature = "cloud")]
        ProfileSpec::Cloud {
            account_id,
            environment,
        } => {
            profile["environment_type"] = value("cloud");
            profile["ingress_base_url"] = value(&environment.ingress_base_url);
            profile["admin_base_url"] = value(&environment.admin_base_url);

            let cloud = profile["cloud"].or_insert(table());
            cloud["account_id"] = value(*account_id);
            cloud["environment_id"] = value(&environment.environment_id);
        }
    }
}

/// Make `name` the current environment. Returns whether it changed.
pub(crate) fn select_profile(env: &CliEnv, name: &str) -> Result<bool> {
    if env.saved_profile()?.as_deref() == Some(name) {
        return Ok(false);
    }
    env.save_profile(name)?;
    Ok(true)
}

/// Whether a section of the config file is the Cloud environment `environment_id`.
#[cfg(feature = "cloud")]
pub(crate) fn is_cloud_environment(section: &toml_edit::Item, environment_id: &str) -> bool {
    section
        .get("cloud")
        .and_then(|cloud| cloud.get("environment_id"))
        .and_then(toml_edit::Item::as_str)
        == Some(environment_id)
}

/// Write the section for a chosen Cloud environment and return its name: always the Cloud
/// profile's. A name already used by a different profile is an error. The config
/// file isn't written.
#[cfg(feature = "cloud")]
pub(crate) fn add_cloud_section(
    env: &CliEnv,
    doc: &mut DocumentMut,
    chosen: &crate::commands::cloud::profile::ChosenEnvironment,
) -> Result<String> {
    let environment_id = &chosen.environment.environment_id;
    let name = chosen.environment.name.clone();
    let taken = is_reserved(&name)
        || is_local(&name)
        || doc
            .get(&name)
            .is_some_and(|existing| !is_cloud_environment(existing, environment_id));
    if taken {
        return Err(RestateCliError::bad_input(format!(
            "The CLI config file {} already has an environment named \"{name}\" that isn't Cloud environment {environment_id}; Cloud environments are always named after themselves",
            env.config_file.display()
        ))
        .with_next_step(
            format!("restate profile rename {name} <new-name>"),
            "free the name",
        )
        .with_next_step(format!("restate profile remove {name}"), "or drop the other one")
        .into());
    }
    write_section(
        doc,
        &name,
        &ProfileSpec::Cloud {
            account_id: &chosen.account_id,
            environment: &chosen.environment,
        },
    );
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_profile_replaces_the_section() {
        let mut doc = "[global]\nx = 1\n\n[staging]\nenvironment_type = \"cloud\"\nadmin_base_url = \"http://old:9070\"\n"
            .parse::<DocumentMut>()
            .unwrap();
        let admin: AdvertisedAddress<AdminPort> =
            "https://restate.example.com:9070".parse().unwrap();
        write_section(
            &mut doc,
            "staging",
            &ProfileSpec::SelfHosted {
                admin_base_url: &admin,
                ingress_base_url: None,
                bearer_token: Some("tok"),
            },
        );
        assert_eq!(list_profiles(&doc), vec!["staging"]);
        let staging = doc["staging"].as_table().unwrap();
        assert!(staging.get("environment_type").is_none());
        assert!(staging.get("ingress_base_url").is_none());
        assert_eq!(
            staging["admin_base_url"].as_str(),
            Some("https://restate.example.com:9070/")
        );
        assert_eq!(staging["bearer_token"].as_str(), Some("tok"));
    }

    #[test]
    fn check_name_rejects_reserved_local_and_specifiers() {
        assert!(check_name("staging").is_ok());
        for bad in ["global", "Default", "local", "acct/env", " "] {
            assert!(check_name(bad).is_err(), "{bad} should be rejected");
        }
    }
}
