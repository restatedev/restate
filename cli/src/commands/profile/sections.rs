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
#[cfg(feature = "cloud")]
use toml_edit::Item;
use toml_edit::{DocumentMut, table, value};

use restate_types::net::address::{AdminPort, AdvertisedAddress, HttpIngressPort};

use crate::cli_env::{CliEnv, LOCAL_PROFILE};
use crate::error::RestateCliError;

/// What a section of the config file says about its profile.
pub(crate) enum ProfileSpec<'a> {
    SelfHosted {
        admin_base_url: &'a AdvertisedAddress<AdminPort>,
        ingress_base_url: Option<&'a AdvertisedAddress<HttpIngressPort>>,
        bearer_token: Option<&'a str>,
    },
    #[cfg(feature = "cloud")]
    Cloud {
        user_id: &'a str,
        environment: &'a CloudEnvironment,
    },
}

/// A Restate Cloud environment as a profile stores it: the account it belongs to and the
/// details from `DescribeEnvironment`.
#[cfg(feature = "cloud")]
pub(crate) struct CloudEnvironment {
    pub account_id: String,
    pub account_name: String,
    pub environment: crate::clients::cloud::types::DescribeEnvironmentResponse,
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

/// Fails for a name that can't be a profile's: empty, reserved, or the built-in `local`.
pub(crate) fn check_name(name: &str) -> Result<()> {
    let reason = if name.trim().is_empty() {
        "it is empty"
    } else if is_reserved(name) {
        "it is reserved for the config file's shared settings"
    } else if is_local(name) {
        "it is the built-in profile for a server on this machine"
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
            user_id,
            environment,
        } => {
            profile["environment_type"] = value("cloud");
            write_cloud_fields(profile, user_id, environment);
        }
    }
}

/// Make `name` the current profile. Returns whether it changed.
pub(crate) fn select_profile(env: &CliEnv, name: &str) -> Result<bool> {
    if env.saved_profile()?.as_deref() == Some(name) {
        return Ok(false);
    }
    env.save_profile(name)?;
    Ok(true)
}

/// Set the URLs and the `cloud` table of a Cloud profile, keeping anything else in it.
#[cfg(feature = "cloud")]
fn write_cloud_fields(profile: &mut Item, user_id: &str, environment: &CloudEnvironment) {
    profile["admin_base_url"] = value(&environment.environment.admin_base_url);
    profile["ingress_base_url"] = value(&environment.environment.ingress_base_url);
    let cloud = profile["cloud"].or_insert(table());
    cloud["user_id"] = value(user_id);
    cloud["account_id"] = value(&environment.account_id);
    cloud["account_name"] = value(&environment.account_name);
    cloud["environment_id"] = value(&environment.environment.environment_id);
    cloud["environment_name"] = value(&environment.environment.name);
}

#[cfg(feature = "cloud")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Change {
    Added,
    Updated,
}

#[cfg(feature = "cloud")]
impl Change {
    pub fn as_str(self) -> &'static str {
        match self {
            Change::Added => "added",
            Change::Updated => "updated",
        }
    }
}

/// What [`sync_cloud_profiles`] did, as profile names: `profiles` has one entry per
/// environment given, in the same order.
#[cfg(feature = "cloud")]
#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct SyncReport {
    pub profiles: Vec<(String, Change)>,
    pub removed: Vec<String>,
}

#[cfg(feature = "cloud")]
impl SyncReport {
    /// The profiles of the user after the sync.
    pub fn kept(&self) -> impl Iterator<Item = &str> {
        self.profiles.iter().map(|(name, _)| name.as_str())
    }
}

/// Make the config document's Cloud profiles of user `user_id` match `environments`, the
/// ones they can reach: a profile exists per (user, account, environment). Existing ones
/// are refreshed in place under their current name, new ones are added as
/// `<user>-<account>-<environment>` (`user_label` is the email, else the user id; the
/// environment id is appended when that name is taken), and the user's profiles for
/// environments they can no longer reach are removed, clearing the saved selection when
/// it pointed at one. Profiles written before credentials were kept per user, which name
/// the environment but no user, are adopted. The config file isn't written.
#[cfg(feature = "cloud")]
pub(crate) fn sync_cloud_profiles(
    doc: &mut DocumentMut,
    user_id: &str,
    user_label: &str,
    environments: &[CloudEnvironment],
) -> SyncReport {
    use crate::cli_env::set_saved_profile;

    let mut report = SyncReport::default();
    let mut stale: Vec<String> = list_profiles(doc)
        .into_iter()
        .filter(|name| cloud_user_of(&doc[name]) == Some(user_id))
        .collect();

    for environment in environments {
        let (account_id, environment_id) = (
            environment.account_id.as_str(),
            environment.environment.environment_id.as_str(),
        );
        let existing = list_profiles(doc).into_iter().find(|name| {
            let section = &doc[name];
            cloud_user_of(section).is_none_or(|user| user == user_id)
                && is_cloud_environment(section, account_id, environment_id)
        });
        match existing {
            Some(name) => {
                write_cloud_fields(&mut doc[&name], user_id, environment);
                stale.retain(|s| *s != name);
                report.profiles.push((name, Change::Updated));
            }
            None => {
                let mut name = format!(
                    "{user_label}-{}-{}",
                    environment.account_name, environment.environment.name
                );
                if is_reserved(&name) || is_local(&name) || doc.contains_key(&name) {
                    name = format!("{name}-{environment_id}");
                }
                write_section(
                    doc,
                    &name,
                    &ProfileSpec::Cloud {
                        user_id,
                        environment,
                    },
                );
                report.profiles.push((name, Change::Added));
            }
        }
    }

    let saved = doc
        .get("global")
        .and_then(|global| global.get(crate::cli_env::PROFILE_KEY))
        .and_then(Item::as_str)
        .map(str::to_owned);
    for name in stale {
        doc.remove(&name);
        if saved.as_deref() == Some(name.as_str()) {
            set_saved_profile(doc, None);
        }
        report.removed.push(name);
    }
    report
}

/// The `cloud.user_id` of a section, if it's a Cloud profile with one.
#[cfg(feature = "cloud")]
fn cloud_user_of(section: &Item) -> Option<&str> {
    section
        .get("cloud")
        .and_then(|cloud| cloud.get("user_id"))
        .and_then(Item::as_str)
}

/// Whether a section is the Cloud environment `environment_id` of account `account_id`.
#[cfg(feature = "cloud")]
fn is_cloud_environment(section: &Item, account_id: &str, environment_id: &str) -> bool {
    let field = |key: &str| {
        section
            .get("cloud")
            .and_then(|cloud| cloud.get(key))
            .and_then(Item::as_str)
    };
    field("account_id") == Some(account_id) && field("environment_id") == Some(environment_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_section_replaces_the_section() {
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
    fn check_name_rejects_empty_reserved_and_local() {
        assert!(check_name("staging").is_ok());
        assert!(check_name("acct/env").is_ok());
        for bad in ["global", "Default", "local", " "] {
            assert!(check_name(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn sync_adds_refreshes_adopts_and_removes_the_users_profiles() {
        use crate::clients::cloud::types::DescribeEnvironmentResponse;

        let environment = |account: &str, id: &str, name: &str| CloudEnvironment {
            account_id: format!("acc_{account}"),
            account_name: account.to_owned(),
            environment: DescribeEnvironmentResponse {
                environment_id: id.to_owned(),
                region: "eu".to_owned(),
                name: name.to_owned(),
                ingress_base_url: format!("https://{id}:8080"),
                admin_base_url: format!("https://{id}:9070"),
                proxy_base_url: String::new(),
                tunnel_srv: String::new(),
                signing_public_key: String::new(),
            },
        };
        let mut doc = r#"
[global]
profile = "gone"

[renamed]
environment_type = "cloud"
admin_base_url = "https://stale:9070"
cloud = { user_id = "u1", account_id = "acc_acme", environment_id = "env_1" }

[gone]
environment_type = "cloud"
cloud = { user_id = "u1", account_id = "acc_acme", environment_id = "env_old" }

[legacy]
environment_type = "cloud"
cloud = { account_id = "acc_acme", environment_id = "env_2" }

[other-user]
environment_type = "cloud"
cloud = { user_id = "u2", account_id = "acc_acme", environment_id = "env_old" }

["alice@acme.dev-acme-new"]
admin_base_url = "http://self-hosted:9070"
"#
        .parse::<DocumentMut>()
        .unwrap();

        let environments = [
            environment("acme", "env_1", "prod"),
            environment("acme", "env_2", "staging"),
            environment("acme", "env_3", "new"),
        ];
        let report = sync_cloud_profiles(&mut doc, "u1", "alice@acme.dev", &environments);

        assert_eq!(
            report,
            SyncReport {
                profiles: vec![
                    ("renamed".to_owned(), Change::Updated),
                    ("legacy".to_owned(), Change::Updated),
                    ("alice@acme.dev-acme-new-env_3".to_owned(), Change::Added),
                ],
                removed: vec!["gone".to_owned()],
            }
        );
        // Refreshed in place under its name; the legacy section now has a user.
        assert_eq!(
            doc["renamed"]["admin_base_url"].as_str(),
            Some("https://env_1:9070")
        );
        assert_eq!(
            doc["renamed"]["cloud"]["environment_name"].as_str(),
            Some("prod")
        );
        assert_eq!(doc["legacy"]["cloud"]["user_id"].as_str(), Some("u1"));
        // Other users' profiles and self-hosted ones are untouched; the selection is cleared.
        assert!(doc.contains_key("other-user"));
        assert!(doc.contains_key("alice@acme.dev-acme-new"));
        assert!(!doc.contains_key("gone"));
        assert!(doc["global"].get("profile").is_none());
    }
}
