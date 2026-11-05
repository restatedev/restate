// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use cling::prelude::*;
use figment::Profile;

use crate::{
    cli_env::{CliConfig, CliEnv, EnvironmentType, LOCAL_PROFILE, ProfileSource},
    ui::fmt::{Field, Formatter, IfEmpty, IncludeFormatting, OutputFormatter},
};

use super::sections::list_profiles;

/// List the profiles of the CLI config file, marking the current one
///
/// Cloud profiles show the user (email), account and environment they reach; ids are in
/// the JSON output. `restate profile describe` shows a profile's URLs.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list")]
#[command(after_help = after_help!(
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
#[clap(visible_alias = "ls")]
pub struct List {}

pub async fn run_list(State(env): State<CliEnv>, _opts: &List) -> Result<()> {
    let mut names = vec![LOCAL_PROFILE.as_str().to_string()];
    names.extend(list_profiles(&env.load_config_document()?));

    let mut rows = Vec::with_capacity(names.len());
    for name in names {
        let profile = Profile::new(&name);
        let config = CliEnv::profile_config(&env.config_file, &profile)?;
        let current =
            profile == env.profile && !matches!(env.profile_source, ProfileSource::Variables);
        let (user, account, environment) = cloud_columns(&config);
        rows.push([
            Field::with_display(current, if current { "*" } else { "" }),
            Field::new(name),
            Field::new(match config.environment_type {
                EnvironmentType::Default => "self_hosted",
                #[cfg(feature = "cloud")]
                EnvironmentType::Cloud => "cloud",
            }),
            user,
            account,
            environment,
        ]);
    }

    let mut f = Formatter::new();
    f.table(
        "profiles",
        &["current", "name", "type", "user", "account", "environment"],
        rows,
        IfEmpty::Nothing,
    );
    f.next_step(
        "restate profile use <name>",
        "switch to another profile",
        IncludeFormatting::No,
    );
    f.next_step(
        "restate profile describe <name>",
        "see a profile's URLs",
        IncludeFormatting::No,
    );
    f.finish()
}

/// The user, account and environment of a Cloud profile: names for humans (ids when a
/// profile predates them), both in JSON; blank for other profiles.
#[cfg(feature = "cloud")]
pub(super) fn cloud_columns(config: &CliConfig) -> (Field, Field, Field) {
    let named = |id: Option<&str>, name: Option<&str>| match id {
        Some(id) => Field::with_display(
            serde_json::json!({ "id": id, "name": name }),
            name.unwrap_or(id).to_owned(),
        ),
        None => Field::with_display(None::<String>, ""),
    };
    let info = config.cloud.environment_info.as_ref();
    let user_id = info.and_then(|info| info.user_id.as_deref());
    (
        named(
            user_id,
            user_id
                .and_then(|id| config.cloud.users.get(id))
                .and_then(|user| user.email.as_deref()),
        ),
        named(
            info.map(|info| info.account_id.as_str()),
            info.and_then(|info| info.account_name.as_deref()),
        ),
        named(
            info.map(|info| info.environment_id.as_str()),
            info.and_then(|info| info.environment_name.as_deref()),
        ),
    )
}

#[cfg(not(feature = "cloud"))]
pub(super) fn cloud_columns(_config: &CliConfig) -> (Field, Field, Field) {
    let blank = || Field::with_display(None::<String>, "");
    (blank(), blank(), blank())
}
