// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Resolves restate's CLI default data/config directory paths

#[cfg(test)]
use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use dotenvy::dotenv;
use url::Url;

use crate::app::{GlobalOpts, UiConfig};

pub const CONFIG_FILENAME: &str = "config.yaml";

pub const RESTATE_HOST_ENV: &str = "RESTATE_HOST";
pub const RESTATE_HOST_SCHEME_ENV: &str = "RESTATE_HOST_SCHEME";
// The default is localhost unless the CLI configuration states a different default.
pub const RESTATE_HOST_DEFAULT: &str = "localhost";
pub const RESTATE_HOST_SCHEME_DEFAULT: &str = "http";
// Default request timeout.
pub const REQUEST_TIMEOUT_DEFAULT: Duration = Duration::from_secs(10);

/// Environment variable to override the default config dir path
pub const CLI_CONFIG_HOME_ENV: &str = "RESTATE_CLI_CONFIG_HOME";
// This is CONFIG and not CONFIG_FILE to be consistent with RESTATE_CONFIG (server)
pub const CLI_CONFIG_FILE_ENV: &str = "RESTATE_CLI_CONFIG";

pub const RESTATE_AUTH_TOKEN_ENV: &str = "RESTATE_AUTH_TOKEN";
pub const INGRESS_URL_ENV: &str = "RESTATE_INGRESS_URL";
pub const META_URL_ENV: &str = "RESTATE_META_URL";
pub const DATAFUSION_HTTP_URL_ENV: &str = "RESTATE_DATAFUSION_HTTP_URL";

#[derive(Clone, Default)]
pub struct CliConfig {}

#[derive(Clone)]
pub struct CliEnv {
    pub loaded_env_file: Option<PathBuf>,
    pub config_home: PathBuf,
    pub config_file: PathBuf,
    pub ingress_base_url: Url,
    pub meta_base_url: Url,
    pub datafusion_http_base_url: Url,
    pub bearer_token: Option<String>,
    pub request_timeout: Duration,
    /// Should we use colors and emojis or not?
    pub colorful: bool,
    /// Auto answer yes to prompts that asks for confirmation
    pub auto_confirm: bool,
    /// UI Configuration
    pub ui_config: UiConfig,
}

impl CliEnv {
    /// Uses GlobalOpts to override some options and to set others that are
    /// not accessible through the config/env.
    pub fn load(global_opts: &GlobalOpts) -> Result<Self> {
        let os_env = OsEnv::default();
        Self::load_from_env(&os_env, global_opts)
    }

    /// Loading CliEnv with a custom OsEnv. OsEnv can be customised in cfg(test)
    pub fn load_from_env(os_env: &OsEnv, global_opts: &GlobalOpts) -> Result<Self> {
        // Load .env file. Best effort.
        let maybe_env = dotenv();

        let restate_host = os_env
            .get(RESTATE_HOST_ENV)
            .as_deref()
            .unwrap_or(RESTATE_HOST_DEFAULT)
            .to_owned();

        let restate_host_scheme = os_env
            .get(RESTATE_HOST_SCHEME_ENV)
            .as_deref()
            .unwrap_or(RESTATE_HOST_SCHEME_DEFAULT)
            .to_owned();

        let config_home = os_env
            .get(CLI_CONFIG_HOME_ENV)
            .map(|x| Ok(PathBuf::from(x)))
            .unwrap_or_else(default_config_home)?;

        let config_file = os_env
            .get(CLI_CONFIG_FILE_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| config_home.join(CONFIG_FILENAME));

        let bearer_token = os_env.get(RESTATE_AUTH_TOKEN_ENV);

        let ingress_base_url = os_env
            .get(INGRESS_URL_ENV)
            .as_deref()
            .map(Url::parse)
            .unwrap_or_else(|| {
                Url::parse(&format!("{}://{}:8080/", restate_host_scheme, restate_host))
            })?;

        let meta_base_url = os_env
            .get(META_URL_ENV)
            .as_deref()
            .map(Url::parse)
            .unwrap_or_else(|| {
                Url::parse(&format!("{}://{}:9070/", restate_host_scheme, restate_host))
            })?;

        let datafusion_http_base_url = os_env
            .get(DATAFUSION_HTTP_URL_ENV)
            .as_deref()
            .map(Url::parse)
            .unwrap_or_else(|| {
                Url::parse(&format!("{}://{}:9072/", restate_host_scheme, restate_host))
            })?;

        // color setup
        // NO_COLOR=1 with any value other than "0" means user doesn't want colors.
        // e.g.
        //  NO_COLOR=1 (no colors)
        //  NO_COLOR=true (no colors)
        //  NO_COLOR=something (no colors)
        //  NO_COLOR=0 or unset (yes *color* if term supports it)
        let should_color = os_env
            .get("NO_COLOR")
            .map(|x| x == "0")
            .unwrap_or_else(|| true);

        // dumb terminal? no colors or fancy stuff
        let smart_term = os_env
            .get("TERM")
            .map(|x| x != "dumb")
            .unwrap_or_else(|| true);

        // CLICOLOR_FORCE is set? enforce coloring..
        // Se http://bixense.com/clicolors/ for details.
        let force_colorful = os_env
            .get("CLICOLOR_FORCE")
            .map(|x| x != "0")
            .unwrap_or_else(|| false);

        let colorful = if force_colorful {
            // CLICOLOR_FORCE is set, we enforce coloring
            true
        } else {
            // We colorize only if it's a smart terminal (not TERM=dumb, nor pipe)
            // and NO_COLOR is anything but "0"
            let is_terminal = std::io::stdout().is_terminal();
            is_terminal && smart_term && should_color
        };

        // Ensure we follows our colorful setting in our console utilities
        // without passing the environment around.
        crate::console::set_colors_enabled(colorful);

        Ok(Self {
            loaded_env_file: maybe_env.ok(),
            config_home,
            config_file,
            ingress_base_url,
            meta_base_url,
            datafusion_http_base_url,
            bearer_token,
            request_timeout: REQUEST_TIMEOUT_DEFAULT,
            colorful,
            auto_confirm: global_opts.yes,
            ui_config: global_opts.ui_config.clone(),
        })
    }
}

#[cfg(not(windows))]
fn default_config_home() -> Result<PathBuf> {
    use anyhow::Context;

    Ok(dirs::home_dir()
        .context("Could not detect the home directory")?
        .join(".config")
        .join("restate"))
}

#[cfg(windows)]
fn default_config_home() -> Result<PathBuf> {
    use anyhow::Context;
    Ok(dirs::config_local_dir()
        .context("Could not detect the local configuration directory")?
        .join("Restate"))
}

/// Wrapper over the OS environment variables that uses a hashmap in test cfg to
/// enable testing.
#[derive(Default)]
pub struct OsEnv<'a> {
    /// Environment variable mocks
    #[cfg(test)]
    pub env: HashMap<&'a str, String>,

    #[cfg(not(test))]
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> OsEnv<'a> {
    // Retrieves a environment variable from the os or from a table if in testing mode
    #[cfg(test)]
    pub fn get<K: AsRef<str>>(&self, key: K) -> Option<String> {
        self.env.get(key.as_ref()).map(ToString::to_string)
    }

    #[cfg(not(test))]
    #[inline]
    pub fn get<K: AsRef<str>>(&self, key: K) -> Option<String> {
        std::env::var(key.as_ref()).ok()
    }

    #[cfg(test)]
    pub fn insert(&mut self, k: &'a str, v: String) -> Option<String> {
        self.env.insert(k, v)
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.env.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_home_order() -> Result<()> {
        let mut os_env = OsEnv::default();
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;

        let default_home = default_config_home()?;

        // By default, config home is the default
        assert_eq!(cli_env.config_home, default_home);

        assert_eq!(cli_env.config_file, default_home.join(CONFIG_FILENAME));

        // RESTATE_CLI_CONFIG_HOME overrides the default home
        let new_home = PathBuf::from("/random/path");
        // Overriding the config home impacts everything
        os_env.insert(CLI_CONFIG_HOME_ENV, new_home.display().to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        assert_eq!(cli_env.config_home, new_home);
        assert_eq!(cli_env.config_file, new_home.join(CONFIG_FILENAME));

        // RESTATE_CLI_CONFIG_FILE overrides the config file only!
        os_env.clear();
        let new_config_file = PathBuf::from("/to/infinity/and/beyond.yaml");
        os_env.insert(CLI_CONFIG_FILE_ENV, new_config_file.display().to_string());

        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        // Notice that the config home is the default
        assert_eq!(cli_env.config_home, default_home);
        assert_eq!(cli_env.config_file, new_config_file);

        Ok(())
    }

    #[test]
    fn test_base_url_override() -> Result<()> {
        // By default, we use the const value defined in this file.
        let mut os_env = OsEnv::default();
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        assert_eq!(
            cli_env.ingress_base_url.to_string(),
            "http://localhost:8080/".to_string()
        );
        assert_eq!(
            cli_env.meta_base_url.to_string(),
            "http://localhost:9070/".to_string()
        );

        assert_eq!(
            cli_env.datafusion_http_base_url.to_string(),
            "http://localhost:9072/".to_string()
        );

        // Defaults are templated over RESTATE_HOST
        os_env.clear();

        os_env.insert(RESTATE_HOST_ENV, "example.com".to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;

        assert_eq!(
            cli_env.ingress_base_url.to_string(),
            "http://example.com:8080/".to_string()
        );
        assert_eq!(
            cli_env.meta_base_url.to_string(),
            "http://example.com:9070/".to_string()
        );

        assert_eq!(
            cli_env.datafusion_http_base_url.to_string(),
            "http://example.com:9072/".to_string()
        );

        // RESTATE_INGRESS_URL/RESTATE_META_URL override the base URLs!
        os_env.clear();
        os_env.insert(INGRESS_URL_ENV, "https://api.restate.dev:4567".to_string());
        os_env.insert(META_URL_ENV, "https://admin.restate.dev:4567".to_string());
        os_env.insert(RESTATE_HOST_SCHEME_ENV, "https".to_string());

        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        // Note that Uri adds a trailing slash to the path as expected
        assert_eq!(
            cli_env.ingress_base_url.to_string(),
            "https://api.restate.dev:4567/".to_string()
        );
        assert_eq!(
            cli_env.meta_base_url.to_string(),
            "https://admin.restate.dev:4567/".to_string()
        );
        // datafusion still inherits the default from host
        assert_eq!(
            cli_env.datafusion_http_base_url.to_string(),
            // note "https"
            "https://localhost:9072/".to_string()
        );

        Ok(())
    }

    #[test]
    fn test_default_timeout_applied() {
        let os_env = OsEnv::default();
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.request_timeout, REQUEST_TIMEOUT_DEFAULT);
    }

    #[test]
    fn test_bearer_token_applied() {
        let mut os_env = OsEnv::default();
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.bearer_token, None);

        os_env.clear();
        os_env.insert(RESTATE_AUTH_TOKEN_ENV, "token".to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.bearer_token, Some("token".to_string()));
    }
}
