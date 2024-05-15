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
use std::fmt::Display;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Result};
use dotenvy::dotenv;
use figment::providers::{Format, Serialized, Toml};
use figment::{Figment, Profile};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::app::{GlobalOpts, UiConfig};

pub const CONFIG_FILENAME: &str = "config.toml";

pub const ENVIRONMENT_FILENAME: &str = "environment";
pub const ENVIRONMENT_ENV: &str = "RESTATE_ENVIRONMENT";

pub const RESTATE_HOST_ENV: &str = "RESTATE_HOST";
pub const RESTATE_HOST_SCHEME_ENV: &str = "RESTATE_HOST_SCHEME";
pub const RESTATE_HOST_SCHEME_DEFAULT: &str = "http";

/// Environment variable to override the default config dir path
pub const CLI_CONFIG_HOME_ENV: &str = "RESTATE_CLI_CONFIG_HOME";
// This is CONFIG and not CONFIG_FILE to be consistent with RESTATE_CONFIG (server)
pub const CLI_CONFIG_FILE_ENV: &str = "RESTATE_CLI_CONFIG";

pub const RESTATE_AUTH_TOKEN_ENV: &str = "RESTATE_AUTH_TOKEN";
// TODO: Deprecated, will be removed once this is provided by the admin server
pub const INGRESS_URL_ENV: &str = "RESTATE_INGRESS_URL";
pub const ADMIN_URL_ENV: &str = "RESTATE_ADMIN_URL";
pub const EDITOR_ENV: &str = "RESTATE_EDITOR";

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    pub environment_type: EnvironmentType,

    pub ingress_base_url: Option<Url>,
    pub admin_base_url: Option<Url>,
    pub bearer_token: Option<String>,

    #[cfg(feature = "cloud")]
    pub cloud: crate::commands::cloud::CloudConfig,
}

pub const LOCAL_PROFILE: Profile = Profile::const_new("local");

impl CliConfig {
    pub fn local() -> Self {
        Self {
            environment_type: EnvironmentType::Default,

            ingress_base_url: Some(Url::parse("http://localhost:8080/").unwrap()),
            admin_base_url: Some(Url::parse("http://localhost:9070/").unwrap()),
            bearer_token: None,

            #[cfg(feature = "cloud")]
            cloud: crate::commands::cloud::CloudConfig::default(),
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentType {
    #[default]
    Default,
    #[cfg(feature = "cloud")]
    Cloud,
}

#[derive(Clone)]
pub struct CliEnv {
    pub loaded_env_file: Option<PathBuf>,
    pub config_home: PathBuf,
    pub config_file: PathBuf,
    pub environment_file: PathBuf,
    /// Should we use colors and emojis or not?
    pub colorful: bool,
    /// Auto answer yes to prompts that asks for confirmation
    pub auto_confirm: bool,
    /// Timeout for the connect phase of the request.
    pub connect_timeout: Duration,
    /// Overall request timeout.
    pub request_timeout: Option<Duration>,
    /// UI Configuration
    pub ui_config: UiConfig,

    /// Default text editor for state editing
    pub editor: Option<String>,

    /// Current environment
    pub environment: Profile,
    pub environment_source: EnvironmentSource,
    /// Environment-specific config
    pub config: CliConfig,
}

#[derive(Clone)]
pub enum EnvironmentSource {
    Argument,
    Environment,
    File,
    None,
}

impl Display for EnvironmentSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Argument => write!(f, "argument"),
            Self::Environment => write!(f, "${}", ENVIRONMENT_ENV),
            Self::File => write!(f, "file"),
            Self::None => write!(f, "default"),
        }
    }
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

        let config_home = os_env
            .get(CLI_CONFIG_HOME_ENV)
            .map(|x| Ok(PathBuf::from(x)))
            .unwrap_or_else(default_config_home)?;

        let config_file = os_env
            .get(CLI_CONFIG_FILE_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| config_home.join(CONFIG_FILENAME));

        let environment_file = config_home.join(ENVIRONMENT_FILENAME);

        let (environment, environment_source) = if let Some(environment) = &global_opts.environment
        {
            // 1. command line argument
            (environment.clone(), EnvironmentSource::Argument)
        } else if let Some(environment) = os_env.get(ENVIRONMENT_ENV) {
            // 2. RESTATE_ENVIRONMENT env
            (Profile::new(&environment), EnvironmentSource::Environment)
        } else if environment_file.is_file() {
            // 3. environment file
            (
                Profile::new(std::fs::read_to_string(&environment_file)?.trim()),
                EnvironmentSource::File,
            )
        } else {
            // 4. default to 'local'
            (LOCAL_PROFILE, EnvironmentSource::None)
        };

        let default_editor = os_env
            .get(EDITOR_ENV)
            .or_else(|| os_env.get("VISUAL"))
            .or_else(|| os_env.get("EDITOR"));

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

        let defaults = CliConfig::default();
        let local = CliConfig::local();

        let mut figment = Figment::from(Serialized::defaults(defaults))
            .merge(Serialized::from(local, LOCAL_PROFILE))
            .select(environment.clone());

        // Load configuration file
        if config_file.as_path().is_file() {
            figment = figment.merge(Toml::file_exact(config_file.as_path()).nested());
        };
        figment = Self::merge_with_env(os_env, figment)?;

        Ok(Self {
            loaded_env_file: maybe_env.ok(),
            config_home,
            config_file,
            environment_file,
            connect_timeout: Duration::from_millis(global_opts.connect_timeout),
            request_timeout: global_opts.request_timeout.map(Duration::from_millis),
            colorful,
            auto_confirm: global_opts.yes,
            ui_config: global_opts.ui_config.clone(),
            editor: default_editor,
            environment,
            environment_source,
            config: figment.extract()?,
        })
    }

    fn merge_with_env(os_env: &OsEnv, figment: Figment) -> Result<Figment> {
        let figment = if let Some(restate_host) = os_env.get(RESTATE_HOST_ENV) {
            let restate_host_scheme = os_env
                .get(RESTATE_HOST_SCHEME_ENV)
                .as_deref()
                .unwrap_or(RESTATE_HOST_SCHEME_DEFAULT)
                .to_owned();

            figment
                .join((
                    "ingress_base_url",
                    Url::parse(&format!("{}://{}:8080/", restate_host_scheme, restate_host))?,
                ))
                .join((
                    "admin_base_url",
                    Url::parse(&format!("{}://{}:9070/", restate_host_scheme, restate_host))?,
                ))
        } else {
            figment
        };

        let figment = if let Some(ingress_url) = os_env.get(INGRESS_URL_ENV) {
            figment.join(("ingress_base_url", Url::parse(&ingress_url)?))
        } else {
            figment
        };

        let figment = if let Some(admin_url) = os_env.get(ADMIN_URL_ENV) {
            figment.join(("admin_base_url", Url::parse(&admin_url)?))
        } else {
            figment
        };

        let figment = if let Some(bearer_token) = os_env.get(RESTATE_AUTH_TOKEN_ENV) {
            figment.join(("bearer_token", bearer_token))
        } else {
            figment
        };

        Ok(figment)
    }

    pub fn open_default_editor(&self, path: &Path) -> anyhow::Result<()> {
        // if nothing else is defined, we use vim.
        let editor = self.editor.as_deref().unwrap_or("vi").to_owned();

        let mut child = std::process::Command::new(editor.clone())
            .arg(path)
            .spawn()?;

        let status = child.wait()?;

        if status.success() {
            Ok(())
        } else {
            Err(anyhow!(
                "editor {editor} exited with a non-successful exit code {:?}, \
                if you would like to use a specific editor, please either use: \
                (1) $VISUAL env variable \
                (2) EDITOR env variable or \
                (3) set the {EDITOR_ENV} env variable to a default editor.  ",
                status.code()
            ))
        }
    }

    pub fn ingress_base_url(&self) -> Result<&Url> {
        match self.config.ingress_base_url.as_ref() {
            Some(ingress_base_url) => Ok(ingress_base_url),
            None => Err(anyhow!("No Restate ingress endpoint has been configured for environment '{}'; provide it using ${}, or add to the config file with `restate config edit`", self.environment.as_str(), RESTATE_HOST_ENV)),
        }
    }

    pub fn admin_base_url(&self) -> Result<&Url> {
        match self.config.admin_base_url.as_ref() {
            Some(admin_base_url) => Ok(admin_base_url),
            None => Err(anyhow!("No Restate admin endpoint has been configured for environment '{}'; provide it using ${}, or add to the config file with `restate config edit`", self.environment.as_str(), RESTATE_HOST_ENV)),
        }
    }

    pub fn bearer_token(&self) -> Result<Option<&str>> {
        match self.config.environment_type {
            EnvironmentType::Default => Ok(self.config.bearer_token.as_deref()),
            #[cfg(feature = "cloud")]
            EnvironmentType::Cloud => {
                // first check for manual overrides for this environment / env vars
                if let Some(bearer_token) = &self.config.bearer_token {
                    return Ok(Some(bearer_token));
                }
                if let Some(cloud_credentials) = &self.config.cloud.credentials {
                    return Ok(Some(cloud_credentials.access_token()?));
                }
                Err(anyhow::anyhow!(
                    "Restate Cloud credentials have not been provided; first run `restate cloud login`"
                ))
            }
        }
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
        let new_config_file = PathBuf::from("/to/infinity/and/beyond.toml");
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
        // avoid using any files from the test runner
        os_env.insert(CLI_CONFIG_HOME_ENV, "/dev/null".into());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        assert_eq!(
            cli_env
                .config
                .ingress_base_url
                .expect("ingress_base_url must be provided")
                .to_string(),
            "http://localhost:8080/".to_string()
        );
        assert_eq!(
            cli_env
                .config
                .admin_base_url
                .expect("admin_base_url must be provided")
                .to_string(),
            "http://localhost:9070/".to_string()
        );

        // Defaults are templated over RESTATE_HOST
        os_env.clear();

        os_env.insert(RESTATE_HOST_ENV, "example.com".to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;

        assert_eq!(
            cli_env
                .config
                .ingress_base_url
                .expect("ingress_base_url must be provided")
                .to_string(),
            "http://example.com:8080/".to_string()
        );
        assert_eq!(
            cli_env
                .config
                .admin_base_url
                .expect("admin_base_url must be provided")
                .to_string(),
            "http://example.com:9070/".to_string()
        );

        // RESTATE_INGRESS_URL/RESTATE_META_URL override the base URLs!
        os_env.clear();
        os_env.insert(INGRESS_URL_ENV, "https://api.restate.dev:4567".to_string());
        os_env.insert(ADMIN_URL_ENV, "https://admin.restate.dev:4567".to_string());
        os_env.insert(RESTATE_HOST_SCHEME_ENV, "https".to_string());

        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        // Note that Uri adds a trailing slash to the path as expected
        assert_eq!(
            cli_env
                .config
                .ingress_base_url
                .expect("ingress_base_url must be provided")
                .to_string(),
            "https://api.restate.dev:4567/".to_string()
        );
        assert_eq!(
            cli_env
                .config
                .admin_base_url
                .expect("admin_base_url must be provided")
                .to_string(),
            "https://admin.restate.dev:4567/".to_string()
        );

        Ok(())
    }

    #[test]
    fn test_default_timeout_applied() {
        let opts = &GlobalOpts {
            connect_timeout: 1000,
            request_timeout: Some(5000),
            ..GlobalOpts::default()
        };
        let cli_env = CliEnv::load_from_env(&OsEnv::default(), opts).unwrap();
        assert_eq!(cli_env.connect_timeout, Duration::from_millis(1000));
        assert_eq!(cli_env.request_timeout, Some(Duration::from_millis(5000)));
    }

    #[test]
    fn test_bearer_token_applied() {
        let mut os_env = OsEnv::default();
        // avoid using any files from the test runner
        os_env.insert(CLI_CONFIG_HOME_ENV, "/dev/null".into());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.config.bearer_token, None);

        os_env.clear();
        os_env.insert(RESTATE_AUTH_TOKEN_ENV, "token".to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.config.bearer_token, Some("token".to_string()));
    }
}
