// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Resolves restate's CLI default data/config directory paths

use std::fmt::Display;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use figment::providers::{Format, Serialized, Toml};
use figment::{Figment, Profile};
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, table, value};

use restate_cli_util::OsEnv;
use restate_types::net::address::{AdminPort, AdvertisedAddress, HttpIngressPort};

use crate::app::GlobalOpts;

pub const CONFIG_FILENAME: &str = "config.toml";

/// Where the current environment was saved before it moved to `[global] environment` of the
/// config file. Still read when that key is absent, deleted on the next write.
pub const LEGACY_ENVIRONMENT_FILENAME: &str = "environment";
/// The key of the config file's `[global]` section naming the current environment.
pub const ENVIRONMENT_KEY: &str = "environment";
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

    pub ingress_base_url: Option<AdvertisedAddress<HttpIngressPort>>,
    pub admin_base_url: Option<AdvertisedAddress<AdminPort>>,
    pub bearer_token: Option<String>,

    #[cfg(feature = "cloud")]
    pub cloud: crate::commands::cloud::CloudConfig,
}

pub const LOCAL_PROFILE: Profile = Profile::const_new("local");

impl CliConfig {
    pub fn local() -> Self {
        Self {
            environment_type: EnvironmentType::Default,

            ingress_base_url: Some(AdvertisedAddress::default()),
            admin_base_url: Some(AdvertisedAddress::default()),
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
    pub config_home: PathBuf,
    pub config_file: PathBuf,
    legacy_environment_file: PathBuf,

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
    /// `-e/--environment`
    Argument,
    /// `$RESTATE_ADMIN_URL`, `$RESTATE_INGRESS_URL`, `$RESTATE_AUTH_TOKEN` or `$RESTATE_HOST`:
    /// an environment of their own, the config file isn't consulted.
    Variables,
    /// `$RESTATE_ENVIRONMENT`
    Environment,
    /// `[global] environment` of the config file (`restate env use`)
    File,
    /// Nothing selected: `local`.
    None,
}

/// The name of the environment made of the `RESTATE_*` variables.
pub const VARIABLES_PROFILE: Profile = Profile::const_new("variables");

impl Display for EnvironmentSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Argument => write!(f, "argument"),
            Self::Variables => write!(f, "environment variables"),
            Self::Environment => write!(f, "${ENVIRONMENT_ENV}"),
            Self::File => write!(f, "config file"),
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
        let config_home = os_env
            .get(CLI_CONFIG_HOME_ENV)
            .map(|x| Ok(PathBuf::from(x)))
            .unwrap_or_else(default_config_home)?;

        let config_file = os_env
            .get(CLI_CONFIG_FILE_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| config_home.join(CONFIG_FILENAME));

        let legacy_environment_file = config_home.join(LEGACY_ENVIRONMENT_FILENAME);

        let default_editor = os_env
            .get(EDITOR_ENV)
            .or_else(|| os_env.get("VISUAL"))
            .or_else(|| os_env.get("EDITOR"));

        // The chain: the first level present wins outright, nothing layers.
        let (environment, environment_source, config) = if let Some(environment) =
            &global_opts.environment
        {
            // 1. command line argument
            let config = Self::profile_config(&config_file, environment)?;
            (environment.clone(), EnvironmentSource::Argument, config)
        } else if let Some(config) = adhoc_config(os_env)? {
            // 2. the RESTATE_* variables, an environment of their own
            (VARIABLES_PROFILE, EnvironmentSource::Variables, config)
        } else if let Some(environment) = os_env.get(ENVIRONMENT_ENV) {
            // 3. RESTATE_ENVIRONMENT env
            let environment = Profile::new(&environment);
            let config = Self::profile_config(&config_file, &environment)?;
            (environment, EnvironmentSource::Environment, config)
        } else if let Some(environment) = saved_environment(&config_file, &legacy_environment_file)?
        {
            // 4. the config file (`restate env use`)
            let environment = Profile::new(&environment);
            let config = Self::profile_config(&config_file, &environment)?;
            (environment, EnvironmentSource::File, config)
        } else {
            // 5. default to 'local'
            let config = Self::profile_config(&config_file, &LOCAL_PROFILE)?;
            (LOCAL_PROFILE, EnvironmentSource::None, config)
        };

        Ok(Self {
            config_home,
            config_file,
            legacy_environment_file,
            editor: default_editor,
            environment,
            environment_source,
            config,
        })
    }

    /// The settings of the environment `profile` of the config file (`local` is built in).
    fn profile_config(config_file: &Path, profile: &Profile) -> Result<CliConfig> {
        let mut figment = Figment::from(Serialized::defaults(CliConfig::default()))
            .merge(Serialized::from(CliConfig::local(), LOCAL_PROFILE))
            .select(profile.clone());
        if config_file.is_file() {
            figment = figment.merge(Toml::file_exact(config_file).nested());
        }
        Ok(figment.extract()?)
    }

    /// Opens `path` in the user's editor. In non-interactive mode it fails instead,
    /// suggesting `alternative` (e.g. the matching `patch` command).
    pub fn open_default_editor(&self, path: &Path, alternative: &str) -> anyhow::Result<()> {
        if !restate_cli_util::CliContext::get().is_interactive() {
            return Err(crate::error::RestateCliError::bad_input(format!(
                "cannot open an editor in non-interactive mode; {alternative}"
            ))
            .into());
        }

        // if nothing else is defined, we use vim.
        let editor = self.editor.as_deref().unwrap_or("vi").to_owned();

        let mut child = std::process::Command::new(editor.clone())
            .arg(path)
            .spawn()
            .with_context(|| {
                format!(
                    "could not start the editor '{editor}'; set the {EDITOR_ENV}, VISUAL or \
                     EDITOR env variable to the editor to use, e.g. EDITOR=nano"
                )
            })?;

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

    pub fn ingress_base_url(&self) -> Result<&AdvertisedAddress<HttpIngressPort>> {
        match self.config.ingress_base_url.as_ref() {
            Some(ingress_base_url) => Ok(ingress_base_url),
            None => Err(anyhow!(
                "No Restate ingress endpoint has been configured for environment '{}'; set ${INGRESS_URL_ENV}, or add it with `restate env add`",
                self.environment.as_str()
            )),
        }
    }

    pub fn admin_base_url(&self) -> Result<&AdvertisedAddress<AdminPort>> {
        match self.config.admin_base_url.as_ref() {
            Some(admin_base_url) => Ok(admin_base_url),
            None => Err(anyhow!(
                "No Restate admin endpoint has been configured for environment '{}'; set ${ADMIN_URL_ENV}, or add it with `restate env add`",
                self.environment.as_str()
            )),
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

    /// The config file as a TOML document, empty when there is none yet.
    pub fn load_config_document(&self) -> Result<DocumentMut> {
        let config_data = if self.config_file.is_file() {
            std::fs::read_to_string(self.config_file.as_path())
                .with_context(|| format!("Failed to read {}", self.config_file.display()))?
        } else {
            String::new()
        };
        config_data
            .parse::<DocumentMut>()
            .with_context(|| format!("Failed to parse {} as TOML", self.config_file.display()))
    }

    /// The environment saved as the current one (`restate env use`), if any.
    pub fn saved_environment(&self) -> Result<Option<String>> {
        saved_environment(&self.config_file, &self.legacy_environment_file)
    }

    /// Save `environment` as the current one, in `[global] environment` of the config file.
    pub fn write_environment(&self, environment: &str) -> Result<()> {
        let mut doc = self.load_config_document()?;
        set_saved_environment(&mut doc, Some(environment));
        self.write_config(&doc.to_string())
    }

    /// Write the config file, and drop the legacy `environment` file: the config file is the
    /// only place the current environment is saved from now on.
    pub fn write_config(&self, config: &str) -> Result<()> {
        if let Some(parent) = self.config_file.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create {}", parent.display()))?;
        }
        std::fs::write(self.config_file.as_path(), config)
            .with_context(|| format!("Failed to write {}", self.config_file.display()))?;
        match std::fs::remove_file(&self.legacy_environment_file) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).with_context(|| {
                format!(
                    "Failed to remove {}",
                    self.legacy_environment_file.display()
                )
            }),
        }
    }
}

/// Set (or with `None`, clear) the current environment in a config document.
pub fn set_saved_environment(doc: &mut DocumentMut, environment: Option<&str>) {
    match environment {
        Some(environment) => {
            doc["global"].or_insert(table())[ENVIRONMENT_KEY] = value(environment);
        }
        None => {
            if let Some(global) = doc.get_mut("global").and_then(|item| item.as_table_mut()) {
                global.remove(ENVIRONMENT_KEY);
            }
        }
    }
}

/// The environment the `RESTATE_*` variables describe, when any of them is set: URLs from
/// `RESTATE_ADMIN_URL` / `RESTATE_INGRESS_URL`, else from `RESTATE_HOST` (and
/// `RESTATE_HOST_SCHEME`), else those of `local`; the token from `RESTATE_AUTH_TOKEN`.
fn adhoc_config(os_env: &OsEnv) -> Result<Option<CliConfig>> {
    let host = os_env.get(RESTATE_HOST_ENV);
    let admin_url = os_env.get(ADMIN_URL_ENV);
    let ingress_url = os_env.get(INGRESS_URL_ENV);
    let bearer_token = os_env.get(RESTATE_AUTH_TOKEN_ENV);
    if host.is_none() && admin_url.is_none() && ingress_url.is_none() && bearer_token.is_none() {
        return Ok(None);
    }
    let scheme = os_env
        .get(RESTATE_HOST_SCHEME_ENV)
        .unwrap_or_else(|| RESTATE_HOST_SCHEME_DEFAULT.to_owned());
    fn parse<P: restate_types::net::address::ListenerPort>(
        url: &str,
        variable: &str,
    ) -> Result<AdvertisedAddress<P>> {
        url.parse()
            .map_err(|err| anyhow!("${variable} isn't a valid URL ({url}): {err}"))
    }
    let local = CliConfig::local();
    let admin_base_url = match (&admin_url, &host) {
        (Some(url), _) => Some(parse(url, ADMIN_URL_ENV)?),
        (None, Some(host)) => Some(parse(
            &format!("{scheme}://{host}:9070/"),
            RESTATE_HOST_ENV,
        )?),
        (None, None) => local.admin_base_url,
    };
    let ingress_base_url = match (&ingress_url, &host) {
        (Some(url), _) => Some(parse(url, INGRESS_URL_ENV)?),
        (None, Some(host)) => Some(parse(
            &format!("{scheme}://{host}:8080/"),
            RESTATE_HOST_ENV,
        )?),
        (None, None) => local.ingress_base_url,
    };
    Ok(Some(CliConfig {
        environment_type: EnvironmentType::Default,
        admin_base_url,
        ingress_base_url,
        bearer_token,
        #[cfg(feature = "cloud")]
        cloud: crate::commands::cloud::CloudConfig::default(),
    }))
}

/// `[global] environment` of the config file, else the legacy `environment` file.
fn saved_environment(config_file: &Path, legacy_file: &Path) -> Result<Option<String>> {
    if config_file.is_file() {
        let config: toml::Value = toml::from_str(
            &std::fs::read_to_string(config_file)
                .with_context(|| format!("Failed to read {}", config_file.display()))?,
        )
        .with_context(|| format!("Failed to parse {} as TOML", config_file.display()))?;
        let saved = config
            .get("global")
            .and_then(|global| global.get(ENVIRONMENT_KEY))
            .and_then(toml::Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty());
        if let Some(saved) = saved {
            return Ok(Some(saved.to_owned()));
        }
    }
    if legacy_file.is_file() {
        let saved = std::fs::read_to_string(legacy_file)
            .with_context(|| format!("Failed to read {}", legacy_file.display()))?;
        let saved = saved.trim();
        if !saved.is_empty() {
            return Ok(Some(saved.to_owned()));
        }
    }
    Ok(None)
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

#[cfg(test)]
mod tests {
    use restate_cli_util::OsEnv;

    use super::*;

    #[test]
    fn config_home_order() -> Result<()> {
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
    fn the_variables_make_an_environment_of_their_own() -> Result<()> {
        let mut os_env = OsEnv::default();
        // avoid using any files from the test runner
        os_env.insert(CLI_CONFIG_HOME_ENV, "/dev/null".into());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        assert_eq!(cli_env.environment, LOCAL_PROFILE);
        assert_eq!(
            cli_env.admin_base_url()?.to_string(),
            "http://127.0.0.1:9070/"
        );
        assert_eq!(
            cli_env.admin_base_url()?.to_string(),
            "http://127.0.0.1:9070/"
        );

        // RESTATE_HOST templates both URLs
        os_env.insert(RESTATE_HOST_ENV, "example.com".to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        assert_eq!(cli_env.environment, VARIABLES_PROFILE);
        assert!(matches!(
            cli_env.environment_source,
            EnvironmentSource::Variables
        ));
        assert_eq!(
            cli_env.ingress_base_url()?.to_string(),
            "http://example.com:8080/"
        );
        assert_eq!(
            cli_env.admin_base_url()?.to_string(),
            "http://example.com:9070/"
        );

        // RESTATE_INGRESS_URL / RESTATE_ADMIN_URL beat RESTATE_HOST
        os_env.insert(INGRESS_URL_ENV, "https://api.restate.dev:4567".to_string());
        os_env.insert(ADMIN_URL_ENV, "https://admin.restate.dev:4567".to_string());
        os_env.insert(RESTATE_HOST_SCHEME_ENV, "https".to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default())?;
        assert_eq!(
            cli_env.ingress_base_url()?.to_string(),
            "https://api.restate.dev:4567/"
        );
        assert_eq!(
            cli_env.admin_base_url()?.to_string(),
            "https://admin.restate.dev:4567/"
        );

        // ... but -e wins over the variables
        let opts = GlobalOpts {
            environment: Some(LOCAL_PROFILE),
        };
        let cli_env = CliEnv::load_from_env(&os_env, &opts)?;
        assert_eq!(cli_env.environment, LOCAL_PROFILE);
        assert_eq!(
            cli_env.admin_base_url()?.to_string(),
            "http://127.0.0.1:9070/"
        );
        Ok(())
    }

    #[test]
    fn a_token_alone_targets_localhost_and_beats_the_selected_environment() {
        let home = tempfile::tempdir().unwrap();
        let mut os_env = OsEnv::default();
        os_env.insert(CLI_CONFIG_HOME_ENV, home.path().display().to_string());
        std::fs::write(
            home.path().join(CONFIG_FILENAME),
            "[global]\nenvironment = \"staging\"\n\n[staging]\nadmin_base_url = \"https://staging:9070\"\nbearer_token = \"staging-token\"\n",
        )
        .unwrap();
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.environment, Profile::new("staging"));
        assert_eq!(cli_env.bearer_token().unwrap(), Some("staging-token"));

        os_env.insert(RESTATE_AUTH_TOKEN_ENV, "token".to_string());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.environment, VARIABLES_PROFILE);
        assert_eq!(cli_env.bearer_token().unwrap(), Some("token"));
        assert_eq!(
            cli_env.admin_base_url().unwrap().to_string(),
            "http://127.0.0.1:9070/"
        );
    }

    #[test]
    fn saved_environment_comes_from_the_config_file_then_the_legacy_file() {
        let home = tempfile::tempdir().unwrap();
        let mut os_env = OsEnv::default();
        os_env.insert(CLI_CONFIG_HOME_ENV, home.path().display().to_string());

        // Nothing saved: `local`.
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.environment, LOCAL_PROFILE);

        // The legacy file is still honoured.
        std::fs::write(home.path().join(LEGACY_ENVIRONMENT_FILENAME), "old\n").unwrap();
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.environment, Profile::new("old"));

        // Writing moves it into the config file and drops the legacy file.
        cli_env.write_environment("staging").unwrap();
        assert!(!home.path().join(LEGACY_ENVIRONMENT_FILENAME).exists());
        let cli_env = CliEnv::load_from_env(&os_env, &GlobalOpts::default()).unwrap();
        assert_eq!(cli_env.environment, Profile::new("staging"));
        assert_eq!(
            cli_env.saved_environment().unwrap().as_deref(),
            Some("staging")
        );
    }

    #[test]
    fn bearer_token_applied() {
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
