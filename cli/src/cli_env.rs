//! Resolves restate's CLI default data/config directory paths

#[cfg(test)]
use std::collections::HashMap;

// Should be replaced by std::io::IsTerminal once MSRV is 1.70+
use is_terminal::IsTerminal;

use std::path::{Path, PathBuf};

use anyhow::Result;
use dotenvy::dotenv;
use http::Uri;

static CONFIG_FILENAME: &str = "config.toml";

/// Environment variable to override the default config dir path
pub const CLI_CONFIG_HOME_ENV: &str = "RESTATE_CLI_CONFIG_HOME";
pub const CLI_CONFIG_FILE_ENV: &str = "RESTATE_CLI_CONFIG_FILE";

pub const INGRESS_URL_ENV: &str = "RESTATE_INGRESS_URL";
pub const META_URL_ENV: &str = "RESTATE_META_URL";

// Default values
pub const INGRESS_URL_DEFAULT: &str = "http://localhost:8080/";
pub const META_URL_DEFAULT: &str = "http://localhost:9070/";

#[derive(Clone, Default)]
pub struct CliConfig {}

#[derive(Clone, Default)]
pub struct CliEnv {
    loaded_env_file: Option<std::path::PathBuf>,
    config_home: PathBuf,
    config_file: PathBuf,
    ingress_base_url: Uri,
    meta_base_url: Uri,
    is_terminal: bool,
    colorful: bool,
}

impl CliEnv {
    pub fn load() -> Result<Self> {
        let os_env = OsEnv::default();
        Self::load_from_env(&os_env)
    }

    /// Loading CliEnv with a custom OsEnv. OsEnv can be customised in cfg(test)
    pub fn load_from_env(os_env: &OsEnv) -> Result<Self> {
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

        let ingress_base_url = os_env
            .get(INGRESS_URL_ENV)
            .map(Uri::try_from)
            .unwrap_or_else(|| Uri::try_from(INGRESS_URL_DEFAULT))?;

        let meta_base_url = os_env
            .get(META_URL_ENV)
            .map(Uri::try_from)
            .unwrap_or_else(|| Uri::try_from(META_URL_DEFAULT))?;

        // color setup
        // We respect NO_COLOR if set, otherwise, we default to colorful unless
        // stdout is not a terminal
        let colorful = os_env
            .get("NO_COLOR")
            .map(|x| x == "0")
            .unwrap_or_else(|| true);
        let is_terminal = std::io::stdout().is_terminal();
        let colorful = is_terminal && colorful;

        Ok(Self {
            loaded_env_file: maybe_env.ok(),
            config_home,
            config_file,
            ingress_base_url,
            meta_base_url,
            colorful,
            is_terminal,
        })
    }

    pub fn config_home(&self) -> &Path {
        self.config_home.as_path()
    }

    pub fn config_file_path(&self) -> &Path {
        self.config_file.as_path()
    }

    pub fn env_file_path(&self) -> Option<&Path> {
        self.loaded_env_file.as_deref()
    }

    pub fn ingress_base_url(&self) -> &Uri {
        &self.ingress_base_url
    }

    pub fn meta_base_url(&self) -> &Uri {
        &self.meta_base_url
    }

    pub fn is_terminal(&self) -> bool {
        self.is_terminal
    }

    pub fn colorful(&self) -> bool {
        self.colorful
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
        self.env
            .get(key.as_ref())
            .map(std::string::ToString::to_string)
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
        let cli_env = CliEnv::load_from_env(&os_env)?;

        let default_home = default_config_home()?;

        // By default, config home is the default
        assert_eq!(cli_env.config_home(), default_home);

        assert_eq!(
            cli_env.config_file_path(),
            default_home.join(CONFIG_FILENAME)
        );

        // RESTATE_CLI_CONFIG_HOME overrides the default home
        let new_home = PathBuf::from("/random/path");
        // Overriding the config home impacts everything
        os_env.insert(CLI_CONFIG_HOME_ENV, new_home.display().to_string());
        let cli_env = CliEnv::load_from_env(&os_env)?;
        assert_eq!(cli_env.config_home(), new_home);
        assert_eq!(cli_env.config_file_path(), new_home.join(CONFIG_FILENAME));

        // RESTATE_CLI_CONFIG_FILE overrides the config file only!
        os_env.clear();
        let new_config_file = PathBuf::from("/to/infinity/and/beyond.toml");
        os_env.insert(CLI_CONFIG_FILE_ENV, new_config_file.display().to_string());

        let cli_env = CliEnv::load_from_env(&os_env)?;
        // Notice that the config home is the default
        assert_eq!(cli_env.config_home(), default_home);
        assert_eq!(cli_env.config_file_path(), new_config_file);

        Ok(())
    }

    #[test]
    fn test_base_url_override() -> Result<()> {
        // By default, we use the const value defined in this file.
        let mut os_env = OsEnv::default();
        let cli_env = CliEnv::load_from_env(&os_env)?;
        assert_eq!(
            cli_env.ingress_base_url().to_string(),
            INGRESS_URL_DEFAULT.to_string()
        );
        assert_eq!(
            cli_env.meta_base_url().to_string(),
            META_URL_DEFAULT.to_string()
        );

        // RESTATE_INGRESS_URL/RESTATE_META_URL override the base URLs!
        os_env.clear();
        os_env.insert(INGRESS_URL_ENV, "https://api.restate.dev:4567".to_string());
        os_env.insert(META_URL_ENV, "https://admin.restate.dev:4567".to_string());

        let cli_env = CliEnv::load_from_env(&os_env)?;
        // Note that Uri adds a trailing slash to the path as expected
        assert_eq!(
            cli_env.ingress_base_url().to_string(),
            "https://api.restate.dev:4567/".to_string()
        );
        assert_eq!(
            cli_env.meta_base_url().to_string(),
            "https://admin.restate.dev:4567/".to_string()
        );

        Ok(())
    }
}
