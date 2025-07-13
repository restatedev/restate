use anyhow::{Context, Result};
use clap::CommandFactory;
use clap_complete::{Shell, generate};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::{c_println, c_success};

/// Macro to create standard completion commands for CLI applications
#[macro_export]
macro_rules! completion_commands {
    ($app_type:ty) => {
        /// Generate shell completions
        #[derive(cling::prelude::Run, cling::prelude::Subcommand, Clone, Debug)]
        pub enum Completions {
            /// Generate completions to stdout
            Generate(Generate),
            /// Install completions automatically to shell configuration
            Install(Install),
        }

        #[derive(
            cling::prelude::Run, cling::prelude::Parser, cling::prelude::Collect, Clone, Debug,
        )]
        #[cling(run = "run_generate")]
        pub struct Generate {
            /// Shell to generate completions for (auto-detect if not specified)
            #[clap(value_enum)]
            shell: Option<clap_complete::Shell>,
        }

        #[derive(
            cling::prelude::Run, cling::prelude::Parser, cling::prelude::Collect, Clone, Debug,
        )]
        #[cling(run = "run_install")]
        pub struct Install {
            /// Shell to install completions for (auto-detect if not specified)
            #[clap(value_enum)]
            shell: Option<clap_complete::Shell>,
        }

        pub async fn run_generate(opts: &Generate) -> anyhow::Result<()> {
            restate_cli_util::completions::generate_completions::<$app_type>(opts.shell)
        }

        pub async fn run_install(opts: &Install) -> anyhow::Result<()> {
            restate_cli_util::completions::install_completions::<$app_type>(opts.shell)
        }
    };
}

/// Trait that CLI applications must implement to support shell completions
pub trait CompletionProvider: CommandFactory {
    /// Get the binary name for completion generation
    fn completion_binary_name() -> String {
        Self::command()
            // Get the binary name from the command factory (if specified)
            .get_bin_name()
            .map(|name| name.to_string())
            // Fallback to custom binary name (if defined)
            .or_else(Self::default_binary_name)
            // Fallback to command name if bin name is the same as package name
            .unwrap_or_else(|| Self::command().get_name().to_string())
            .into()
    }

    fn default_binary_name() -> Option<String> {
        None
    }
}

/// Configuration for shell completion installation
pub struct CompletionConfig<'a> {
    pub shell: Shell,
    pub name: &'a str,
    pub completion_dir: PathBuf,
    pub file_name: String,
    pub post_install_message: &'a str,
}

/// Shell-specific configuration constants
pub struct ShellConfig {
    pub completion_path: &'static str,
    pub file_extension: &'static str,
    pub post_install_message: &'static str,
}

impl ShellConfig {
    pub const BASH: Self = Self {
        completion_path: ".local/share/bash-completion/completions",
        file_extension: "",
        post_install_message: "You may need to restart your shell or source your ~/.bashrc",
    };

    pub const ZSH: Self = Self {
        completion_path: ".local/share/zsh/site-functions",
        file_extension: "",
        post_install_message: "You may need to restart your shell or run: autoload -U compinit && compinit",
    };

    pub const FISH: Self = Self {
        completion_path: ".config/fish/completions",
        file_extension: ".fish",
        post_install_message: "Completions should be available immediately in new fish sessions",
    };

    /// Get all possible completion paths for ZSH (primary + fallback)
    pub fn get_zsh_paths() -> &'static [&'static str] {
        &[Self::ZSH.completion_path, ".zsh/completions"]
    }
}

/// PowerShell-specific messages
pub struct PowerShellMessages;
impl PowerShellMessages {
    pub const INSTALL_HINT: &'static str = "PowerShell completions generated. To install, add the following to your PowerShell profile:";
    pub const PROFILE_HINT: &'static str =
        "You can find your profile location by running: echo $PROFILE";
}

/// Error messages for unsupported shells
pub struct UnsupportedShellMessages;
impl UnsupportedShellMessages {
    pub const ELVISH: &'static str = "Automatic installation for Elvish is not supported yet. Please use 'generate' command and manually install the completions.";
    pub const GENERIC: &'static str = "Automatic installation for this shell is not supported yet. Please use 'generate' command and manually install the completions.";
}

/// Get shell from options or detect it, defaulting to `Shell::Bash`
pub fn get_or_detect_shell(shell: Option<Shell>) -> Shell {
    shell.unwrap_or_else(|| Shell::from_env().unwrap_or(Shell::Bash))
}

/// Generate completions to stdout
pub fn generate_completions<T: CompletionProvider>(shell: Option<Shell>) -> Result<()> {
    let detected_shell = get_or_detect_shell(shell);
    let binary_name = T::completion_binary_name();
    let mut cmd = T::command();

    generate(detected_shell, &mut cmd, &binary_name, &mut io::stdout());
    Ok(())
}

/// Install completions for the given shell
pub fn install_completions<T: CompletionProvider>(shell: Option<Shell>) -> Result<()> {
    let detected_shell = get_or_detect_shell(shell);
    install_completions_for_shell::<T>(detected_shell)
}

/// Generate and write completion file
fn write_completion_file<T: CompletionProvider>(config: &CompletionConfig) -> Result<()> {
    // Create completion directory if it doesn't exist
    fs::create_dir_all(&config.completion_dir)
        .with_context(|| format!("Failed to create {} completion directory", config.shell))?;

    let completion_file = config.completion_dir.join(&config.file_name);

    // Generate completions to buffer
    let mut output = Vec::new();
    let mut cmd = T::command();
    generate(config.shell, &mut cmd, config.name.to_string(), &mut output);

    // Write to completion file
    fs::write(&completion_file, &output)
        .with_context(|| format!("Failed to write {} completion file", config.shell))?;

    c_success!(
        "{} completions installed to: {}",
        config.shell,
        completion_file.display()
    );
    c_println!("{}", config.post_install_message);

    Ok(())
}

fn get_zsh_completion_dir(home: &Path) -> PathBuf {
    // Try common zsh completion directories
    let completion_dirs: Vec<PathBuf> = ShellConfig::get_zsh_paths()
        .iter()
        .map(|path| home.join(path))
        .collect();

    completion_dirs
        .iter()
        .find(|dir| dir.exists())
        .cloned()
        .unwrap_or_else(|| completion_dirs[0].clone())
}

/// Install completions for standard shells (Bash, Zsh, Fish)
fn install_standard_shell_completions<T: CompletionProvider>(
    shell: Shell,
    name: &str,
) -> Result<()> {
    let home = dirs::home_dir().context("Could not detect the home directory")?;

    let (completion_dir, file_name) = match shell {
        Shell::Bash => (
            home.join(ShellConfig::BASH.completion_path),
            name.to_string(),
        ),
        Shell::Zsh => (get_zsh_completion_dir(&home), format!("_{name}")),
        Shell::Fish => (
            home.join(ShellConfig::FISH.completion_path),
            format!("{name}{}", ShellConfig::FISH.file_extension),
        ),
        _ => unreachable!("This function only handles Bash, Zsh, and Fish"),
    };

    let shell_config = match shell {
        Shell::Bash => &ShellConfig::BASH,
        Shell::Zsh => &ShellConfig::ZSH,
        Shell::Fish => &ShellConfig::FISH,
        _ => unreachable!("This function only handles Bash, Zsh, and Fish"),
    };

    let config = CompletionConfig {
        shell,
        name,
        completion_dir,
        file_name,
        post_install_message: shell_config.post_install_message,
    };

    write_completion_file::<T>(&config)
}

fn install_completions_for_shell<T: CompletionProvider>(shell: Shell) -> Result<()> {
    let name = T::completion_binary_name();

    match shell {
        Shell::Bash | Shell::Zsh | Shell::Fish => {
            install_standard_shell_completions::<T>(shell, &name)
        }
        Shell::PowerShell => {
            // PowerShell is special - just print to stdout
            let mut output = Vec::new();
            let mut cmd = T::command();
            generate(Shell::PowerShell, &mut cmd, name, &mut output);

            let completion_script = String::from_utf8(output)
                .context("Failed to convert PowerShell completions to string")?;

            c_println!("{}", PowerShellMessages::INSTALL_HINT);
            c_println!("{}", PowerShellMessages::PROFILE_HINT);
            c_println!("\n{completion_script}");
            Ok(())
        }
        Shell::Elvish => {
            anyhow::bail!(UnsupportedShellMessages::ELVISH);
        }
        _ => {
            anyhow::bail!(UnsupportedShellMessages::GENERIC);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::env;
    use tempfile::tempdir;

    #[derive(Parser)]
    #[command(name = "test-cli")]
    struct TestCli;

    impl CompletionProvider for TestCli {
        fn completion_binary_name() -> String {
            Self::command()
                .get_bin_name()
                .unwrap_or("test-cli")
                .to_string()
        }
    }

    #[test]
    fn test_get_or_detect_shell() {
        // Test with explicit shell
        assert_eq!(get_or_detect_shell(Some(Shell::Zsh)), Shell::Zsh);

        // Test with None - should use env detection or default to Bash
        let detected = get_or_detect_shell(None);
        assert!(matches!(
            detected,
            Shell::Bash | Shell::Zsh | Shell::Fish | Shell::PowerShell
        ));
    }

    #[test]
    fn test_completion_provider() {
        let name = TestCli::completion_binary_name();
        assert_eq!(name, "test-cli");
    }

    #[test]
    fn test_shell_config_constants() {
        assert_eq!(
            ShellConfig::BASH.completion_path,
            ".local/share/bash-completion/completions"
        );
        assert_eq!(
            ShellConfig::ZSH.completion_path,
            ".local/share/zsh/site-functions"
        );
        assert_eq!(
            ShellConfig::FISH.completion_path,
            ".config/fish/completions"
        );
        assert_eq!(ShellConfig::FISH.file_extension, ".fish");
    }

    #[test]
    fn test_generate_completions() {
        let result = generate_completions::<TestCli>(Some(Shell::Bash));
        assert!(result.is_ok());
    }

    #[test]
    fn test_install_bash_completions() {
        let temp_dir = tempdir().unwrap();
        let home_dir = temp_dir.path().to_str().unwrap();

        // Set temporary HOME
        unsafe { env::set_var("HOME", home_dir) };

        let result = install_completions_for_shell::<TestCli>(Shell::Bash);
        assert!(result.is_ok());

        // Check that the completion file was created
        let completion_file = PathBuf::from(home_dir)
            .join(".local/share/bash-completion/completions")
            .join("test-cli");

        assert!(completion_file.exists());
    }

    #[test]
    fn test_install_unsupported_shell() {
        let result = install_completions_for_shell::<TestCli>(Shell::Elvish);
        assert!(result.is_err());
    }
}
