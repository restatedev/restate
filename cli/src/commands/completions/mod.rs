use anyhow::{Context, Result};
use clap::CommandFactory;
use clap_complete::{Shell, generate};
use cling::prelude::*;
use std::fs;
use std::io;
use std::path::PathBuf;

use crate::CliApp;
use crate::cli_env::CliEnv;
use restate_cli_util::{c_println, c_success};

mod config;
use config::{CompletionConfig, PowerShellMessages, ShellConfig, UnsupportedShellMessages};

/// Generate shell completions for the Restate CLI
#[derive(Run, Subcommand, Clone)]
pub enum Completions {
    /// Generate completions to stdout
    Generate(Generate),
    /// Install completions automatically to shell configuration
    Install(Install),
}

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_generate")]
pub struct Generate {
    /// Shell to generate completions for (auto-detect if not specified)
    #[arg(value_enum)]
    shell: Option<Shell>,
}

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_install")]
pub struct Install {
    /// Shell to install completions for (auto-detect if not specified)
    #[arg(value_enum)]
    shell: Option<Shell>,
}

/// Get shell from options or detect it
fn get_shell_or_detect(shell: Option<Shell>) -> Shell {
    shell.unwrap_or_else(|| detect_shell().unwrap_or(Shell::Bash))
}

/// Get the binary name for completion generation
fn get_binary_name() -> String {
    CliApp::command()
        .get_bin_name()
        .unwrap_or("restate")
        .to_string()
}

pub async fn run_generate(State(_env): State<CliEnv>, opts: &Generate) -> Result<()> {
    let detected_shell = get_shell_or_detect(opts.shell);

    let mut cmd = CliApp::command();
    let binary_name = get_binary_name();
    generate(detected_shell, &mut cmd, &binary_name, &mut io::stdout());

    Ok(())
}

pub async fn run_install(State(_env): State<CliEnv>, opts: &Install) -> Result<()> {
    let detected_shell = get_shell_or_detect(opts.shell);
    install_completions(detected_shell)?;
    Ok(())
}

fn detect_shell() -> Result<Shell> {
    // Use clap_complete's built-in shell detection
    let detected = Shell::from_env()
        .or_else(|| {
            // Fallback to parsing SHELL variable with from_shell_path
            std::env::var("SHELL")
                .ok()
                .and_then(|path| Shell::from_shell_path(&path))
        })
        .unwrap_or_else(|| {
            // Platform-specific defaults
            if cfg!(target_os = "windows") {
                Shell::PowerShell
            } else {
                Shell::Bash
            }
        });

    Ok(detected)
}

/// Generate and write completion file
fn write_completion_file(cmd: &mut clap::Command, config: &CompletionConfig) -> Result<()> {
    // Create completion directory if it doesn't exist
    fs::create_dir_all(&config.completion_dir)
        .with_context(|| format!("Failed to create {} completion directory", config.shell))?;

    let completion_file = config.completion_dir.join(&config.file_name);

    // Generate completions to buffer
    let mut output = Vec::new();
    generate(config.shell, cmd, config.name.to_string(), &mut output);

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

fn get_zsh_completion_dir(home: &PathBuf) -> PathBuf {
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
fn install_standard_shell_completions(
    shell: Shell,
    cmd: &mut clap::Command,
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

    write_completion_file(cmd, &config)
}

fn install_completions(shell: Shell) -> Result<()> {
    let mut cmd = CliApp::command();
    let name = get_binary_name();

    match shell {
        Shell::Bash | Shell::Zsh | Shell::Fish => {
            install_standard_shell_completions(shell, &mut cmd, &name)
        }
        Shell::PowerShell => {
            // PowerShell is special - just print to stdout
            let mut output = Vec::new();
            generate(Shell::PowerShell, &mut cmd, name.to_string(), &mut output);

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
    use std::env;
    use std::fs;
    use tempfile::tempdir;

    fn get_expected_binary_name() -> String {
        "restate".to_string()
    }

    #[test]
    fn test_get_binary_name() {
        // Test that get_binary_name returns the expected binary name
        let binary_name = get_binary_name();
        assert_eq!(binary_name, "restate");

        // Also verify it matches our test helper
        assert_eq!(binary_name, get_expected_binary_name());
    }

    #[test]
    fn test_detect_shell_from_env() {
        // Test bash detection
        unsafe { env::set_var("SHELL", "/bin/bash") };
        let shell = detect_shell().unwrap();
        assert_eq!(shell, Shell::Bash);

        // Test zsh detection
        unsafe { env::set_var("SHELL", "/usr/bin/zsh") };
        let shell = detect_shell().unwrap();
        assert_eq!(shell, Shell::Zsh);

        // Test fish detection
        unsafe { env::set_var("SHELL", "/usr/local/bin/fish") };
        let shell = detect_shell().unwrap();
        assert_eq!(shell, Shell::Fish);

        // Test default fallback
        unsafe { env::set_var("SHELL", "/some/unknown/shell") };
        let shell = detect_shell().unwrap();
        assert_eq!(shell, Shell::Bash);
    }

    #[test]
    fn test_detect_shell_no_env() {
        // Remove SHELL env var
        unsafe { env::remove_var("SHELL") };
        let shell = detect_shell().unwrap();
        // Should default to bash (or PowerShell on Windows)
        if cfg!(target_os = "windows") {
            assert_eq!(shell, Shell::PowerShell);
        } else {
            assert_eq!(shell, Shell::Bash);
        }
    }

    #[test]
    fn test_bash_completion_paths() {
        let temp_dir = tempdir().unwrap();
        let home_dir = temp_dir.path().to_str().unwrap();

        // Set temporary HOME
        unsafe { env::set_var("HOME", home_dir) };

        let completion_dir =
            PathBuf::from(home_dir).join(".local/share/bash-completion/completions");

        // The directory should be created by install_bash_completions
        assert!(!completion_dir.exists());

        // Test that the expected path is constructed correctly
        let binary_name = get_expected_binary_name();
        let expected_file = completion_dir.join(&binary_name);
        assert_eq!(expected_file.extension(), None);
        assert_eq!(expected_file.file_name().unwrap(), binary_name.as_str());
    }

    #[test]
    fn test_zsh_completion_paths() {
        let temp_dir = tempdir().unwrap();
        let home_dir = temp_dir.path().to_str().unwrap();

        // Set temporary HOME
        unsafe { env::set_var("HOME", home_dir) };

        let completion_dir = PathBuf::from(home_dir).join(".local/share/zsh/site-functions");
        let binary_name = get_expected_binary_name();
        let expected_file = completion_dir.join(format!("_{binary_name}"));

        assert_eq!(
            expected_file.file_name().unwrap(),
            format!("_{binary_name}").as_str()
        );
        assert!(expected_file.to_str().unwrap().contains("site-functions"));
    }

    #[test]
    fn test_fish_completion_paths() {
        let temp_dir = tempdir().unwrap();
        let home_dir = temp_dir.path().to_str().unwrap();

        // Set temporary HOME
        unsafe { env::set_var("HOME", home_dir) };

        let completion_dir = PathBuf::from(home_dir).join(".config/fish/completions");
        let binary_name = get_expected_binary_name();
        let expected_file = completion_dir.join(format!("{binary_name}.fish"));

        assert_eq!(expected_file.extension().unwrap(), "fish");
        assert_eq!(expected_file.file_stem().unwrap(), binary_name.as_str());
    }

    #[test]
    fn test_install_completions_unsupported_shell() {
        // Test that Elvish returns an error
        let result = install_completions(Shell::Elvish);
        assert!(result.is_err());
        // Just check that it's an error, not the specific message
    }

    #[test]
    fn test_install_bash_completions() {
        let temp_dir = tempdir().unwrap();
        let home_dir = temp_dir.path().to_str().unwrap();

        // Set temporary HOME
        unsafe { env::set_var("HOME", home_dir) };

        let result = install_completions(Shell::Bash);

        assert!(result.is_ok());

        // Check that the completion file was created
        let completion_file = PathBuf::from(home_dir)
            .join(".local/share/bash-completion/completions")
            .join("restate");

        assert!(completion_file.exists());

        // Check that file has content
        let content = fs::read_to_string(&completion_file).unwrap();
        assert!(!content.is_empty());
    }

    #[test]
    fn test_install_zsh_completions() {
        let temp_dir = tempdir().unwrap();
        let home_dir = temp_dir.path().to_str().unwrap();

        // Set temporary HOME
        unsafe { env::set_var("HOME", home_dir) };

        let result = install_completions(Shell::Zsh);

        assert!(result.is_ok());

        // Check that the completion file was created
        let completion_file = PathBuf::from(home_dir)
            .join(".local/share/zsh/site-functions")
            .join("_restate");

        assert!(completion_file.exists());

        // Check that file has content
        let content = fs::read_to_string(&completion_file).unwrap();
        assert!(!content.is_empty());
    }

    #[test]
    fn test_install_fish_completions() {
        let temp_dir = tempdir().unwrap();
        let home_dir = temp_dir.path().to_str().unwrap();

        // Set temporary HOME
        unsafe { env::set_var("HOME", home_dir) };

        let result = install_completions(Shell::Fish);

        assert!(result.is_ok());

        // Check that the completion file was created
        let completion_file = PathBuf::from(home_dir)
            .join(".config/fish/completions")
            .join("restate.fish");

        assert!(completion_file.exists());

        // Check that file has content
        let content = fs::read_to_string(&completion_file).unwrap();
        assert!(!content.is_empty());
    }

    #[test]
    fn test_install_powershell_completions() {
        let result = install_completions(Shell::PowerShell);

        // PowerShell installation should always succeed (it just prints to stdout)
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_uses_correct_binary_name() {
        // Test that generate function uses the correct binary name
        let mut cmd = CliApp::command();
        let mut output = Vec::new();
        let binary_name = get_binary_name();

        // Generate bash completions
        generate(Shell::Bash, &mut cmd, &binary_name, &mut output);

        let content = String::from_utf8(output).unwrap();

        // Check that the generated content contains the binary name
        assert!(content.contains("restate"));
        assert!(!content.contains("restate-cli"));
    }

    #[test]
    fn test_completion_files_use_binary_name() {
        // This test verifies that all shell completion files use the binary name
        // not the package name
        let binary_name = get_binary_name();

        // Test bash
        let bash_file = binary_name.to_string();
        assert_eq!(bash_file, "restate");

        // Test zsh
        let zsh_file = format!("_{binary_name}");
        assert_eq!(zsh_file, "_restate");

        // Test fish
        let fish_file = format!("{binary_name}.fish");
        assert_eq!(fish_file, "restate.fish");
    }
}
