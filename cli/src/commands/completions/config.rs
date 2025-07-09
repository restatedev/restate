use clap_complete::Shell;
use std::path::PathBuf;

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
